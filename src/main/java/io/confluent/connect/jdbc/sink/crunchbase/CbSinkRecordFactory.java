/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink.crunchbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;

public class CbSinkRecordFactory {

  private final Converter jsonConverter;
  private final ObjectMapper jsonObjectMapper;
  private final SchemaRegistryClient schemaRegistryClient;
  private final AvroData avroData;
  private final Cache<String, Schema> schemaCache;

  public CbSinkRecordFactory(String schemaRegistryUrl) {
    final int capacity = 1024;
    jsonConverter = new JsonConverter();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
    jsonObjectMapper = new ObjectMapper();
    schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, capacity);
    avroData = new AvroData(capacity);
    schemaCache = new SynchronizedCache<>(new LRUCache<String, Schema>(capacity));
  }

  public CbSinkRecord build(SinkRecord record) {
    final Map keyJson = getKeyJson(record);
    final String table = (record.topic() + "_" + keyJson.get("table").toString()).toLowerCase();
    final Schema keySchema = getSchema(table + "-key");
    final Schema valueSchema = getSchema(table + "-value");
    final Struct key;
    final Struct value;

    if (record.key() != null) {
      key = new Struct(keySchema);
      keySchema.fields().forEach(f -> {
        Object v = ((Map) record.key()).get(f.name());
        key.put(f.name(), v);
      });
    } else {
      key = null;
    }

    if (record.value() != null) {
      value = new Struct(valueSchema);
      valueSchema.fields().forEach(f -> {
        Object v = ((Map) record.value()).get(f.name());
        value.put(f.name(), v);
      });
    } else {
      value = null;
    }

    SinkRecord newRecord = new SinkRecord(
        record.topic(),
        record.kafkaPartition(),
        keySchema,
        key,
        valueSchema,
        value,
        record.kafkaOffset(),
        record.timestamp(),
        record.timestampType()
    );

    return new CbSinkRecord(newRecord, table);
  }

  private Schema getSchema(String subject) {
    try {
      Schema schema = schemaCache.get(subject);
      if (schema == null) {
        SchemaMetadata avroSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
        org.apache.avro.Schema avroSchema = schemaRegistryClient.getByID(avroSchemaMetadata.getId());
        schema = avroData.toConnectSchema(avroSchema);
        schemaCache.put(subject, schema);
      }
      return schema;
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
  }

  private Map getKeyJson(SinkRecord record) {
    try {
      byte[] keyBytes = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
      String keyString = new String(keyBytes, StandardCharsets.UTF_8);
      return jsonObjectMapper.readValue(keyString, Map.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
