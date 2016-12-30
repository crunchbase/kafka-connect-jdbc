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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Everything in this class is embarrassing and I am ashamed to have written it.
 */
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
    final String table = keyJson.get("table").toString().toLowerCase();
    final String subject = record.topic() + "_" + table;
    final Schema keySchema = getSchema(subject + "-key");
    final Schema valueSchema = getSchema(subject + "-value");
    final Struct recordKey;
    final Struct recordValue;

    if (record.key() != null) {
      recordKey = new Struct(keySchema);
      keySchema.fields().forEach(f -> {
        Object v = ((Map) record.key()).get(f.name());
        recordKey.put(f.name(), v);
      });
    } else {
      recordKey = null;
    }

    if (record.value() != null) {
      recordValue = new Struct(valueSchema);
      valueSchema.fields().forEach(field -> {
        Object value = ((Map) record.value()).get(field.name());
        try {
          if (value != null) {
            if (isStruct(field.schema(), value)) {
              value = getStruct(field.schema(), value);
            } else if (isStructArray(field.schema(), value)) {
              value = getStructArray(field.schema().valueSchema(), value);
            }
            recordValue.put(field.name(), value);
          }
        } catch (RuntimeException e) {
          System.out.println("failed on table " + table + ", field: " + field.name() + ", value: " + value);
          throw e;
        }
      });
    } else {
      recordValue = null;
    }

    SinkRecord newRecord = new SinkRecord(
        record.topic(),
        record.kafkaPartition(),
        keySchema,
        recordKey,
        valueSchema,
        recordValue,
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
      System.out.println("failed to get schema for: " + subject);
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

  private boolean isStruct(Schema schema, Object value) {
    return value != null && schema.type() == Schema.Type.STRUCT;
  }

  private boolean isStructArray(Schema schema, Object value) {
    return value != null &&
        schema.type() == Schema.Type.ARRAY &&
        schema.valueSchema().type() == Schema.Type.STRUCT;
  }

  private Struct getStruct(Schema schema, Object value) {
    Struct s = new Struct(schema);
    ((HashMap) value).forEach((k, v2) -> {
      try {
        if (v2 != null) {
          String field = k.toString();
          Schema fieldSchema = schema.field(field).schema();
          if (isStruct(fieldSchema, v2)) {
            v2 = getStruct(fieldSchema, v2);
          } else if (isStructArray(fieldSchema, v2)) {
            v2 = getStructArray(fieldSchema.valueSchema(), value);
          }
          s.put(field, v2);
        }
      } catch (RuntimeException e) {
        System.out.println("SPECIFICALLY failed on v2: " + v2 + ", class: " + v2.getClass() + ", from value: " + value);
        throw e;
      }
    });
    return s;
  }

  private ArrayList getStructArray(Schema schema, Object value) {
    ArrayList a = (ArrayList) value;
    for (int i = 0; i < a.size(); i++) {
      a.set(i, getStruct(schema, a.get(i)));
    }
    return a;
  }

}
