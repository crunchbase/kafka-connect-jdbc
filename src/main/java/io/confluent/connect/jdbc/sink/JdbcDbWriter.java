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

package io.confluent.connect.jdbc.sink;

import io.confluent.connect.jdbc.sink.crunchbase.CbSinkRecord;
import io.confluent.connect.jdbc.sink.crunchbase.CbSinkRecordFactory;
import io.confluent.connect.jdbc.sink.dialect.DbDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class JdbcDbWriter {

  private final JdbcSinkConfig config;
  private final DbDialect dbDialect;
  private final DbStructure dbStructure;
  private CbSinkRecordFactory cbSinkRecordFactory;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DbDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    if (config.cbRecords) {
      this.cbSinkRecordFactory = new CbSinkRecordFactory(config.schemaUrl);
    }

    this.cachedConnectionProvider = new CachedConnectionProvider(config.connectionUrl, config.connectionUser, config.connectionPassword) {
      @Override
      protected void onConnect(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getValidConnection();

    final Map<String, BufferedRecords> bufferByTable = new HashMap<>();
    for (SinkRecord record : records) {
      final String table;
      if (config.cbRecords) {
        /*
         * SPECIAL CRUNCHBASE-SPECIFIC JSON KEY CONSISTING OF A UUID+INDEX PAIR THAT OVERRIDES THE STANDARD CONNECTOR LOGIC
         */
        final CbSinkRecord cbRecord = cbSinkRecordFactory.build(record);
        table = cbRecord.table;
        record = cbRecord.record;
      } else {
        /*
         * STANDARD CONNECTOR LOGIC
         */
        table = destinationTable(record.topic());
      }
      bufferByTable
          .computeIfAbsent(table, x -> new BufferedRecords(config, table, dbDialect, dbStructure, connection))
          .add(record);
    }
    for (BufferedRecords buffer : bufferByTable.values()) {
      buffer.flush();
    }
    connection.commit();
  }

  void closeQuietly() {
    cachedConnectionProvider.closeQuietly();
  }

  private String destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format("Destination table name for topic '%s' is empty using the format string '%s'", topic, config.tableNameFormat));
    }
    return tableName;
  }
}
