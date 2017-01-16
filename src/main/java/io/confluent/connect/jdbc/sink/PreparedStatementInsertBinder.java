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

import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PreparedStatementInsertBinder extends PreparedStatementBinder {

  public PreparedStatementInsertBinder(PreparedStatement statement, JdbcSinkConfig.PrimaryKeyMode pkMode,
                                       SchemaPair schemaPair, FieldsMetadata fieldsMetadata) {
    super(statement, pkMode, schemaPair, fieldsMetadata);
  }

  public void bindRecord(SinkRecord record) throws SQLException {
    int index = bindKey(record, 1);
    final Struct valueStruct = (Struct) record.value();
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      final Field field = record.valueSchema().field(fieldName);
      bindField(index++, fieldName, field.schema(), valueStruct.get(field));
    }
    statement.addBatch();
  }

}
