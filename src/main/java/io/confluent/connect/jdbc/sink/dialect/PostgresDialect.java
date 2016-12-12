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

package io.confluent.connect.jdbc.sink.dialect;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.Collection;

import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.joinToBuilder;
import static io.confluent.connect.jdbc.sink.dialect.StringBuilderUtil.nCopiesToBuilder;

public class PostgresDialect extends DbDialect {

  public PostgresDialect() {
    super("\"", "\"");
  }

  @Override
  protected String getSqlType(SinkRecordField f) {
    System.out.println(f);
    if (f.schemaName() != null) {
      switch (f.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "DECIMAL";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "TIME";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
      }
    }
    switch (f.schemaType()) {
      case ARRAY:
        return getSqlType(f.schema().valueSchema().type()) + "[]";
    }
    return getSqlType(f.schemaType());
  }

  protected String getSqlType(Schema.Type type) {
    switch (type) {
      case INT8:
        return "SMALLINT";
      case INT16:
        return "SMALLINT";
      case INT32:
        return "INT";
      case INT64:
        return "BIGINT";
      case FLOAT32:
        return "REAL";
      case FLOAT64:
        return "DOUBLE PRECISION";
      case BOOLEAN:
        return "BOOLEAN";
      case STRING:
        return "TEXT";
      case BYTES:
        return "BLOB";
      case MAP:
        return "JSONB";
    }
    throw new ConnectException(String.format("%s type doesn't have a mapping to the SQL database column type", type));
  }

  @Override
  public String getDeleteQuery(final String tableName, final Collection<String> keyColumns, final Collection<String> nonKeyColumns) {
    StringBuilder builder = new StringBuilder("DELETE FROM ");
    builder.append(escaped(tableName));
    builder.append(" WHERE ROW(");
    joinToBuilder(builder, ",", keyColumns, escaper());
    builder.append(") IN (ROW(");
    nCopiesToBuilder(builder, ",", "?", keyColumns.size());
    builder.append("))");
    return builder.toString();
  }

  @Override
  public String getUpsertQuery(final String table, final Collection<String> keyCols, final Collection<String> cols) {
    final StringBuilder builder = new StringBuilder();
    builder.append("INSERT INTO ");
    builder.append(escaped(table));
    builder.append(" (");
    joinToBuilder(builder, ",", keyCols, cols, escaper());
    builder.append(") VALUES (");
    nCopiesToBuilder(builder, ",", "?", cols.size() + keyCols.size());
    builder.append(") ON CONFLICT (");
    joinToBuilder(builder, ",", keyCols, escaper());
    builder.append(") DO UPDATE SET ");
    joinToBuilder(
        builder,
        ",",
        cols,
        new StringBuilderUtil.Transform<String>() {
          @Override
          public void apply(StringBuilder builder, String col) {
            builder.append(escaped(col)).append("=EXCLUDED.").append(escaped(col));
          }
        }
    );
    return builder.toString();
  }

}
