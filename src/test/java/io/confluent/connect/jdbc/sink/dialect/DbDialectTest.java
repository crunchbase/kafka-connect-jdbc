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

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Test;

import java.math.BigDecimal;

import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;

import static org.junit.Assert.assertEquals;

public class DbDialectTest {

  public static final DbDialect DUMMY_DIALECT = new DbDialect("`", "`") {
    @Override
    protected String getSqlType(SinkRecordField f) {
      return "DUMMY";
    }
  };

  @Test
  public void formatColumnValue() {
    verifyFormatColumnValue("42", Schema.INT8_SCHEMA, (byte) 42);
    verifyFormatColumnValue("42", Schema.INT16_SCHEMA, (short) 42);
    verifyFormatColumnValue("42", Schema.INT32_SCHEMA, 42);
    verifyFormatColumnValue("42", Schema.INT64_SCHEMA, 42L);
    verifyFormatColumnValue("42.5", Schema.FLOAT32_SCHEMA, 42.5f);
    verifyFormatColumnValue("42.5", Schema.FLOAT64_SCHEMA, 42.5d);
    verifyFormatColumnValue("0", Schema.BOOLEAN_SCHEMA, false);
    verifyFormatColumnValue("1", Schema.BOOLEAN_SCHEMA, true);
    verifyFormatColumnValue("'quoteit'", Schema.STRING_SCHEMA, "quoteit");
    verifyFormatColumnValue("x'2A'", Schema.BYTES_SCHEMA, new byte[]{42});

    verifyFormatColumnValue("42.42", Decimal.schema(2), new BigDecimal("42.42"));

    final java.util.Date instant = new java.util.Date(1474661402123L);
    verifyFormatColumnValue("'2016-09-23'", Date.SCHEMA, instant);
    verifyFormatColumnValue("'20:10:02.123'", Time.SCHEMA, instant);
    verifyFormatColumnValue("'2016-09-23 20:10:02.123'", Timestamp.SCHEMA, instant);
  }

  private void verifyFormatColumnValue(String expected, Schema schema, Object value) {
    final StringBuilder builder = new StringBuilder();
    DUMMY_DIALECT.formatColumnValue(builder, schema.name(), schema.parameters(), schema.type(), value);
    assertEquals(expected, builder.toString());
  }

  @Test
  public void writeColumnSpec() {
    verifyWriteColumnSpec("`foo` DUMMY DEFAULT 42", new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "foo", true));
    verifyWriteColumnSpec("`foo` DUMMY DEFAULT 42", new SinkRecordField(SchemaBuilder.int32().defaultValue(42).build(), "foo", false));
    verifyWriteColumnSpec("`foo` DUMMY DEFAULT 42", new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", true));
    verifyWriteColumnSpec("`foo` DUMMY DEFAULT 42", new SinkRecordField(SchemaBuilder.int32().optional().defaultValue(42).build(), "foo", false));
    verifyWriteColumnSpec("`foo` DUMMY NOT NULL", new SinkRecordField(Schema.INT32_SCHEMA, "foo", true));
    verifyWriteColumnSpec("`foo` DUMMY NOT NULL", new SinkRecordField(Schema.INT32_SCHEMA, "foo", false));
    verifyWriteColumnSpec("`foo` DUMMY NOT NULL", new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", true));
    verifyWriteColumnSpec("`foo` DUMMY NULL", new SinkRecordField(Schema.OPTIONAL_INT32_SCHEMA, "foo", false));
  }

  private void verifyWriteColumnSpec(String expected, SinkRecordField field) {
    final StringBuilder builder = new StringBuilder();
    DUMMY_DIALECT.writeColumnSpec(builder, field);
    assertEquals(expected, builder.toString());
  }

  @Test(expected = ConnectException.class)
  public void extractProtocolInvalidUrl() {
    DbDialect.extractProtocolFromUrl("jdbc:protocol:somethingelse;field=value;");
  }

  @Test(expected = ConnectException.class)
  public void extractProtocolNoJdbcPrefix() {
    DbDialect.extractProtocolFromUrl("mysql://Server:port");
  }

  @Test
  public void extractProtocol() {
    assertEquals("protocol_test", DbDialect.extractProtocolFromUrl("jdbc:protocol_test://SERVER:21421;field=value"));
  }

  @Test
  public void detectPostgres() {
    assertEquals(PostgresDialect.class, DbDialect.fromConnectionString("jdbc:postgresql://HOST:1433;DatabaseName=DATABASE").getClass());
  }

}
