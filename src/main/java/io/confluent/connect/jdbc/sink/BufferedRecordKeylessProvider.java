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


import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BufferedRecordKeylessProvider implements BufferedRecordProvider {

  private static int capacity = 1000;
  private ArrayList<SinkRecord> records;

  BufferedRecordKeylessProvider() {
    records = new ArrayList<>(capacity);
  }

  @Override
  public void add(SinkRecord record) {
    records.add(record);
  }

  @Override
  public List<SinkRecord> reset() {
    List<SinkRecord> flushed = records;
    records = new ArrayList<>(capacity);
    return flushed;
  }

  @Override
  public int size() {
    return records.size();
  }

  @Override
  public boolean isEmpty() {
    return records.isEmpty();
  }

  @Override
  public Iterator<SinkRecord> iterator() {
    return records.iterator();
  }

}
