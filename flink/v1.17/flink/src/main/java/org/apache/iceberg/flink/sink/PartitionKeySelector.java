/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink.sink;

import cn.hutool.core.map.SafeConcurrentHashMap;
import com.shopline.jdp.linkhub.act.etl.cdc2iceberg.bean.SinkElement;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;

/**
 * Create a {@link KeySelector} to shuffle by partition key, then each partition/bucket will be
 * wrote by only one task. That will reduce lots of small files in partitioned fanout write policy
 * for {@link FlinkSink}.
 */
class PartitionKeySelector implements KeySelector<SinkElement, String> {

  private final Map<TableIdentifier, PartitionKeySelector.Wrapper>
      holderMap = new SafeConcurrentHashMap<>();

  @Override
  public String getKey(SinkElement row) {
    TableIdentifier tableIdentifier = row.getTableIdentifier();
    if (!holderMap.containsKey(tableIdentifier)) {
      TableLoader tableLoader = row.getTableLoader();
      Table table;
      tableLoader.open();
      try (TableLoader loader = tableLoader) {
        table = loader.loadTable();
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to load iceberg table from table loader: " + tableLoader, e);
      }

      PartitionKeySelector.Wrapper wrapperHolder =
          new PartitionKeySelector.Wrapper(tableIdentifier.toString(), table.spec(), table.schema(),
              FlinkSchemaUtil.convert(table.schema()));
      holderMap.put(tableIdentifier, wrapperHolder);
    }
    return holderMap.get(tableIdentifier).getKey(row.getRowData());
  }

  private static class Wrapper {
    private final String name;
    private final Schema schema;
    private final PartitionKey partitionKey;
    private final RowType flinkSchema;

    private transient RowDataWrapper rowDataWrapper;

    Wrapper(String name, PartitionSpec spec, Schema schema, RowType flinkSchema) {
      this.name = name;
      this.schema = schema;
      this.partitionKey = new PartitionKey(spec, schema);
      this.flinkSchema = flinkSchema;
    }

    /**
     * Construct the {@link RowDataWrapper} lazily here because few members in it are not
     * serializable. In this way, we don't have to serialize them with forcing.
     */
    private RowDataWrapper lazyRowDataWrapper() {
      if (rowDataWrapper == null) {
        rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
      }
      return rowDataWrapper;
    }

    public String getKey(RowData row) {
      partitionKey.partition(lazyRowDataWrapper().wrap(row));
      return name + "#" + partitionKey.toPath();
    }
  }
}
