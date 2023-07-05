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

import com.shopline.jdp.linkhub.act.etl.cdc2iceberg.bean.SinkElement;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.StructProjection;

/**
 * Create a {@link KeySelector} to shuffle by equality fields, to ensure same equality fields record
 * will be emitted to same writer in order.
 */
class EqualityFieldKeySelector implements KeySelector<SinkElement, Integer> {
  private Map<TableIdentifier, Wrapper> holderMap = new ConcurrentHashMap<>();

  @Override
  public Integer getKey(SinkElement row) {
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

      Wrapper wrapper =
          new Wrapper(table.schema(), FlinkSchemaUtil.convert(table.schema()),
              Lists.newArrayList(table.schema().identifierFieldIds()));
      holderMap.put(tableIdentifier, wrapper);
    }
    return holderMap.get(tableIdentifier).getKey(row);
  }

  private static class Wrapper {

    private final Schema schema;
    private final RowType flinkSchema;
    private final Schema deleteSchema;

    private transient RowDataWrapper rowDataWrapper;
    private transient StructProjection structProjection;
    private transient StructLikeWrapper structLikeWrapper;

    Wrapper(Schema schema, RowType flinkSchema, List<Integer> equalityFieldIds) {
      this.schema = schema;
      this.flinkSchema = flinkSchema;
      this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    }

    /**
     * Construct the {@link RowDataWrapper} lazily here because few members in it are not
     * serializable. In this way, we don't have to serialize them with forcing.
     */
    protected RowDataWrapper lazyRowDataWrapper() {
      if (rowDataWrapper == null) {
        rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
      }
      return rowDataWrapper;
    }

    /**
     * Construct the {@link StructProjection} lazily because it is not serializable.
     */
    protected StructProjection lazyStructProjection() {
      if (structProjection == null) {
        structProjection = StructProjection.create(schema, deleteSchema);
      }
      return structProjection;
    }

    /**
     * Construct the {@link StructLikeWrapper} lazily because it is not serializable.
     */
    protected StructLikeWrapper lazyStructLikeWrapper() {
      if (structLikeWrapper == null) {
        structLikeWrapper = StructLikeWrapper.forType(deleteSchema.asStruct());
      }
      return structLikeWrapper;
    }

    public Integer getKey(SinkElement row) {
      RowDataWrapper wrappedRowData = lazyRowDataWrapper().wrap(row.getRowData());
      StructProjection projectedRowData = lazyStructProjection().wrap(wrappedRowData);
      StructLikeWrapper wrapper = lazyStructLikeWrapper().set(projectedRowData);
      return wrapper.hashCode();
    }

  }
}
