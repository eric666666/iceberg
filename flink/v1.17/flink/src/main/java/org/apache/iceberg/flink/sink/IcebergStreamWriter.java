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

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;

import com.shopline.jdp.linkhub.act.etl.cdc2iceberg.bean.SinkElement;
import com.shopline.jdp.linkhub.act.etl.cdc2iceberg.bean.StpWriteResult;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

class IcebergStreamWriter extends AbstractStreamOperator<StpWriteResult>
    implements OneInputStreamOperator<SinkElement, StpWriteResult>, BoundedOneInput {

  private static final long serialVersionUID = 1L;

  /**
   * Writer name
   */
  private final String fullTableName;
  //private final TaskWriterFactory<SinkElement> taskWriterFactory;
  private final Map<TableIdentifier, TaskWriterFactory<RowData>> taskWriterFactoryHolder =
      new ConcurrentHashMap<>();

  //private transient TaskWriter<SinkElement> writer;
  private transient int subTaskId;
  private transient int attemptId;
  private transient IcebergStreamWriterMetrics writerMetrics;
  private final Map<TableIdentifier, TaskWriter<RowData>> taskWriterHolder =
      new ConcurrentHashMap<>();
  private final Map<TableIdentifier, String> latestUpdateTimeHolder =
      new ConcurrentHashMap<>();
  private final Map<TableIdentifier, TableLoader> tableLoaderMap =
      new ConcurrentHashMap<>();


  IcebergStreamWriter(String fullTableName) {
    this.fullTableName = fullTableName;
    setChainingStrategy(ChainingStrategy.ALWAYS);
  }

  private void init(SinkElement sinkElement) {
    TableLoader tableLoader = sinkElement.getTableLoader();
    tableLoader.open();
    Table serializableTable = tableLoader.loadTable();
    // write options
    Map<String, String> writeOptions = new HashMap<>();
    FlinkWriteConf flinkWriteConf =
        new FlinkWriteConf(serializableTable, writeOptions, new Configuration());
    FileFormat format = flinkWriteConf.dataFileFormat();

    TaskWriterFactory<RowData> taskWriterFactory =
        new RowDataTaskWriterFactory(
            serializableTable,
            FlinkSchemaUtil.convert(sinkElement.getIcebergSchema()),
            flinkWriteConf.targetDataFileSize(),
            format,
            writeProperties(serializableTable, format, flinkWriteConf),
            Lists.newArrayList(serializableTable.schema().identifierFieldIds()),
            flinkWriteConf.upsertMode());
    // Initialize the task writer factory.
    taskWriterFactory.initialize(subTaskId, attemptId);
    // Initialize the task writer.
    TaskWriter<RowData> writer = taskWriterFactory.create();
    TableIdentifier tableIdentifier = sinkElement.getTableIdentifier();
    taskWriterFactoryHolder.put(tableIdentifier, taskWriterFactory);
    taskWriterHolder.put(tableIdentifier, writer);
    tableLoaderMap.put(tableIdentifier, tableLoader);
  }

  /**
   * Based on the {@link FileFormat} overwrites the table level compression properties for the table
   * write.
   *
   * @param table  The table to get the table level settings
   * @param format The FileFormat to use
   * @param conf   The write configuration
   * @return The properties to use for writing
   */
  private static Map<String, String> writeProperties(
      Table table, FileFormat format, FlinkWriteConf conf) {
    Map<String, String> writeProperties = Maps.newHashMap(table.properties());

    switch (format) {
      case PARQUET:
        writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
        String parquetCompressionLevel = conf.parquetCompressionLevel();
        if (parquetCompressionLevel != null) {
          writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
        }

        break;
      case AVRO:
        writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
        String avroCompressionLevel = conf.avroCompressionLevel();
        if (avroCompressionLevel != null) {
          writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
        }

        break;
      case ORC:
        writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
        writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown file format %s", format));
    }

    return writeProperties;
  }

  @Override
  public void open() {
    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();
    this.writerMetrics = new IcebergStreamWriterMetrics(super.metrics, fullTableName);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    flush();
    for (Map.Entry<TableIdentifier, TaskWriterFactory<RowData>> entry : taskWriterFactoryHolder.entrySet()) {
      taskWriterHolder.put(entry.getKey(), entry.getValue().create());
    }
  }

  @Override
  public void processElement(StreamRecord<SinkElement> element) throws Exception {
    SinkElement sinkElement = element.getValue();
    TableIdentifier tableIdentifier = sinkElement.getTableIdentifier();
    String latestUpdateTime = latestUpdateTimeHolder.get(tableIdentifier);
    if (!taskWriterFactoryHolder.containsKey(tableIdentifier)) {
      init(sinkElement);
    }
    // route writer
    taskWriterHolder.get(tableIdentifier)
        .write(sinkElement.getRowData(), sinkElement.getIcebergSchema());
    String updateTime = sinkElement.getUpdateTime();
    if (StringUtils.isNotBlank(updateTime)) {
      if (latestUpdateTime == null) {
        latestUpdateTime = updateTime;
      } else {
        latestUpdateTime =
            updateTime.compareTo(latestUpdateTime) > 0 ? updateTime : latestUpdateTime;
      }
      latestUpdateTimeHolder.put(tableIdentifier, latestUpdateTime);

    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (MapUtils.isNotEmpty(taskWriterHolder)) {
      for (Map.Entry<TableIdentifier, TaskWriter<RowData>> entry : taskWriterHolder.entrySet()) {
        TableIdentifier tableIdentifier = entry.getKey();
        TaskWriter<RowData> writer = entry.getValue();
        if (writer != null) {
          writer.close();
          writer = null;
        }
      }
    }
  }

  @Override
  public void endInput() throws IOException {
    // For bounded stream, it may don't enable the checkpoint mechanism so we'd better to emit the
    // remaining completed files to downstream before closing the writer so that we won't miss any
    // of them.
    // Note that if the task is not closed after calling endInput, checkpoint may be triggered again
    // causing files to be sent repeatedly, the writer is marked as null after the last file is sent
    // to guard against duplicated writes.
    flush();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("table_name", fullTableName)
        .add("subtask_id", subTaskId)
        .add("attempt_id", attemptId)
        .toString();
  }

  /**
   * close all open files and emit files to downstream committer operator
   */
  private void flush() throws IOException {
    if (MapUtils.isEmpty(taskWriterHolder)) {
      return;
    }

    Map<TableIdentifier, StpWriteResult.WriteInfo> holder = new HashMap<>();
    for (Map.Entry<TableIdentifier, TaskWriter<RowData>> entry : taskWriterHolder.entrySet()) {
      TableIdentifier tableIdentifier = entry.getKey();
      TaskWriter<RowData> writer = entry.getValue();

      if (writer == null) {
        continue;
      }

      long startNano = System.nanoTime();

      WriteResult result = writer.complete();
      holder.put(tableIdentifier,
          new StpWriteResult.WriteInfo(result, latestUpdateTimeHolder.get(tableIdentifier),
              tableLoaderMap.get(tableIdentifier)));
      writerMetrics.updateFlushResult(result);
      writerMetrics.flushDuration(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));

      // Set writer to null to prevent duplicate flushes in the corner case of
      // prepareSnapshotPreBarrier happening after endInput.
      writer = null;
    }

    output.collect(new StreamRecord<>(new StpWriteResult(holder)));
  }

}
