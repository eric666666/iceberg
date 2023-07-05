package com.shopline.jdp.linkhub.act.etl.cdc2iceberg.bean;

import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;

/**
 * Sink 输入元素类型.
 *
 * @author <a href="mailto:zengbao@joyy.com">zengbao</a>
 * @since 2023/6/7 10:21
 */
public class SinkElement {
  private Schema icebergSchema;
  private RowData rowData;
  private TableIdentifier tableIdentifier;
  private TableLoader tableLoader;

  /**
   * binlog时间戳.
   */
  private long binlogTsMs;
  private String updateTime;

  public SinkElement() {
  }

  public SinkElement(Schema icebergSchema, RowData rowData, TableIdentifier tableIdentifier,
                     TableLoader tableLoader, long binlogTsMs, String updateTime) {
    this.icebergSchema = icebergSchema;
    this.rowData = rowData;
    this.tableIdentifier = tableIdentifier;
    this.tableLoader = tableLoader;
    this.binlogTsMs = binlogTsMs;
    this.updateTime = updateTime;
  }

  public Schema getIcebergSchema() {
    return icebergSchema;
  }

  public void setIcebergSchema(Schema icebergSchema) {
    this.icebergSchema = icebergSchema;
  }

  public RowData getRowData() {
    return rowData;
  }

  public void setRowData(RowData rowData) {
    this.rowData = rowData;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(TableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public TableLoader getTableLoader() {
    return tableLoader;
  }

  public void setTableLoader(TableLoader tableLoader) {
    this.tableLoader = tableLoader;
  }

  public long getBinlogTsMs() {
    return binlogTsMs;
  }

  public void setBinlogTsMs(long binlogTsMs) {
    this.binlogTsMs = binlogTsMs;
  }

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }
}