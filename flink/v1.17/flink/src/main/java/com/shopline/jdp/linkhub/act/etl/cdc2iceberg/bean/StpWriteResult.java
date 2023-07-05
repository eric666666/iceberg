package com.shopline.jdp.linkhub.act.etl.cdc2iceberg.bean;

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;

/**
 * @Author: eric
 * @Date: 2022/10/9 14:19
 * @Description:
 */
public class StpWriteResult implements Serializable {
  private Map<TableIdentifier, WriteInfo> holder;

  public StpWriteResult() {
  }

  public StpWriteResult(Map<TableIdentifier, WriteInfo> holder) {
    this.holder = holder;
  }

  public Map<TableIdentifier, WriteInfo> getHolder() {
    return holder;
  }

  public void setHolder(
      Map<TableIdentifier, WriteInfo> holder) {
    this.holder = holder;
  }

  public static class WriteInfo {
    private WriteResult writeResult;
    private String updateTime;
    private TableLoader tableLoader;

    public WriteInfo() {
    }

    public WriteInfo(WriteResult writeResult, String updateTime, TableLoader tableLoader) {
      this.writeResult = writeResult;
      this.updateTime = updateTime;
      this.tableLoader = tableLoader;
    }

    public WriteResult getWriteResult() {
      return writeResult;
    }

    public void setWriteResult(WriteResult writeResult) {
      this.writeResult = writeResult;
    }

    public String getUpdateTime() {
      return updateTime;
    }

    public void setUpdateTime(String updateTime) {
      this.updateTime = updateTime;
    }

    public TableLoader getTableLoader() {
      return tableLoader;
    }

    public void setTableLoader(TableLoader tableLoader) {
      this.tableLoader = tableLoader;
    }
  }
}
