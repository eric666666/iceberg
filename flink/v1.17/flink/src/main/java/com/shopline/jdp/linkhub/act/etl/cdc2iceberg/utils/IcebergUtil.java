package com.shopline.jdp.linkhub.act.etl.cdc2iceberg.utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * @Author: eric
 * @Date: 2022/9/29 15:42
 * @Description:
 */
@Slf4j
public class IcebergUtil {
  public static TableLoader getTableLoader(String icebergTable, String hiveSitePath,
                                           String keytabPath) {
    Map<String, String> map = new HashMap<>();
    if (StringUtils.isNotBlank(hiveSitePath)) {
      map.put(FlinkCatalogFactory.HIVE_CONF_DIR, hiveSitePath);
      log.info("Load custom hive-site.xml file on path: {} .", hiveSitePath);
      if (StringUtils.isNotBlank(keytabPath)) {
        map.put(FlinkCatalogFactory.DIST_KEYTAB, keytabPath);
        log.info("Load custom kerberos keytab file on path: {} .", hiveSitePath);
      }
    }
    TableLoader tableLoader = TableLoader.fromCatalog(
        CatalogLoader.hive("iceberg_hive", new Configuration(), map),
        TableIdentifier.parse(icebergTable));
    return Preconditions.checkNotNull(tableLoader);
  }

  /**
   * Get iceberg table.
   *
   * @param icebergTable icebergTable
   * @param hiveSitePath hiveSitePath
   * @param keytabPath   keytabPath
   * @return
   * @throws IOException IOException
   */
  public static Table getTable(String icebergTable, String hiveSitePath, String keytabPath)
      throws IOException {
    TableLoader tableLoader = getTableLoader(icebergTable, hiveSitePath, keytabPath);
    tableLoader.open();
    Table table = tableLoader.loadTable();
    table.refresh();
    tableLoader.close();
    return table;

  }

  /**
   * 检查iceberg表schema一致.
   * eg: v1,v2
   *
   * @param hiveSitePath
   * @param keytabPath
   * @param tables
   * @throws IOException
   */
  public static void checkSameSchema(String hiveSitePath, String keytabPath, List<String> tables)
      throws IOException {
    Preconditions.checkState(tables != null, "Tables must great than 1.");
    List<Types.NestedField> columns = null;
    for (int i = 0; i < tables.size(); i++) {
      Table table = getTable(tables.get(i), hiveSitePath, keytabPath);
      if (i == 0) {
        columns = table.schema().columns();
      } else {
        List<Types.NestedField> otherColumns = table.schema().columns();
        compareColumns(columns, otherColumns);
        columns = otherColumns;
      }
    }

  }

  private static void compareColumns(List<Types.NestedField> columns,
                                     List<Types.NestedField> otherColumns) {
    if (columns.size() == otherColumns.size()) {
      for (int i = 0; i < columns.size(); i++) {
        Types.NestedField thisField = columns.get(i);
        Types.NestedField otherField = otherColumns.get(i);
        String thisName = thisField.name();
        Type thisType = thisField.type();
        String otherName = otherField.name();
        Type otherType = otherField.type();
        if (!StringUtils.equals(thisName, otherName)) {
          throw new IllegalStateException(
              String.format("多个sink的字段名不一致, index: %d,this name: %s,that name: %s", i,
                  thisName, otherName)
          );
        }

        if (!thisType.toString().equals(otherType.toString())) {
          throw new IllegalStateException(
              String.format(
                  "多个sink的字段类型不一致, index: %d,this name: %s,that name: %s ,this type: %s,that type: %s",
                  i, thisName, otherName, thisType, otherType)
          );
        }

      }
    } else {
      throw new IllegalStateException(
          String.format("多个sink的Schema字段数不一致！[%d]!=[%d]", columns.size(),
              otherColumns.size()));
    }

  }


}
