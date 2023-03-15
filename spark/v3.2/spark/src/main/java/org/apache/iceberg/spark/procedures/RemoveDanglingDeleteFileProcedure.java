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
package org.apache.iceberg.spark.procedures;

import java.util.List;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.execution.datasources.SparkExpressionConverter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A procedure that rewrites datafiles in a table.
 *
 * @see org.apache.iceberg.spark.actions.SparkActions#rewriteDataFiles(Table)
 */
class RemoveDanglingDeleteFileProcedure extends BaseProcedure {
  private boolean dryRun;

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("where", DataTypes.StringType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType)
      };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("delete_file_location", DataTypes.StringType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new Builder<RemoveDanglingDeleteFileProcedure>() {
      @Override
      protected RemoveDanglingDeleteFileProcedure doBuild() {
        return new RemoveDanglingDeleteFileProcedure(tableCatalog());
      }
    };
  }

  private RemoveDanglingDeleteFileProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());

    return modifyIcebergTable(
        tableIdent,
        table -> {
          String quotedFullIdentifier =
              Spark3Util.quotedFullIdentifier(tableCatalog().name(), tableIdent);
          RemoveDanglingDeleteFiles action = actions().removeDanglingDeleteFiles(table);

          String where = args.isNullAt(1) ? null : args.getString(1);
          dryRun = args.isNullAt(2) ? false : args.getBoolean(2);

          action = checkAndApplyFilter(action, where, quotedFullIdentifier);
          action.dryRun(dryRun);
          RemoveDanglingDeleteFiles.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private RemoveDanglingDeleteFiles checkAndApplyFilter(
      RemoveDanglingDeleteFiles action, String where, String tableName) {
    if (where != null) {
      try {
        Expression expression =
            SparkExpressionConverter.collectResolvedSparkExpression(spark(), tableName, where);
        return action.filter(SparkExpressionConverter.convertToIcebergExpression(expression));
      } catch (AnalysisException e) {
        throw new IllegalArgumentException("Cannot parse predicates in where option: " + where);
      }
    }
    return action;
  }

  //  private RewriteDataFiles checkAndApplyOptions(InternalRow args, RewriteDataFiles action) {
  //    Map<String, String> options = Maps.newHashMap();
  //    args.getMap(3)
  //        .foreach(
  //            DataTypes.StringType,
  //            DataTypes.StringType,
  //            (k, v) -> {
  //              options.put(k.toString(), v.toString());
  //              return BoxedUnit.UNIT;
  //            });
  //    return action.options(options);
  //  }

  //  private RewriteDataFiles checkAndApplyStrategy(
  //      RewriteDataFiles action, String strategy, String sortOrderString, Schema schema) {
  //    List<Zorder> zOrderTerms = Lists.newArrayList();
  //    List<ExtendedParser.RawOrderField> sortOrderFields = Lists.newArrayList();
  //    if (sortOrderString != null) {
  //      ExtendedParser.parseSortOrder(spark(), sortOrderString)
  //          .forEach(
  //              field -> {
  //                if (field.term() instanceof Zorder) {
  //                  zOrderTerms.add((Zorder) field.term());
  //                } else {
  //                  sortOrderFields.add(field);
  //                }
  //              });
  //
  //      if (!zOrderTerms.isEmpty() && !sortOrderFields.isEmpty()) {
  //        // TODO: we need to allow this in future when SparkAction has handling for this.
  //        throw new IllegalArgumentException(
  //            "Cannot mix identity sort columns and a Zorder sort expression: " +
  // sortOrderString);
  //      }
  //    }
  //
  //    // caller of this function ensures that between strategy and sortOrder, at least one of them
  // is
  //    // not null.
  //    if (strategy == null || strategy.equalsIgnoreCase("sort")) {
  //      if (!zOrderTerms.isEmpty()) {
  //        String[] columnNames =
  //            zOrderTerms.stream()
  //                .flatMap(zOrder -> zOrder.refs().stream().map(NamedReference::name))
  //                .toArray(String[]::new);
  //        return action.zOrder(columnNames);
  //      } else if (!sortOrderFields.isEmpty()) {
  //        return action.sort(buildSortOrder(sortOrderFields, schema));
  //      } else {
  //        return action.sort();
  //      }
  //    }
  //    if (strategy.equalsIgnoreCase("binpack")) {
  //      RewriteDataFiles rewriteDataFiles = action.binPack();
  //      if (sortOrderString != null) {
  //        // calling below method to throw the error as user has set both binpack strategy and
  // sort
  //        // order
  //        return rewriteDataFiles.sort(buildSortOrder(sortOrderFields, schema));
  //      }
  //      return rewriteDataFiles;
  //    } else {
  //      throw new IllegalArgumentException(
  //          "unsupported strategy: " + strategy + ". Only binpack or sort is supported");
  //    }
  //  }
  //
  //  private SortOrder buildSortOrder(
  //      List<ExtendedParser.RawOrderField> rawOrderFields, Schema schema) {
  //    SortOrder.Builder builder = SortOrder.builderFor(schema);
  //    rawOrderFields.forEach(
  //        rawField -> builder.sortBy(rawField.term(), rawField.direction(),
  // rawField.nullOrder()));
  //    return builder.build();
  //  }

  private InternalRow[] toOutputRows(RemoveDanglingDeleteFiles.Result result) {
    List<DeleteFile> deleteFiles = result.removedDeleteFiles();
    InternalRow[] rows = new InternalRow[deleteFiles.size()];
    int index = 0;
    for (DeleteFile deleteFile : deleteFiles) {
      rows[index] = newInternalRow(UTF8String.fromString(deleteFile.path().toString()));
      index++;
    }
    return rows;
  }

  @Override
  public String description() {
    return "RemoveDanglingDeleteFileProcedure";
  }
}
