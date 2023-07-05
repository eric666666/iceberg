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

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.SafeConcurrentHashMap;
import com.shopline.jdp.linkhub.act.etl.cdc2iceberg.bean.StpWriteResult;
import com.shopline.jdp.linkhub.act.etl.cdc2iceberg.config.AppOptions;
import com.shopline.jdp.linkhub.act.etl.cdc2iceberg.utils.IcebergUtil;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReplacePartitions;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 多表committer
 */
class DynamicIcebergFilesCommitter
    extends AbstractStreamOperator<Void>
    implements OneInputStreamOperator<StpWriteResult, Void>, BoundedOneInput {

  private static final long serialVersionUID = 1L;
  private static final long INITIAL_CHECKPOINT_ID = -1L;
  private static final byte[] EMPTY_MANIFEST_DATA = new byte[0];

  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicIcebergFilesCommitter.class);
  private static final String FLINK_JOB_ID = "flink.job-id";
  private static final String OPERATOR_ID = "flink.operator-id";

  // The max checkpoint id we've committed to iceberg table. As the flink's checkpoint is always
  // increasing, so we could correctly commit all the data files whose checkpoint id is greater than
  // the max committed one to iceberg table, for avoiding committing the same data files twice. This
  // id will be attached to iceberg's meta when committing the iceberg transaction.
  private static final String MAX_COMMITTED_CHECKPOINT_ID = "flink.max-committed-checkpoint-id";
  static final String MAX_CONTINUOUS_EMPTY_COMMITS = "flink.max-continuous-empty-commits";

  // disable replace partition
  private final boolean replacePartitions = false;
  private final Map<String, String> snapshotProperties;

  // A sorted map to maintain the completed data files for each pending checkpointId (which have not
  // been committed to iceberg table). We need a sorted map here because there's possible that few
  // checkpoints snapshot failed, for example: the 1st checkpoint have 2 data files <1, <file0,
  // file1>>, the 2st checkpoint have 1 data files <2, <file3>>. Snapshot for checkpoint#1
  // interrupted because of network/disk failure etc, while we don't expect any data loss in iceberg
  // table. So we keep the finished files <1, <file0, file1>> in memory and retry to commit iceberg
  // table when the next checkpoint happen.
  //private final NavigableMap<Long, byte[]> dataFilesPerCheckpoint = Maps.newTreeMap();
  private final Map<TableIdentifier, NavigableMap<Long, byte[]>> dataFilesPerCheckpointMap =
      new SafeConcurrentHashMap<>();

  // The completed files cache for current checkpoint. Once the snapshot barrier received, it will
  // be flushed to the 'dataFilesPerCheckpoint'.
  private final List<StpWriteResult> writeResultsOfCurrentCkpt = Lists.newArrayList();

  private final Map<TableIdentifier, List<StpWriteResult>> tableWriteResultsOfCurrentCkptMap =
      new SafeConcurrentHashMap<>();

  // table holder list state
  private ListStateDescriptor<Map<TableIdentifier, SingleTableInfo>> tableHolderStateDescribe =
      new ListStateDescriptor<>("tableHolderStateDescribe",
          new MapTypeInfo<>(TableIdentifier.class, SingleTableInfo.class));
  private ListState<Map<TableIdentifier, SingleTableInfo>> tableHolderState;

  private final Map<TableIdentifier, SingleTableInfo> tableHolder =
      new SafeConcurrentHashMap<>();

  private final String branch;

  // It will have an unique identifier for one job.
  private transient String flinkJobId;
  private transient String operatorUniqueId;


  private final Map<TableIdentifier, Long> maxCommittedCheckpointIdMap =
      new SafeConcurrentHashMap<>();


  private transient int continuousEmptyCheckpoints;
  //  最大持续空提交，达到次数后会提交一次
  private final transient int maxContinuousEmptyCommits = 10;
  // There're two cases that we restore from flink checkpoints: the first case is restoring from
  // snapshot created by the same flink job; another case is restoring from snapshot created by
  // another different job. For the second case, we need to maintain the old flink job's id in flink
  // state backend to find the max-committed-checkpoint-id when traversing iceberg table's
  // snapshots.
  private static final ListStateDescriptor<String> JOB_ID_DESCRIPTOR =
      new ListStateDescriptor<>("iceberg-flink-job-id", BasicTypeInfo.STRING_TYPE_INFO);
  private transient ListState<String> jobIdState;
  // All pending checkpoints states for this function.


  /**
   * 保存结构变更
   */
  private static final ListStateDescriptor<Map<TableIdentifier, List<SortedMap<Long, byte[]>>>>
      STATE_DESCRIPTOR =
      buildStateDescriptor();
  /**
   * chk状态.保存每一次checkpointId->data file
   * 因为: chk可能快照提交失败，但是iceberg表的数据不会丢的情况，将chk的过程记录下来，方便在后续的chk重新提交快照
   */
  private transient ListState<Map<TableIdentifier, List<SortedMap<Long, byte[]>>>> checkpointsState;

  private final Integer workerPoolSize;
  private transient ExecutorService workerPool;

  private ParameterTool pt;
  private int subTaskId;
  private int attemptId;

  @Data
  private static class SingleTableInfo {
    private final TableIdentifier tableIdentifier;
    private final PartitionSpec spec;
    private final String flinkJobId;
    private final String operatorUniqueId;
    private final int subTaskId;
    private final long attemptId;
    private transient Table table;
    private transient IcebergFilesCommitterMetrics committerMetrics;
    // TableLoader to load iceberg table lazily.
    private final TableLoader tableLoader;
    private ManifestOutputFileFactory manifestOutputFileFactory;
    private String latestUpdateTime;


    public SingleTableInfo(TableIdentifier tableIdentifier, ParameterTool pt, MetricGroup metrics,
                           String flinkJobId,
                           String operatorUniqueId,
                           int subTaskId,
                           long attemptId) {
      this.tableIdentifier = tableIdentifier;
      this.flinkJobId = flinkJobId;
      this.operatorUniqueId = operatorUniqueId;
      this.subTaskId = subTaskId;
      this.attemptId = attemptId;
      this.tableLoader = IcebergUtil.getTableLoader(tableIdentifier.toString(),
          pt.getRequired(AppOptions.HDFS_HIVE_SITE.key()),
          pt.getRequired(AppOptions.HDFS_KEYTAB.key())
      );
      tableLoader.open();
      this.table = tableLoader.loadTable();
      this.spec = this.table.spec();
      committerMetrics = new IcebergFilesCommitterMetrics(metrics, tableIdentifier.toString());
      this.manifestOutputFileFactory =
          FlinkManifestUtil.createOutputFileFactory(
              table, flinkJobId, operatorUniqueId, subTaskId, attemptId);

    }
  }

  DynamicIcebergFilesCommitter(
      Map<String, String> snapshotProperties,
      Integer workerPoolSize,
      String branch) {
    //this.tableLoader = tableLoader;
    //this.replacePartitions = replacePartitions;
    // force disable overwrite partition
    this.snapshotProperties = snapshotProperties;
    this.workerPoolSize = workerPoolSize;
    this.branch = branch;

    //this.tableLoader.open();
    //this.table = tableLoader.loadTable();
    //this.spec = this.table.spec();


  }

  public void initializeState(StateInitializationContext context) throws Exception {
    //this.context = context;
    this.flinkJobId = getContainingTask().getEnvironment().getJobID().toString();
    this.operatorUniqueId = getRuntimeContext().getOperatorUniqueID();

    /*
     *表注册时初始化
     */
    // Open the table loader and load the table.
    //this.tableLoader.open();
    //this.table = tableLoader.loadTable();
    //this.committerMetrics = new IcebergFilesCommitterMetrics(super.metrics, table.name());

    // 最大空commit 10次
    //maxContinuousEmptyCommits = 10;
    //Preconditions.checkArgument(
    //    maxContinuousEmptyCommits > 0, MAX_CONTINUOUS_EMPTY_COMMITS + " must be positive");

    this.subTaskId = getRuntimeContext().getIndexOfThisSubtask();
    this.attemptId = getRuntimeContext().getAttemptNumber();


    maxCommittedCheckpointIdMap.forEach((tableIdentifier, maxCommittedCheckpointId) -> {
      maxCommittedCheckpointId = INITIAL_CHECKPOINT_ID;
      maxCommittedCheckpointIdMap.put(tableIdentifier, maxCommittedCheckpointId);
    });


    this.checkpointsState = context.getOperatorStateStore().getListState(STATE_DESCRIPTOR);
    this.jobIdState = context.getOperatorStateStore().getListState(JOB_ID_DESCRIPTOR);
    this.tableHolderState = context.getOperatorStateStore().getListState(tableHolderStateDescribe);
    if (context.isRestored()) {
      Iterable<String> jobIdIterable = jobIdState.get();
      if (jobIdIterable == null || !jobIdIterable.iterator().hasNext()) {
        LOG.warn(
            "Failed to restore committer state. This can happen when operator uid changed and Flink "
                +
                "allowNonRestoredState is enabled. Best practice is to explicitly set the operator id "
                + "via FlinkSink#Builder#uidPrefix() so that the committer operator uid is stable. "
                + "Otherwise, Flink auto generate an operator uid based on job topology."
                + "With that, operator uid is subjective to change upon topology change.");
        return;
      }

      String restoredFlinkJobId = jobIdIterable.iterator().next();
      Preconditions.checkState(
          !Strings.isNullOrEmpty(restoredFlinkJobId),
          "Flink job id parsed from checkpoint snapshot shouldn't be null or empty");

      Iterator<Map<TableIdentifier, SingleTableInfo>> iterator = tableHolderState.get().iterator();
      if (!iterator.hasNext()) {
        return;
      }

      Map<TableIdentifier, SingleTableInfo> restoredTableInfoMap = iterator.next();
      for (Map<TableIdentifier, List<SortedMap<Long, byte[]>>> tableIdentifierSortedMapMap : checkpointsState.get()) {
        for (Map.Entry<TableIdentifier, List<SortedMap<Long, byte[]>>> entry : tableIdentifierSortedMapMap.entrySet()) {
          TableIdentifier tableIdentifier = entry.getKey();
          List<SortedMap<Long, byte[]>> longSortedMap = entry.getValue();
          // Since flink's checkpoint id will start from the max-committed-checkpoint-id + 1 in the new
          // flink job even if it's restored from a snapshot created by another different flink job, so
          // it's safe to assign the max committed checkpoint id from restored flink job to the current
          // flink job.
          SingleTableInfo singleTableInfo = restoredTableInfoMap.get(tableIdentifier);

          Long maxCommittedCheckpointId =
              getMaxCommittedCheckpointId(singleTableInfo.getTable(),
                  restoredFlinkJobId, operatorUniqueId, branch);
          maxCommittedCheckpointIdMap.put(tableIdentifier, maxCommittedCheckpointId);

          NavigableMap<Long, byte[]> uncommittedDataFiles =
              Maps.newTreeMap(longSortedMap.iterator().next())
                  .tailMap(maxCommittedCheckpointId, false);
          if (!uncommittedDataFiles.isEmpty()) {
            // Committed all uncommitted data files from the old flink job to iceberg table.
            long maxUncommittedCheckpointId = uncommittedDataFiles.lastKey();
            commitUpToCheckpoint(
                uncommittedDataFiles, restoredFlinkJobId, operatorUniqueId,
                maxUncommittedCheckpointId, singleTableInfo.getTable(),
                singleTableInfo.getCommitterMetrics(), singleTableInfo.getLatestUpdateTime());
          }
        }

      }

    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    super.snapshotState(context);
    long checkpointId = context.getCheckpointId();
    LOG.info(
        "Start to flush snapshot state to state backend, table: {}, checkpointId: {}",
        tableHolder.keySet(),
        checkpointId);

    // Update the checkpoint state.
    long startNano = System.nanoTime();

    Map<TableIdentifier, List<WriteResult>>
        map = getTableIdentifierWriteResultListMap();

    // 遍历当前注册的表
    for (Map.Entry<TableIdentifier, SingleTableInfo> entry : tableHolder.entrySet()) {
      TableIdentifier identifier = entry.getKey();
      SingleTableInfo singleTableInfo = entry.getValue();
      NavigableMap<Long, byte[]> dataFilesPerCheckpoint =
          dataFilesPerCheckpointMap.getOrDefault(identifier, Maps.newTreeMap());
      dataFilesPerCheckpoint.put(checkpointId, writeToManifest(checkpointId,
          singleTableInfo.getManifestOutputFileFactory(),
          map.get(identifier),
          singleTableInfo.getSpec()));

      dataFilesPerCheckpointMap.put(identifier, dataFilesPerCheckpoint);
      // Reset the snapshot state to the latest state.
      checkpointsState.clear();
      checkpointsState.add(new HashMap<TableIdentifier, List<SortedMap<Long, byte[]>>>() {{
        put(identifier, Collections.singletonList(dataFilesPerCheckpoint));
      }});

      jobIdState.clear();
      jobIdState.add(flinkJobId);

      // Clear the local buffer for current checkpoint.
      writeResultsOfCurrentCkpt.clear();
      singleTableInfo.getCommitterMetrics().checkpointDuration(
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano));

    }


  }

  @NotNull
  private Map<TableIdentifier, List<WriteResult>> getTableIdentifierWriteResultListMap() {
    Map<TableIdentifier, List<WriteResult>> map = new HashMap<>();
    for (StpWriteResult stpWriteResult : writeResultsOfCurrentCkpt) {
      stpWriteResult.getHolder().forEach((identifier, writeInfo) -> {
        List<WriteResult> resultList = map.getOrDefault(identifier, new LinkedList<>());
        resultList.add(writeInfo.getWriteResult());
        map.put(identifier, resultList);
      });
    }
    return map;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);

    for (Map.Entry<TableIdentifier, SingleTableInfo> entry : tableHolder.entrySet()) {
      TableIdentifier identifier = entry.getKey();
      SingleTableInfo singleTableInfo = entry.getValue();
      long maxCommittedCheckpointId =
          maxCommittedCheckpointIdMap.getOrDefault(identifier, INITIAL_CHECKPOINT_ID);
      NavigableMap<Long, byte[]> dataFilesPerCheckpoint =
          dataFilesPerCheckpointMap.getOrDefault(identifier, Maps.newTreeMap());
      // It's possible that we have the following events:
      //   1. snapshotState(ckpId);
      //   2. snapshotState(ckpId+1);
      //   3. notifyCheckpointComplete(ckpId+1);
      //   4. notifyCheckpointComplete(ckpId);
      // For step#4, we don't need to commit iceberg table again because in step#3 we've committed all
      // the files,
      // Besides, we need to maintain the max-committed-checkpoint-id to be increasing.
      if (checkpointId > maxCommittedCheckpointId) {
        LOG.info("Checkpoint {} completed. Attempting commit.", checkpointId);
        commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId, checkpointId,
            singleTableInfo.getTable(), singleTableInfo.getCommitterMetrics(),
            singleTableInfo.getLatestUpdateTime());
        maxCommittedCheckpointId = checkpointId;
        maxCommittedCheckpointIdMap.put(identifier, maxCommittedCheckpointId);
      } else {
        LOG.info(
            "Skipping committing checkpoint {}. {} is already committed.",
            checkpointId,
            maxCommittedCheckpointId);
      }
    }

  }

  /**
   * iceberg单表chk提交.
   *
   * @param deltaManifestsMap
   * @param newFlinkJobId
   * @param operatorId
   * @param checkpointId
   * @param table
   * @param committerMetrics
   * @throws IOException
   */
  private void commitUpToCheckpoint(
      NavigableMap<Long, byte[]> deltaManifestsMap,
      String newFlinkJobId,
      String operatorId,
      long checkpointId,
      Table table,
      IcebergFilesCommitterMetrics committerMetrics, String latestUpdateTime)
      throws IOException {
    NavigableMap<Long, byte[]> pendingMap = deltaManifestsMap.headMap(checkpointId, true);
    List<ManifestFile> manifests = Lists.newArrayList();
    NavigableMap<Long, WriteResult> pendingResults = Maps.newTreeMap();
    for (Map.Entry<Long, byte[]> e : pendingMap.entrySet()) {
      if (Arrays.equals(EMPTY_MANIFEST_DATA, e.getValue())) {
        // Skip the empty flink manifest.
        continue;
      }

      DeltaManifests deltaManifests =
          SimpleVersionedSerialization.readVersionAndDeSerialize(
              DeltaManifestsSerializer.INSTANCE, e.getValue());
      pendingResults.put(
          e.getKey(),
          FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs()));
      manifests.addAll(deltaManifests.manifests());
    }

    CommitSummary summary = new CommitSummary(pendingResults);
    commitPendingResult(pendingResults, summary, newFlinkJobId, operatorId, checkpointId, table,
        committerMetrics);
    committerMetrics.updateCommitSummary(summary);
    pendingMap.clear();
    deleteCommittedManifests(manifests, newFlinkJobId, checkpointId, table);
    // sync latest update time to hive
    String key = AppOptions.CDC_LATEST_TIME_PROPERTY.defaultValue();

    String oldValue = table.properties().get(key);
    if (StringUtils.isNotBlank(latestUpdateTime) &&
        !StringUtils.equals(latestUpdateTime, oldValue)) {
      LOG.info("Sync table property to hive. key[{}],value=[{}]", key, latestUpdateTime);
      table.updateProperties()
          .set(key, latestUpdateTime)
          .commit();
    }
  }

  private void commitPendingResult(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId,
      Table table, IcebergFilesCommitterMetrics committerMetrics) {
    long totalFiles = summary.dataFilesCount() + summary.deleteFilesCount();
    continuousEmptyCheckpoints = totalFiles == 0 ? continuousEmptyCheckpoints + 1 : 0;
    if (totalFiles != 0 || continuousEmptyCheckpoints % maxContinuousEmptyCommits == 0) {
      if (replacePartitions) {
        replacePartitions(pendingResults, summary, newFlinkJobId, operatorId, checkpointId, table,
            committerMetrics);
      } else {
        commitDeltaTxn(pendingResults, summary, newFlinkJobId, operatorId, checkpointId, table,
            committerMetrics);
      }
      continuousEmptyCheckpoints = 0;
    } else {
      LOG.info("Skip commit for checkpoint {} due to no data files or delete files.", checkpointId);
    }
  }

  private void deleteCommittedManifests(
      List<ManifestFile> manifests, String newFlinkJobId, long checkpointId, Table table) {
    for (ManifestFile manifest : manifests) {
      try {
        table.io().deleteFile(manifest.path());
      } catch (Exception e) {
        // The flink manifests cleaning failure shouldn't abort the completed checkpoint.
        String details =
            MoreObjects.toStringHelper(this)
                .add("flinkJobId", newFlinkJobId)
                .add("checkpointId", checkpointId)
                .add("manifestPath", manifest.path())
                .toString();
        LOG.warn(
            "The iceberg transaction has been committed, but we failed to clean the temporary flink manifests: {}",
            details,
            e);
      }
    }
  }

  private void replacePartitions(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId,
      Table table, IcebergFilesCommitterMetrics committerMetrics) {
    Preconditions.checkState(
        summary.deleteFilesCount() == 0, "Cannot overwrite partitions with delete files.");
    // Commit the overwrite transaction.
    ReplacePartitions dynamicOverwrite = table.newReplacePartitions().scanManifestsWith(workerPool);
    for (WriteResult result : pendingResults.values()) {
      Preconditions.checkState(
          result.referencedDataFiles().length == 0, "Should have no referenced data files.");
      Arrays.stream(result.dataFiles()).forEach(dynamicOverwrite::addFile);
    }

    commitOperation(
        dynamicOverwrite,
        summary,
        "dynamic partition overwrite",
        newFlinkJobId,
        operatorId,
        checkpointId, table, committerMetrics);
  }

  private void commitDeltaTxn(
      NavigableMap<Long, WriteResult> pendingResults,
      CommitSummary summary,
      String newFlinkJobId,
      String operatorId,
      long checkpointId,
      Table table,
      IcebergFilesCommitterMetrics committerMetrics) {
    if (summary.deleteFilesCount() == 0) {
      // To be compatible with iceberg format V1.
      AppendFiles appendFiles = table.newAppend().scanManifestsWith(workerPool);
      for (WriteResult result : pendingResults.values()) {
        Preconditions.checkState(
            result.referencedDataFiles().length == 0,
            "Should have no referenced data files for append.");
        Arrays.stream(result.dataFiles()).forEach(appendFiles::appendFile);
      }
      commitOperation(appendFiles, summary, "append", newFlinkJobId, operatorId, checkpointId,
          table, committerMetrics);
    } else {
      // To be compatible with iceberg format V2.
      for (Map.Entry<Long, WriteResult> e : pendingResults.entrySet()) {
        // We don't commit the merged result into a single transaction because for the sequential
        // transaction txn1 and txn2, the equality-delete files of txn2 are required to be applied
        // to data files from txn1. Committing the merged one will lead to the incorrect delete
        // semantic.
        WriteResult result = e.getValue();

        // Row delta validations are not needed for streaming changes that write equality deletes.
        // Equality deletes are applied to data in all previous sequence numbers, so retries may
        // push deletes further in the future, but do not affect correctness. Position deletes
        // committed to the table in this path are used only to delete rows from data files that are
        // being added in this commit. There is no way for data files added along with the delete
        // files to be concurrently removed, so there is no need to validate the files referenced by
        // the position delete files that are being committed.
        RowDelta rowDelta = table.newRowDelta().scanManifestsWith(workerPool);

        Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
        Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
        commitOperation(rowDelta, summary, "rowDelta", newFlinkJobId, operatorId, e.getKey(), table,
            committerMetrics);
      }
    }
  }

  private void commitOperation(
      SnapshotUpdate<?> operation,
      CommitSummary summary,
      String description,
      String newFlinkJobId,
      String operatorId,
      long checkpointId, Table table, IcebergFilesCommitterMetrics committerMetrics) {
    LOG.info(
        "Committing {} for checkpoint {} to table {} branch {} with summary: {}",
        description,
        checkpointId,
        table.name(),
        branch,
        summary);
    snapshotProperties.forEach(operation::set);
    // custom snapshot metadata properties will be overridden if they conflict with internal ones
    // used by the sink.
    operation.set(MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(FLINK_JOB_ID, newFlinkJobId);
    operation.set(OPERATOR_ID, operatorId);
    operation.toBranch(branch);

    long startNano = System.nanoTime();
    operation.commit(); // abort is automatically called if this fails.
    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNano);
    LOG.info(
        "Committed {} to table: {}, branch: {}, checkpointId {} in {} ms",
        description,
        table.name(),
        branch,
        checkpointId,
        durationMs);
    committerMetrics.commitDuration(durationMs);
  }

  @Override
  public void processElement(StreamRecord<StpWriteResult> element) {
    Map<TableIdentifier, StpWriteResult.WriteInfo> holder = element.getValue().getHolder();
    Set<TableIdentifier> writedTable = holder.keySet();
    Set<TableIdentifier> currentTables = tableHolder.keySet();
    Collection<TableIdentifier> diffTables = CollUtil.subtract(writedTable, currentTables);
    for (TableIdentifier diffTable : diffTables) {
      tableHolder.computeIfAbsent(diffTable,
          tableIdentifier -> new SingleTableInfo(tableIdentifier, pt, super.metrics, flinkJobId,
              operatorUniqueId, subTaskId, attemptId));
    }

    holder.forEach((identifier, writeInfo) -> {
      String updateTime = writeInfo.getUpdateTime();
      SingleTableInfo singleTableInfo = tableHolder.get(identifier);
      String latestUpdateTime = singleTableInfo.getLatestUpdateTime();
      if (StringUtils.isNotBlank(updateTime)) {
        if (latestUpdateTime == null) {
          singleTableInfo.setLatestUpdateTime(updateTime);
        } else {
          // push time field timeline
          singleTableInfo.setLatestUpdateTime(
              updateTime.compareTo(latestUpdateTime) > 0 ? updateTime : latestUpdateTime);
        }
      }
    });


    this.writeResultsOfCurrentCkpt.add(element.getValue());
  }

  @Override
  public void endInput() throws IOException {
    // Flush the buffered data files into 'dataFilesPerCheckpoint' firstly.
    long currentCheckpointId = Long.MAX_VALUE;
    Map<TableIdentifier, List<WriteResult>> tableIdentifierWriteResultListMap =
        getTableIdentifierWriteResultListMap();
    for (Map.Entry<TableIdentifier, SingleTableInfo> entry : tableHolder.entrySet()) {
      TableIdentifier identifier = entry.getKey();
      SingleTableInfo singleTableInfo = entry.getValue();
      NavigableMap<Long, byte[]> dataFilesPerCheckpoint =
          dataFilesPerCheckpointMap.getOrDefault(identifier, Maps.newTreeMap());

      dataFilesPerCheckpoint.put(currentCheckpointId,
          writeToManifest(currentCheckpointId, singleTableInfo.getManifestOutputFileFactory(),
              tableIdentifierWriteResultListMap.get(identifier),
              singleTableInfo.getSpec()));
      dataFilesPerCheckpointMap.put(identifier, dataFilesPerCheckpoint);
      writeResultsOfCurrentCkpt.clear();

      commitUpToCheckpoint(dataFilesPerCheckpoint, flinkJobId, operatorUniqueId,
          currentCheckpointId, singleTableInfo.getTable(), singleTableInfo.getCommitterMetrics(),
          singleTableInfo.getLatestUpdateTime());
    }

  }

  /**
   * Write all the complete data files to a newly created manifest file and return the manifest's
   * avro serialized bytes.
   */
  private byte[] writeToManifest(long checkpointId,
                                 ManifestOutputFileFactory manifestOutputFileFactory,
                                 List<WriteResult> writeResultsOfCurrentCkpt,
                                 PartitionSpec spec
  ) throws IOException {
    if (writeResultsOfCurrentCkpt.isEmpty()) {
      return EMPTY_MANIFEST_DATA;
    }

    WriteResult result = WriteResult.builder().addAll(writeResultsOfCurrentCkpt).build();
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            result, () -> manifestOutputFileFactory.create(checkpointId), spec);

    return SimpleVersionedSerialization.writeVersionAndSerialize(
        DeltaManifestsSerializer.INSTANCE, deltaManifests);
  }

  @Override
  public void open() throws Exception {
    super.open();

    final String operatorID = getRuntimeContext().getOperatorUniqueID();
    pt = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    this.workerPool =
        ThreadPools.newWorkerPool("iceberg-worker-pool-" + operatorID, workerPoolSize);
  }

  @Override
  public void close() throws Exception {
    for (Map.Entry<TableIdentifier, SingleTableInfo> entry : tableHolder.entrySet()) {
      SingleTableInfo singleTableInfo = entry.getValue();
      TableLoader tableLoader = singleTableInfo.getTableLoader();
      if (tableLoader != null) {
        tableLoader.close();
      }
    }

    if (workerPool != null) {
      workerPool.shutdown();
    }
  }

  @VisibleForTesting
  static ListStateDescriptor<Map<TableIdentifier, List<SortedMap<Long, byte[]>>>> buildStateDescriptor() {
    Comparator<Long> longComparator = Comparators.forType(Types.LongType.get());
    // Construct a SortedMapTypeInfo.
    TypeInformation<Map<TableIdentifier, List<SortedMap<Long, byte[]>>>> sortedMapTypeInfo =
        new MapTypeInfo<>(TypeInformation.of(new TypeHint<TableIdentifier>() {
        }), new ListTypeInfo<>(new SortedMapTypeInfo<>(
            BasicTypeInfo.LONG_TYPE_INFO,
            PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
            longComparator)));
    return new ListStateDescriptor<>("iceberg-files-committer-state", sortedMapTypeInfo);
  }

  static long getMaxCommittedCheckpointId(
      Table table, String flinkJobId, String operatorId, String branch) {
    Snapshot snapshot = table.snapshot(branch);
    long lastCommittedCheckpointId = INITIAL_CHECKPOINT_ID;

    while (snapshot != null) {
      Map<String, String> summary = snapshot.summary();
      String snapshotFlinkJobId = summary.get(FLINK_JOB_ID);
      String snapshotOperatorId = summary.get(OPERATOR_ID);
      if (flinkJobId.equals(snapshotFlinkJobId)
          && (snapshotOperatorId == null || snapshotOperatorId.equals(operatorId))) {
        String value = summary.get(MAX_COMMITTED_CHECKPOINT_ID);
        if (value != null) {
          lastCommittedCheckpointId = Long.parseLong(value);
          break;
        }
      }
      Long parentSnapshotId = snapshot.parentId();
      snapshot = parentSnapshotId != null ? table.snapshot(parentSnapshotId) : null;
    }

    return lastCommittedCheckpointId;
  }
}
