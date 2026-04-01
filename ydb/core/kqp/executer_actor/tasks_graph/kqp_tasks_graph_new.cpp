#include "kqp_tasks_graph_new.h"

#include "kqp_partition_helper.h"

#include <ydb/core/kqp/common/control.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>

namespace NKikimr::NKqp {

using namespace NYql::NDq;

TKqpTasksGraphNew::TKqpTasksGraphNew(
    const TString& database,
        const TVector<IKqpGateway::TPhysicalTxData>& transactions,
        const NKikimr::NKqp::TTxAllocatorState::TPtr& txAlloc,
        const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregationSettings,
        const NKikimrConfig::TTableServiceConfig::TResourceManager&,
        const TKqpRequestCounters::TPtr& counters,
        TActorId bufferActorId,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken
)
    : TKqpTasksGraph(database, transactions, txAlloc, aggregationSettings, counters, bufferActorId, userToken)
    , MaxTasksGraph(5000)
{
}

size_t TKqpTasksGraphNew::DoBuildAllTasks(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, TQueryExecutionStats* stats) {
    // Counting tasks via MaxTasksGraph

    if (!resourcesSnapshot.empty()) {
        MaxTasksGraph.AddNodes(resourcesSnapshot);
    }

    // TODO: remove this part later. The nodes from snapshot should be sufficient.
    for (const auto [_, node] : GetMeta().ShardIdToNodeId) {
        MaxTasksGraph.AddNode(node);
    }

    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        auto scheduledTaskCount = ScheduleByCost(tx, resourcesSnapshot);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            // TODO: move this check to FillStages() - after all necessary params are set in KqpTasksGraph ctor.

            // Check which type of tasks to build later
            const bool maybeOlapRead = (GetMeta().AllowOlapDataQuery || GetMeta().StreamResult) && stageInfo.Meta.IsOlap();

            const bool buildFromSourceTasks = stage.SourcesSize() > 0;
            const bool buildSysViewTasks = stageInfo.Meta.IsSysView();
            const bool buildComputeTasks = stageInfo.Meta.ShardOperations.empty() || (!GetMeta().IsScan && stage.SinksSize() + stage.OutputTransformsSize() > 0 && !(maybeOlapRead && stageInfo.Meta.HasReads()));
            const bool buildScanTasks = GetMeta().IsScan
                ? stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()
                : maybeOlapRead && (stage.SinksSize() + stage.OutputTransformsSize() == 0 || stageInfo.Meta.HasReads())
                ;

            if (buildFromSourceTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::SOURCE_TASKS;
            } else if (buildSysViewTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::SYSVIEW_TASKS;
            } else if (buildComputeTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::COMPUTE_TASKS;
            } else if (buildScanTasks) {
                stageInfo.Meta.TasksType = TStageInfoMeta::SCAN_TASKS;
            }

            switch(stageInfo.Meta.TasksType) {
                case TStageInfoMeta::SOURCE_TASKS: {
                    switch (stage.GetSources(0).GetTypeCase()) {
                        case NKqpProto::TKqpSource::kReadRangesSource: {
                            bool limitTasksPerNode = IsEnabledReadsMerge();
                            if (!GetMeta().IsScan) {
                                limitTasksPerNode |= GetMeta().StreamResult;
                            }
                            CountScanTasksFromSource(stageInfo, limitTasksPerNode);
                        } break;
                        case NKqpProto::TKqpSource::kFullTextSource: {
                            CountFullTextScanTasksFromSource(stageInfo);
                        } break;
                        case NKqpProto::TKqpSource::kSysViewSource: {
                            CountSysViewTasksFromSource(stageInfo);
                        } break;
                        case NKqpProto::TKqpSource::kExternalSource: {
                            YQL_ENSURE(!GetMeta().IsScan);
                            auto it = scheduledTaskCount.find(stageIdx);
                            CountReadTasksFromSource(stageInfo, resourcesSnapshot.size(), it != scheduledTaskCount.end() ? it->second.TaskCount : 0);
                        } break;
                        default:
                            YQL_ENSURE(false, "unknown source type");
                    }
                } break;
                case TStageInfoMeta::SYSVIEW_TASKS: {
                    CountSysViewScanTasks(stageInfo); // TODO: remove after switching to SysView source
                } break;
                case TStageInfoMeta::COMPUTE_TASKS: {
                    // TODO: is it possible for "shards on node" to be greater than "resources snapshot"?
                    auto nodesCount = GetMeta().ShardsOnNode.size();
                    if (!GetMeta().IsScan) {
                        nodesCount = std::max<ui32>(resourcesSnapshot.size(), nodesCount);
                    }
                    CountComputeTasks(stageInfo, nodesCount);
                } break;
                case TStageInfoMeta::SCAN_TASKS: {
                    CountScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination());
                } break;
                case TStageInfoMeta::UNKNOWN_TASKS: {
                    AFL_ENSURE(false);
                } break;
            }
        }
    }

    MaxTasksGraph.Shrink();

    MaxTasksGraph.Print();

    size_t sourceScanPartitionsCount = 0;

    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        auto scheduledTaskCount = ScheduleByCost(tx, resourcesSnapshot);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            switch(stageInfo.Meta.TasksType) {
                case TStageInfoMeta::SOURCE_TASKS: {
                    switch (stage.GetSources(0).GetTypeCase()) {
                        case NKqpProto::TKqpSource::kReadRangesSource: {
                            if (auto partitionsCount = BuildScanTasksFromSource(stageInfo, stats)) {
                                sourceScanPartitionsCount += *partitionsCount;
                            } else {
                                GetMeta().UnknownAffectedShardCount = true;
                            }
                        } break;
                        case NKqpProto::TKqpSource::kFullTextSource: {
                            BuildFullTextScanTasksFromSource(stageInfo, stats);
                            GetMeta().UnknownAffectedShardCount = true;
                        } break;
                        case NKqpProto::TKqpSource::kSysViewSource: {
                            BuildSysViewTasksFromSource(stageInfo);
                            GetMeta().UnknownAffectedShardCount = true;
                        } break;
                        case NKqpProto::TKqpSource::kExternalSource: {
                            YQL_ENSURE(!GetMeta().IsScan);
                            auto it = scheduledTaskCount.find(stageIdx);
                            BuildReadTasksFromSource(stageInfo, resourcesSnapshot, it != scheduledTaskCount.end() ? it->second.TaskCount : 0);
                        } break;
                        default:
                            YQL_ENSURE(false, "unknown source type");
                    }
                } break;
                case TStageInfoMeta::SYSVIEW_TASKS: {
                    BuildSysViewScanTasks(stageInfo); // TODO: remove after switching to SysView source
                } break;
                case TStageInfoMeta::COMPUTE_TASKS: {
                    BuildComputeTasks(stageInfo);
                } break;
                case TStageInfoMeta::SCAN_TASKS: {
                    BuildScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination(), CollectProfileStats(GetMeta().StatsMode) ? stats : nullptr);
                } break;
                case TStageInfoMeta::UNKNOWN_TASKS: {
                    AFL_ENSURE(false);
                } break;
            }
        }
    }

    return sourceScanPartitionsCount;
}

void TKqpTasksGraphNew::CountScanTasksFromSource(const TStageInfo& stageInfo, bool limitTasksPerNode) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
    const auto& source = stage.GetSources(0).GetReadRangesSource();
    bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards();
    bool singlePartitionedStage = stage.GetIsSinglePartition();

    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.push_back(NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex()));
    }
    MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::ANY, inputs);

    if (partitions.empty()) {
        return;
    }

    if (isSequentialInFlight || singlePartitionedStage) {
        ui64 nodeId;
        if (singlePartitionedStage) {
            nodeId = GetMeta().ExecuterId.NodeId();
        } else {
            Y_ENSURE(stageInfo.Meta.VirtualPartition);
            nodeId = GetMeta().ShardIdToNodeId.at(stageInfo.Meta.VirtualPartition->ShardId);
        }

        MaxTasksGraph.AddTasks(stageId, nodeId, 1);
    } else {
        THashMap<ui64, ui64> tasksPerNode;
        THashMap<ui64, ui64> maxTasksPerNode;

        for (const auto& [shardId, shardInfo] : partitions) {
            ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
            auto& nodeTasks = tasksPerNode[nodeId];

            if (limitTasksPerNode) {
                auto maxTasks = maxTasksPerNode.find(nodeId);
                if (maxTasks == maxTasksPerNode.end()) {
                    maxTasks = maxTasksPerNode.emplace(nodeId, GetScanTasksPerNode(stageInfo, /* isOlapScan */ false, nodeId).first).first;
                }

                if (nodeTasks < maxTasks->second) {
                    ++nodeTasks;
                }
            } else {
                ++nodeTasks;
            }
        }

        for (const auto [nodeId, tasks] : tasksPerNode) {
            MaxTasksGraph.AddTasks(stageId, nodeId, tasks);
        }
    }
}

void TKqpTasksGraphNew::CountFullTextScanTasksFromSource(const TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.push_back(NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex()));
    }
    MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::ANY, inputs);

    ui64 nodeId = GetMeta().ExecuterId.NodeId();
    MaxTasksGraph.AddTasks(stageId, nodeId, 1);
}

void TKqpTasksGraphNew::CountSysViewTasksFromSource(const TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.push_back(NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex()));
    }
    MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::ANY, inputs);

    ui64 nodeId = GetMeta().ExecuterId.NodeId();
    MaxTasksGraph.AddTasks(stageId, nodeId, 1);
}

void TKqpTasksGraphNew::CountReadTasksFromSource(const TStageInfo& stageInfo, size_t resourceSnapshotSize, ui32 scheduledTaskCount) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    const auto& externalSource = stage.GetSources(0).GetExternalSource();

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.push_back(NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex()));
    }
    MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::ANY, inputs);

    ui32 taskCount = externalSource.GetPartitionedTaskParams().size();
    if (scheduledTaskCount) {
        taskCount = std::min<ui32>(taskCount, scheduledTaskCount);
    } else if (resourceSnapshotSize) {
        taskCount = std::min<ui32>(taskCount, resourceSnapshotSize * 2);
    }

    MaxTasksGraph.AddTasks(stageId, taskCount);
}

void TKqpTasksGraphNew::CountSysViewScanTasks(const TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.push_back(NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex()));
    }
    MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::FIXED, inputs);

    MaxTasksGraph.AddTasks(stageId, stage.GetTableOps().size());
}

void TKqpTasksGraphNew::CountComputeTasks(const TStageInfo& stageInfo, const ui32 nodesCount) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    ui32 partitionsCount = 1;
    ui32 inputTasks = 0;
    bool isShuffle = false;
    bool forceMapTasks = false;
    ui32 mapConnectionCount = 0;

    std::list<TStageId> inputs;
    std::optional<TStageId> copyInput;
    TMaxTasksGraph::EStageType stageType = TMaxTasksGraph::ANY;
    for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
        const auto& input = stage.GetInputs(inputIndex);

        // Current assumptions:
        // 1. All stage's inputs, except 1st one, must be a `Broadcast` or `UnionAll`
        // 2. Stages where 1st input is `Broadcast` are not partitioned.
        if (inputIndex > 0) {
            switch (input.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kBroadcast:
                case NKqpProto::TKqpPhyConnection::kHashShuffle:
                case NKqpProto::TKqpPhyConnection::kUnionAll:
                case NKqpProto::TKqpPhyConnection::kMerge:
                case NKqpProto::TKqpPhyConnection::kStreamLookup:
                case NKqpProto::TKqpPhyConnection::kMap:
                case NKqpProto::TKqpPhyConnection::kParallelUnionAll:
                case NKqpProto::TKqpPhyConnection::kVectorResolve:
                case NKqpProto::TKqpPhyConnection::kDqSourceStreamLookup:
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected connection type: " << (ui32)input.GetTypeCase() << Endl);
                    // TODO: << this->DebugString());
            }
        }

        const auto& inputStageId = NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex());
        inputs.push_back(inputStageId);

        switch (input.GetTypeCase()) {
            case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                inputTasks += MaxTasksGraph.GetStageTasksCount(inputStageId);
                isShuffle = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                stageType = TMaxTasksGraph::COPY;
                copyInput = inputStageId;
                partitionsCount = MaxTasksGraph.GetStageTasksCount(inputStageId); // TODO: what if `partitionsCount` is already set?
                break;
            }
            case NKqpProto::TKqpPhyConnection::kMap: {
                stageType = TMaxTasksGraph::COPY;
                copyInput = inputStageId;
                partitionsCount = MaxTasksGraph.GetStageTasksCount(inputStageId); // TODO: what if `partitionsCount` is already set?
                forceMapTasks = true;
                ++mapConnectionCount;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                partitionsCount = std::max<ui64>(partitionsCount, MaxTasksGraph.GetStageTasksCount(inputStageId));
                break;
            }
            case NKqpProto::TKqpPhyConnection::kVectorResolve: {
                stageType = TMaxTasksGraph::COPY;
                copyInput = inputStageId;
                partitionsCount = MaxTasksGraph.GetStageTasksCount(inputStageId); // TODO: what if `partitionsCount` is already set?
                break;
            }
            case NKqpProto::TKqpPhyConnection::kSequencer: {
                stageType = TMaxTasksGraph::COPY;
                copyInput = inputStageId;
                partitionsCount = MaxTasksGraph.GetStageTasksCount(inputStageId); // TODO: what if `partitionsCount` is already set?
                break;
            }
            default:
                break;
        }
    }

    Y_ENSURE(mapConnectionCount <= 1, "Only a single map connection is allowed");

    if (isShuffle && !forceMapTasks) {
        auto [newPartitionCount, _] = GetMaxTasksAggregation(stageInfo, inputTasks, nodesCount);
        if (newPartitionCount > partitionsCount) {
            partitionsCount = newPartitionCount;
        }
    }

    MaxTasksGraph.AddStage(stageId, stageType, inputs, copyInput);
    MaxTasksGraph.AddTasks(stageId, partitionsCount);
}

void TKqpTasksGraphNew::CountScanTasksFromShards(const TStageInfo& stageInfo, bool enableShuffleElimination) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);
    bool shuffleEliminated = enableShuffleElimination && stage.GetIsShuffleEliminated();

    // TODO: can it have any inputs at all?
    std::list<TStageId> inputs;
    for (const auto& input : stage.GetInputs()) {
        inputs.push_back(NYql::NDq::TStageId(stageId.TxId, input.GetStageIndex()));
    }

    THashMap<ui64 /* nodeId */, ui64> nodeShards;

    for (int i = 0, s = stage.TableOpsSize(); i < s; ++i) {
        const auto& op = stage.GetTableOps(i);
        const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
        const bool isSorted = ExtractReadSettings(op, stageInfo, TxAlloc->HolderFactory, TxAlloc->TypeEnv).IsSorted();

        const auto& partitions = stageInfo.Meta.PrunedPartitions.at(i);
        for (const auto& [shardId, _]: partitions) {
            ++nodeShards[GetMeta().ShardIdToNodeId.at(shardId)];
        }

        if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() && !shuffleEliminated || (!isOlapScan && isSorted)) {
            if (stageInfo.Meta.IsOlap() && isSorted) {
                MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::FIXED, inputs);
                for (const auto& [nodeId, shards] : nodeShards) {
                    MaxTasksGraph.AddTasks(stageId, nodeId, shards);
                }
            } else {
                MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::ANY, inputs);
                for (const auto& [nodeId, shards] : nodeShards) {
                    const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId).first;
                    MaxTasksGraph.AddTasks(stageId, nodeId, std::min<ui32>(shards, maxTasksPerNode));
                }
            }
        } else if (shuffleEliminated) {
            MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::FIXED, inputs);

            for (const auto& [nodeId, shards] : nodeShards) {
                const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId, true).first;
                MaxTasksGraph.AddTasks(stageId, nodeId, std::min<ui32>(shards, maxTasksPerNode));
            }
        } else {
            MaxTasksGraph.AddStage(stageId, TMaxTasksGraph::ANY, inputs);

            // It's ok to create tasks only on nodes with shards - to prevent cross-network traffic.
            for (const auto& [nodeId, _] : nodeShards) {
                const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId).first;
                MaxTasksGraph.AddTasks(stageId, nodeId, maxTasksPerNode);
            }
        }
    }
}

TMaybe<size_t> TKqpTasksGraphNew::BuildScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* stats) {
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    const bool singlePartitionedStage = stage.GetIsSinglePartition();

    // TODO: describe in comments the exact expected stage state.
    YQL_ENSURE(stage.GetSources(0).HasReadRangesSource());
    YQL_ENSURE(stage.GetSources(0).GetInputIndex() == 0 && stage.SourcesSize() == 1);
    for (const auto& input : stage.GetInputs()) {
        YQL_ENSURE(input.HasBroadcast());
    }

    const auto& source = stage.GetSources(0).GetReadRangesSource();
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    // TODO: what is the difference with `stageInfo.Meta.IsOlap()`?
    YQL_ENSURE(tableInfo->TableKind != NKikimr::NKqp::ETableKind::Olap);

    auto columns = BuildKqpColumns(source, tableInfo);
    const auto& snapshot = GetMeta().Snapshot;
    const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);
    const bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0
        && partitions.size() > source.GetSequentialInFlightShards();

    auto createNewTask = [&](ui64 nodeId, TMaybe<ui64> maxInFlightShards) -> TTask& {
        auto& task = AddTask(stageInfo, TTaskType::UNKNOWN);
        task.Meta.Type = TTaskMeta::TTaskType::Scan;
        task.Meta.NodeId = nodeId;

        const auto& stageSource = stage.GetSources(0);
        auto& input = task.Inputs.at(stageSource.GetInputIndex());
        input.SourceType = NYql::KqpReadRangesSourceName;
        input.ConnectionInfo = NYql::NDq::TSourceInput{};

        // allocating source settings

        input.Meta.SourceSettings = GetMeta().Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
        NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;
        settings->SetDatabase(GetMeta().Database);

        auto* meta = settings->MutableTable();
        meta->SetTablePath(stageInfo.Meta.TablePath);
        meta->MutableTableId()->SetTableId(stageInfo.Meta.TableId.PathId.LocalPathId);
        meta->MutableTableId()->SetOwnerId(stageInfo.Meta.TableId.PathId.OwnerId);
        meta->SetSchemaVersion(stageInfo.Meta.TableId.SchemaVersion);
        meta->SetSysViewInfo(stageInfo.Meta.TableId.SysViewInfo);
        meta->SetTableKind((ui32)stageInfo.Meta.TableKind);

        settings->SetIsTableImmutable(source.GetIsTableImmutable());
        settings->SetIsolationLevel(GetMeta().RequestIsolationLevel);

        for (auto& keyColumn : keyTypes) {
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(keyColumn, "");
            *settings->AddKeyColumnTypeInfos() = columnType.TypeInfo ?
                *columnType.TypeInfo :
                NKikimrProto::TTypeInfo();
            settings->AddKeyColumnTypes(static_cast<ui32>(keyColumn.GetTypeId()));
        }

        for (auto& column : columns) {
            auto* protoColumn = settings->AddColumns();
            protoColumn->SetId(column.Id);
            auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
            protoColumn->SetType(columnType.TypeId);
            protoColumn->SetNotNull(column.NotNull);
            protoColumn->SetIsPrimary(column.IsPrimary);
            if (columnType.TypeInfo) {
                *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
            }
            protoColumn->SetName(column.Name);
        }

        if (GetMeta().CheckDuplicateRows) {
            for (auto& colName : tableInfo->KeyColumns) {
                const auto& tableColumn = tableInfo->Columns.at(colName);
                auto* protoColumn = settings->AddDuplicateCheckColumns();
                protoColumn->SetId(tableColumn.Id);
                auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(tableColumn.Type, tableColumn.TypeMod);
                protoColumn->SetType(columnType.TypeId);
                if (columnType.TypeInfo) {
                    *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
                }
                protoColumn->SetName(colName);
            }
        }

        if (AppData()->FeatureFlags.GetEnableArrowFormatAtDatashard()) {
            settings->SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
        } else {
            settings->SetDataFormat(NKikimrDataEvents::FORMAT_CELLVEC);
        }

        if (snapshot.IsValid()) {
            settings->MutableSnapshot()->SetStep(snapshot.Step);
            settings->MutableSnapshot()->SetTxId(snapshot.TxId);
        }

        if (GetMeta().RequestIsolationLevel == NKqpProto::ISOLATION_LEVEL_READ_UNCOMMITTED) {
            settings->SetAllowInconsistentReads(true);
        }

        settings->SetReverse(source.GetReverse());
        settings->SetSorted(source.GetSorted());

        if (maxInFlightShards) {
            settings->SetMaxInFlightShards(*maxInFlightShards);
        }

        if (GetMeta().MaxBatchSize) {
            settings->SetItemsLimit(*GetMeta().MaxBatchSize);
            settings->SetIsBatch(true);
        } else {
            ui64 itemsLimit = ExtractPhyValue(stageInfo, source.GetItemsLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod((ui32)0)).Get<ui64>();
            settings->SetItemsLimit(itemsLimit);
        }

        if (source.HasVectorTopK()) {
            const auto& in = source.GetVectorTopK();
            auto& out = *settings->MutableVectorTopK();
            out.SetColumn(in.GetColumn());
            *out.MutableSettings() = in.GetSettings();
            auto target = ExtractPhyValue(stageInfo, in.GetTargetVector(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
            out.SetTargetVector(TString(target.AsStringRef()));
            out.SetLimit((ui32)ExtractPhyValue(stageInfo, in.GetLimit(), TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod()).Get<ui64>());
        }

        FillScanTaskLockTxId(*settings);

        if (GetMeta().LockMode) {
            settings->SetLockMode(*GetMeta().LockMode);
        }

        return task;
    };

    if (stats) {
        for (const auto& [shardId, _] : partitions) {
            stats->AffectedShards.insert(shardId);
        }
    }

    if (!partitions.empty() && (isSequentialInFlight || singlePartitionedStage)) {
        Y_ENSURE(stageInfo.Meta.VirtualPartition);

        auto startShard = stageInfo.Meta.VirtualPartition->ShardId;
        auto& shardInfo = *stageInfo.Meta.VirtualPartition;

        TMaybe<ui64> inFlightShards = Nothing();
        if (isSequentialInFlight) {
            inFlightShards = source.GetSequentialInFlightShards();
        }

        Y_ENSURE(shardInfo.KeyReadRanges); // TODO: redundant check - remove later.

        const ui64 nodeId = singlePartitionedStage ? GetMeta().ExecuterId.NodeId() : GetMeta().ShardIdToNodeId.at(startShard);

        YQL_ENSURE(!shardInfo.KeyWriteRanges);

        auto& task = createNewTask(nodeId, inFlightShards);
        auto& input = task.Inputs[stage.GetSources(0).GetInputIndex()];
        const bool hasRanges = shardInfo.KeyReadRanges->HasRanges();

        shardInfo.KeyReadRanges->SerializeTo(input.Meta.SourceSettings, !hasRanges);

        return singlePartitionedStage ? TMaybe<size_t>(partitions.size()) : Nothing();
    }

    struct TShardRangesWithShardId {
        TMaybe<ui64> ShardId;
        const TShardKeyRanges* Ranges;
    };
    using TShardRangesVector = TVector<TShardRangesWithShardId>;

    THashMap<ui64, TShardRangesVector> nodeIdToShardKeyRanges;
    for (const auto& [shardId, shardInfo] : partitions) {
        YQL_ENSURE(!shardInfo.KeyWriteRanges);

        const ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
        nodeIdToShardKeyRanges[nodeId].push_back(TShardRangesWithShardId{shardId, &*shardInfo.KeyReadRanges});
    }

    auto DistributeShardsToTasks = [&](TShardRangesVector& shardsRanges, const size_t tasksCount, const TVector<NScheme::TTypeInfo>& keyTypes) {
        // TODO:
        // if (IsDebugLogEnabled()) {
        //     TStringBuilder sb;
        //     sb << "Distributing shards to tasks: [";
        //     for(size_t i = 0; i < shardsRanges.size(); i++) {
        //         sb << "# " << i << ": " << shardsRanges[i].Ranges->ToString(keyTypes, *AppData()->TypeRegistry);
        //     }

        //     sb << " ].";
        //     LOG_D(sb);
        // }

        std::sort(std::begin(shardsRanges), std::end(shardsRanges), [&](const TShardRangesWithShardId& lhs, const TShardRangesWithShardId& rhs) {
                return CompareBorders<true, true>(
                    lhs.Ranges->GetRightBorder().first->GetCells(),
                    rhs.Ranges->GetRightBorder().first->GetCells(),
                    lhs.Ranges->GetRightBorder().second,
                    rhs.Ranges->GetRightBorder().second,
                    keyTypes) < 0;
            });

        // One shard (ranges set) can be assigned only to one task. Otherwise, we can break some optimizations like removing unnecessary shuffle.
        TVector<TShardRangesVector> result(tasksCount);
        size_t shardIndex = 0;
        for (size_t taskIndex = 0; taskIndex < tasksCount; ++taskIndex) {
            const size_t tasksLeft = tasksCount - taskIndex;
            const size_t shardsLeft = shardsRanges.size() - shardIndex;
            const size_t shardsPerCurrentTask = (shardsLeft + tasksLeft - 1) / tasksLeft;

            for (size_t currentShardIndex = 0; currentShardIndex < shardsPerCurrentTask; ++currentShardIndex, ++shardIndex) {
                result[taskIndex].push_back(shardsRanges[shardIndex]);
            }
        }
        return result;
    };

    const auto& stageId = stageInfo.Id;
    for (auto& [nodeId, shardsRanges] : nodeIdToShardKeyRanges) {
        const ui32 tasksCount = MaxTasksGraph.GetStageTasksCount(stageId, nodeId);

        TVector<ui64> taskIds;
        taskIds.reserve(tasksCount);

        for (size_t i = 0; i < tasksCount; ++i) {
            const auto& task = createNewTask(nodeId, Nothing());
            taskIds.push_back(task.Id);
        }

        const auto rangesDistribution = DistributeShardsToTasks(shardsRanges, tasksCount, keyTypes);
        YQL_ENSURE(rangesDistribution.size() == tasksCount);

        for (size_t taskIndex = 0; taskIndex < tasksCount; ++taskIndex) {
            auto& task = GetTask(taskIds[taskIndex]);
            auto& input = task.Inputs[stage.GetSources(0).GetInputIndex()];
            auto* settings = input.Meta.SourceSettings;

            const auto& shardsRangesForTask = rangesDistribution[taskIndex];

            if (shardsRangesForTask.size() == 1 && shardsRangesForTask[0].ShardId) {
                settings->SetShardIdHint(*shardsRangesForTask[0].ShardId);
            }

            bool hasRanges = false;
            for (const auto& shardRanges : shardsRangesForTask) {
                hasRanges |= shardRanges.Ranges->HasRanges();
            }

            for (const auto& shardRanges : shardsRangesForTask) {
                shardRanges.Ranges->SerializeTo(settings, !hasRanges);
            }
        }
    }

    return partitions.size();
}

void TKqpTasksGraphNew::BuildFullTextScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats*) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    YQL_ENSURE(stage.GetSources(0).HasFullTextSource());

    const auto& source = stage.GetSources(0);
    const auto& fullTextSource = source.GetFullTextSource();

    YQL_ENSURE(fullTextSource.GetIndex());

    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SOURCE_READ);
    task.Meta.Type = TTaskMeta::TTaskType::Scan;
    task.Meta.NodeId = GetMeta().ExecuterId.NodeId();
    const auto& stageSource = stage.GetSources(0);
    auto& input = task.Inputs.at(stageSource.GetInputIndex());
    input.SourceType = NYql::KqpFullTextSourceName;
    input.ConnectionInfo = NYql::NDq::TSourceInput{};

    input.Meta.FullTextSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpFullTextSourceSettings>();
    NKikimrKqp::TKqpFullTextSourceSettings* settings = input.Meta.FullTextSourceSettings;

    settings->SetIndex(fullTextSource.GetIndex());
    settings->SetDatabase(GetMeta().Database);
    settings->MutableTable()->CopyFrom(fullTextSource.GetTable());
    settings->SetIndexType(fullTextSource.GetIndexType());
    settings->MutableIndexDescription()->CopyFrom(fullTextSource.GetIndexDescription());

    auto guard = TxAlloc->TypeEnv.BindAllocator();
    {
        TStringBuilder queryBuilder;
        for (const auto& query : fullTextSource.GetQuerySettings().GetQueryValue()) {
            auto value = ExtractPhyValue(
                stageInfo, query,
                TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
            queryBuilder << TString(value.AsStringRef());
        }

        settings->MutableQuerySettings()->SetQuery(TString(queryBuilder));
    }

    if (fullTextSource.HasTakeLimit()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetTakeLimit(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetLimit(value.Get<ui64>());
        }
    }

    if (fullTextSource.HasBFactor()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetBFactor(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());

        if (value.HasValue()) {
            settings->SetBFactor(value.Get<double>());
        }
    }

    if (fullTextSource.HasDefaultOperator()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetDefaultOperator(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetDefaultOperator(TString(value.AsStringRef()));
        }
    }

    if (fullTextSource.HasMinimumShouldMatch()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetMinimumShouldMatch(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetMinimumShouldMatch(TString(value.AsStringRef()));
        }
    }

    if (fullTextSource.HasK1Factor()) {
        auto value = ExtractPhyValue(
            stageInfo, fullTextSource.GetK1Factor(),
            TxAlloc->HolderFactory, TxAlloc->TypeEnv, NUdf::TUnboxedValuePod());
        if (value.HasValue()) {
            settings->SetK1Factor(value.Get<double>());
        }
    }

    settings->MutableQuerySettings()->MutableColumns()->CopyFrom(fullTextSource.GetQuerySettings().GetColumns());

    for(const auto& indexTable : fullTextSource.GetIndexTables()) {
        auto* indexTableProto = settings->AddIndexTables();
        indexTableProto->MutableTable()->CopyFrom(indexTable.GetTable());
        indexTableProto->MutableKeyColumns()->CopyFrom(indexTable.GetKeyColumns());
        indexTableProto->MutableColumns()->CopyFrom(indexTable.GetColumns());
    }

    settings->MutableKeyColumns()->CopyFrom(fullTextSource.GetKeyColumns());
    settings->MutableColumns()->CopyFrom(fullTextSource.GetColumns());
    if (GetMeta().Snapshot.IsValid()) {
        settings->MutableSnapshot()->SetStep(GetMeta().Snapshot.Step);
        settings->MutableSnapshot()->SetTxId(GetMeta().Snapshot.TxId);
    }
}

void TKqpTasksGraphNew::BuildSysViewTasksFromSource(TStageInfo& stageInfo) {
    YQL_ENSURE(stageInfo.Meta.GetStage(stageInfo.Id).GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kSysViewSource);

    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    const auto& source = stage.GetSources(0);
    const auto& sysViewSource = source.GetSysViewSource();

    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SOURCE_READ);
    task.Meta.Type = TTaskMeta::TTaskType::Compute;
    task.Meta.NodeId = GetMeta().ExecuterId.NodeId();

    const auto& stageSource = stage.GetSources(0);
    auto& input = task.Inputs.at(stageSource.GetInputIndex());
    input.SourceType = NYql::KqpSysViewSourceName;
    input.ConnectionInfo = NYql::NDq::TSourceInput{};

    input.Meta.SysViewSourceSettings = GetMeta().Allocate<NKikimrKqp::TKqpSysViewSourceSettings>();
    NKikimrKqp::TKqpSysViewSourceSettings* settings = input.Meta.SysViewSourceSettings;

    settings->SetDatabase(GetMeta().Database);
    settings->MutableTable()->CopyFrom(sysViewSource.GetTable());
    settings->SetTablePath(sysViewSource.GetTable().GetPath());
    settings->SetSysViewInfo(sysViewSource.GetTable().GetSysView());
    settings->SetReverse(sysViewSource.GetReverse());

    // Fill SysViewDescription from table metadata
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    if (tableInfo && tableInfo->SysViewInfo) {
        *settings->MutableSysViewDescription() = *tableInfo->SysViewInfo;
    }

    // Fill columns: convert TKqpPhyColumnId to TKqpColumnMetadataProto using table metadata
    for (const auto& phyCol : sysViewSource.GetColumns()) {
        auto* col = settings->AddColumns();
        col->SetId(phyCol.GetId());
        col->SetName(phyCol.GetName());
        if (tableInfo) {
            auto it = tableInfo->Columns.find(phyCol.GetName());
            if (it != tableInfo->Columns.end()) {
                col->SetTypeId(it->second.Type.GetTypeId());
                if (NScheme::NTypeIds::IsParametrizedType(it->second.Type.GetTypeId())) {
                    NScheme::ProtoFromTypeInfo(it->second.Type, {}, *col->MutableTypeInfo());
                }
            }
        }
    }

    // Fill key columns from table info
    if (tableInfo) {
        for (size_t i = 0; i < tableInfo->KeyColumns.size(); ++i) {
            auto* kc = settings->AddKeyColumns();
            kc->SetName(tableInfo->KeyColumns[i]);
            if (i < tableInfo->KeyColumnTypes.size()) {
                kc->SetTypeId(tableInfo->KeyColumnTypes[i].GetTypeId());
            }
        }
    }

    // Fill key ranges from the source proto
    const auto& holderFactory = TxAlloc->HolderFactory;
    const auto& typeEnv = TxAlloc->TypeEnv;
    const auto& keyTypes = tableInfo ? tableInfo->KeyColumnTypes : TVector<NScheme::TTypeInfo>();

    auto guard = TxAlloc->TypeEnv.BindAllocator();

    switch (sysViewSource.GetRangesExprCase()) {
        case NKqpProto::TKqpSysViewSource::kKeyRange: {
            auto range = MakeKeyRange(keyTypes, sysViewSource.GetKeyRange(), stageInfo, holderFactory, typeEnv);
            range.Serialize(*settings->AddKeyRanges());
            break;
        }
        case NKqpProto::TKqpSysViewSource::kRanges: {
            auto ranges = FillRangesFromParameter(keyTypes, sysViewSource.GetRanges(), stageInfo, typeEnv);
            for (auto& pointOrRange : ranges) {
                if (auto* range = std::get_if<TSerializedTableRange>(&pointOrRange)) {
                    range->Serialize(*settings->AddKeyRanges());
                } else {
                    // Convert point to an inclusive range [point, point]
                    auto& point = std::get<TSerializedCellVec>(pointOrRange);
                    TSerializedTableRange rangeFromPoint(point.GetCells(), true, point.GetCells(), true);
                    rangeFromPoint.Point = true;
                    rangeFromPoint.Serialize(*settings->AddKeyRanges());
                }
            }
            break;
        }
        default: {
            // Full scan — construct a range from [NULL..NULL] (beginning) to [] (end), both inclusive.
            // Scan actors expect From to have at least one NULL cell for full scans;
            // a default-constructed TSerializedTableRange has empty From cells which some
            // scan actors interpret as +inf.
            TVector<TCell> fromCells(keyTypes.size()); // NULL cells = "from the very beginning"
            TSerializedTableRange fullRange(fromCells, true, TConstArrayRef<TCell>(), true);
            fullRange.Serialize(*settings->AddKeyRanges());
            break;
        }
    }

    // Pass user token for access control in scan actors (e.g., Auth* sys views)
    if (UserToken) {
        settings->SetUserToken(UserToken->SerializeAsString());
    }
}

void TKqpTasksGraphNew::BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount) {
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    YQL_ENSURE(stage.GetSources(0).HasExternalSource());
    YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

    const auto& stageSource = stage.GetSources(0);
    const auto& externalSource = stageSource.GetExternalSource();

    // TODO: refactor to use MaxTasksGraph.

    auto tasksReason = TTaskType::DEFAULT_SOURCE_READ;
    ui32 taskCount = externalSource.GetPartitionedTaskParams().size();

    auto tasksReasonHint = TTaskType::FORCED;
    auto taskCountHint = stage.GetTaskCount();

    if (taskCountHint == 0) {
        tasksReasonHint = TTaskType::SCHEDULED_SOURCE_READ;
        taskCountHint = scheduledTaskCount;
    }

    if (taskCountHint) {
        if (taskCount > taskCountHint) {
            tasksReason = tasksReasonHint;
            taskCount = taskCountHint;
        }
    } else if (!resourceSnapshot.empty()) {
        ui32 maxTaskCount = resourceSnapshot.size() * 2;
        if (taskCount > maxTaskCount) {
            tasksReason = TTaskType::SNAPSHOT_SOURCE_READ;
            taskCount = maxTaskCount;
        }
    }

    auto sourceName = externalSource.GetSourceName();
    TString structuredToken;
    if (sourceName) {
        structuredToken = ReplaceStructuredTokenReferences(externalSource.GetAuthInfo());
    }

    // TODO: what is this mechanic for? Maybe use default assignment for `compute` tasks.
    ui64 nodeOffset = 0;
    for (size_t i = 0; i < resourceSnapshot.size(); ++i) {
        if (resourceSnapshot[i].GetNodeId() == GetMeta().ExecuterId.NodeId()) {
            nodeOffset = i;
            break;
        }
    }

    TVector<ui64> tasksIds;
    tasksIds.reserve(taskCount);

    // generate all tasks
    for (ui32 i = 0; i < taskCount; i++) {
        auto& task = AddTask(stageInfo, tasksReason);

        if (!externalSource.GetEmbedded()) {
            auto& input = task.Inputs[stageSource.GetInputIndex()];
            input.ConnectionInfo = NYql::NDq::TSourceInput{};
            input.SourceSettings = externalSource.GetSettings();
            input.SourceType = externalSource.GetType();
            if (externalSource.HasWatermarksSettings()) {
                const auto& watermarksSettings = externalSource.GetWatermarksSettings();
                input.WatermarksMode = NYql::NDqProto::EWatermarksMode::WATERMARKS_MODE_DEFAULT;
                if (watermarksSettings.HasIdleTimeoutUs()) {
                    input.WatermarksIdleTimeoutUs = watermarksSettings.GetIdleTimeoutUs();
                }
            }
        }

        FillReadTaskFromSource(task, sourceName, structuredToken, resourceSnapshot, nodeOffset++);

        TString queryPath = "default";
        if (GetMeta().UserRequestContext && GetMeta().UserRequestContext->StreamingQueryPath) {
            queryPath = GetMeta().UserRequestContext->StreamingQueryPath;
        }
        task.Meta.TaskParams.emplace("query_path", queryPath);

        tasksIds.push_back(task.Id);
    }

    // distribute read ranges between them
    ui32 currentTaskIndex = 0;
    for (const TString& partitionParam : externalSource.GetPartitionedTaskParams()) {
        GetTask(tasksIds[currentTaskIndex]).Meta.ReadRanges.push_back(partitionParam);
        if (++currentTaskIndex >= tasksIds.size()) {
            currentTaskIndex = 0;
        }
    }
}

void TKqpTasksGraphNew::BuildSysViewScanTasks(TStageInfo& stageInfo) {
    Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.IsSysView());

    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    const auto& holderFactory = TxAlloc->HolderFactory;
    const auto& typeEnv = TxAlloc->TypeEnv;
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    for (auto& op : stage.GetTableOps()) {
        Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

        auto& task = AddTask(stageInfo, TTaskType::SYSVIEW_COMPUTE);
        TShardKeyRanges keyRanges;

        switch (op.GetTypeCase()) {
            case NKqpProto::TKqpPhyTableOperation::kReadRange:
                stageInfo.Meta.SkipNullKeys.assign(
                    op.GetReadRange().GetSkipNullKeys().begin(),
                    op.GetReadRange().GetSkipNullKeys().end()
                );
                keyRanges.Add(MakeKeyRange(
                    keyTypes, op.GetReadRange().GetKeyRange(),
                    stageInfo, holderFactory, typeEnv)
                );
                break;
            case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                keyRanges.CopyFrom(FillReadRanges(keyTypes, op.GetReadRanges(), stageInfo, typeEnv));
                break;
            default:
                YQL_ENSURE(false, "Unexpected table scan operation: " << (ui32) op.GetTypeCase());
        }

        TTaskMeta::TShardReadInfo readInfo = {
            .Ranges = std::move(keyRanges),
            .Columns = BuildKqpColumns(op, tableInfo),
        };

        auto readSettings = ExtractReadSettings(op, stageInfo, holderFactory, typeEnv);
        task.Meta.Reads.ConstructInPlace();
        task.Meta.Reads->emplace_back(std::move(readInfo));
        task.Meta.ReadInfo.SetSorting(readSettings.GetSorting());
        task.Meta.Type = TTaskMeta::TTaskType::Compute;
    }
}

void TKqpTasksGraphNew::BuildComputeTasks(TStageInfo& stageInfo) {
    const auto& stageId = stageInfo.Id;
    const auto& stage = stageInfo.Meta.GetStage(stageId);

    for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
        const auto& input = stage.GetInputs(inputIndex);
        GetMeta().UnknownAffectedShardCount |= input.HasStreamLookup() || input.HasVectorResolve();
    }

    auto tasksCount = MaxTasksGraph.GetStageTasksCount(stageId);
    for (ui32 i = 0; i < tasksCount; ++i) {
        auto& task = AddTask(stageInfo, TTaskType::UNKNOWN);
        task.Meta.Type = TTaskMeta::TTaskType::Compute;
    }
}

void TKqpTasksGraphNew::BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination, TQueryExecutionStats* stats) {
    THashMap<ui64, std::vector<ui64>> nodeTasks;
    THashMap<ui64 /* nodeId */, std::vector<TShardInfoWithId>> nodeShards;
    THashMap<ui64, ui64> assignedShardsCount;
    auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    auto& columnShardHashV1Params = stageInfo.Meta.ColumnShardHashV1Params;
    bool shuffleEliminated = enableShuffleElimination && stage.GetIsShuffleEliminated();
    if (shuffleEliminated && stageInfo.Meta.ColumnTableInfoPtr) {
        const auto& tableDesc = stageInfo.Meta.ColumnTableInfoPtr->Description;
        columnShardHashV1Params.SourceShardCount = tableDesc.GetColumnShardCount();
        columnShardHashV1Params.SourceTableKeyColumnTypes = std::make_shared<TVector<NScheme::TTypeInfo>>();
        for (const auto& column: tableDesc.GetSharding().GetHashSharding().GetColumns()) {
            Y_ENSURE(stageInfo.Meta.TableConstInfo->Columns.contains(column), TStringBuilder{} << "Table doesn't have column: " << column);
            auto columnType = stageInfo.Meta.TableConstInfo->Columns.at(column).Type;
            columnShardHashV1Params.SourceTableKeyColumnTypes->push_back(columnType);
        }
    }

    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;
    for (int i = 0, s = stage.TableOpsSize(); i < s; ++i) {
        const auto& op = stage.GetTableOps(i);
        Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

        auto columns = BuildKqpColumns(op, tableInfo);
        auto& partitions = stageInfo.Meta.PrunedPartitions.at(i);
        const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
        auto readSettings = ExtractReadSettings(op, stageInfo, TxAlloc->HolderFactory, TxAlloc->TypeEnv);

        if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadRange) {
            stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(), op.GetReadRange().GetSkipNullKeys().end());
            YQL_ENSURE(!readSettings.IsReverse(), "Not supported for scan queries");
        }

        for (auto&& [shardId, shardInfo]: partitions) {
            const ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
            nodeShards[nodeId].emplace_back(TShardInfoWithId(shardId, std::move(shardInfo)));
        }

        if (stats) {
            for (const auto& [nodeId, shardsInfo] : nodeShards) {
                stats->AddNodeShardsCount(stageInfo.Id.StageId, nodeId, shardsInfo.size());
            }
        }

        if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() && !shuffleEliminated || (!isOlapScan && readSettings.IsSorted())) {
            THashMap<ui64 /* nodeId */, ui64 /* tasks count */> olapAndSortedTasksCount;

            auto AssignScanTaskToShard = [&](ui64 nodeId, const bool sorted) -> TTask& {
                if (stageInfo.Meta.IsOlap() && sorted) {
                    auto& task = AddTask(stageInfo, TTaskType::OLAP_SORT_SCAN);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    ++olapAndSortedTasksCount[nodeId];
                    return task;
                }

                auto& tasks = nodeTasks[nodeId];
                auto& tasksCount = assignedShardsCount[nodeId];
                const auto [maxScansPerNode, maxTasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);
                if (tasksCount < maxScansPerNode) {
                    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SHARD_SCAN);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    tasks.push_back(task.Id);
                    ++tasksCount;

                    if (tasksCount == maxScansPerNode) {
                        for (const auto& taskId : tasks) {
                            GetTask(taskId).Reason = maxTasksReason;
                        }
                    }

                    return task;
                } else {
                    ui64 taskIdx = tasksCount % maxScansPerNode;
                    ++tasksCount;
                    return GetTask(tasks[taskIdx]);
                }
            };

            for (auto& [nodeId, shardsInfo] : nodeShards) {
                for (auto& shardInfo : shardsInfo) {
                    auto& task = AssignScanTaskToShard(nodeId, readSettings.IsSorted());
                    MergeReadInfoToTaskMeta(task.Meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings, columns, op, /*isPersistentScan*/ true);
                }
            }

            for (const auto& [nodeId, tasksId] : nodeTasks) {
                for (const auto& taskIdx : tasksId) {
                    auto& task = GetTask(taskIdx);
                    task.Meta.SetEnableShardsSequentialScan(readSettings.IsSorted());
                    PrepareScanMetaForUsage(task.Meta, keyTypes);
                }
            }
        } else if (shuffleEliminated /* save partitioning for shuffle elimination */) {
            std::size_t stageInternalTaskId = 0;
            columnShardHashV1Params.TaskIndexByHash = std::make_shared<TVector<ui64>>();
            columnShardHashV1Params.TaskIndexByHash->resize(columnShardHashV1Params.SourceShardCount);

            for (auto&& [nodeId, shardsInfo] : nodeShards) {
                auto tasksReason = TTaskType::SHUFFLE_ELIMINATE_SCAN;
                std::size_t tasksPerNode = shardsInfo.size();
                auto [maxTasksPerNode, maxTasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId, true);

                if (maxTasksPerNode < tasksPerNode) {
                    tasksReason = maxTasksReason;
                    tasksPerNode = maxTasksPerNode;
                }

                std::vector<TTaskMeta> metas(tasksPerNode, TTaskMeta());
                {
                    for (std::size_t i = 0; i < shardsInfo.size(); ++i) {
                        auto&& shardInfo = shardsInfo[i];
                        MergeReadInfoToTaskMeta(
                            metas[i % tasksPerNode],
                            shardInfo.ShardId,
                            shardInfo.KeyReadRanges,
                            readSettings,
                            columns, op,
                            /*isPersistentScan*/ true
                        );
                    }

                    for (auto& meta: metas) {
                        PrepareScanMetaForUsage(meta, keyTypes);
                    }
                }

                // in runtime we calc hash, which will be in [0; shardcount]
                // so we merge to mappings : hash -> shardID and shardID -> channelID for runtime
                THashMap<ui64, ui64> hashByShardId;
                Y_ENSURE(stageInfo.Meta.ColumnTableInfoPtr != nullptr, "ColumnTableInfoPtr is nullptr, maybe information about shards haven't beed delivered yet.");
                const auto& tableDesc = stageInfo.Meta.ColumnTableInfoPtr->Description;
                const auto& sharding = tableDesc.GetSharding();
                for (std::size_t i = 0; i < sharding.ColumnShardsSize(); ++i) {
                    hashByShardId.insert({sharding.GetColumnShards(i), i});
                }

                for (ui32 t = 0; t < tasksPerNode; ++t, ++stageInternalTaskId) {
                    auto& task = AddTask(stageInfo, tasksReason);
                    task.Meta = metas[t];
                    task.Meta.SetEnableShardsSequentialScan(false);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    task.SetMetaId(t);

                    for (const auto& readInfo: *task.Meta.Reads) {
                        Y_ENSURE(hashByShardId.contains(readInfo.ShardId));
                        (*columnShardHashV1Params.TaskIndexByHash)[hashByShardId[readInfo.ShardId]] = stageInternalTaskId;
                    }
                }
            }
        } else {
            ui32 metaId = 0;
            for (auto&& [nodeId, shardsInfo] : nodeShards) {
                const ui32 metaGlueingId = ++metaId;
                TTaskMeta meta;
                {
                    for (auto&& shardInfo : shardsInfo) {
                        MergeReadInfoToTaskMeta(meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                            columns, op, /*isPersistentScan*/ true);
                    }
                    PrepareScanMetaForUsage(meta, keyTypes);
                }

                const auto [maxTasksPerNode, tasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);

                for (ui32 t = 0; t < maxTasksPerNode; ++t) {
                    auto& task = AddTask(stageInfo, tasksReason);
                    task.Meta = meta;
                    task.Meta.SetEnableShardsSequentialScan(false);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::TTaskType::Scan;
                    task.SetMetaId(metaGlueingId);
                }
            }
        }
    }
}

} // namespace NKikimr::NKqp
