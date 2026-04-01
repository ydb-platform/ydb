#include "kqp_tasks_graph_old.h"

#include "kqp_partition_helper.h"

#include <ydb/core/kqp/common/control.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, stream)

namespace NKikimr::NKqp {

size_t TKqpTasksGraphOld::DoBuildAllTasks(const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, TQueryExecutionStats* stats) {
    size_t sourceScanPartitionsCount = 0;

    bool limitTasksPerNode = IsEnabledReadsMerge();

    if (!GetMeta().IsScan) {
        limitTasksPerNode |= GetMeta().StreamResult;
    }

    for (ui32 txIdx = 0; txIdx < Transactions.size(); ++txIdx) {
        const auto& tx = Transactions.at(txIdx);
        auto scheduledTaskCount = ScheduleByCost(tx, resourcesSnapshot);
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            const auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

            // TODO:
            // if (EnableReadsMerge) {
            //     stageInfo.Introspections.push_back("Using tasks count limit because of enabled reads merge");
            // }

            const bool maybeOlapRead = (GetMeta().AllowOlapDataQuery || GetMeta().StreamResult) && stageInfo.Meta.IsOlap();

            // build task conditions
            const bool buildFromSourceTasks = stage.SourcesSize() > 0;
            const bool buildSysViewTasks = stageInfo.Meta.IsSysView();
            const bool buildComputeTasks = stageInfo.Meta.ShardOperations.empty() || (!GetMeta().IsScan && stage.SinksSize() + stage.OutputTransformsSize() > 0 && !(maybeOlapRead && stageInfo.Meta.HasReads()));
            const bool buildScanTasks = GetMeta().IsScan
                ? stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()
                : maybeOlapRead && (stage.SinksSize() + stage.OutputTransformsSize() == 0 || stageInfo.Meta.HasReads())
                ;

            // TODO: doesn't work right now - multiple conditions are possible in tests
            // YQL_ENSURE(buildFromSourceTasks + buildSysViewTasks + buildComputeTasks + buildScanTasks <= 1,
            //     "Multiple task conditions: " <<
            //     "isScan = " << (isScan) << ", "
            //     "stage.SourcesSize() > 0 = " << (stage.SourcesSize() > 0) << ", "
            //     "stageInfo.Meta.IsSysView() = " << (stageInfo.Meta.IsSysView()) << ", "
            //     "stageInfo.Meta.ShardOperations.empty() = " << (stageInfo.Meta.ShardOperations.empty()) << ", "
            //     "stage.SinksSize() = " << (stage.SinksSize()) << ", "
            //     "stageInfo.Meta.IsOlap() = " << (stageInfo.Meta.IsOlap()) << ", "
            //     "stageInfo.Meta.IsDatashard() = " << (stageInfo.Meta.IsDatashard()) << ", "
            //     "AllowOlapDataQuery = " << (AllowOlapDataQuery) << ", "
            //     "StreamResult = " << (StreamResult)
            // );

            LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

            if (buildFromSourceTasks) {
                switch (stage.GetSources(0).GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource: {
                        // TODO: is this case only for reading OLTP table with datashards?
                        if (auto partitionsCount = BuildScanTasksFromSource(stageInfo, limitTasksPerNode, stats)) {
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
            } else if (buildSysViewTasks) {
                BuildSysViewScanTasks(stageInfo);
            } else if (buildComputeTasks) {
                auto nodesCount = GetMeta().ShardsOnNode.size();
                if (!GetMeta().IsScan) {
                    nodesCount = std::max<ui32>(resourcesSnapshot.size(), nodesCount);
                }
                GetMeta().UnknownAffectedShardCount |= BuildComputeTasks(stageInfo, nodesCount);
            } else if (buildScanTasks) {
                BuildScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination(), CollectProfileStats(GetMeta().StatsMode) ? stats : nullptr);
            } else {
                AFL_ENSURE(false);
            }
        }
    }

    return sourceScanPartitionsCount;
}

TMaybe<size_t> TKqpTasksGraphOld::BuildScanTasksFromSource(TStageInfo& stageInfo, bool limitTasksPerNode, TQueryExecutionStats* stats) {
    THashMap<ui64, std::vector<ui64>> nodeTasks;
    THashMap<ui64, ui64> assignedShardsCount;

    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    bool singlePartitionedStage = stage.GetIsSinglePartition();

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

    TVector<ui64> createdTasksIds;
    auto createNewTask = [&](
            TMaybe<ui64> nodeId,
            ui64 taskShardId,
            bool hintShardId,
            TMaybe<ui64> maxInFlightShards,
            TTaskType::ECreateReason taskReason) -> TTask&
    {
        auto& task = AddTask(stageInfo, taskReason);
        task.Meta.Type = TTaskMeta::ETaskType::Scan; // TODO: can this scan task not have NodeId?
        if (nodeId) {
            task.Meta.NodeId = *nodeId;
        }

        if (!nodeId || !GetMeta().ShardsResolved) {
            YQL_ENSURE(!GetMeta().ShardsResolved);
            task.Meta.ShardId = taskShardId;
        }

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

        for (const auto& keyColumn : keyTypes) {
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
            for (const auto& colName : tableInfo->KeyColumns) {
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

        if (hintShardId) {
            settings->SetShardIdHint(taskShardId);
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

        createdTasksIds.push_back(task.Id);
        return task;
    };

    struct TShardRangesWithShardId {
        TMaybe<ui64> ShardId;
        const TShardKeyRanges* Ranges;
    };
    using TShardRangesVector = TVector<TShardRangesWithShardId>;

    THashMap<ui64, TVector<ui64>> nodeIdToTasks;
    THashMap<ui64, TShardRangesVector> nodeIdToShardKeyRanges;

    auto addPartition = [&](
        ui64 taskShardId,
        TMaybe<ui64> nodeId,
        bool hintShardId,
        const TShardInfo& shardInfo,
        TMaybe<ui64> maxInFlightShards,
        TTaskType::ECreateReason tasksReason)
    {
        YQL_ENSURE(!shardInfo.KeyWriteRanges);

        if (!nodeId) {
            const auto* nodeIdPtr = GetMeta().ShardIdToNodeId.FindPtr(taskShardId);
            nodeId = nodeIdPtr ? TMaybe<ui64>{*nodeIdPtr} : Nothing();
        }

        YQL_ENSURE(!GetMeta().ShardsResolved || nodeId);

        if (hintShardId && stats) {
            stats->AffectedShards.insert(taskShardId);
        }

        if (limitTasksPerNode && GetMeta().ShardsResolved) {
            const auto [maxScanTasksPerNode, maxTasksReason] = GetScanTasksPerNode(stageInfo, /* isOlapScan */ false, *nodeId);
            auto& nodeTasks = nodeIdToTasks[*nodeId];
            if (nodeTasks.size() < maxScanTasksPerNode) {
                const auto& task = createNewTask(nodeId, taskShardId, false, maxInFlightShards, tasksReason);
                nodeTasks.push_back(task.Id);

                if (nodeTasks.size() == maxScanTasksPerNode) {
                    for (const auto& taskId : nodeTasks) {
                        GetTask(taskId).Reason = maxTasksReason;
                    }
                }
            }
            nodeIdToShardKeyRanges[*nodeId].push_back(TShardRangesWithShardId{hintShardId ? TMaybe<ui64>(taskShardId) : Nothing(), &*shardInfo.KeyReadRanges});
        } else {
            auto& task = createNewTask(nodeId, taskShardId, hintShardId, maxInFlightShards, tasksReason);
            const auto& stageSource = stage.GetSources(0);
            auto& input = task.Inputs[stageSource.GetInputIndex()];
            NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;

            shardInfo.KeyReadRanges->SerializeTo(settings);
        }
    };

    auto DistributeShardsToTasks = [](TVector<TShardRangesWithShardId> shardsRanges, const size_t tasksCount, const TVector<NScheme::TTypeInfo>& keyTypes) {
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

    auto fillRangesForTasks = [&]() {
        for (const auto& [nodeId, shardsRanges] : nodeIdToShardKeyRanges) {
            const auto& tasks = nodeIdToTasks.at(nodeId);

            const auto rangesDistribution = DistributeShardsToTasks(shardsRanges, tasks.size(), keyTypes);
            YQL_ENSURE(rangesDistribution.size() == tasks.size());

            for (size_t taskIndex = 0; taskIndex < tasks.size(); ++taskIndex) {
                const auto taskId = tasks[taskIndex];
                auto& task = GetTask(taskId);
                const auto& stageSource = stage.GetSources(0);
                auto& input = task.Inputs[stageSource.GetInputIndex()];
                NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;

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
    };

    const auto& partitions = stageInfo.Meta.PrunedPartitions.at(0);

    bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards();

    if (!partitions.empty() && (isSequentialInFlight || singlePartitionedStage)) {
        Y_ENSURE(stageInfo.Meta.VirtualPartition);

        auto startShard = stageInfo.Meta.VirtualPartition->ShardId;
        auto& shardInfo = *stageInfo.Meta.VirtualPartition;

        if (stats) {
            for (const auto& [shardId, _] : partitions) {
                stats->AffectedShards.insert(shardId);
            }
        }

        TMaybe<ui64> inFlightShards = Nothing();
        if (isSequentialInFlight) {
            inFlightShards = source.GetSequentialInFlightShards();
        }

        Y_ENSURE(shardInfo.KeyReadRanges); // TODO: redundant check - remove later.

        const TMaybe<ui64> nodeId = singlePartitionedStage ? TMaybe<ui64>{GetMeta().ExecuterId.NodeId()} : Nothing();
        addPartition(startShard, nodeId, false, shardInfo, inFlightShards, TTaskType::SINGLE_SOURCE_SCAN);
        fillRangesForTasks();

        return singlePartitionedStage ? TMaybe<size_t>(partitions.size()) : Nothing();
    } else {
        for (const auto& [shardId, shardInfo] : partitions) {
            addPartition(shardId, {}, true, shardInfo, {}, TTaskType::DEFAULT_SOURCE_SCAN);
        }
        fillRangesForTasks();
        return partitions.size();
    }
}

void TKqpTasksGraphOld::BuildFullTextScanTasksFromSource(TStageInfo& stageInfo, TQueryExecutionStats* stats) {
    YQL_ENSURE(stageInfo.Meta.GetStage(stageInfo.Id).GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kFullTextSource);
    Y_UNUSED(stats);

    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    const auto& source = stageInfo.Meta.GetStage(stageInfo.Id).GetSources(0);
    const auto& fullTextSource = source.GetFullTextSource();

    YQL_ENSURE(fullTextSource.GetIndex());

    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SOURCE_READ);
    task.Meta.Type = TTaskMeta::ETaskType::Scan;
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

    if (fullTextSource.HasTakeLimit())
    {
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

void TKqpTasksGraphOld::BuildSysViewTasksFromSource(TStageInfo& stageInfo) {
    YQL_ENSURE(stageInfo.Meta.GetStage(stageInfo.Id).GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kSysViewSource);

    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
    const auto& source = stage.GetSources(0);
    const auto& sysViewSource = source.GetSysViewSource();

    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SOURCE_READ);
    task.Meta.Type = TTaskMeta::ETaskType::Compute;
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

    LOG_D("Stage " << stageInfo.Id << " create sys view source task: " << task.Id);
}

void TKqpTasksGraphOld::BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount) {
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    YQL_ENSURE(stage.GetSources(0).HasExternalSource());
    YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

    const auto& stageSource = stage.GetSources(0);
    const auto& externalSource = stageSource.GetExternalSource();

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

void TKqpTasksGraphOld::BuildSysViewScanTasks(TStageInfo& stageInfo) {
    Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.IsSysView());

    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    const auto& holderFactory = TxAlloc->HolderFactory;
    const auto& typeEnv = TxAlloc->TypeEnv;
    const auto& tableInfo = stageInfo.Meta.TableConstInfo;
    const auto& keyTypes = tableInfo->KeyColumnTypes;

    for (const auto& op : stage.GetTableOps()) {
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
        task.Meta.Type = TTaskMeta::ETaskType::Compute;

        LOG_D("Stage " << stageInfo.Id << " create sysview scan task: " << task.Id);
    }
}

bool TKqpTasksGraphOld::BuildComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount) {
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

    TTaskType::ECreateReason tasksReason = TTaskType::MINIMUM_COMPUTE;
    bool unknownAffectedShardCount = false;
    ui32 partitionsCount = 1;
    ui32 inputTasks = 0;
    bool isShuffle = false;
    bool forceMapTasks = false;
    ui32 mapConnectionCount = 0;

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

        auto& originStageInfo = GetStageInfo(NYql::NDq::TStageId(stageInfo.Id.TxId, input.GetStageIndex()));

        switch (input.GetTypeCase()) {
            case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                inputTasks += originStageInfo.Tasks.size();
                isShuffle = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                tasksReason = TTaskType::PREV_STAGE_COMPUTE;
                partitionsCount = originStageInfo.Tasks.size();
                unknownAffectedShardCount = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kMap: {
                tasksReason = TTaskType::PREV_STAGE_COMPUTE;
                partitionsCount = originStageInfo.Tasks.size();
                forceMapTasks = true;
                ++mapConnectionCount;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                partitionsCount = std::max<ui64>(partitionsCount, originStageInfo.Tasks.size());
                break;
            }
            case NKqpProto::TKqpPhyConnection::kVectorResolve: {
                tasksReason = TTaskType::PREV_STAGE_COMPUTE;
                partitionsCount = originStageInfo.Tasks.size();
                unknownAffectedShardCount = true;
                break;
            }
            case NKqpProto::TKqpPhyConnection::kSequencer: {
                tasksReason = TTaskType::PREV_STAGE_COMPUTE;
                partitionsCount = originStageInfo.Tasks.size();
                break;
            }
            default:
                break;
        }
    }

    Y_ENSURE(mapConnectionCount <= 1, "Only a single map connection is allowed");

    if (isShuffle && !forceMapTasks) {
        if (stage.GetTaskCount()) {
            tasksReason = TTaskType::FORCED;
            partitionsCount = stage.GetTaskCount();
        } else {
            auto [newPartitionCount, minTasksReason] = GetMaxTasksAggregation(stageInfo, inputTasks, nodesCount);
            if (newPartitionCount > partitionsCount) {
                tasksReason = minTasksReason;
                partitionsCount = newPartitionCount;
            }
        }
    }

    for (ui32 i = 0; i < partitionsCount; ++i) {
        auto& task = AddTask(stageInfo, tasksReason);
        task.Meta.Type = TTaskMeta::ETaskType::Compute;
        LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
    }

    return unknownAffectedShardCount;
}

void TKqpTasksGraphOld::BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination, TQueryExecutionStats* stats) {
    THashMap<ui64, std::vector<ui64>> nodeTasks;
    THashMap<ui64 /* nodeId */, std::vector<TShardInfoWithId>> nodeShards;
    THashMap<ui64, ui64> assignedShardsCount;
    const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

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
    for (auto i = 0ul, s = stage.TableOpsSize(); i < s; ++i) {
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

        YQL_ENSURE(GetMeta().ShardsResolved);

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

            auto AssignScanTaskToShard = [&](const ui64 shardId, const bool sorted) -> TTask& {
                ui64 nodeId = GetMeta().ShardIdToNodeId.at(shardId);
                if (stageInfo.Meta.IsOlap() && sorted) {
                    auto& task = AddTask(stageInfo, TTaskType::OLAP_SORT_SCAN);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::ETaskType::Scan;
                    ++olapAndSortedTasksCount[nodeId];
                    return task;
                }

                auto& tasks = nodeTasks[nodeId];
                auto& cnt = assignedShardsCount[nodeId];
                const auto [maxScansPerNode, maxTasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);
                if (cnt < maxScansPerNode) {
                    auto& task = AddTask(stageInfo, TTaskType::DEFAULT_SHARD_SCAN);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::ETaskType::Scan;
                    tasks.push_back(task.Id);
                    ++cnt;

                    if (cnt == maxScansPerNode) {
                        for (const auto& taskId : tasks) {
                            GetTask(taskId).Reason = maxTasksReason;
                        }
                    }

                    return task;
                } else {
                    ui64 taskIdx = cnt % maxScansPerNode;
                    ++cnt;
                    return GetTask(tasks[taskIdx]);
                }
            };

            for (auto& [_, shardsInfo] : nodeShards) {
                for (auto& shardInfo : shardsInfo) {
                    auto& task = AssignScanTaskToShard(shardInfo.ShardId, readSettings.IsSorted());
                    MergeReadInfoToTaskMeta(task.Meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                        columns, op, /*isPersistentScan*/ true);
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
                        LOG_D( "Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                            << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
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
                    task.Meta.Type = TTaskMeta::ETaskType::Scan;
                    task.SetMetaId(t);

                    for (const auto& readInfo: *task.Meta.Reads) {
                        Y_ENSURE(hashByShardId.contains(readInfo.ShardId));
                        (*columnShardHashV1Params.TaskIndexByHash)[hashByShardId[readInfo.ShardId]] = stageInternalTaskId;
                    }
                }
            }

            LOG_D( "Stage with scan " << "[" << stageInfo.Id.TxId << ":" << stageInfo.Id.StageId << "]"
                << " has keys: " << columnShardHashV1Params.KeyTypesToString() << " and task count: " << stageInternalTaskId;
            );
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
                    LOG_D( "Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                        << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
                }

                const auto [maxTasksPerNode, tasksReason] = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);

                for (ui32 t = 0; t < maxTasksPerNode; ++t) {
                    auto& task = AddTask(stageInfo, tasksReason);
                    task.Meta = meta;
                    task.Meta.SetEnableShardsSequentialScan(false);
                    task.Meta.NodeId = nodeId;
                    task.Meta.ScanTask = true;
                    task.Meta.Type = TTaskMeta::ETaskType::Scan;
                    task.SetMetaId(metaGlueingId);
                }
            }
        }
    }

    LOG_D("Stage " << stageInfo.Id << " will be executed on " << nodeTasks.size() << " nodes.");
}

} // namespace NKikimr::NKqp
