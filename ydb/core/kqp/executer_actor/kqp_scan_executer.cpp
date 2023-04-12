#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_partition_helper.h"
#include "kqp_result_channel.h"
#include "kqp_tasks_graph.h"
#include "kqp_tasks_validate.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/log.h>
#include <util/system/info.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

namespace {

TTaskMeta::TReadInfo::EReadType ReadTypeFromProto(const NKqpProto::TKqpPhyOpReadOlapRanges::EReadType& type) {
    switch (type) {
        case NKqpProto::TKqpPhyOpReadOlapRanges::ROWS:
            return TTaskMeta::TReadInfo::EReadType::Rows;
        case NKqpProto::TKqpPhyOpReadOlapRanges::BLOCKS:
            return TTaskMeta::TReadInfo::EReadType::Blocks;
        default:
            YQL_ENSURE(false, "Invalid read type from TKqpPhyOpReadOlapRanges protobuf.");
    }
}

class TKqpScanExecuter : public TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan> {
    using TBase = TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
        const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregation,
        const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig)
        : TBase(std::move(request), database, userToken, counters, executerRetriesConfig, TWilsonKqp::ScanExecuter, "ScanExecuter")
        , AggregationSettings(aggregation)
    {
        YQL_ENSURE(Request.Transactions.size() == 1);
        YQL_ENSURE(Request.DataShardLocks.empty());
        YQL_ENSURE(!Request.ValidateLocks);
        YQL_ENSURE(!Request.EraseLocks);
        YQL_ENSURE(Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_UNDEFINED);
        YQL_ENSURE(Request.Snapshot.IsValid());

        size_t resultsSize = Request.Transactions[0].Body->ResultsSize();
        YQL_ENSURE(resultsSize != 0);

        bool streamResult = Request.Transactions[0].Body->GetResults(0).GetIsStream();

        if (streamResult) {
            YQL_ENSURE(resultsSize == 1);
        } else {
            for (size_t i = 1; i < resultsSize; ++i) {
                YQL_ENSURE(Request.Transactions[0].Body->GetResults(i).GetIsStream() == streamResult);
            }
        }
    }

public:
    STATEFN(WaitResolveState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
                hFunc(TEvKqpExecuter::TEvShardsResolveStatus, HandleResolve);
                hFunc(TEvPrivate::TEvResourcesSnapshot, HandleResolve);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                default:
                    UnexpectedEvent("WaitResolveState", ev->GetTypeRewrite());
            }

        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

private:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDqCompute::TEvState, HandleComputeStats);
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvents::TEvUndelivered, HandleUndelivered);
                hFunc(TEvPrivate::TEvRetry, HandleRetry);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleStartKqpTasksResponse);
                IgnoreFunc(TEvKqpNode::TEvCancelKqpTasksResponse);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        LOG_T("Recv stream data ack, seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", freeSpace: " << ev->Get()->Record.GetFreeSpace()
            << ", enough: " << ev->Get()->Record.GetEnough()
            << ", from: " << ev->Sender);

        auto& resultChannelProxies = GetResultChannelProxies();
        if (resultChannelProxies.empty()) {
            return;
        }

        // Forward only for stream results, data results acks event theirselves.
        YQL_ENSURE(!ResponseEv->TxResults.empty() && ResponseEv->TxResults[0].IsStream);

        auto channelIt = resultChannelProxies.begin();
        auto handle = ev->Forward(channelIt->second->SelfId());
        channelIt->second->Receive(handle, TlsActivationContext->AsActorContext());
    }

private:

    void FillReadInfo(TTaskMeta& taskMeta, ui64 itemsLimit, bool reverse, bool sorted,
        NKikimr::NMiniKQL::TType* resultType, const TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>& readOlapRange) const
    {
        if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
            // Validate parameters
            YQL_ENSURE(taskMeta.ReadInfo.ItemsLimit == itemsLimit);
            YQL_ENSURE(taskMeta.ReadInfo.Reverse == reverse);

            if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
                YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program.empty());
                return;
            }

            YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program == readOlapRange->GetOlapProgram());
            return;
        }

        taskMeta.ReadInfo.ItemsLimit = itemsLimit;
        taskMeta.ReadInfo.Reverse = reverse;
        taskMeta.ReadInfo.Sorted = sorted;
        taskMeta.ReadInfo.ReadType = TTaskMeta::TReadInfo::EReadType::Rows;

        if (resultType) {
            YQL_ENSURE(resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct
                || resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Tuple);

            auto* resultStructType = static_cast<NKikimr::NMiniKQL::TStructType*>(resultType);
            ui32 resultColsCount = resultStructType->GetMembersCount();

            taskMeta.ReadInfo.ResultColumnsTypes.reserve(resultColsCount);
            for (ui32 i = 0; i < resultColsCount; ++i) {
                auto memberType = resultStructType->GetMemberType(i);
                if (memberType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Optional) {
                    memberType = static_cast<NKikimr::NMiniKQL::TOptionalType*>(memberType)->GetItemType();
                }
                // TODO: support pg types
                YQL_ENSURE(memberType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Data,
                    "Expected simple data types to be read from column shard");
                auto memberDataType = static_cast<NKikimr::NMiniKQL::TDataType*>(memberType);
                taskMeta.ReadInfo.ResultColumnsTypes.push_back(NScheme::TTypeInfo(memberDataType->GetSchemeType()));
            }
        }

        if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
            return;
        }

        taskMeta.ReadInfo.ReadType = ReadTypeFromProto(readOlapRange->GetReadType());
        taskMeta.ReadInfo.OlapProgram.Program = readOlapRange->GetOlapProgram();
        for (auto& name: readOlapRange->GetOlapProgramParameterNames()) {
            taskMeta.ReadInfo.OlapProgram.ParameterNames.insert(name);
        }
    };

    ui32 GetTasksPerNode(TStageInfo& stageInfo, const bool isOlapScan, const ui64 /*nodeId*/, const ui32 shardsCountCompute, const ui32 shardsCountNode) const {
        ui32 result = 0;
        if (isOlapScan) {
            if (AggregationSettings.HasCSScanThreadsPerNode()) {
                result = AggregationSettings.GetCSScanThreadsPerNode() * 1.0 * shardsCountCompute / shardsCountNode;
            } else {
                const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
                result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), {}) * 1.0 * shardsCountCompute / shardsCountNode;
            }
        } else {
            const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
            result = AggregationSettings.GetDSScanMinimalThreads();
            if (stage.GetProgram().GetSettings().GetHasSort()) {
                result = std::max(result, AggregationSettings.GetDSBaseSortScanThreads());
            }
            if (stage.GetProgram().GetSettings().GetHasMapJoin()) {
                result = std::max(result, AggregationSettings.GetDSBaseJoinScanThreads());
            }
        }
        return Max<ui32>(1, result);
    }

    ui32 GetMaxTasksAggregation(TStageInfo& stageInfo, const ui32 previousTasksCount, const ui32 nodesCount) const {
        if (AggregationSettings.HasAggregationComputeThreads()) {
            return std::max<ui32>(1, AggregationSettings.GetAggregationComputeThreads());
        } else if (nodesCount) {
            const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
            return predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), previousTasksCount / nodesCount) * nodesCount;
        } else {
            return 1;
        }
    }

    TTask& AssignTaskToShard(
        TStageInfo& stageInfo, const ui64 shardId,
        THashMap<ui64, std::vector<ui64>>& nodeTasks,
        THashMap<ui64, ui64>& assignedShardsCount,
        const bool sorted, const bool isOlapScan)
    {
        ui64 nodeId = ShardIdToNodeId.at(shardId);
        if (stageInfo.Meta.IsOlap() && sorted) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.ExecuterId = SelfId();
            task.Meta.NodeId = nodeId;
            task.Meta.ScanTask = true;
            return task;
        }

        auto& tasks = nodeTasks[nodeId];
        auto& cnt = assignedShardsCount[nodeId];
        const ui32 maxScansPerNode = GetTasksPerNode(stageInfo, isOlapScan, nodeId, 1, 1);
        if (cnt < maxScansPerNode) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.NodeId = nodeId;
            task.Meta.ScanTask = true;
            tasks.push_back(task.Id);
            ++cnt;
            return task;
        } else {
            ui64 taskIdx = cnt % maxScansPerNode;
            ++cnt;
            return TasksGraph.GetTask(tasks[taskIdx]);
        }
    }

    void MergeToTaskMeta(TTaskMeta& meta, TShardInfoWithId& shardInfo, const TPhysicalShardReadSettings& readSettings, const TVector<TTaskMeta::TColumn>& columns,
        const NKqpProto::TKqpPhyTableOperation& op) const {
        YQL_ENSURE(!shardInfo.KeyWriteRanges);
        for (auto& [name, value] : shardInfo.Params) {
            auto ret = meta.Params.emplace(name, std::move(value));
            YQL_ENSURE(ret.second);
        }

        TTaskMeta::TShardReadInfo readInfo = {
            .Ranges = std::move(*shardInfo.KeyReadRanges), // sorted & non-intersecting
            .Columns = columns,
            .ShardId = shardInfo.ShardId,
        };

        if (readSettings.ItemsLimitParamName && !meta.Params.contains(readSettings.ItemsLimitParamName)) {
            meta.Params.emplace(readSettings.ItemsLimitParamName, readSettings.ItemsLimitBytes);
        }

        if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
            const auto& readRange = op.GetReadOlapRange();
            FillReadInfo(meta, readSettings.ItemsLimit, readSettings.Reverse, readSettings.Sorted, readSettings.ResultType, readRange);
        } else {
            FillReadInfo(meta, readSettings.ItemsLimit, readSettings.Reverse, readSettings.Sorted, nullptr, TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>());
        }

        if (!meta.Reads) {
            meta.Reads.ConstructInPlace();
        }

        meta.Reads->emplace_back(std::move(readInfo));
    }

    void PrepareMetaForUsage(TTaskMeta& meta, const TVector<NScheme::TTypeInfo>& keyTypes) const {
        YQL_ENSURE(meta.Reads.Defined());
        auto& taskReads = meta.Reads.GetRef();

        /*
         * Sort read ranges so that sequential scan of that ranges produce sorted result.
         *
         * Partition pruner feed us with set of non-intersecting ranges with filled right boundary.
         * So we may sort ranges based solely on the their rightmost point.
         */
        std::sort(taskReads.begin(), taskReads.end(), [&](const auto& lhs, const auto& rhs) {
            if (lhs.ShardId == rhs.ShardId) {
                return false;
            }

            const std::pair<const TSerializedCellVec*, bool> k1 = lhs.Ranges.GetRightBorder();
            const std::pair<const TSerializedCellVec*, bool> k2 = rhs.Ranges.GetRightBorder();

            const int cmp = CompareBorders<false, false>(
                k1.first->GetCells(),
                k2.first->GetCells(),
                k1.second,
                k2.second,
                keyTypes);

            return (cmp < 0);
            });
    }

    void BuildScanTasks(TStageInfo& stageInfo) {
        THashMap<ui64, std::vector<ui64>> nodeTasks;
        THashMap<ui64, std::vector<TShardInfoWithId>> nodeShards;
        THashMap<ui64, ui64> assignedShardsCount;
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        const auto& table = GetTableKeys().GetTable(stageInfo.Meta.TableId);
        const auto& keyTypes = table.KeyColumnTypes;

        for (auto& op : stage.GetTableOps()) {
            Y_VERIFY_DEBUG(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto columns = BuildKqpColumns(op, table);
            auto partitions = PrunePartitions(GetTableKeys(), op, stageInfo, HolderFactory(), TypeEnv());
            const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
            auto readSettings = ExtractReadSettings(op, stageInfo, HolderFactory(), TypeEnv());

            if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadRange) {
                stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(),
                                                   op.GetReadRange().GetSkipNullKeys().end());
                // not supported for scan queries
                YQL_ENSURE(!readSettings.Reverse);
            }

            for (auto&& i: partitions) {
                const ui64 nodeId = ShardIdToNodeId.at(i.first);
                nodeShards[nodeId].emplace_back(TShardInfoWithId(i.first, std::move(i.second)));
            }

            if (Stats && CollectProfileStats(Request.StatsMode)) {
                for (auto&& i : nodeShards) {
                    Stats->AddNodeShardsCount(stageInfo.Id.StageId, i.first, i.second.size());
                }
            }

            if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead()) {
                for (auto&& pair : nodeShards) {
                    auto& shardsInfo = pair.second;
                    for (auto&& shardInfo : shardsInfo) {
                        auto& task = AssignTaskToShard(stageInfo, shardInfo.ShardId, nodeTasks, assignedShardsCount, readSettings.Sorted, isOlapScan);
                        MergeToTaskMeta(task.Meta, shardInfo, readSettings, columns, op);
                    }
                }

                for (const auto& pair : nodeTasks) {
                    for (const auto& taskIdx : pair.second) {
                        auto& task = TasksGraph.GetTask(taskIdx);
                        PrepareMetaForUsage(task.Meta, keyTypes);
                    }
                }

            } else if (!readSettings.Sorted) {
                for (auto&& pair : nodeShards) {
                    const auto nodeId = pair.first;
                    auto& shardsInfo = pair.second;
                    TTaskMeta meta;
                    {
                        for (auto&& shardInfo : shardsInfo) {
                            MergeToTaskMeta(meta, shardInfo, readSettings, columns, op);
                        }
                        PrepareMetaForUsage(meta, keyTypes);
                        LOG_D("Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                            << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
                    }
                    for (ui32 t = 0; t < GetTasksPerNode(stageInfo, isOlapScan, nodeId, shardsInfo.size(), shardsInfo.size()); ++t) {
                        auto& task = TasksGraph.AddTask(stageInfo);
                        task.Meta = meta;
                        task.Meta.ExecuterId = SelfId();
                        task.Meta.NodeId = nodeId;
                        task.Meta.ScanTask = true;
                    }
                }
            } else {
                for (auto&& pair : nodeShards) {
                    const auto nodeId = pair.first;
                    auto& shardsInfo = pair.second;
                    for (auto&& shardInfo : shardsInfo) {
                        YQL_ENSURE(!shardInfo.KeyWriteRanges);
                        TTaskMeta meta;
                        MergeToTaskMeta(meta, shardInfo, readSettings, columns, op);
                        PrepareMetaForUsage(meta, keyTypes);

                        LOG_D("Stage " << stageInfo.Id << " create datashard scan task meta for node: " << nodeId
                            << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
                        for (ui32 t = 0; t < GetTasksPerNode(stageInfo, isOlapScan, nodeId, 1, shardsInfo.size()); ++t) {
                            auto& task = TasksGraph.AddTask(stageInfo);
                            task.Meta = meta;
                            task.Meta.ExecuterId = SelfId();
                            task.Meta.NodeId = nodeId;
                            task.Meta.ScanTask = true;
                        }
                    }
                }
            }
                
        }

        LOG_D("Stage " << stageInfo.Id << " will be executed on " << nodeTasks.size() << " nodes.");

    }

    void BuildComputeTasks(TStageInfo& stageInfo) {
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        ui32 partitionsCount = 1;
        ui32 inputTasks = 0;
        bool isShuffle = false;
        for (ui32 inputIndex = 0; inputIndex < stage.InputsSize(); ++inputIndex) {
            const auto& input = stage.GetInputs(inputIndex);

            // Current assumptions:
            // 1. `Broadcast` can not be the 1st stage input unless it's a single input
            // 2. All stage's inputs, except 1st one, must be a `Broadcast` or `UnionAll`
            if (inputIndex == 0) {
                if (stage.InputsSize() > 1) {
                    YQL_ENSURE(input.GetTypeCase() != NKqpProto::TKqpPhyConnection::kBroadcast);
                }
            } else {
                switch (input.GetTypeCase()) {
                    case NKqpProto::TKqpPhyConnection::kBroadcast:
                    case NKqpProto::TKqpPhyConnection::kHashShuffle:
                    case NKqpProto::TKqpPhyConnection::kUnionAll:
                    case NKqpProto::TKqpPhyConnection::kMerge:
                    case NKqpProto::TKqpPhyConnection::kStreamLookup:
                        break;
                    default:
                        YQL_ENSURE(false, "Unexpected connection type: " << (ui32)input.GetTypeCase());
                }
            }

            auto& originStageInfo = TasksGraph.GetStageInfo(TStageId(stageInfo.Id.TxId, input.GetStageIndex()));

            switch (input.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                    inputTasks += originStageInfo.Tasks.size();
                    isShuffle = true;
                    break;
                }

                case NKqpProto::TKqpPhyConnection::kMap:
                case NKqpProto::TKqpPhyConnection::kStreamLookup:
                    partitionsCount = originStageInfo.Tasks.size();
                    break;

                default:
                    break;
            }
        }

        if (isShuffle) {
            partitionsCount = std::max(partitionsCount, GetMaxTasksAggregation(stageInfo, inputTasks, ShardsOnNode.size()));
        }

        for (ui32 i = 0; i < partitionsCount; ++i) {
            auto& task = TasksGraph.AddTask(stageInfo);
            LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
        }
    }

    void GetResourcesSnapshot() {
        GetKqpResourceManager()->RequestClusterResourcesInfo(
            [as = TlsActivationContext->ActorSystem(), self = SelfId()](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                TAutoPtr<IEventHandle> eh = new IEventHandle(self, self, new TEvPrivate::TEvResourcesSnapshot(std::move(resources)));
                as->Send(eh);
            });
    }

    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        TSet<ui64> shardIds;
        for (auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
            if (stageInfo.Meta.ShardKey) {
                for (auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                    shardIds.insert(partition.ShardId);
                }
            }
        }
        if (shardIds) {
            LOG_D("Start resolving tablets nodes... (" << shardIds.size() << ")");
            auto kqpShardsResolver = CreateKqpShardsResolver(this->SelfId(), TxId, std::move(shardIds));
            KqpShardsResolverId = this->RegisterWithSameMailbox(kqpShardsResolver);
        } else {
            GetResourcesSnapshot();
        }
    }

    void HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        if (!TBase::HandleResolve(ev)) return;
        GetResourcesSnapshot();
    }

    void HandleResolve(TEvPrivate::TEvResourcesSnapshot::TPtr& ev) {
        if (ev->Get()->Snapshot.empty()) {
            LOG_E("Can not find default state storage group for database " << Database);
        }

        Execute(std::move(ev->Get()->Snapshot));
    }

    void Execute(TVector<NKikimrKqp::TKqpNodeResources>&& snapshot) {
        LWTRACK(KqpScanExecuterStartExecute, ResponseEv->Orbit, TxId);
        NWilson::TSpan prepareTasksSpan(TWilsonKqp::ScanExecuterPrepareTasks, ExecuterStateSpan.GetTraceId(), "PrepareTasks", NWilson::EFlags::AUTO_END);

        auto& tx = Request.Transactions[0];
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = TasksGraph.GetStageInfo(TStageId(0, stageIdx));

            LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

            Y_VERIFY_DEBUG(!stage.GetIsEffectsStage());

            if (stage.SourcesSize() > 0) {
                switch (stage.GetSources(0).GetTypeCase()) {
                    case NKqpProto::TKqpSource::kReadRangesSource:
                        BuildScanTasksFromSource(stageInfo);
                        break;
                    default:
                        YQL_ENSURE(false, "unknown source type");
                }
            } else if (stageInfo.Meta.ShardOperations.empty()) {
                BuildComputeTasks(stageInfo);
            } else if (stageInfo.Meta.IsSysView()) {
                BuildSysViewScanTasks(stageInfo);
            } else if (stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()) {
                BuildScanTasks(stageInfo);
            } else {
                YQL_ENSURE(false, "Unexpected stage type " << (int) stageInfo.Meta.TableKind);
            }

            if (stage.GetIsSinglePartition()) {
                YQL_ENSURE(stageInfo.Tasks.size() == 1, "Unexpected multiple tasks in single-partition stage");
            }

            BuildKqpStageChannels(TasksGraph, GetTableKeys(), stageInfo, TxId, AppData()->EnableKqpSpilling);
        }

        ResponseEv->InitTxResult(tx.Body);
        BuildKqpTaskGraphResultChannels(TasksGraph, tx.Body, 0);

        TIssue validateIssue;
        if (!ValidateTasks(TasksGraph, EExecType::Scan, AppData()->EnableKqpSpilling, validateIssue)) {
            TBase::ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, validateIssue);
            return;
        }

        // NodeId -> {Tasks}
        THashMap<ui64, TVector<NYql::NDqProto::TDqTask>> scanTasks;
        ui32 nShardScans = 0;
        ui32 nScanTasks = 0;

        TVector<NYql::NDqProto::TDqTask> computeTasks;

        InitializeChannelProxies();

        for (auto& task : TasksGraph.GetTasks()) {
            auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
            NYql::NDqProto::TDqTask taskDesc = SerializeTaskToProto(TasksGraph, task);

            if (task.Meta.NodeId || stageInfo.Meta.IsSysView()) {
                // Task with source
                if (!task.Meta.Reads) {
                    scanTasks[task.Meta.NodeId].emplace_back(std::move(taskDesc));
                    nScanTasks++;
                    continue;
                }

                if (stageInfo.Meta.IsSysView()) {
                    computeTasks.emplace_back(std::move(taskDesc));
                } else {
                    scanTasks[task.Meta.NodeId].emplace_back(std::move(taskDesc));
                    nScanTasks++;
                }

                nShardScans += task.Meta.Reads->size();
                if (Stats) {
                    for(const auto& read: *task.Meta.Reads) {
                        Stats->AffectedShards.insert(read.ShardId);
                    }
                }

            } else {
                computeTasks.emplace_back(std::move(taskDesc));
            }
        }


        if (computeTasks.size() + nScanTasks > Request.MaxComputeActors) {
            LOG_N("Too many compute actors: computeTasks=" << computeTasks.size() << ", scanTasks=" << nScanTasks);
            TBase::ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                    << "Requested too many execution units: " << (computeTasks.size() + nScanTasks)));
            return;
        }

        bool fitSize = AllOf(scanTasks, [this](const auto& x){ return ValidateTaskSize(x.second); })
                    && ValidateTaskSize(computeTasks);
        if (!fitSize) {
            return;
        }

        if (prepareTasksSpan) {
            prepareTasksSpan.End();
        }

        LOG_D("Total tasks: " << TasksGraph.GetTasks().size() << ", readonly: true"
            << ", " << nScanTasks << " scan tasks on " << scanTasks.size() << " nodes"
            << ", totalShardScans: " << nShardScans << ", execType: Scan"
            << ", snapshot: {" << GetSnapshot().TxId << ", " << GetSnapshot().Step << "}");

        ExecuteScanTx(std::move(computeTasks), std::move(scanTasks), std::move(snapshot));

        Become(&TKqpScanExecuter::ExecuteState);
        if (ExecuterStateSpan) {
            ExecuterStateSpan.End();
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ScanExecuterExecuteState, ExecuterSpan.GetTraceId(), "ExecuteState", NWilson::EFlags::AUTO_END);
        }
    }

public:
    void Finalize() {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(Ydb::StatusIds::SUCCESS);

        if (Stats) {
            ReportEventElapsedTime();

            Stats->FinishTs = TInstant::Now();
            Stats->Finish();

            if (CollectFullStats(Request.StatsMode)) {
                const auto& tx = Request.Transactions[0].Body;
                auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), response.GetResult().GetStats());
                response.MutableResult()->MutableStats()->AddTxPlansWithStats(planWithStats);
            }
        }

        LWTRACK(KqpScanExecuterFinalize, ResponseEv->Orbit, TxId, LastTaskId, LastComputeActorId, ResponseEv->ResultsSize());

        if (ExecuterSpan) {
            ExecuterSpan.EndOk();
        }

        LOG_D("Sending response to: " << Target);
        Send(Target, ResponseEv.release());
        PassAway();
    }

private:
    void ExecuteScanTx(TVector<NYql::NDqProto::TDqTask>&& computeTasks, THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& scanTasks,
        TVector<NKikimrKqp::TKqpNodeResources>&& snapshot) {
        LWTRACK(KqpScanExecuterStartTasksAndTxs, ResponseEv->Orbit, TxId, computeTasks.size(), scanTasks.size());
        for (const auto& [_, tasks]: scanTasks) {
            for (const auto& task : tasks) {
                PendingComputeTasks.insert(task.GetId());
            }
        }

        for (auto& taskDesc : computeTasks) {
            PendingComputeTasks.insert(taskDesc.GetId());
        }

        Planner = CreateKqpPlanner(TxId, SelfId(), std::move(computeTasks),
            std::move(scanTasks), GetSnapshot(),
            Database, UserToken, Deadline.GetOrElse(TInstant::Zero()), Request.StatsMode,
            Request.DisableLlvmForUdfStages, Request.LlvmEnabled, AppData()->EnableKqpSpilling,
            Request.RlPath, ExecuterSpan, std::move(snapshot), ExecuterRetriesConfig);
        LOG_D("Execute scan tx, computeTasks: " << Planner->GetComputeTasksNumber() << ", scanTasks: " << Planner->GetMainTasksNumber());

        Planner->ProcessTasksForScanExecuter();
    }

private:
    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues) override
    {
        if (!PendingComputeTasks.empty()) {
            LOG_D("terminate pending resources request: " << Ydb::StatusIds::StatusCode_Name(status));

            auto ev = MakeHolder<TEvKqpNode::TEvCancelKqpTasksRequest>();
            ev->Record.SetTxId(TxId);
            ev->Record.SetReason(Ydb::StatusIds::StatusCode_Name(status));

            Send(MakeKqpNodeServiceID(SelfId().NodeId()), ev.Release());
        }

        TBase::ReplyErrorAndDie(status, issues);
    }

    void PassAway() override {
        for (auto channelPair: GetResultChannelProxies()) {
            LOG_D("terminate result channel " << channelPair.first << " proxy at " << channelPair.second->SelfId());

            TAutoPtr<IEventHandle> ev = new IEventHandle(
                channelPair.second->SelfId(), SelfId(), new TEvents::TEvPoison
            );
            channelPair.second->Receive(ev, TActivationContext::AsActorContext());
        }

        for (auto& [shardId, nodeId] : ShardIdToNodeId) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }

        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->ScanTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());

        TBase::PassAway();
    }
private:
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig AggregationSettings;
};

} // namespace

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregation,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig)
{
    return new TKqpScanExecuter(std::move(request), database, userToken, counters, aggregation, executerRetriesConfig);
}

} // namespace NKqp
} // namespace NKikimr
