#include "kqp_executer.h"
#include "kqp_executer_impl.h"
#include "kqp_partition_helper.h"
#include "kqp_planner.h"
#include "kqp_result_channel.h"
#include "kqp_tasks_graph.h"
#include "kqp_tasks_validate.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/client/minikql_compile/db_key_resolver.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/kqp.h>
#include <ydb/core/kqp/node/kqp_node.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/prepare/kqp_query_plan.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/dq/runtime/dq_columns_resolve.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;
using namespace NYql::NDq;

namespace {

class TKqpScanExecuter : public TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan> {
    using TBase = TKqpExecuterBase<TKqpScanExecuter, EExecType::Scan>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_EXECUTER_ACTOR;
    }

    TKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
        const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters)
        : TBase(std::move(request), database, userToken, counters, TWilsonKqp::ScanExecuter, "ScanExecuter")
    {
        YQL_ENSURE(Request.Transactions.size() == 1);
        YQL_ENSURE(Request.Locks.empty());
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
    TActorId KqpShardsResolverId;

    STATEFN(WaitResolveState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
            hFunc(TEvKqpExecuter::TEvShardsResolveStatus, HandleResolve);
            hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
            hFunc(TEvents::TEvWakeup, HandleTimeout);
            default:
                UnexpectedEvent("WaitResolveState", ev->GetTypeRewrite());
        }
        ReportEventElapsedTime();
    }

    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        auto& reply = *ev->Get();

        KqpTableResolverId = {};

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            TBase::ReplyErrorAndDie(reply.Status, reply.Issues);
            return;
        }

        TSet<ui64> shardIds;
        for (auto& [stageId, stageInfo] : TasksGraph.GetStagesInfo()) {
            if (stageInfo.Meta.ShardKey) {
                for (auto& partition : stageInfo.Meta.ShardKey->GetPartitions()) {
                    shardIds.insert(partition.ShardId);
                }
            }
        }

        if (ExecuterTableResolveSpan) {
            ExecuterTableResolveSpan.End();
        }

        if (shardIds.size() > 0) {
            LOG_D("Start resolving tablets nodes... (" << shardIds.size() << ")");
            auto kqpShardsResolver = CreateKqpShardsResolver(SelfId(), TxId, std::move(shardIds));
            KqpShardsResolverId = RegisterWithSameMailbox(kqpShardsResolver);
        } else {
            Execute();
        }
    }

    void HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        auto& reply = *ev->Get();

        KqpShardsResolverId = {};

        // TODO: count resolve time in CpuTime

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            LOG_W("Shards nodes resolve failed, status: " << Ydb::StatusIds_StatusCode_Name(reply.Status)
                << ", issues: " << reply.Issues.ToString());
            TBase::ReplyErrorAndDie(reply.Status, reply.Issues);
            return;
        }

        LOG_D("Shards nodes resolved, success: " << reply.ShardNodes.size() << ", failed: " << reply.Unresolved);

        ShardIdToNodeId = std::move(reply.ShardNodes);
        for (auto& [shardId, nodeId] : ShardIdToNodeId) {
            ShardsOnNode[nodeId].push_back(shardId);
        }

        if (IsDebugLogEnabled()) {
            TStringBuilder sb;
            sb << "Shards on nodes: " << Endl;
            for (auto& pair : ShardsOnNode) {
                sb << "  node " << pair.first << ": [";
                if (pair.second.size() <= 20) {
                    sb << JoinSeq(", ", pair.second) << "]" << Endl;
                } else {
                    sb << JoinRange(", ", pair.second.begin(), std::next(pair.second.begin(), 20)) << ", ...] "
                       << "(total " << pair.second.size() << ") " << Endl;
                }
            }
            LOG_D(sb);
        }

        Execute();
    }

private:
    STATEFN(ExecuteState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDqCompute::TEvState, HandleExecute);
                hFunc(TEvKqpExecuter::TEvStreamDataAck, HandleExecute);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvents::TEvWakeup, HandleTimeout);
                hFunc(TEvents::TEvUndelivered, HandleExecute);
                hFunc(TEvKqpNode::TEvStartKqpTasksResponse, HandleExecute);
                IgnoreFunc(TEvKqpNode::TEvCancelKqpTasksResponse);
                hFunc(TEvInterconnect::TEvNodeDisconnected, HandleExecute);
                IgnoreFunc(TEvInterconnect::TEvNodeConnected);
                default:
                    UnexpectedEvent("ExecuteState", ev->GetTypeRewrite());
            }
        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void HandleExecute(TEvDqCompute::TEvState::TPtr& ev) {
        TActorId computeActor = ev->Sender;
        auto& state = ev->Get()->Record;
        ui64 taskId = state.GetTaskId();

        LOG_D("Got execution state from compute actor: " << computeActor
            << ", task: " << taskId
            << ", state: " << NDqProto::EComputeState_Name((NDqProto::EComputeState) state.GetState())
            << ", stats: " << state.GetStats());

        switch (state.GetState()) {
            case NDqProto::COMPUTE_STATE_UNKNOWN: {
                YQL_ENSURE(false, "unexpected state from " << computeActor << ", task: " << taskId);
                return;
            }

            case NDqProto::COMPUTE_STATE_FAILURE: {
                ReplyErrorAndDie(DqStatusToYdbStatus(state.GetStatusCode()), state.MutableIssues());
                return;
            }

            case NDqProto::COMPUTE_STATE_EXECUTING: {
                // initial TEvState event from Compute Actor
                // there can be race with RM answer
                if (PendingComputeTasks.erase(taskId)) {
                    auto it = PendingComputeActors.emplace(computeActor, TProgressStat());
                    YQL_ENSURE(it.second);

                    if (state.HasStats()) {
                        it.first->second.Set(state.GetStats());
                    }

                    auto& task = TasksGraph.GetTask(taskId);
                    task.ComputeActorId = computeActor;

                    THashMap<TActorId, THashSet<ui64>> updates;
                    CollectTaskChannelsUpdates(task, updates);
                    PropagateChannelsUpdates(updates);
                } else {
                    auto it = PendingComputeActors.find(computeActor);
                    if (it != PendingComputeActors.end()) {
                        if (state.HasStats()) {
                            it->second.Set(state.GetStats());
                        }
                    }
                }
                break;
            }

            case NDqProto::COMPUTE_STATE_FINISHED: {
                if (Stats) {
                    Stats->AddComputeActorStats(computeActor.NodeId(), std::move(*state.MutableStats()));
                }

                LastTaskId = taskId;
                LastComputeActorId = computeActor.ToString();

                auto it = PendingComputeActors.find(computeActor);
                if (it == PendingComputeActors.end()) {
                    LOG_W("Got execution state for compute actor: " << computeActor
                        << ", task: " << taskId
                        << ", state: " << NDqProto::EComputeState_Name((NDqProto::EComputeState) state.GetState())
                        << ", too early (waiting reply from RM)");

                    if (PendingComputeTasks.erase(taskId)) {
                        LOG_E("Got execution state for compute actor: " << computeActor
                            << ", for unknown task: " << state.GetTaskId()
                            << ", state: " << NDqProto::EComputeState_Name((NDqProto::EComputeState) state.GetState()));
                        return;
                    }
                } else {
                    if (state.HasStats()) {
                        it->second.Set(state.GetStats());
                    }
                    LastStats.emplace_back(std::move(it->second));
                    PendingComputeActors.erase(it);
                    YQL_ENSURE(PendingComputeTasks.find(taskId) == PendingComputeTasks.end());
                }
            }
        }

        CheckExecutionComplete();
    }

    void HandleExecute(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        LOG_T("Recv stream data ack, seqNo: " << ev->Get()->Record.GetSeqNo()
            << ", freeSpace: " << ev->Get()->Record.GetFreeSpace()
            << ", enough: " << ev->Get()->Record.GetEnough()
            << ", from: " << ev->Sender);

        if (ResultChannelProxies.empty()) {
            return;
        }

        // Forward only for stream results, data results acks event theirselves.
        YQL_ENSURE(!Results.empty() && Results[0].IsStream);

        auto channelIt = ResultChannelProxies.begin();
        auto handle = ev->Forward(channelIt->second->SelfId());
        channelIt->second->Receive(handle, TlsActivationContext->AsActorContext());
    }

    void HandleExecute(TEvKqpNode::TEvStartKqpTasksResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        YQL_ENSURE(record.GetTxId() == TxId);

        if (record.NotStartedTasksSize() != 0) {
            auto reason = record.GetNotStartedTasks()[0].GetReason();
            auto& message = record.GetNotStartedTasks()[0].GetMessage();

            LOG_E("Stop executing, reason: " << NKikimrKqp::TEvStartKqpTasksResponse_ENotStartedTaskReason_Name(reason)
                << ", message: " << message);

            switch (reason) {
                case NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY: {
                    TBase::ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                        YqlIssue({}, TIssuesIds::KIKIMR_OVERLOADED, "Not enough memory to execute query"));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS: {
                    TBase::ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                        YqlIssue({}, TIssuesIds::KIKIMR_OVERLOADED, "Not enough computation units to execute query"));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED: {
                    TBase::ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                        YqlIssue({}, TIssuesIds::KIKIMR_PRECONDITION_FAILED, "Memory limit exceeded"));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::QUERY_EXECUTION_UNITS_LIMIT_EXCEEDED: {
                    TBase::ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                         YqlIssue({}, TIssuesIds::KIKIMR_OVERLOADED, "Not enough computation units to execute query"));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR: {
                    InternalError("KqpNode internal error");
                    break;
                }
            }

            return;
        }

        THashMap<TActorId, THashSet<ui64>> channelsUpdates;

        for (auto& startedTask : record.GetStartedTasks()) {
            auto taskId = startedTask.GetTaskId();
            auto& task = TasksGraph.GetTask(taskId);

            task.ComputeActorId = ActorIdFromProto(startedTask.GetActorId());

            LOG_D("Executing task: " << taskId << " on compute actor: " << task.ComputeActorId);

            if (PendingComputeTasks.erase(taskId) == 0) {
                LOG_D("Executing task: " << taskId << ", compute actor: " << task.ComputeActorId << ", already finished");
            } else {
                auto result = PendingComputeActors.emplace(std::make_pair(task.ComputeActorId, TProgressStat()));
                YQL_ENSURE(result.second);

                CollectTaskChannelsUpdates(task, channelsUpdates);
            }
        }

        PropagateChannelsUpdates(channelsUpdates);
    }

    void HandleExecute(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 eventType = ev->Get()->SourceType;
        auto reason = ev->Get()->Reason;
        switch (eventType) {
            case TEvKqpNode::TEvStartKqpTasksRequest::EventType: {
                return InternalError(TStringBuilder()
                    << "TEvKqpNode::TEvStartKqpTasksRequest lost: " << reason);
            }
            default: {
                LOG_E("Event lost, type: " << eventType << ", reason: " << reason);
            }
        }
    }

    void HandleExecute(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        auto nodeId = ev->Get()->NodeId;
        LOG_N("Disconnected node " << nodeId);

        for (auto computeActor : PendingComputeActors) {
            if (computeActor.first.NodeId() == nodeId) {
                return ReplyUnavailable(TStringBuilder() << "Connection with node " << nodeId << " lost.");
            }
        }

        for (auto& task : TasksGraph.GetTasks()) {
            if (task.Meta.NodeId == nodeId && PendingComputeTasks.contains(task.Id)) {
                return ReplyUnavailable(TStringBuilder() << "Connection with node " << nodeId << " lost.");
            }
        }
    }

private:
    void FillReadInfo(TTaskMeta& taskMeta, ui64 itemsLimit, bool reverse, bool sorted,
        NKikimr::NMiniKQL::TType* resultType, const TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>& readOlapRange)
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

        taskMeta.ReadInfo.OlapProgram.Program = readOlapRange->GetOlapProgram();

        for (auto& name: readOlapRange->GetOlapProgramParameterNames()) {
            taskMeta.ReadInfo.OlapProgram.ParameterNames.insert(name);
        }
    };

    static ui32 GetMaxTasksPerNodeEstimate(TStageInfo& stageInfo) {
        // TODO: take into account number of active scans on node
        const auto& stage = GetStage(stageInfo);
        const bool heavyProgram = stage.GetProgram().GetSettings().GetHasSort() ||
                            stage.GetProgram().GetSettings().GetHasMapJoin();

        if (heavyProgram) {
            return 4;
        } else {
            return 16;
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
            task.Meta.NodeId = nodeId;
            return task;
        }

        auto& tasks = nodeTasks[nodeId];
        auto& cnt = assignedShardsCount[nodeId];

        const ui32 maxScansPerNode = isOlapScan ? 1 : GetMaxTasksPerNodeEstimate(stageInfo);
        if (cnt < maxScansPerNode) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.NodeId = nodeId;
            tasks.push_back(task.Id);
            ++cnt;
            return task;
        } else {
            ui64 taskIdx = cnt % maxScansPerNode;
            ++cnt;
            return TasksGraph.GetTask(tasks[taskIdx]);
        }
    }

    void BuildScanTasks(TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory,
        const NMiniKQL::TTypeEnvironment& typeEnv)
    {
        THashMap<ui64, std::vector<ui64>> nodeTasks;
        THashMap<ui64, ui64> assignedShardsCount;

        auto& stage = GetStage(stageInfo);

        const auto& table = TableKeys.GetTable(stageInfo.Meta.TableId);
        const auto& keyTypes = table.KeyColumnTypes;

        for (auto& op : stage.GetTableOps()) {
            Y_VERIFY_DEBUG(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto columns = BuildKqpColumns(op, table);
            THashMap<ui64, TShardInfo> partitions = PrunePartitions(TableKeys, op, stageInfo, holderFactory, typeEnv);

            bool reverse = false;
            const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
            ui64 itemsLimit = 0;
            bool sorted = true;
            TString itemsLimitParamName;
            NDqProto::TData itemsLimitBytes;
            NKikimr::NMiniKQL::TType* itemsLimitType = nullptr;
            NKikimr::NMiniKQL::TType* resultType = nullptr;

            // TODO: Support reverse, skipnull and limit for kReadRanges
            if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadRange) {
                ExtractItemsLimit(stageInfo, op.GetReadRange().GetItemsLimit(), holderFactory,
                    typeEnv, itemsLimit, itemsLimitParamName, itemsLimitBytes, itemsLimitType);
                reverse = op.GetReadRange().GetReverse();

                YQL_ENSURE(!reverse); // TODO: not supported yet

                stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(),
                                                   op.GetReadRange().GetSkipNullKeys().end());
            } else if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
                sorted = op.GetReadOlapRange().GetSorted();
                reverse = op.GetReadOlapRange().GetReverse();
                ExtractItemsLimit(stageInfo, op.GetReadOlapRange().GetItemsLimit(), holderFactory, typeEnv,
                    itemsLimit, itemsLimitParamName, itemsLimitBytes, itemsLimitType);
                NKikimrMiniKQL::TType minikqlProtoResultType;
                ConvertYdbTypeToMiniKQLType(op.GetReadOlapRange().GetResultType(), minikqlProtoResultType);
                resultType = ImportTypeFromProto(minikqlProtoResultType, typeEnv);
            }

            for (auto& [shardId, shardInfo] : partitions) {
                YQL_ENSURE(!shardInfo.KeyWriteRanges);

                auto& task = AssignTaskToShard(stageInfo, shardId, nodeTasks, assignedShardsCount, sorted, isOlapScan);

                for (auto& [name, value] : shardInfo.Params) {
                    auto ret = task.Meta.Params.emplace(name, std::move(value));
                    YQL_ENSURE(ret.second);
                    auto typeIterator = shardInfo.ParamTypes.find(name);
                    YQL_ENSURE(typeIterator != shardInfo.ParamTypes.end());
                    auto retType = task.Meta.ParamTypes.emplace(name, typeIterator->second);
                    YQL_ENSURE(retType.second);
                }

                TTaskMeta::TShardReadInfo readInfo = {
                    .Ranges = std::move(*shardInfo.KeyReadRanges), // sorted & non-intersecting
                    .Columns = columns,
                    .ShardId = shardId,
                };

                if (itemsLimitParamName && !task.Meta.Params.contains(itemsLimitParamName)) {
                    task.Meta.Params.emplace(itemsLimitParamName, itemsLimitBytes);
                    task.Meta.ParamTypes.emplace(itemsLimitParamName, itemsLimitType);
                }

                if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
                    const auto& readRange = op.GetReadOlapRange();
                    FillReadInfo(task.Meta, itemsLimit, reverse, sorted, resultType, readRange);
                } else {
                    FillReadInfo(task.Meta, itemsLimit, reverse, sorted, nullptr, TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>());
                }

                if (!task.Meta.Reads) {
                    task.Meta.Reads.ConstructInPlace();
                }

                task.Meta.Reads->emplace_back(std::move(readInfo));
            }
        }

        LOG_D("Stage " << stageInfo.Id << " will be executed on " << nodeTasks.size() << " nodes.");

        for (const auto& pair : nodeTasks) {
            for (const auto& taskIdx : pair.second) {
                auto& task = TasksGraph.GetTask(taskIdx);
                YQL_ENSURE(task.Meta.Reads.Defined());
                auto& taskReads = task.Meta.Reads.GetRef();

                /*
                 * Sort read ranges so that sequential scan of that ranges produce sorted result.
                 *
                 * Partition pruner feed us with set of non-intersecting ranges with filled right boundary.
                 * So we may sort ranges based solely on the their rightmost point.
                 */
                std::sort(taskReads.begin(), taskReads.end(), [&](const auto& lhs, const auto& rhs){
                    if (lhs.ShardId == rhs.ShardId)
                        return false;

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

                LOG_D("Stage " << stageInfo.Id << " create datashard scan task: " << taskIdx
                    << ", node: " << pair.first
                    << ", meta: " << task.Meta.ToString(keyTypes, *AppData()->TypeRegistry));
            }
        }
    }

    void BuildComputeTasks(TStageInfo& stageInfo) {
        auto& stage = GetStage(stageInfo);

        ui32 partitionsCount = 1;
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
                    partitionsCount = std::max(partitionsCount, (ui32)originStageInfo.Tasks.size() / 2);
                    ui32 nodes = ShardsOnNode.size();
                    if (nodes) {
                        // <= 2 tasks on node
                        partitionsCount = std::min(partitionsCount, std::min(24u, nodes * 2));
                    } else {
                        partitionsCount = std::min(partitionsCount, 24u);
                    }
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

        for (ui32 i = 0; i < partitionsCount; ++i) {
            auto& task = TasksGraph.AddTask(stageInfo);
            LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
        }
    }

    void Execute() {
        LWTRACK(KqpScanExecuterStartExecute, ResponseEv->Orbit, TxId);
        auto& funcRegistry = *AppData()->FunctionRegistry;
        NMiniKQL::TScopedAlloc alloc(__LOCATION__, TAlignedPagePoolCounters(), funcRegistry.SupportsSizedAllocators());
        NMiniKQL::TTypeEnvironment typeEnv(alloc);

        NWilson::TSpan prepareTasksSpan(TWilsonKqp::ScanExecuterPrepareTasks, ExecuterStateSpan.GetTraceId(), "PrepareTasks", NWilson::EFlags::AUTO_END);

        NMiniKQL::TMemoryUsageInfo memInfo("PrepareTasks");
        NMiniKQL::THolderFactory holderFactory(alloc.Ref(), memInfo, &funcRegistry);

        auto& tx = Request.Transactions[0];
        for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
            auto& stage = tx.Body->GetStages(stageIdx);
            auto& stageInfo = TasksGraph.GetStageInfo(TStageId(0, stageIdx));

            LOG_D("Stage " << stageInfo.Id << " AST: " << stage.GetProgramAst());

            Y_VERIFY_DEBUG(!stage.GetIsEffectsStage());

            if (stageInfo.Meta.ShardOperations.empty()) {
                BuildComputeTasks(stageInfo);
            } else if (stageInfo.Meta.IsSysView()) {
                BuildSysViewScanTasks(stageInfo, holderFactory, typeEnv);
            } else if (stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()) {
                BuildScanTasks(stageInfo, holderFactory, typeEnv);
            } else {
                YQL_ENSURE(false, "Unexpected stage type " << (int) stageInfo.Meta.TableKind);
            }

            BuildKqpStageChannels(TasksGraph, TableKeys, stageInfo, TxId, AppData()->EnableKqpSpilling);
        }

        BuildKqpExecuterResults(*tx.Body, Results);
        BuildKqpTaskGraphResultChannels(TasksGraph, *tx.Body, 0);

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

        for (auto& task : TasksGraph.GetTasks()) {
            auto& stageInfo = TasksGraph.GetStageInfo(task.StageId);
            auto& stage = GetStage(stageInfo);

            NYql::NDqProto::TDqTask taskDesc;
            taskDesc.SetId(task.Id);
            ActorIdToProto(SelfId(), taskDesc.MutableExecuter()->MutableActorId());

            for (auto& input : task.Inputs) {
                FillInputDesc(*taskDesc.AddInputs(), input);
            }

            for (auto& output : task.Outputs) {
                FillOutputDesc(*taskDesc.AddOutputs(), output);
            }

            taskDesc.MutableProgram()->CopyFrom(stage.GetProgram());
            taskDesc.SetStageId(task.StageId.StageId);

            PrepareKqpTaskParameters(stage, stageInfo, task, taskDesc, typeEnv, holderFactory);

            if (task.Meta.NodeId || stageInfo.Meta.IsSysView()) {
                NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta protoTaskMeta;

                FillTableMeta(stageInfo, protoTaskMeta.MutableTable());

                const auto& tableInfo = TableKeys.GetTable(stageInfo.Meta.TableId);
                for (const auto& keyColumnName : tableInfo.KeyColumns) {
                    const auto& keyColumn = tableInfo.Columns.at(keyColumnName);
                    auto columnType = NScheme::ProtoColumnTypeFromTypeInfo(keyColumn.Type);
                    protoTaskMeta.AddKeyColumnTypes(columnType.TypeId);
                    if (columnType.TypeInfo) {
                        *protoTaskMeta.AddKeyColumnTypeInfos() = *columnType.TypeInfo;
                    } else {
                        *protoTaskMeta.AddKeyColumnTypeInfos() = NKikimrProto::TTypeInfo();
                    }
                }

                switch (tableInfo.TableKind) {
                    case ETableKind::Unknown:
                    case ETableKind::SysView: {
                        protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::CELLVEC);
                        break;
                    }
                    case ETableKind::Datashard: {
                        if (AppData()->FeatureFlags.GetEnableArrowFormatAtDatashard()) {
                            protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::ARROW);
                        } else {
                            protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::CELLVEC);
                        }
                        break;
                    }
                    case ETableKind::Olap: {
                        protoTaskMeta.SetDataFormat(NKikimrTxDataShard::EScanDataFormat::ARROW);
                        break;
                    }
                }

                for (bool skipNullKey : stageInfo.Meta.SkipNullKeys) {
                    protoTaskMeta.AddSkipNullKeys(skipNullKey);
                }

                YQL_ENSURE(task.Meta.Reads);
                YQL_ENSURE(!task.Meta.Writes);

                if (!task.Meta.Reads->empty()) {
                    protoTaskMeta.SetReverse(task.Meta.ReadInfo.Reverse);
                    protoTaskMeta.SetItemsLimit(task.Meta.ReadInfo.ItemsLimit);
                    protoTaskMeta.SetSorted(task.Meta.ReadInfo.Sorted);

                    for (auto columnType : task.Meta.ReadInfo.ResultColumnsTypes) {
                        auto* protoResultColumn = protoTaskMeta.AddResultColumns();
                        protoResultColumn->SetId(0);
                        auto protoColumnType = NScheme::ProtoColumnTypeFromTypeInfo(columnType);
                        protoResultColumn->SetType(protoColumnType.TypeId);
                        if (protoColumnType.TypeInfo) {
                            *protoResultColumn->MutableTypeInfo() = *protoColumnType.TypeInfo;
                        }
                    }

                    if (tableInfo.TableKind == ETableKind::Olap) {
                        auto* olapProgram = protoTaskMeta.MutableOlapProgram();
                        olapProgram->SetProgram(task.Meta.ReadInfo.OlapProgram.Program);

                        auto [schema, parameters] = SerializeKqpTasksParametersForOlap(stage, stageInfo, task,
                            holderFactory, typeEnv);

                        olapProgram->SetParametersSchema(schema);
                        olapProgram->SetParameters(parameters);
                    } else {
                        YQL_ENSURE(task.Meta.ReadInfo.OlapProgram.Program.empty());
                    }

                    for (auto& column : task.Meta.Reads->front().Columns) {
                        auto* protoColumn = protoTaskMeta.AddColumns();
                        protoColumn->SetId(column.Id);
                        auto columnType = NScheme::ProtoColumnTypeFromTypeInfo(column.Type);
                        protoColumn->SetType(columnType.TypeId);
                        if (columnType.TypeInfo) {
                            *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
                        }
                        protoColumn->SetName(column.Name);
                    }
                }

                for (auto& read : *task.Meta.Reads) {
                    auto* protoReadMeta = protoTaskMeta.AddReads();
                    protoReadMeta->SetShardId(read.ShardId);
                    read.Ranges.SerializeTo(protoReadMeta);

                    YQL_ENSURE((int) read.Columns.size() == protoTaskMeta.GetColumns().size());
                    for (ui64 i = 0; i < read.Columns.size(); ++i) {
                        YQL_ENSURE(read.Columns[i].Id == protoTaskMeta.GetColumns()[i].GetId());
                        YQL_ENSURE(read.Columns[i].Type.GetTypeId() == protoTaskMeta.GetColumns()[i].GetType());
                    }

                    nShardScans++;
                    if (Stats) {
                        Stats->AffectedShards.insert(read.ShardId);
                    }
                }

                LOG_D(
                    "task: " << task.Id <<
                    ", node: " << task.Meta.NodeId <<
                    ", meta: " << protoTaskMeta.ShortDebugString()
                );

                taskDesc.MutableMeta()->PackFrom(protoTaskMeta);

                if (stageInfo.Meta.IsSysView()) {
                    computeTasks.emplace_back(std::move(taskDesc));
                } else {
                    scanTasks[task.Meta.NodeId].emplace_back(std::move(taskDesc));
                    nScanTasks++;
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
            << ", snapshot: {" << Request.Snapshot.TxId << ", " << Request.Snapshot.Step << "}");

        ExecuteScanTx(std::move(computeTasks), std::move(scanTasks));

        Become(&TKqpScanExecuter::ExecuteState);
        if (ExecuterStateSpan) {
            ExecuterStateSpan.End();
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ScanExecuterExecuteState, ExecuterSpan.GetTraceId(), "ExecuteState", NWilson::EFlags::AUTO_END);
        }

    }

    void ExecuteScanTx(TVector<NYql::NDqProto::TDqTask>&& computeTasks, THashMap<ui64, TVector<NYql::NDqProto::TDqTask>>&& scanTasks) {
        LWTRACK(KqpScanExecuterStartTasksAndTxs, ResponseEv->Orbit, TxId, computeTasks.size(), scanTasks.size());
        LOG_D("Execute scan tx, computeTasks: " << computeTasks.size() << ", scanTasks: " << scanTasks.size());
        for (const auto& [_, tasks]: scanTasks) {
            for (const auto& task : tasks) {
                PendingComputeTasks.insert(task.GetId());
            }
        }

        for (auto& taskDesc : computeTasks) {
            PendingComputeTasks.insert(taskDesc.GetId());
        }

        auto planner = CreateKqpPlanner(TxId, SelfId(), std::move(computeTasks),
            std::move(scanTasks), Request.Snapshot,
            Database, UserToken, Deadline.GetOrElse(TInstant::Zero()), Request.StatsMode,
            Request.DisableLlvmForUdfStages, Request.LlvmEnabled, AppData()->EnableKqpSpilling, Request.RlPath, ExecuterSpan.GetTraceId());
        RegisterWithSameMailbox(planner);
    }

    void Finalize() {
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(Ydb::StatusIds::SUCCESS);

        TKqpProtoBuilder protoBuilder(*AppData()->FunctionRegistry);

        for (auto& result : Results) {
            auto* protoResult = response.MutableResult()->AddResults();

            if (result.IsStream) {
                // There is no support for multiple streaming results currently
                YQL_ENSURE(Results.size() == 1);
                protoBuilder.BuildStream(result.Data, result.ItemType, result.ResultItemType.Get(), protoResult);
                continue;
            }

            protoBuilder.BuildValue(result.Data, result.ItemType, protoResult);
        }

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

        LWTRACK(KqpScanExecuterFinalize, ResponseEv->Orbit, TxId, LastTaskId, LastComputeActorId, Results.size());

        if (ExecuterSpan) {
            ExecuterSpan.EndOk();
        }

        LOG_D("Sending response to: " << Target);
        Send(Target, ResponseEv.release());
        PassAway();
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
        for (auto channelPair: ResultChannelProxies) {
            LOG_D("terminate result channel " << channelPair.first << " proxy at " << channelPair.second->SelfId());

            TAutoPtr<IEventHandle> ev = new IEventHandle(
                channelPair.second->SelfId(), SelfId(), new TEvents::TEvPoison
            );
            channelPair.second->Receive(ev, TActivationContext::AsActorContext());
        }

        if (KqpShardsResolverId) {
            Send(KqpShardsResolverId, new TEvents::TEvPoison);
        }

        for (auto& [shardId, nodeId] : ShardIdToNodeId) {
            Send(TActivationContext::InterconnectProxy(nodeId), new TEvents::TEvUnsubscribe());
        }

        auto totalTime = TInstant::Now() - StartTime;
        Counters->Counters->ScanTxTotalTimeHistogram->Collect(totalTime.MilliSeconds());

        TBase::PassAway();
    }

private:
    bool CheckExecutionComplete() {
        if (PendingComputeActors.empty() && PendingComputeTasks.empty()) {
            Finalize();
            UpdateResourcesUsage(true);
            return true;
        }

        UpdateResourcesUsage(false);

        if (IsDebugLogEnabled()) {
            TStringBuilder sb;
            sb << "Waiting for: ";
            for (auto ct : PendingComputeTasks) {
                sb << "CT " << ct << ", ";
            }
            for (auto ca : PendingComputeActors) {
                sb << "CA " << ca.first << ", ";
            }
            LOG_D(sb);
        }

        return false;
    }

public:
    void FillEndpointDesc(NYql::NDqProto::TEndpoint& endpoint, const TTask& task) {
        if (task.ComputeActorId) {
            ActorIdToProto(task.ComputeActorId, endpoint.MutableActorId());
        }
    }

    IActor* GetOrCreateChannelProxy(const TChannel& channel) {
        IActor* proxy;

        if (Results[0].IsStream) {
            if (!ResultChannelProxies.empty()) {
                return ResultChannelProxies.begin()->second;
            }

            proxy = CreateResultStreamChannelProxy(TxId, channel.Id, Results[0].ItemType,
                Results[0].ResultItemType.Get(), Target, Stats.get(), SelfId());
        } else {
            YQL_ENSURE(channel.DstInputIndex < Results.size());

            auto channelIt = ResultChannelProxies.find(channel.Id);

            if (channelIt != ResultChannelProxies.end()) {
                return channelIt->second;
            }

            proxy = CreateResultDataChannelProxy(TxId, channel.Id, Stats.get(), SelfId(),
                &Results[channel.DstInputIndex].Data);
        }

        RegisterWithSameMailbox(proxy);
        ResultChannelProxies.emplace(std::make_pair(channel.Id, proxy));

        return proxy;
    }

    void FillChannelDesc(NYql::NDqProto::TChannel& channelDesc, const TChannel& channel) {
        channelDesc.SetId(channel.Id);
        channelDesc.SetSrcTaskId(channel.SrcTask);
        channelDesc.SetDstTaskId(channel.DstTask);

        YQL_ENSURE(channel.SrcTask);
        FillEndpointDesc(*channelDesc.MutableSrcEndpoint(), TasksGraph.GetTask(channel.SrcTask));

        if (channel.DstTask) {
            FillEndpointDesc(*channelDesc.MutableDstEndpoint(), TasksGraph.GetTask(channel.DstTask));
        } else {
            auto proxy = GetOrCreateChannelProxy(channel);
            ActorIdToProto(proxy->SelfId(), channelDesc.MutableDstEndpoint()->MutableActorId());
        }

        channelDesc.SetIsPersistent(IsCrossShardChannel(TasksGraph, channel));
        channelDesc.SetInMemory(channel.InMemory);
    }

private:
    TVector<TKqpExecuterTxResult> Results;
    std::unordered_map<ui64, IActor*> ResultChannelProxies;
    THashSet<ui64> PendingComputeTasks; // Not started yet, waiting resources
    TMap<ui64, ui64> ShardIdToNodeId;
    TMap<ui64, TVector<ui64>> ShardsOnNode;

    ui64 LastTaskId = 0;
    TString LastComputeActorId = "";
};

} // namespace

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters)
{
    return new TKqpScanExecuter(std::move(request), database, userToken, counters);
}

} // namespace NKqp
} // namespace NKikimr
