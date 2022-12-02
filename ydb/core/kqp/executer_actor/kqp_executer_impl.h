#pragma once

#include "kqp_executer.h"
#include "kqp_executer_stats.h"
#include "kqp_partition_helper.h"
#include "kqp_table_resolver.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/wilson.h>
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/kqp/executer_actor/kqp_tasks_graph.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/grpc_services/local_rate_limiter.h>

#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/proto/dq_transport.pb.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/wilson/wilson_span.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/log.h>

#include <util/generic/size_literals.h>

LWTRACE_USING(KQP_PROVIDER);

namespace NKikimr {
namespace NKqp {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "TxId: " << TxId << ". " << stream)

enum class EExecType {
    Data,
    Scan
};

const ui64 MaxTaskSize = 48_MB;

struct TKqpExecuterTxResult {
    NKikimrMiniKQL::TType ItemType;
    TMaybe<NKikimrMiniKQL::TType> ResultItemType;
    TVector<NYql::NDqProto::TData> Data; // used in KqpDataExecuter
    NMiniKQL::TUnboxedValueVector Rows;  // used in KqpLiteralExecuter
    bool IsStream = true;
};

void BuildKqpExecuterResult(const NKqpProto::TKqpPhyResult& txResult, TKqpExecuterTxResult& result);
void BuildKqpExecuterResults(const NKqpProto::TKqpPhyTx& tx, TVector<TKqpExecuterTxResult>& results);

void PrepareKqpTaskParameters(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, const TTask& task,
    NYql::NDqProto::TDqTask& dqTask, const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory);

std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const NKqpProto::TKqpPhyStage& stage,
    const TStageInfo& stageInfo, const TTask& task, const NMiniKQL::THolderFactory& holderFactory,
    const NMiniKQL::TTypeEnvironment& typeEnv);

inline bool IsDebugLogEnabled() {
    return TlsActivationContext->LoggerSettings() &&
           TlsActivationContext->LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER);
}

TActorId ReportToRl(ui64 ru, const TString& database, const TString& userToken,
    const NKikimrKqp::TRlPath& path);

template <class TDerived, EExecType ExecType>
class TKqpExecuterBase : public TActorBootstrapped<TDerived> {
public:
    TKqpExecuterBase(IKqpGateway::TExecPhysicalRequest&& request, const TString& database, const TMaybe<TString>& userToken,
        TKqpRequestCounters::TPtr counters, ui64 spanVerbosity = 0, TString spanName = "no_name")
        : Request(std::move(request))
        , Database(database)
        , UserToken(userToken)
        , Counters(counters)
        , ExecuterSpan(spanVerbosity, std::move(Request.TraceId), spanName)
    {
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>();
        ResponseEv->Orbit = std::move(Request.Orbit);
        Stats = std::make_unique<TQueryExecutionStats>(Request.StatsMode, &TasksGraph,
            ResponseEv->Record.MutableResponse()->MutableResult()->MutableStats());
    }

    void Bootstrap() {
        StartTime = TAppData::TimeProvider->Now();
        if (Request.Timeout) {
            Deadline = StartTime + Request.Timeout;
        }
        if (Request.CancelAfter) {
            CancelAt = StartTime + *Request.CancelAfter;
        }

        LOG_T("Bootstrap done, become ReadyState");
        this->Become(&TKqpExecuterBase::ReadyState);
        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterReadyState, ExecuterSpan.GetTraceId(), "ReadyState", NWilson::EFlags::AUTO_END);
    }

    void ReportEventElapsedTime() {
        if (Stats) {
            ui64 elapsedMicros = TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
            Stats->ExecuterCpuTime += TDuration::MicroSeconds(elapsedMicros);
        }
    }

protected:
    TActorId KqpShardsResolverId;

    STATEFN(WaitResolveState) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvKqpExecuter::TEvTableResolveStatus, HandleResolve);
                hFunc(TEvKqpExecuter::TEvShardsResolveStatus, HandleResolve);
                hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
                hFunc(TEvents::TEvWakeup, HandleTimeout);
                default:
                    UnexpectedEvent("WaitResolveState", ev->GetTypeRewrite());
            }

        } catch (const yexception& e) {
            InternalError(e.what());
        }
        ReportEventElapsedTime();
    }

    void HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        auto& reply = *ev->Get();

        KqpTableResolverId = {};

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            ReplyErrorAndDie(reply.Status, reply.Issues);
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
            auto kqpShardsResolver = CreateKqpShardsResolver(this->SelfId(), TxId, std::move(shardIds));
            KqpShardsResolverId = this->RegisterWithSameMailbox(kqpShardsResolver);
        } else {
            static_cast<TDerived*>(this)->Execute();
        }
    }

    void HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        auto& reply = *ev->Get();

        KqpShardsResolverId = {};

        // TODO: count resolve time in CpuTime

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            LOG_W("Shards nodes resolve failed, status: " << Ydb::StatusIds_StatusCode_Name(reply.Status)
                << ", issues: " << reply.Issues.ToString());
            ReplyErrorAndDie(reply.Status, reply.Issues);
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

        static_cast<TDerived*>(this)->Execute();
    }

    void HandleComputeStats(NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
        TActorId computeActor = ev->Sender;
        auto& state = ev->Get()->Record;
        ui64 taskId = state.GetTaskId();

        LOG_D("Got execution state from compute actor: " << computeActor
            << ", task: " << taskId
            << ", state: " << NYql::NDqProto::EComputeState_Name((NYql::NDqProto::EComputeState) state.GetState())
            << ", stats: " << state.GetStats());

        switch (state.GetState()) {
            case NYql::NDqProto::COMPUTE_STATE_UNKNOWN: {
                YQL_ENSURE(false, "unexpected state from " << computeActor << ", task: " << taskId);
                return;
            }

            case NYql::NDqProto::COMPUTE_STATE_FAILURE: {
                ReplyErrorAndDie(NYql::NDq::DqStatusToYdbStatus(state.GetStatusCode()), state.MutableIssues());
                return;
            }

            case NYql::NDqProto::COMPUTE_STATE_EXECUTING: {
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

            case NYql::NDqProto::COMPUTE_STATE_FINISHED: {
                if (Stats) {
                    Stats->AddComputeActorStats(computeActor.NodeId(), std::move(*state.MutableStats()));
                }

                LastTaskId = taskId;
                LastComputeActorId = computeActor.ToString();

                auto it = PendingComputeActors.find(computeActor);
                if (it == PendingComputeActors.end()) {
                    LOG_W("Got execution state for compute actor: " << computeActor
                        << ", task: " << taskId
                        << ", state: " << NYql::NDqProto::EComputeState_Name((NYql::NDqProto::EComputeState) state.GetState())
                        << ", too early (waiting reply from RM)");

                    if (PendingComputeTasks.erase(taskId)) {
                        LOG_E("Got execution state for compute actor: " << computeActor
                            << ", for unknown task: " << state.GetTaskId()
                            << ", state: " << NYql::NDqProto::EComputeState_Name((NYql::NDqProto::EComputeState) state.GetState()));
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

        static_cast<TDerived*>(this)->CheckExecutionComplete();
    }

    STATEFN(ReadyState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpExecuter::TEvTxRequest, HandleReady);
            default: {
                UnexpectedEvent("ReadyState", ev->GetTypeRewrite());
            }
        }
        ReportEventElapsedTime();
    }

    void HandleReady(TEvKqpExecuter::TEvTxRequest::TPtr& ev) {
        TxId = ev->Get()->Record.GetRequest().GetTxId();
        Target = ActorIdFromProto(ev->Get()->Record.GetTarget());

        LWTRACK(KqpBaseExecuterHandleReady, ResponseEv->Orbit, TxId);

        LOG_D("Report self actorId " << this->SelfId() << " to " << Target);
        auto progressEv = MakeHolder<TEvKqpExecuter::TEvExecuterProgress>();
        ActorIdToProto(this->SelfId(), progressEv->Record.MutableExecuterActorId());
        this->Send(Target, progressEv.Release());

        const auto now = TAppData::TimeProvider->Now();
        const auto& ctx = TlsActivationContext->AsActorContext();
        TMaybe<TDuration> timeout;
        if (Deadline) {
            timeout = *Deadline - now;
            DeadlineActor = CreateLongTimer(ctx, *timeout,
                new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup(0)));
        }

        TMaybe<TDuration> cancelAfter;
        if (CancelAt) {
            cancelAfter = *CancelAt - now;
            CancelAtActor = CreateLongTimer(ctx, *cancelAfter,
                new IEventHandle(this->SelfId(), this->SelfId(), new TEvents::TEvWakeup(1)));
        }

        LOG_I("Begin execution. Operation timeout: " << timeout << ", cancelAfter: " << cancelAfter
            << ", txs: " << Request.Transactions.size());

        if (IsDebugLogEnabled()) {
            for (auto& tx : Request.Transactions) {
                LOG_D("Executing physical tx, type: " << (ui32) tx.Body->GetType() << ", stages: " << tx.Body->StagesSize());
            }
        }

        auto kqpTableResolver = CreateKqpTableResolver(this->SelfId(), TxId, UserToken, Request.Transactions,
            TableKeys, TasksGraph);
        KqpTableResolverId = this->RegisterWithSameMailbox(kqpTableResolver);

        LOG_T("Got request, become WaitResolveState");
        this->Become(&TKqpExecuterBase::WaitResolveState);
        if (ExecuterStateSpan) {
            ExecuterStateSpan.End();
            ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterWaitResolveState, ExecuterSpan.GetTraceId(), "WaitResolveState", NWilson::EFlags::AUTO_END);
        }

        ExecuterTableResolveSpan = NWilson::TSpan(TWilsonKqp::ExecuterTableResolve, ExecuterStateSpan.GetTraceId(), "ExecuterTableResolve", NWilson::EFlags::AUTO_END);

        StartResolveTime = now;

        if (Stats) {
            Stats->StartTs = now;
        }
    }

    TMaybe<size_t> FindReadRangesSource(const NKqpProto::TKqpPhyStage& stage) {
        TMaybe<size_t> res;
        for (size_t i = 0; i < stage.SourcesSize(); ++i) {
            auto& source = stage.GetSources(i);
            if (source.HasReadRangesSource()) {
                YQL_ENSURE(!res);
                res = i;

            }
        }
        return res;
    }

protected:
    bool CheckExecutionComplete() {
        if (PendingComputeActors.empty() && PendingComputeTasks.empty()) {
            static_cast<TDerived*>(this)->Finalize();
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

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
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

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
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

    void HandleStartKqpTasksResponse(TEvKqpNode::TEvStartKqpTasksResponse::TPtr& ev) {
        auto& record = ev->Get()->Record;
        YQL_ENSURE(record.GetTxId() == TxId);

        if (record.NotStartedTasksSize() != 0) {
            auto reason = record.GetNotStartedTasks()[0].GetReason();
            auto& message = record.GetNotStartedTasks()[0].GetMessage();

            LOG_E("Stop executing, reason: " << NKikimrKqp::TEvStartKqpTasksResponse_ENotStartedTaskReason_Name(reason)
                << ", message: " << message);

            switch (reason) {
                case NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY: {
                    ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_OVERLOADED, "Not enough memory to execute query"));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS: {
                    ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_OVERLOADED, "Not enough computation units to execute query"));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED: {
                    ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, "Memory limit exceeded"));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::QUERY_EXECUTION_UNITS_LIMIT_EXCEEDED: {
                    ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                         YqlIssue({}, NYql::TIssuesIds::KIKIMR_OVERLOADED, "Not enough computation units to execute query"));
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

    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        NYql::TIssues issues = ev->Get()->GetIssues();
        LOG_D("Got EvAbortExecution, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(msg.GetStatusCode())
            << ", message: " << issues.ToOneLineString());
        auto statusCode = NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode());
        if (statusCode == Ydb::StatusIds::INTERNAL_ERROR) {
            InternalError(issues);
        } else if (statusCode == Ydb::StatusIds::TIMEOUT) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::TIMEOUT, "Request timeout exceeded");

            if (ExecuterSpan) {
                ExecuterSpan.EndError("timeout");
            }

            this->Send(Target, abortEv.Release());

            TerminateComputeActors(Ydb::StatusIds::TIMEOUT, "timeout");
            this->PassAway();
        } else {
            RuntimeError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), issues);
        }
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr& ev) {
        bool cancel = ev->Get()->Tag == 1;
        LOG_I((cancel ? "CancelAt" : "Timeout") << " exceeded. Send timeout event to the rpc actor " << Target);

        if (cancel) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, "Request timeout exceeded");

            if (ExecuterSpan) {
                ExecuterSpan.EndError("timeout");
            }

            this->Send(Target, abortEv.Release());
            CancelAtActor = {};
        } else {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::TIMEOUT, "Request timeout exceeded");

            if (ExecuterSpan) {
                ExecuterSpan.EndError("timeout");
            }

            this->Send(Target, abortEv.Release());
            DeadlineActor = {};
        }

        TerminateComputeActors(Ydb::StatusIds::TIMEOUT, "timeout");
        this->PassAway();
    }

protected:
    void CollectTaskChannelsUpdates(const TKqpTasksGraph::TTaskType& task, THashMap<TActorId, THashSet<ui64>>& updates) {
        YQL_ENSURE(task.ComputeActorId);

        LOG_T("Collect channels updates for task: " << task.Id << " at actor " << task.ComputeActorId);

        auto& selfUpdates = updates[task.ComputeActorId];

        for (auto& input : task.Inputs) {
            for (auto channelId : input.Channels) {
                auto& channel = TasksGraph.GetChannel(channelId);
                YQL_ENSURE(channel.DstTask == task.Id);
                YQL_ENSURE(channel.SrcTask);

                auto& srcTask = TasksGraph.GetTask(channel.SrcTask);
                if (srcTask.ComputeActorId) {
                    updates[srcTask.ComputeActorId].emplace(channelId);
                    selfUpdates.emplace(channelId);
                }

                LOG_T("Task: " << task.Id << ", input channelId: " << channelId << ", src task: " << channel.SrcTask
                    << ", at actor " << srcTask.ComputeActorId);
            }
        }

        for (auto& output : task.Outputs) {
            for (auto channelId : output.Channels) {
                selfUpdates.emplace(channelId);

                auto& channel = TasksGraph.GetChannel(channelId);
                YQL_ENSURE(channel.SrcTask == task.Id);

                if (channel.DstTask) {
                    auto& dstTask = TasksGraph.GetTask(channel.DstTask);
                    if (dstTask.ComputeActorId) {
                        // not a optimal solution
                        updates[dstTask.ComputeActorId].emplace(channelId);
                    }

                    LOG_T("Task: " << task.Id << ", output channelId: " << channelId << ", dst task: " << channel.DstTask
                        << ", at actor " << dstTask.ComputeActorId);
                }
            }
        }
    }

    void PropagateChannelsUpdates(const THashMap<TActorId, THashSet<ui64>>& updates) {
        for (auto& pair : updates) {
            auto computeActorId = pair.first;
            auto& channelIds = pair.second;

            auto channelsInfoEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelsInfo>();
            auto& record = channelsInfoEv->Record;

            for (auto& channelId : channelIds) {
                static_cast<TDerived*>(this)->FillChannelDesc(*record.AddUpdate(), TasksGraph.GetChannel(channelId));
            }

            LOG_T("Sending channels info to compute actor: " << computeActorId << ", channels: " << channelIds.size());
            bool sent = this->Send(computeActorId, channelsInfoEv.Release());
            YQL_ENSURE(sent, "Failed to send event to " << computeActorId.ToString());
        }
    }

    void UpdateResourcesUsage(bool force) {
        TInstant now = TActivationContext::Now();
        if ((now - LastResourceUsageUpdate < ResourceUsageUpdateInterval) && !force)
            return;

        LastResourceUsageUpdate = now;

        TProgressStat::TEntry consumption;
        for (const auto& p : PendingComputeActors) {
            const auto& t = p.second.GetLastUsage();
            consumption += t;
        }

        for (const auto& p : LastStats) {
            const auto& t = p.GetLastUsage();
            consumption += t;
        }

        auto ru = NRuCalc::CalcRequestUnit(consumption);

        // Some heuristic to reduce overprice due to round part stats
        if (ru <= 100 && !force)
            return;

        for (auto& p : PendingComputeActors) {
            p.second.Update();
        }

        for (auto& p : LastStats) {
            p.Update();
        }

        if (Request.RlPath) {
            auto actorId = ReportToRl(ru, Database, UserToken.GetOrElse(""),
                Request.RlPath.GetRef());

            LOG_D("Resource usage for last stat interval: " << consumption
                  << " ru: " << ru << " rl path: " << Request.RlPath.GetRef()
                  << " rl actor: " << actorId
                  << " force flag: " << force);
        } else {
            LOG_D("Resource usage for last stat interval: " << consumption
                  << " ru: " << ru << " rate limiter was not found"
                  << " force flag: " << force);
        }
    }



protected:
    void BuildSysViewScanTasks(TStageInfo& stageInfo, const NMiniKQL::THolderFactory& holderFactory,
        const NMiniKQL::TTypeEnvironment& typeEnv)
    {
        Y_VERIFY_DEBUG(stageInfo.Meta.IsSysView());

        auto& stage = GetStage(stageInfo);

        const auto& table = TableKeys.GetTable(stageInfo.Meta.TableId);
        const auto& keyTypes = table.KeyColumnTypes;

        for (auto& op : stage.GetTableOps()) {
            Y_VERIFY_DEBUG(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto& task = TasksGraph.AddTask(stageInfo);
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
                    keyRanges.CopyFrom(FillReadRanges(keyTypes, op.GetReadRanges(), stageInfo, holderFactory, typeEnv));
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected table scan operation: " << (ui32) op.GetTypeCase());
            }

            TTaskMeta::TShardReadInfo readInfo = {
                .Ranges = std::move(keyRanges),
                .Columns = BuildKqpColumns(op, table),
            };

            task.Meta.Reads.ConstructInPlace();
            task.Meta.Reads->emplace_back(std::move(readInfo));
            task.Meta.ReadInfo.Reverse = op.GetReadRange().GetReverse();

            LOG_D("Stage " << stageInfo.Id << " create sysview scan task: " << task.Id);
        }
    }

protected:
    // in derived classes
    // void FillEndpointDesc(NYql::NDqProto::TEndpoint& endpoint, const TTask& task);
    // void FillChannelDesc(NYql::NDqProto::TChannel& channelDesc, const TChannel& channel);

    void FillInputDesc(NYql::NDqProto::TTaskInput& inputDesc, const TTaskInput& input) {
        switch (input.Type()) {
            case NYql::NDq::TTaskInputType::Source:
                inputDesc.MutableSource()->SetType(input.SourceType);
                inputDesc.MutableSource()->SetWatermarksMode(input.WatermarksMode);
                inputDesc.MutableSource()->MutableSettings()->CopyFrom(*input.SourceSettings);
                break;
            case NYql::NDq::TTaskInputType::UnionAll: {
                inputDesc.MutableUnionAll();
                break;
            }
            case NYql::NDq::TTaskInputType::Merge: {
                auto& mergeProto = *inputDesc.MutableMerge();
                YQL_ENSURE(std::holds_alternative<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo));
                auto& sortColumns = std::get<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo).SortColumns;
                for (const auto& sortColumn : sortColumns) {
                    auto newSortCol = mergeProto.AddSortColumns();
                    newSortCol->SetColumn(sortColumn.Column.c_str());
                    newSortCol->SetAscending(sortColumn.Ascending);
                }
                break;
            }
            default:
                YQL_ENSURE(false, "Unexpected task input type: " << (int) input.Type() << Endl << this->DebugString());
        }

        for (ui64 channel : input.Channels) {
            auto& channelDesc = *inputDesc.AddChannels();
            static_cast<TDerived*>(this)->FillChannelDesc(channelDesc, TasksGraph.GetChannel(channel));
        }

        if (input.Transform) {
            auto* transformProto = inputDesc.MutableTransform();
            transformProto->SetType(input.Transform->Type);
            transformProto->SetInputType(input.Transform->InputType);
            transformProto->SetOutputType(input.Transform->OutputType);
            *transformProto->MutableSettings() = input.Transform->Settings;
        }
    }

    void FillOutputDesc(NYql::NDqProto::TTaskOutput& outputDesc, const TTaskOutput& output) {
        switch (output.Type) {
            case TTaskOutputType::Map:
                YQL_ENSURE(output.Channels.size() == 1, "" << this->DebugString());
                outputDesc.MutableMap();
                break;

            case TTaskOutputType::HashPartition: {
                auto& hashPartitionDesc = *outputDesc.MutableHashPartition();
                for (auto& column : output.KeyColumns) {
                    hashPartitionDesc.AddKeyColumns(column);
                }
                hashPartitionDesc.SetPartitionsCount(output.PartitionsCount);
                break;
            }

            case TKqpTaskOutputType::ShardRangePartition: {
                auto& rangePartitionDesc = *outputDesc.MutableRangePartition();
                auto& columns = *rangePartitionDesc.MutableKeyColumns();
                for (auto& column : output.KeyColumns) {
                    *columns.Add() = column;
                }

                auto& partitionsDesc = *rangePartitionDesc.MutablePartitions();
                for (auto& pair : output.Meta.ShardPartitions) {
                    auto& range = *pair.second->Range;
                    auto& partitionDesc = *partitionsDesc.Add();
                    partitionDesc.SetEndKeyPrefix(range.EndKeyPrefix.GetBuffer());
                    partitionDesc.SetIsInclusive(range.IsInclusive);
                    partitionDesc.SetIsPoint(range.IsPoint);
                    partitionDesc.SetChannelId(pair.first);
                }
                break;
            }

            case TTaskOutputType::Broadcast: {
                outputDesc.MutableBroadcast();
                break;
            }

            case TTaskOutputType::Effects: {
                outputDesc.MutableEffects();
                break;
            }

            default: {
                YQL_ENSURE(false, "Unexpected task output type " << output.Type << Endl << this->DebugString());
            }
        }

        for (auto& channel : output.Channels) {
            auto& channelDesc = *outputDesc.AddChannels();
            static_cast<TDerived*>(this)->FillChannelDesc(channelDesc, TasksGraph.GetChannel(channel));
        }
    }

    void FillTableMeta(const TStageInfo& stageInfo, NKikimrTxDataShard::TKqpTransaction_TTableMeta* meta) {
        meta->SetTablePath(stageInfo.Meta.TablePath);
        meta->MutableTableId()->SetTableId(stageInfo.Meta.TableId.PathId.LocalPathId);
        meta->MutableTableId()->SetOwnerId(stageInfo.Meta.TableId.PathId.OwnerId);
        meta->SetSchemaVersion(stageInfo.Meta.TableId.SchemaVersion);
        meta->SetSysViewInfo(stageInfo.Meta.TableId.SysViewInfo);
        meta->SetTableKind((ui32)stageInfo.Meta.TableKind);
    }

protected:
    void TerminateComputeActors(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        for (const auto& task : this->TasksGraph.GetTasks()) {
            if (task.ComputeActorId) {
                LOG_I("aborting compute actor execution, message: " << issues.ToOneLineString()
                    << ", compute actor: " << task.ComputeActorId << ", task: " << task.Id);

                auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDq::YdbStatusToDqStatus(code), issues);
                this->Send(task.ComputeActorId, ev.Release());
            } else {
                LOG_I("task: " << task.Id << ", does not have Compute ActorId yet");
            }
        }
    }

    void TerminateComputeActors(Ydb::StatusIds::StatusCode code, const TString& message) {
        TerminateComputeActors(code, NYql::TIssues({NYql::TIssue(message)}));
    }

protected:
    void UnexpectedEvent(const TString& state, ui32 eventType) {
        LOG_C("TKqpExecuter, unexpected event: " << eventType << ", at state:" << state << ", selfID: " << this->SelfId());
        InternalError(TStringBuilder() << "Unexpected event at TKqpScanExecuter, state: " << state
            << ", event: " << eventType);
    }

    void InternalError(const NYql::TIssues& issues) {
        LOG_E(issues.ToOneLineString());
        TerminateComputeActors(Ydb::StatusIds::INTERNAL_ERROR, issues);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::UNEXPECTED, "Internal error while executing transaction.");
        for (const NYql::TIssue& i : issues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }
        ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, issue);
    }

    void InternalError(const TString& message) {
        InternalError(NYql::TIssues({NYql::TIssue(message)}));
    }

    void ReplyUnavailable(const TString& message) {
        LOG_E("UNAVAILABLE: " << message);
        TerminateComputeActors(Ydb::StatusIds::UNAVAILABLE, message);
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
        issue.AddSubIssue(new NYql::TIssue(message));
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
    }

    void RuntimeError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        LOG_E(Ydb::StatusIds_StatusCode_Name(code) << ": " << issues.ToOneLineString());
        TerminateComputeActors(code, issues);
        ReplyErrorAndDie(code, issues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> protoIssues;
        IssuesToMessage(issues, &protoIssues);
        ReplyErrorAndDie(status, &protoIssues);
    }

    void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue) {
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage> issues;
        IssueToMessage(issue, issues.Add());
        ReplyErrorAndDie(status, &issues);
    }

    virtual void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        for (auto computeActor : PendingComputeActors) {
            LOG_D("terminate compute actor " << computeActor.first);

            auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDq::YdbStatusToDqStatus(status), "Terminate execution");
            this->Send(computeActor.first, ev.Release());
        }

        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        LOG_T("ReplyErrorAndDie. " << response.DebugString());

        if constexpr (ExecType == EExecType::Data) {
            if (status != Ydb::StatusIds::SUCCESS) {
                Counters->TxProxyMon->ReportStatusNotOK->Inc();
            } else {
                Counters->TxProxyMon->ReportStatusOK->Inc();
            }
        }

        LWTRACK(KqpBaseExecuterReplyErrorAndDie, ResponseEv->Orbit, TxId);

        if (ExecuterSpan) {
            ExecuterSpan.EndError(response.DebugString());
        }

        this->Send(Target, ResponseEv.release());
        this->PassAway();
    }

protected:
    template <class TCollection>
    bool ValidateTaskSize(const TCollection& tasks) {
        for (const auto& task : tasks) {
            if (ui32 size = task.ByteSize(); size > MaxTaskSize) {
                LOG_E("Abort execution. Task #" << task.GetId() << " size is too big: " << size << " > " << MaxTaskSize);
                ReplyErrorAndDie(Ydb::StatusIds::ABORTED,
                    MakeIssue(NKikimrIssues::TIssuesIds::SHARD_PROGRAM_SIZE_EXCEEDED, TStringBuilder() <<
                        "Datashard program size limit exceeded (" << size << " > " << MaxTaskSize << ")"));
                return false;
            }
        }
        return true;
    }

protected:
    void PassAway() override {
        LOG_D("terminate execution.");
        if (DeadlineActor) {
            this->Send(DeadlineActor, new TEvents::TEvPoison);
        }
        if (CancelAtActor) {
            this->Send(CancelAtActor, new TEvents::TEvPoison);
        }
        if (KqpTableResolverId) {
            this->Send(KqpTableResolverId, new TEvents::TEvPoison);
            this->Send(this->SelfId(), new TEvents::TEvPoison);
            LOG_T("Terminate, become ZombieState");
            this->Become(&TKqpExecuterBase::ZombieState);
            if (ExecuterStateSpan) {
                ExecuterStateSpan.End();
                ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterZombieState, ExecuterSpan.GetTraceId(), "ZombieState", NWilson::EFlags::AUTO_END);
            }
        } else {
            IActor::PassAway();
        }
    }

    STATEFN(ZombieState) {
        if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType) {
            IActor::PassAway();
        }
    }

protected:
    TString DebugString() const {
        TStringBuilder sb;
        sb << "[KqpExecuter], type: " << (ExecType == EExecType::Data ? "Data" : "Scan")
           << ", Database: " << Database << ", TxId: " << TxId << ", TxCnt: " << Request.Transactions.size()
           << ", Transactions: " << Endl;
        for (const auto& tx : Request.Transactions) {
            sb << "tx: " << tx.Body->DebugString() << Endl;
        }
        return std::move(sb);
    }

protected:
    IKqpGateway::TExecPhysicalRequest Request;
    const TString Database;
    const TMaybe<TString> UserToken;
    TKqpRequestCounters::TPtr Counters;
    std::unique_ptr<TQueryExecutionStats> Stats;
    TInstant StartTime;
    TMaybe<TInstant> Deadline;
    TActorId DeadlineActor;
    TMaybe<TInstant> CancelAt;
    TActorId CancelAtActor;
    TActorId Target;
    ui64 TxId = 0;

    TKqpTasksGraph TasksGraph;
    TKqpTableKeys TableKeys;

    TActorId KqpTableResolverId;
    THashMap<TActorId, TProgressStat> PendingComputeActors; // Running compute actors (pure and DS)
    TVector<TProgressStat> LastStats;

    TInstant StartResolveTime;
    TInstant LastResourceUsageUpdate;

    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    NWilson::TSpan ExecuterSpan;
    NWilson::TSpan ExecuterStateSpan;
    NWilson::TSpan ExecuterTableResolveSpan;

    THashSet<ui64> PendingComputeTasks; // Not started yet, waiting resources
    TMap<ui64, ui64> ShardIdToNodeId;
    TMap<ui64, TVector<ui64>> ShardsOnNode;

    ui64 LastTaskId = 0;
    TString LastComputeActorId = "";
private:
    static constexpr TDuration ResourceUsageUpdateInterval = TDuration::MilliSeconds(100);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpLiteralExecuter(IKqpGateway::TExecPhysicalRequest&& request, TKqpRequestCounters::TPtr counters);

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters);

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters);

} // namespace NKqp
} // namespace NKikimr
