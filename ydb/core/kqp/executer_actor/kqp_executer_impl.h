#pragma once

#include "kqp_executer.h"
#include "kqp_executer_stats.h"
#include "kqp_planner.h"
#include "kqp_partition_helper.h"
#include "kqp_result_channel.h"
#include "kqp_table_resolver.h"
#include "kqp_shards_resolver.h"

#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/kqp/executer_actor/kqp_tasks_graph.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/grpc_services/local_rate_limiter.h>

#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>

#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/dq/common/dq_serialized_batch.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>


#include <util/generic/size_literals.h>


LWTRACE_USING(KQP_PROVIDER);

namespace NKikimr {
namespace NKqp {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext,  NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext,   NKikimrServices::KQP_EXECUTER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << stream)

using EExecType = TEvKqpExecuter::TEvTxResponse::EExecutionType;

const ui64 MaxTaskSize = 48_MB;
constexpr ui64 PotentialUnsigned64OverflowLimit = (std::numeric_limits<ui64>::max() >> 1);

std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const TStageInfo& stageInfo, const TTask& task);

inline bool IsDebugLogEnabled() {
    return TlsActivationContext->LoggerSettings() &&
           TlsActivationContext->LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER);
}

struct TShardRangesWithShardId {
    TMaybe<ui64> ShardId;
    const TShardKeyRanges* Ranges;
};


TActorId ReportToRl(ui64 ru, const TString& database, const TString& userToken,
    const NKikimrKqp::TRlPath& path);

struct TEvPrivate {
    enum EEv {
        EvRetry = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvResourcesSnapshot,
        EvReattachToShard,
    };

    struct TEvRetry : public TEventLocal<TEvRetry, EEv::EvRetry> {
        ui32 RequestId;
        TActorId Target;

        TEvRetry(ui64 requestId, const TActorId& target)
            : RequestId(requestId)
            , Target(target) {}
    };

    struct TEvResourcesSnapshot : public TEventLocal<TEvResourcesSnapshot, EEv::EvResourcesSnapshot> {
        TVector<NKikimrKqp::TKqpNodeResources> Snapshot;

        TEvResourcesSnapshot(TVector<NKikimrKqp::TKqpNodeResources>&& snapshot)
            : Snapshot(std::move(snapshot)) {}
    };

    struct TEvReattachToShard : public TEventLocal<TEvReattachToShard, EvReattachToShard> {
        const ui64 TabletId;

        explicit TEvReattachToShard(ui64 tabletId)
            : TabletId(tabletId) {}
    };
};

template <class TDerived, EExecType ExecType>
class TKqpExecuterBase : public TActorBootstrapped<TDerived> {
    static_assert(ExecType == EExecType::Data || ExecType == EExecType::Scan);
public:
    TKqpExecuterBase(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        TKqpRequestCounters::TPtr counters,
        const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
        const NKikimrConfig::TTableServiceConfig::EChannelTransportVersion chanTransportVersion,
        const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregation,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        ui32 statementResultIndex, ui64 spanVerbosity = 0, TString spanName = "KqpExecuterBase", bool streamResult = false)
        : Request(std::move(request))
        , Database(database)
        , UserToken(userToken)
        , Counters(counters)
        , ExecuterSpan(spanVerbosity, std::move(Request.TraceId), spanName)
        , Planner(nullptr)
        , ExecuterRetriesConfig(executerRetriesConfig)
        , AggregationSettings(aggregation)
        , HasOlapTable(false)
        , StreamResult(streamResult)
        , StatementResultIndex(statementResultIndex)
    {
        TasksGraph.GetMeta().Snapshot = IKqpGateway::TKqpSnapshot(Request.Snapshot.Step, Request.Snapshot.TxId);
        TasksGraph.GetMeta().Arena = MakeIntrusive<NActors::TProtoArenaHolder>();
        TasksGraph.GetMeta().Database = Database;
        TasksGraph.GetMeta().ChannelTransportVersion = chanTransportVersion;
        TasksGraph.GetMeta().UserRequestContext = userRequestContext;
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(Request.TxAlloc, ExecType);
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
    }

    TActorId SelfId() {
       return TActorBootstrapped<TDerived>::SelfId();
    }

    TString BuildMemoryLimitExceptionMessage() const {
        if (Request.TxAlloc) {
            return TStringBuilder() << "Memory limit exception at " << CurrentStateFuncName()
                << ", current limit is " << Request.TxAlloc->Alloc->GetLimit() << " bytes.";
        }
        return TStringBuilder() << "Memory limit exception at " << CurrentStateFuncName();
    }

    void ReportEventElapsedTime() {
        if (Stats) {
            ui64 elapsedMicros = TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
            Stats->ExecuterCpuTime += TDuration::MicroSeconds(elapsedMicros);
        }
    }

protected:
    const NMiniKQL::TTypeEnvironment& TypeEnv() {
        return Request.TxAlloc->TypeEnv;
    }

    const NMiniKQL::THolderFactory& HolderFactory() {
        return Request.TxAlloc->HolderFactory;
    }

    [[nodiscard]]
    bool HandleResolve(TEvKqpExecuter::TEvTableResolveStatus::TPtr& ev) {
        auto& reply = *ev->Get();

        KqpTableResolverId = {};

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            ExecuterStateSpan.EndError(TStringBuilder() << Ydb::StatusIds_StatusCode_Name(reply.Status));
            ReplyErrorAndDie(reply.Status, reply.Issues);
            return false;
        }

        ExecuterStateSpan.EndOk();

        return true;
    }

    [[nodiscard]]
    bool HandleResolve(TEvKqpExecuter::TEvShardsResolveStatus::TPtr& ev) {
        auto& reply = *ev->Get();

        KqpShardsResolverId = {};

        // TODO: count resolve time in CpuTime

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            ExecuterStateSpan.EndError(Ydb::StatusIds_StatusCode_Name(reply.Status));

            LOG_W("Shards nodes resolve failed, status: " << Ydb::StatusIds_StatusCode_Name(reply.Status)
                << ", issues: " << reply.Issues.ToString());
            ReplyErrorAndDie(reply.Status, reply.Issues);
            return false;
        }
        ExecuterStateSpan.EndOk();

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
        return true;
    }

    struct TEvComputeChannelDataOOB {
        NYql::NDqProto::TEvComputeChannelData Proto;
        TRope Payload;

        size_t Size() const {
            return Proto.GetChannelData().GetData().GetRaw().size() + Payload.size();
        }

        ui32 RowCount() const {
            return Proto.GetChannelData().GetData().GetRows();
        }
    };

    void HandleChannelData(NYql::NDq::TEvDqCompute::TEvChannelData::TPtr& ev) {
        auto& record = ev->Get()->Record;
        auto& channelData = record.GetChannelData();
        auto& channel = TasksGraph.GetChannel(channelData.GetChannelId());
        auto& task = TasksGraph.GetTask(channel.SrcTask);
        const TActorId channelComputeActorId = ev->Sender;

        auto& txResult = ResponseEv->TxResults[channel.DstInputIndex];
        auto [it, _] = ResultChannelToComputeActor.emplace(channel.Id, ev->Sender);
        YQL_ENSURE(it->second == channelComputeActorId);

        if (StreamResult && txResult.IsStream && txResult.QueryResultIndex.Defined()) {

            TEvComputeChannelDataOOB computeData;
            computeData.Proto = std::move(ev->Get()->Record);
            if (computeData.Proto.GetChannelData().GetData().HasPayloadId()) {
                computeData.Payload = ev->Get()->GetPayload(computeData.Proto.GetChannelData().GetData().GetPayloadId());
            }

            const bool trailingResults = (
                computeData.Proto.GetChannelData().GetFinished() &&
                Request.IsTrailingResultsAllowed());

            TVector<NYql::NDq::TDqSerializedBatch> batches(1);
            auto& batch = batches.front();

            batch.Proto = std::move(*computeData.Proto.MutableChannelData()->MutableData());
            batch.Payload = std::move(computeData.Payload);

            TKqpProtoBuilder protoBuilder{*AppData()->FunctionRegistry};
            auto resultSet = protoBuilder.BuildYdbResultSet(std::move(batches), txResult.MkqlItemType, txResult.ColumnOrder);

            if (!trailingResults) {
                auto streamEv = MakeHolder<TEvKqpExecuter::TEvStreamData>();
                streamEv->Record.SetSeqNo(computeData.Proto.GetSeqNo());
                streamEv->Record.SetQueryResultIndex(*txResult.QueryResultIndex + StatementResultIndex);
                streamEv->Record.SetChannelId(channel.Id);
                streamEv->Record.MutableResultSet()->Swap(&resultSet);

                LOG_D("Send TEvStreamData to " << Target << ", seqNo: " << streamEv->Record.GetSeqNo()
                    << ", nRows: " << streamEv->Record.GetResultSet().rows().size());

                this->Send(Target, streamEv.Release());

            } else {
                auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();
                ackEv->Record.SetSeqNo(computeData.Proto.GetSeqNo());
                ackEv->Record.SetChannelId(channel.Id);
                ackEv->Record.SetFreeSpace(50_MB);
                this->Send(channelComputeActorId, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ channel.Id);
                txResult.TrailingResult.Swap(&resultSet);
                txResult.HasTrailingResult = true;
                LOG_D("staging TEvStreamData to " << Target << ", seqNo: " << computeData.Proto.GetSeqNo()
                    << ", nRows: " << txResult.TrailingResult.rows().size());
            }

            return;
        }

        NYql::NDq::TDqSerializedBatch batch;
        batch.Proto = std::move(*record.MutableChannelData()->MutableData());
        if (batch.Proto.HasPayloadId()) {
            batch.Payload = ev->Get()->GetPayload(batch.Proto.GetPayloadId());
        }

        YQL_ENSURE(channel.DstTask == 0);

        if (Stats) {
            Stats->ResultBytes += batch.Size();
            Stats->ResultRows += batch.RowCount();
        }

        LOG_T("Got result, channelId: " << channel.Id << ", shardId: " << task.Meta.ShardId
            << ", inputIndex: " << channel.DstInputIndex << ", from: " << ev->Sender
            << ", finished: " << channelData.GetFinished());

        ResponseEv->TakeResult(channel.DstInputIndex, std::move(batch));
        LOG_T("Send ack to channelId: " << channel.Id << ", seqNo: " << record.GetSeqNo() << ", to: " << ev->Sender);

        auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();
        ackEv->Record.SetSeqNo(record.GetSeqNo());
        ackEv->Record.SetChannelId(channel.Id);
        ackEv->Record.SetFreeSpace(50_MB);
        this->Send(channelComputeActorId, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ channel.Id);
    }

    void HandleStreamAck(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        ui64 channelId;
        if (ResponseEv->TxResults.size() == 1) {
            channelId = ResultChannelToComputeActor.begin()->first;
        } else {
            channelId = ev->Get()->Record.GetChannelId();
        }

        auto it = ResultChannelToComputeActor.find(channelId);
        YQL_ENSURE(it != ResultChannelToComputeActor.end());
        const auto channelComputeActorId = it->second;

        ui64 seqNo = ev->Get()->Record.GetSeqNo();
        i64 freeSpace = ev->Get()->Record.GetFreeSpace();

        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_EXECUTER, "TxId: " << TxId
            << ", send ack to channelId: " << channelId
            << ", seqNo: " << seqNo
            << ", enough: " << ev->Get()->Record.GetEnough()
            << ", freeSpace: " << freeSpace
            << ", to: " << channelComputeActorId);

        auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();
        ackEv->Record.SetSeqNo(seqNo);
        ackEv->Record.SetChannelId(channelId);
        ackEv->Record.SetFreeSpace(freeSpace);
        ackEv->Record.SetFinish(ev->Get()->Record.GetEnough());
        this->Send(channelComputeActorId, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ channelId);
    }

    void HandleComputeStats(NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
        TActorId computeActor = ev->Sender;
        auto& state = ev->Get()->Record;
        ui64 taskId = state.GetTaskId();

        LOG_D("ActorState: " << CurrentStateFuncName()
            << ", got execution state from compute actor: " << computeActor
            << ", task: " << taskId
            << ", state: " << NYql::NDqProto::EComputeState_Name((NYql::NDqProto::EComputeState) state.GetState())
            << ", stats: " << state.GetStats());

        if (Stats && state.HasStats() && Request.ProgressStatsPeriod) {
            Stats->UpdateTaskStats(taskId, state.GetStats());
            auto now = TInstant::Now();
            if (LastProgressStats + Request.ProgressStatsPeriod <= now) {
                auto progress = MakeHolder<TEvKqpExecuter::TEvExecuterProgress>();
                auto& execStats = *progress->Record.MutableQueryStats()->AddExecutions();
                Stats->ExportExecStats(execStats);
                for (ui32 txId = 0; txId < Request.Transactions.size(); ++txId) {
                    const auto& tx = Request.Transactions[txId].Body;
                    auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), execStats);
                    execStats.AddTxPlansWithStats(planWithStats);
                }
                this->Send(Target, progress.Release());
                LastProgressStats = now;
            }
        }

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
                if (Planner) {
                    if (Planner->GetPendingComputeTasks().erase(taskId)) {
                        auto it = Planner->GetPendingComputeActors().emplace(computeActor, TProgressStat());
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
                        auto it = Planner->GetPendingComputeActors().find(computeActor);
                        if (it != Planner->GetPendingComputeActors().end()) {
                            if (state.HasStats()) {
                                it->second.Set(state.GetStats());
                            }
                        }
                    }
                }
                break;
            }

            case NYql::NDqProto::COMPUTE_STATE_FINISHED: {
                if (Stats) {
                    Stats->AddComputeActorStats(
                        computeActor.NodeId(),
                        std::move(*state.MutableStats()),
                        TDuration::MilliSeconds(AggregationSettings.GetCollectLongTasksStatsTimeoutMs())
                    );
                }
                ExtraData[computeActor].Swap(state.MutableExtraData());

                LastTaskId = taskId;
                LastComputeActorId = computeActor.ToString();

                if (Planner) {
                    auto it = Planner->GetPendingComputeActors().find(computeActor);
                    if (it == Planner->GetPendingComputeActors().end()) {
                        LOG_W("Got execution state for compute actor: " << computeActor
                            << ", task: " << taskId
                            << ", state: " << NYql::NDqProto::EComputeState_Name((NYql::NDqProto::EComputeState) state.GetState())
                            << ", too early (waiting reply from RM)");

                        if (Planner && Planner->GetPendingComputeTasks().erase(taskId)) {
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
                        Planner->GetPendingComputeActors().erase(it);
                        YQL_ENSURE(Planner->GetPendingComputeTasks().find(taskId) == Planner->GetPendingComputeTasks().end());
                    }
                }
            }
        }

        static_cast<TDerived*>(this)->CheckExecutionComplete();
    }

    STATEFN(ReadyState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpExecuter::TEvTxRequest, HandleReady);
            hFunc(TEvKqp::TEvAbortExecution, HandleAbortExecution);
            default: {
                UnexpectedEvent("ReadyState", ev->GetTypeRewrite());
            }
        }
        ReportEventElapsedTime();
    }

    void HandleReady(TEvKqpExecuter::TEvTxRequest::TPtr& ev) {
        TxId = ev->Get()->Record.GetRequest().GetTxId();
        Target = ActorIdFromProto(ev->Get()->Record.GetTarget());

        auto lockTxId = Request.AcquireLocksTxId;
        if (lockTxId.Defined() && *lockTxId == 0) {
            lockTxId = TxId;
        }

        TasksGraph.GetMeta().SetLockTxId(lockTxId);

        LWTRACK(KqpBaseExecuterHandleReady, ResponseEv->Orbit, TxId);
        if (IsDebugLogEnabled()) {
            for (auto& tx : Request.Transactions) {
                LOG_D("Executing physical tx, type: " << (ui32) tx.Body->GetType() << ", stages: " << tx.Body->StagesSize());
            }
        }

        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterTableResolve, ExecuterSpan.GetTraceId(), "WaitForTableResolve", NWilson::EFlags::AUTO_END);

        auto kqpTableResolver = CreateKqpTableResolver(this->SelfId(), TxId, UserToken, Request.Transactions,
            TasksGraph);
        KqpTableResolverId = this->RegisterWithSameMailbox(kqpTableResolver);

        LOG_T("Got request, become WaitResolveState");
        this->Become(&TDerived::WaitResolveState);

        auto now = TAppData::TimeProvider->Now();
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
        if (Planner && Planner->GetPendingComputeActors().empty() && Planner->GetPendingComputeTasks().empty()) {
            static_cast<TDerived*>(this)->Finalize();
            UpdateResourcesUsage(true);
            return true;
        }

        UpdateResourcesUsage(false);

        if (IsDebugLogEnabled()) {
            TStringBuilder sb;
            sb << "Waiting for: ";
            if (Planner) {
                for (auto ct : Planner->GetPendingComputeTasks()) {
                    sb << "CT " << ct << ", ";
                }
                for (auto ca : Planner->GetPendingComputeActors()) {
                    sb << "CA " << ca.first << ", ";
                }
            }
            LOG_D(sb);
        }

        return false;
    }

    void InvalidateNode(ui64 node) {
        for (auto tablet : ShardsOnNode[node]) {
            auto ev = MakeHolder<TEvPipeCache::TEvForcePipeReconnect>(tablet);
            this->Send(MakePipePerNodeCacheID(false), ev.Release());
        }
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
        ui32 eventType = ev->Get()->SourceType;
        auto reason = ev->Get()->Reason;
        switch (eventType) {
            case TEvKqpNode::TEvStartKqpTasksRequest::EventType: {
                if (reason == TEvents::TEvUndelivered::EReason::ReasonActorUnknown) {
                    LOG_D("Schedule a retry by ActorUnknown reason, nodeId:" << ev->Sender.NodeId() << " requestId: " << ev->Cookie);
                    this->Schedule(TDuration::MilliSeconds(Planner->GetCurrentRetryDelay(ev->Cookie)), new typename TEvPrivate::TEvRetry(ev->Cookie, ev->Sender));
                    return;
                }
                InvalidateNode(ev->Sender.NodeId());
                return InternalError(TStringBuilder()
                    << "TEvKqpNode::TEvStartKqpTasksRequest lost: " << reason);
            }
            default: {
                LOG_E("Event lost, type: " << eventType << ", reason: " << reason);
            }
        }
    }

    void HandleRetry(typename TEvPrivate::TEvRetry::TPtr& ev) {
        if (Planner && Planner->SendStartKqpTasksRequest(ev->Get()->RequestId, ev->Get()->Target)) {
            return;
        }
        InvalidateNode(Target.NodeId());
        return InternalError(TStringBuilder()
            << "TEvKqpNode::TEvStartKqpTasksRequest lost: ActorUnknown");
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        auto nodeId = ev->Get()->NodeId;
        LOG_N("Disconnected node " << nodeId);

        if (Planner) {
            for (auto computeActor : Planner->GetPendingComputeActors()) {
                if (computeActor.first.NodeId() == nodeId) {
                    return ReplyUnavailable(TStringBuilder() << "Connection with node " << nodeId << " lost.");
                }
            }

            for (auto& task : TasksGraph.GetTasks()) {
                if (task.Meta.NodeId == nodeId && Planner->GetPendingComputeTasks().contains(task.Id)) {
                    return ReplyUnavailable(TStringBuilder() << "Connection with node " << nodeId << " lost.");
                }
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
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_OVERLOADED, message));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS: {
                    ReplyErrorAndDie(Ydb::StatusIds::OVERLOADED,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_OVERLOADED, message));
                    break;
                }

                case NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED: {
                    ReplyErrorAndDie(Ydb::StatusIds::PRECONDITION_FAILED,
                        YqlIssue({}, NYql::TIssuesIds::KIKIMR_PRECONDITION_FAILED, message));
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

            if (Planner) {
                if (Planner->GetPendingComputeTasks().erase(taskId) == 0) {
                    LOG_D("Executing task: " << taskId << ", compute actor: " << task.ComputeActorId << ", already finished");
                } else {
                    auto result = Planner->GetPendingComputeActors().emplace(std::make_pair(task.ComputeActorId, TProgressStat()));
                    YQL_ENSURE(result.second);

                    CollectTaskChannelsUpdates(task, channelsUpdates);
                }
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
            AbortExecutionAndDie(ev->Sender, NYql::NDqProto::StatusIds::TIMEOUT, "Request timeout exceeded");
        } else {
            RuntimeError(NYql::NDq::DqStatusToYdbStatus(msg.GetStatusCode()), issues);
        }
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
                FillChannelDesc(TasksGraph, *record.AddUpdate(), TasksGraph.GetChannel(channelId), TasksGraph.GetMeta().ChannelTransportVersion, false);
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
        if (Planner) {
            for (const auto& p : Planner->GetPendingComputeActors()) {
                const auto& t = p.second.GetLastUsage();
                consumption += t;
            }
        }

        for (const auto& p : LastStats) {
            const auto& t = p.GetLastUsage();
            consumption += t;
        }

        auto ru = NRuCalc::CalcRequestUnit(consumption);

        YQL_ENSURE(consumption.ReadIOStat.Rows < PotentialUnsigned64OverflowLimit);
        YQL_ENSURE(ru < PotentialUnsigned64OverflowLimit);

        // Some heuristic to reduce overprice due to round part stats
        if (ru <= 100 && !force)
            return;

        if (Planner) {
            for (auto& p : Planner->GetPendingComputeActors()) {
                p.second.Update();
            }
        }

        for (auto& p : LastStats) {
            p.Update();
        }

        if (Request.RlPath) {
            auto actorId = ReportToRl(ru, Database, UserToken->GetSerializedToken(), Request.RlPath.GetRef());

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
    void FillSecureParamsFromStage(THashMap<TString, TString>& secureParams, const NKqpProto::TKqpPhyStage& stage) {
        for (const auto& [secretName, authInfo] : stage.GetSecureParams()) {
            const auto& structuredToken = NYql::CreateStructuredTokenParser(authInfo).ToBuilder().ReplaceReferences(SecureParams).ToJson();
            const auto& structuredTokenParser = NYql::CreateStructuredTokenParser(structuredToken);
            YQL_ENSURE(structuredTokenParser.HasIAMToken(), "only token authentification supported for compute tasks");
            secureParams.emplace(secretName, structuredTokenParser.GetIAMToken());
        }
    }

    void BuildSysViewScanTasks(TStageInfo& stageInfo) {
        Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.IsSysView());

        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        const auto& tableInfo = stageInfo.Meta.TableConstInfo;
        const auto& keyTypes = tableInfo->KeyColumnTypes;

        for (auto& op : stage.GetTableOps()) {
            Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.ExecuterId = this->SelfId();
            TShardKeyRanges keyRanges;

            switch (op.GetTypeCase()) {
                case NKqpProto::TKqpPhyTableOperation::kReadRange:
                    stageInfo.Meta.SkipNullKeys.assign(
                        op.GetReadRange().GetSkipNullKeys().begin(),
                        op.GetReadRange().GetSkipNullKeys().end()
                    );
                    keyRanges.Add(MakeKeyRange(
                        keyTypes, op.GetReadRange().GetKeyRange(),
                        stageInfo, HolderFactory(), TypeEnv())
                    );
                    break;
                case NKqpProto::TKqpPhyTableOperation::kReadRanges:
                    keyRanges.CopyFrom(FillReadRanges(keyTypes, op.GetReadRanges(), stageInfo, TypeEnv()));
                    break;
                default:
                    YQL_ENSURE(false, "Unexpected table scan operation: " << (ui32) op.GetTypeCase());
            }

            TTaskMeta::TShardReadInfo readInfo = {
                .Ranges = std::move(keyRanges),
                .Columns = BuildKqpColumns(op, tableInfo),
            };

            task.Meta.Reads.ConstructInPlace();
            task.Meta.Reads->emplace_back(std::move(readInfo));
            task.Meta.ReadInfo.Reverse = op.GetReadRange().GetReverse();
            task.Meta.Type = TTaskMeta::TTaskType::Compute;

            FillSecureParamsFromStage(task.Meta.SecureParams, stage);
            BuildSinks(stage, task);

            LOG_D("Stage " << stageInfo.Id << " create sysview scan task: " << task.Id);
        }
    }

    void BuildExternalSinks(const NKqpProto::TKqpSink& sink, TKqpTasksGraph::TTaskType& task) {
        const auto& extSink = sink.GetExternalSink();
        auto sinkName = extSink.GetSinkName();
        if (sinkName) {
            auto structuredToken = NYql::CreateStructuredTokenParser(extSink.GetAuthInfo()).ToBuilder().ReplaceReferences(SecureParams).ToJson();
            task.Meta.SecureParams.emplace(sinkName, structuredToken);
            if (GetUserRequestContext()->TraceId) {
                task.Meta.TaskParams.emplace("fq.job_id", GetUserRequestContext()->CustomerSuppliedId);
                // "fq.restart_count"
            }
        }

        auto& output = task.Outputs[sink.GetOutputIndex()];
        output.Type = TTaskOutputType::Sink;
        output.SinkType = extSink.GetType();
        output.SinkSettings = extSink.GetSettings();
    }

    void BuildInternalSinks(const NKqpProto::TKqpSink& sink, TKqpTasksGraph::TTaskType& task) {
        const auto& intSink = sink.GetInternalSink();
        auto& output = task.Outputs[sink.GetOutputIndex()];
        output.Type = TTaskOutputType::Sink;
        output.SinkType = intSink.GetType();

        if (intSink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
            NKikimrKqp::TKqpTableSinkSettings settings;
            YQL_ENSURE(intSink.GetSettings().UnpackTo(&settings), "Failed to unpack settings");
            auto& lockTxId = TasksGraph.GetMeta().LockTxId;
            if (lockTxId) {
                settings.SetLockTxId(*lockTxId);
                settings.SetLockNodeId(SelfId().NodeId());
            }
            output.SinkSettings.ConstructInPlace();
            output.SinkSettings->PackFrom(settings);
        } else {
            output.SinkSettings = intSink.GetSettings();
        }
    }

    void BuildSinks(const NKqpProto::TKqpPhyStage& stage, TKqpTasksGraph::TTaskType& task) {
        if (stage.SinksSize() > 0) {
            YQL_ENSURE(stage.SinksSize() == 1, "multiple sinks are not supported");
            const auto& sink = stage.GetSinks(0);
            YQL_ENSURE(sink.GetOutputIndex() < task.Outputs.size());

            if (sink.HasInternalSink()) {
                BuildInternalSinks(sink, task);
            } else if (sink.HasExternalSink()) {
                BuildExternalSinks(sink, task);
            } else {
                YQL_ENSURE(false, "unknown sink type");
            }
        }
    }

    void BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot) {
        const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        YQL_ENSURE(stage.GetSources(0).HasExternalSource());
        YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

        const auto& stageSource = stage.GetSources(0);
        const auto& externalSource = stageSource.GetExternalSource();

        ui32 taskCount = externalSource.GetPartitionedTaskParams().size();

        if (!resourceSnapshot.empty()) {
            ui32 maxTaskcount = resourceSnapshot.size() * 2;
            if (taskCount > maxTaskcount) {
                taskCount = maxTaskcount;
            }
        }

        auto sourceName = externalSource.GetSourceName();
        TString structuredToken;
        if (sourceName) {
            structuredToken = NYql::CreateStructuredTokenParser(externalSource.GetAuthInfo()).ToBuilder().ReplaceReferences(SecureParams).ToJson();
        }

        TVector<ui64> tasksIds;

        // generate all tasks
        for (ui32 i = 0; i < taskCount; i++) {
            auto& task = TasksGraph.AddTask(stageInfo);

            auto& input = task.Inputs[stageSource.GetInputIndex()];
            input.ConnectionInfo = NYql::NDq::TSourceInput{};
            input.SourceSettings = externalSource.GetSettings();
            input.SourceType = externalSource.GetType();

            if (structuredToken) {
                task.Meta.SecureParams.emplace(sourceName, structuredToken);
            }
            FillSecureParamsFromStage(task.Meta.SecureParams, stage);

            if (resourceSnapshot.empty()) {
                task.Meta.Type = TTaskMeta::TTaskType::Compute;
            } else {
                task.Meta.NodeId = resourceSnapshot[i % resourceSnapshot.size()].GetNodeId();
                task.Meta.Type = TTaskMeta::TTaskType::Scan;
            }

            tasksIds.push_back(task.Id);
        }

        // distribute read ranges between them
        ui32 currentTaskIndex = 0;
        for (const TString& partitionParam : externalSource.GetPartitionedTaskParams()) {
            TasksGraph.GetTask(tasksIds[currentTaskIndex]).Meta.ReadRanges.push_back(partitionParam);
            if (++currentTaskIndex >= tasksIds.size()) {
                currentTaskIndex = 0;
            }
        }

        // finish building
        for (auto taskId : tasksIds) {
            BuildSinks(stage, TasksGraph.GetTask(taskId));
        }
    }

    TVector<TVector<TShardRangesWithShardId>> DistributeShardsToTasks(TVector<TShardRangesWithShardId> shardsRanges, const size_t tasksCount, const TVector<NScheme::TTypeInfo>& keyTypes) {
        if (IsDebugLogEnabled()) {
            TStringBuilder sb;
            sb << "Distrubiting shards to tasks: [";
            for(size_t i = 0; i < shardsRanges.size(); i++) {
                sb << "# " << i << ": " << shardsRanges[i].Ranges->ToString(keyTypes, *AppData()->TypeRegistry);
            }

            sb << " ].";
            LOG_D(sb);
        }

        std::sort(std::begin(shardsRanges), std::end(shardsRanges), [&](const TShardRangesWithShardId& lhs, const TShardRangesWithShardId& rhs) {
                // Special case for infinity
                if (lhs.Ranges->GetRightBorder().first->GetCells().empty() || rhs.Ranges->GetRightBorder().first->GetCells().empty()) {
                    YQL_ENSURE(!lhs.Ranges->GetRightBorder().first->GetCells().empty() || !rhs.Ranges->GetRightBorder().first->GetCells().empty());
                    return rhs.Ranges->GetRightBorder().first->GetCells().empty();
                }
                return CompareTypedCellVectors(
                    lhs.Ranges->GetRightBorder().first->GetCells().data(),
                    rhs.Ranges->GetRightBorder().first->GetCells().data(),
                    keyTypes.data(), keyTypes.size()) < 0;
            });

        // One shard (ranges set) can be assigned only to one task. Otherwise, we can break some optimizations like removing unnecessary shuffle.
        TVector<TVector<TShardRangesWithShardId>> result(tasksCount);
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
    }

    TMaybe<size_t> BuildScanTasksFromSource(TStageInfo& stageInfo, const bool shardsResolved, const bool limitTasksPerNode) {
        THashMap<ui64, std::vector<ui64>> nodeTasks;
        THashMap<ui64, ui64> assignedShardsCount;

        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        YQL_ENSURE(stage.GetSources(0).HasReadRangesSource());
        YQL_ENSURE(stage.GetSources(0).GetInputIndex() == 0 && stage.SourcesSize() == 1);
        for (auto& input : stage.inputs()) {
            YQL_ENSURE(input.HasBroadcast());
        }

        auto& source = stage.GetSources(0).GetReadRangesSource();

        const auto& tableInfo = stageInfo.Meta.TableConstInfo;
        const auto& keyTypes = tableInfo->KeyColumnTypes;

        YQL_ENSURE(tableInfo->TableKind != NKikimr::NKqp::ETableKind::Olap);

        auto columns = BuildKqpColumns(source, tableInfo);

        const auto& snapshot = GetSnapshot();

        TVector<ui64> createdTasksIds;
        auto createNewTask = [&](
                TMaybe<ui64> nodeId,
                ui64 taskLocation,
                TMaybe<ui64> shardId,
                TMaybe<ui64> maxInFlightShards) -> TTask& {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.Type = TTaskMeta::TTaskType::Scan;
            task.Meta.ExecuterId = this->SelfId();
            if (nodeId) {
                task.Meta.NodeId = *nodeId;
            } else {
                YQL_ENSURE(!shardsResolved);
                task.Meta.ShardId = taskLocation;
            }
            FillSecureParamsFromStage(task.Meta.SecureParams, stage);

            const auto& stageSource = stage.GetSources(0);
            auto& input = task.Inputs[stageSource.GetInputIndex()];
            input.SourceType = NYql::KqpReadRangesSourceName;
            input.ConnectionInfo = NYql::NDq::TSourceInput{};

            // allocating source settings

            input.Meta.SourceSettings = TasksGraph.GetMeta().Allocate<NKikimrTxDataShard::TKqpReadRangesSourceSettings>();
            NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;
            FillTableMeta(stageInfo, settings->MutableTable());

            for (auto& keyColumn : keyTypes) {
                auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(keyColumn, "");
                if (columnType.TypeInfo) {
                    *settings->AddKeyColumnTypeInfos() = *columnType.TypeInfo;
                } else {
                    *settings->AddKeyColumnTypeInfos() = NKikimrProto::TTypeInfo();
                }
                settings->AddKeyColumnTypes(static_cast<ui32>(keyColumn.GetTypeId()));
            }

            for (auto& column : columns) {
                auto* protoColumn = settings->AddColumns();
                protoColumn->SetId(column.Id);
                auto columnType = NScheme::ProtoColumnTypeFromTypeInfoMod(column.Type, column.TypeMod);
                protoColumn->SetType(columnType.TypeId);
                protoColumn->SetNotNull(column.NotNull);
                if (columnType.TypeInfo) {
                    *protoColumn->MutableTypeInfo() = *columnType.TypeInfo;
                }
                protoColumn->SetName(column.Name);
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


            if (Request.IsolationLevel == NKikimrKqp::ISOLATION_LEVEL_READ_UNCOMMITTED) {
                settings->SetAllowInconsistentReads(true);
            }

            settings->SetReverse(source.GetReverse());
            settings->SetSorted(source.GetSorted());

            if (maxInFlightShards) {
                settings->SetMaxInFlightShards(*maxInFlightShards);
            }

            if (shardId) {
                settings->SetShardIdHint(*shardId);
            }

            ui64 itemsLimit = ExtractItemsLimit(stageInfo, source.GetItemsLimit(), Request.TxAlloc->HolderFactory,
                Request.TxAlloc->TypeEnv);
            settings->SetItemsLimit(itemsLimit);

            auto self = static_cast<TDerived*>(this)->SelfId();
            auto& lockTxId = TasksGraph.GetMeta().LockTxId;
            if (lockTxId) {
                settings->SetLockTxId(*lockTxId);
                settings->SetLockNodeId(self.NodeId());
            }

            createdTasksIds.push_back(task.Id);
            return task;
        };

        THashMap<ui64, TVector<ui64>> nodeIdToTasks;
        THashMap<ui64, TVector<TShardRangesWithShardId>> nodeIdToShardKeyRanges;

        auto addPartiton = [&](
            ui64 taskLocation,
            TMaybe<ui64> shardId,
            const TShardInfo& shardInfo,
            TMaybe<ui64> maxInFlightShards = Nothing())
        {
            YQL_ENSURE(!shardInfo.KeyWriteRanges);

            const auto nodeIdPtr = ShardIdToNodeId.FindPtr(taskLocation);
            const auto nodeId = nodeIdPtr
                ? TMaybe<ui64>{*nodeIdPtr}
                : Nothing();

            YQL_ENSURE(!shardsResolved || nodeId);

            if (shardId && Stats) {
                Stats->AffectedShards.insert(*shardId);
            }

            if (limitTasksPerNode) {
                YQL_ENSURE(shardsResolved);
                const auto maxScanTasksPerNode = GetScanTasksPerNode(stageInfo, /* isOlapScan */ false, *nodeId);
                auto& nodeTasks = nodeIdToTasks[*nodeId];
                if (nodeTasks.size() < maxScanTasksPerNode) {
                    const auto& task = createNewTask(nodeId, taskLocation, {}, maxInFlightShards);
                    nodeTasks.push_back(task.Id);
                }

                nodeIdToShardKeyRanges[*nodeId].push_back(TShardRangesWithShardId{shardId, &*shardInfo.KeyReadRanges});
            } else {
                auto& task = createNewTask(nodeId, taskLocation, shardId, maxInFlightShards);
                const auto& stageSource = stage.GetSources(0);
                auto& input = task.Inputs[stageSource.GetInputIndex()];
                NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;

                shardInfo.KeyReadRanges->SerializeTo(settings);
            }
        };

        auto fillRangesForTasks = [&]() {
            for (const auto& [nodeId, shardsRanges] : nodeIdToShardKeyRanges) {
                const auto& tasks = nodeIdToTasks.at(nodeId);

                const auto rangesDistribution = DistributeShardsToTasks(shardsRanges, tasks.size(), keyTypes);
                YQL_ENSURE(rangesDistribution.size() == tasks.size());

                for (size_t taskIndex = 0; taskIndex < tasks.size(); ++taskIndex) {
                    const auto taskId = tasks[taskIndex];
                    auto& task = TasksGraph.GetTask(taskId);
                    const auto& stageSource = stage.GetSources(0);
                    auto& input = task.Inputs[stageSource.GetInputIndex()];
                    NKikimrTxDataShard::TKqpReadRangesSourceSettings* settings = input.Meta.SourceSettings;

                    const auto& shardsRangesForTask = rangesDistribution[taskIndex];

                    if (shardsRangesForTask.size() == 1 && shardsRangesForTask[0].ShardId) {
                        settings->SetShardIdHint(*shardsRangesForTask[0].ShardId);
                    }

                    for (const auto& shardRanges : shardsRangesForTask) {
                        shardRanges.Ranges->SerializeTo(settings);
                    }
                }
            }
        };

        auto buildSinks = [&]() {
            for (const ui64 taskId : createdTasksIds) {
                BuildSinks(stage, TasksGraph.GetTask(taskId));
            }
        };

        bool isFullScan = false;
        const THashMap<ui64, TShardInfo> partitions = SourceScanStageIdToParititions.empty()
            ? PrunePartitions(source, stageInfo, HolderFactory(), TypeEnv(), isFullScan)
            : SourceScanStageIdToParititions.at(stageInfo.Id);

        if (isFullScan && !source.HasItemsLimit()) {
            Counters->Counters->FullScansExecuted->Inc();
        }

        if (partitions.size() > 0 && source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards()) {
            auto [startShard, shardInfo] = MakeVirtualTablePartition(source, stageInfo, HolderFactory(), TypeEnv());
            if (Stats) {
                for (auto& [shardId, _] : partitions) {
                    Stats->AffectedShards.insert(shardId);
                }
            }
            if (shardInfo.KeyReadRanges) {
                addPartiton(startShard, {}, shardInfo, source.GetSequentialInFlightShards());
                fillRangesForTasks();
                buildSinks();
                return Nothing();
            } else {
                return 0;
            }
        } else {
            for (auto& [shardId, shardInfo] : partitions) {
                addPartiton(shardId, shardId, shardInfo, {});
            }
            fillRangesForTasks();
            buildSinks();
            return partitions.size();
        }
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

    void BuildComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount) {
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        ui32 partitionsCount = 1;
        ui32 inputTasks = 0;
        bool isShuffle = false;
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
                        break;
                    default:
                        YQL_ENSURE(false, "Unexpected connection type: " << (ui32)input.GetTypeCase() << Endl
                            << this->DebugString());
                }
            }

            auto& originStageInfo = TasksGraph.GetStageInfo(NYql::NDq::TStageId(stageInfo.Id.TxId, input.GetStageIndex()));

            switch (input.GetTypeCase()) {
                case NKqpProto::TKqpPhyConnection::kHashShuffle: {
                    inputTasks += originStageInfo.Tasks.size();
                    isShuffle = true;
                    break;
                }

                case NKqpProto::TKqpPhyConnection::kStreamLookup:
                    UnknownAffectedShardCount = true;
		    [[fallthrough]];
                case NKqpProto::TKqpPhyConnection::kMap:
                    partitionsCount = originStageInfo.Tasks.size();
                    break;

                default:
                    break;
            }
        }

        if (isShuffle) {
            partitionsCount = std::max(partitionsCount, GetMaxTasksAggregation(stageInfo, inputTasks, nodesCount));
        }

        for (ui32 i = 0; i < partitionsCount; ++i) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.Type = TTaskMeta::TTaskType::Compute;
            task.Meta.ExecuterId = SelfId();
            FillSecureParamsFromStage(task.Meta.SecureParams, stage);
            BuildSinks(stage, task);
            LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
        }
    }

    void FillReadInfo(TTaskMeta& taskMeta, ui64 itemsLimit, bool reverse, bool sorted) const
    {
        if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
            // Validate parameters
            YQL_ENSURE(taskMeta.ReadInfo.ItemsLimit == itemsLimit);
            YQL_ENSURE(taskMeta.ReadInfo.Reverse == reverse);
            return;
        }

        taskMeta.ReadInfo.ItemsLimit = itemsLimit;
        taskMeta.ReadInfo.Reverse = reverse;
        taskMeta.ReadInfo.Sorted = sorted;
        taskMeta.ReadInfo.ReadType = TTaskMeta::TReadInfo::EReadType::Rows;
    }

    TTaskMeta::TReadInfo::EReadType OlapReadTypeFromProto(const NKqpProto::TKqpPhyOpReadOlapRanges::EReadType& type) const {
        switch (type) {
            case NKqpProto::TKqpPhyOpReadOlapRanges::ROWS:
                return TTaskMeta::TReadInfo::EReadType::Rows;
            case NKqpProto::TKqpPhyOpReadOlapRanges::BLOCKS:
                return TTaskMeta::TReadInfo::EReadType::Blocks;
            default:
                YQL_ENSURE(false, "Invalid read type from TKqpPhyOpReadOlapRanges protobuf.");
        }
    }

    void FillOlapReadInfo(TTaskMeta& taskMeta, NKikimr::NMiniKQL::TType* resultType, const TMaybe<::NKqpProto::TKqpPhyOpReadOlapRanges>& readOlapRange) const {
        if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
            // Validate parameters
            if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
                YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program.empty());
                return;
            }

            YQL_ENSURE(taskMeta.ReadInfo.OlapProgram.Program == readOlapRange->GetOlapProgram());
            return;
        }

        if (resultType) {
            YQL_ENSURE(resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Struct
                || resultType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Tuple);

            auto* resultStructType = static_cast<NKikimr::NMiniKQL::TStructType*>(resultType);
            ui32 resultColsCount = resultStructType->GetMembersCount();

            taskMeta.ReadInfo.ResultColumnsTypes.reserve(resultColsCount);
            for (ui32 i = 0; i < resultColsCount; ++i) {
                taskMeta.ReadInfo.ResultColumnsTypes.emplace_back();
                auto memberType = resultStructType->GetMemberType(i);
                if (memberType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Pg) {
                    const auto memberPgType = static_cast<NKikimr::NMiniKQL::TPgType*>(memberType);
                    taskMeta.ReadInfo.ResultColumnsTypes.back() = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, NPg::TypeDescFromPgTypeId(memberPgType->GetTypeId()));
                } else {
                    if (memberType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Optional) {
                        memberType = static_cast<NKikimr::NMiniKQL::TOptionalType*>(memberType)->GetItemType();
                    }
                    YQL_ENSURE(memberType->GetKind() == NKikimr::NMiniKQL::TType::EKind::Data,
                        "Expected simple data types to be read from column shard");
                    const auto memberDataType = static_cast<NKikimr::NMiniKQL::TDataType*>(memberType);
                    taskMeta.ReadInfo.ResultColumnsTypes.back() = NScheme::TTypeInfo(memberDataType->GetSchemeType());
                }
            }
        }
        if (!readOlapRange || readOlapRange->GetOlapProgram().empty()) {
            return;
        }
        {
            Y_ABORT_UNLESS(taskMeta.ReadInfo.GroupByColumnNames.empty());
            std::vector<std::string> groupByColumns;
            for (auto&& i : readOlapRange->GetGroupByColumnNames()) {
                groupByColumns.emplace_back(i);
            }
            std::swap(taskMeta.ReadInfo.GroupByColumnNames, groupByColumns);
        }
        taskMeta.ReadInfo.ReadType = OlapReadTypeFromProto(readOlapRange->GetReadType());
        taskMeta.ReadInfo.OlapProgram.Program = readOlapRange->GetOlapProgram();
        for (auto& name: readOlapRange->GetOlapProgramParameterNames()) {
            taskMeta.ReadInfo.OlapProgram.ParameterNames.insert(name);
        }
    }

    void MergeReadInfoToTaskMeta(TTaskMeta& meta, ui64 shardId, TMaybe<TShardKeyRanges>& keyReadRanges, const TPhysicalShardReadSettings& readSettings, const TVector<TTaskMeta::TColumn>& columns,
        const NKqpProto::TKqpPhyTableOperation& op, bool isPersistentScan) const
    {
        TTaskMeta::TShardReadInfo readInfo = {
            .Columns = columns,
        };
        if (keyReadRanges) {
            readInfo.Ranges = std::move(*keyReadRanges); // sorted & non-intersecting
        }

        if (isPersistentScan) {
            readInfo.ShardId = shardId;
        }

        FillReadInfo(meta, readSettings.ItemsLimit, readSettings.Reverse, readSettings.Sorted);
        if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
            FillOlapReadInfo(meta, readSettings.ResultType, op.GetReadOlapRange());
        }

        if (!meta.Reads) {
            meta.Reads.ConstructInPlace();
        }

        meta.Reads->emplace_back(std::move(readInfo));
    }

    ui32 GetScanTasksPerNode(TStageInfo& stageInfo, const bool isOlapScan, const ui64 /*nodeId*/) const {
        ui32 result = 0;
        if (isOlapScan) {
            if (AggregationSettings.HasCSScanThreadsPerNode()) {
                result = AggregationSettings.GetCSScanThreadsPerNode();
            } else {
                const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
                result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), {});
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

    TTask& AssignScanTaskToShard(
        TStageInfo& stageInfo, const ui64 shardId,
        THashMap<ui64, std::vector<ui64>>& nodeTasks,
        THashMap<ui64, ui64>& assignedShardsCount,
        const bool sorted, const bool isOlapScan)
    {
        const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
        ui64 nodeId = ShardIdToNodeId.at(shardId);
        if (stageInfo.Meta.IsOlap() && sorted) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.ExecuterId = SelfId();
            task.Meta.NodeId = nodeId;
            task.Meta.ScanTask = true;
            task.Meta.Type = TTaskMeta::TTaskType::Scan;
            FillSecureParamsFromStage(task.Meta.SecureParams, stage);
            return task;
        }

        auto& tasks = nodeTasks[nodeId];
        auto& cnt = assignedShardsCount[nodeId];
        const ui32 maxScansPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);
        if (cnt < maxScansPerNode) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.NodeId = nodeId;
            task.Meta.ScanTask = true;
            task.Meta.Type = TTaskMeta::TTaskType::Scan;
            FillSecureParamsFromStage(task.Meta.SecureParams, stage);
            tasks.push_back(task.Id);
            ++cnt;
            return task;
        } else {
            ui64 taskIdx = cnt % maxScansPerNode;
            ++cnt;
            return TasksGraph.GetTask(tasks[taskIdx]);
        }
    }

    void BuildScanTasksFromShards(TStageInfo& stageInfo) {
        THashMap<ui64, std::vector<ui64>> nodeTasks;
        THashMap<ui64, std::vector<TShardInfoWithId>> nodeShards;
        THashMap<ui64, ui64> assignedShardsCount;
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        const auto& tableInfo = stageInfo.Meta.TableConstInfo;
        const auto& keyTypes = tableInfo->KeyColumnTypes;
        ui32 metaId = 0;
        for (auto& op : stage.GetTableOps()) {
            Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto columns = BuildKqpColumns(op, tableInfo);
            bool isFullScan;
            auto partitions = PrunePartitions(op, stageInfo, HolderFactory(), TypeEnv(), isFullScan);
            const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
            auto readSettings = ExtractReadSettings(op, stageInfo, HolderFactory(), TypeEnv());

            if (isFullScan && readSettings.ItemsLimit) {
                Counters->Counters->FullScansExecuted->Inc();
            }

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

            if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() || (!isOlapScan && readSettings.Sorted)) {
                for (auto&& pair : nodeShards) {
                    auto& shardsInfo = pair.second;
                    for (auto&& shardInfo : shardsInfo) {
                        auto& task = AssignScanTaskToShard(stageInfo, shardInfo.ShardId, nodeTasks, assignedShardsCount, readSettings.Sorted, isOlapScan);
                        MergeReadInfoToTaskMeta(task.Meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                            columns, op, /*isPersistentScan*/ true);
                    }
                }

                for (const auto& pair : nodeTasks) {
                    for (const auto& taskIdx : pair.second) {
                        auto& task = TasksGraph.GetTask(taskIdx);
                        task.Meta.SetEnableShardsSequentialScan(readSettings.Sorted);
                        PrepareScanMetaForUsage(task.Meta, keyTypes);
                        BuildSinks(stage, task);
                    }
                }

            } else {
                for (auto&& pair : nodeShards) {
                    const auto nodeId = pair.first;
                    auto& shardsInfo = pair.second;
                    const ui32 metaGlueingId = ++metaId;
                    TTaskMeta meta;
                    {
                        for (auto&& shardInfo : shardsInfo) {
                            MergeReadInfoToTaskMeta(meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                                columns, op, /*isPersistentScan*/ true);
                        }
                        PrepareScanMetaForUsage(meta, keyTypes);
                        LOG_D("Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
                            << ", meta: " << meta.ToString(keyTypes, *AppData()->TypeRegistry));
                    }
                    for (ui32 t = 0; t < GetScanTasksPerNode(stageInfo, isOlapScan, nodeId); ++t) {
                        auto& task = TasksGraph.AddTask(stageInfo);
                        task.Meta = meta;
                        task.Meta.SetEnableShardsSequentialScan(false);
                        task.Meta.ExecuterId = SelfId();
                        task.Meta.NodeId = nodeId;
                        task.Meta.ScanTask = true;
                        task.Meta.Type = TTaskMeta::TTaskType::Scan;
                        task.SetMetaId(metaGlueingId);
                        FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                        BuildSinks(stage, task);
                    }
                }
            }
        }

        LOG_D("Stage " << stageInfo.Id << " will be executed on " << nodeTasks.size() << " nodes.");
    }

    void PrepareScanMetaForUsage(TTaskMeta& meta, const TVector<NScheme::TTypeInfo>& keyTypes) const {
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

    void GetSecretsSnapshot() {
        RegisterDescribeSecretsActor(this->SelfId(), UserToken ? UserToken->GetUserSID() : "", SecretNames, this->ActorContext().ActorSystem());
    }

    void GetResourcesSnapshot() {
        GetKqpResourceManager()->RequestClusterResourcesInfo(
            [as = TlsActivationContext->ActorSystem(), self = SelfId()](TVector<NKikimrKqp::TKqpNodeResources>&& resources) {
                TAutoPtr<IEventHandle> eh = new IEventHandle(self, self, new typename TEvPrivate::TEvResourcesSnapshot(std::move(resources)));
                as->Send(eh);
            });
    }

    void SaveScriptExternalEffect(std::unique_ptr<TEvSaveScriptExternalEffectRequest> scriptEffects) {
        this->Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), scriptEffects.release());
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
        InternalError(TStringBuilder() << "Unexpected event at TKqpExecuter, state: " << state
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

    void AbortExecutionAndDie(TActorId abortSender, NYql::NDqProto::StatusIds::StatusCode status, const TString& message) {
        if (AlreadyReplied) {
            return;
        }

        LOG_E("Abort execution: " << NYql::NDqProto::StatusIds_StatusCode_Name(status) << "," << message);
        if (ExecuterSpan) {
            ExecuterSpan.EndError(TStringBuilder() << NYql::NDqProto::StatusIds_StatusCode_Name(status));
        }

        static_cast<TDerived*>(this)->FillResponseStats(Ydb::StatusIds::TIMEOUT);

        // TEvAbortExecution can come from either ComputeActor or SessionActor (== Target).
        if (abortSender != Target) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(status, "Request timeout exceeded");
            this->Send(Target, abortEv.Release());
        }

        AlreadyReplied = true;
        LOG_E("Sending timeout response to: " << Target);
        this->Send(Target, ResponseEv.release());

        Request.Transactions.crop(0);
        TerminateComputeActors(Ydb::StatusIds::TIMEOUT, message);
        this->PassAway();
    }

    virtual void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        if (AlreadyReplied) {
            return;
        }

        if (Planner) {
            for (auto computeActor : Planner->GetPendingComputeActors()) {
                LOG_D("terminate compute actor " << computeActor.first);

                auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDq::YdbStatusToDqStatus(status), "Terminate execution");
                this->Send(computeActor.first, ev.Release());
            }
        }

        AlreadyReplied = true;
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        response.MutableIssues()->Swap(issues);

        LOG_T("ReplyErrorAndDie. Response: " << response.DebugString()
            << ", to ActorId: " << Target);

        if constexpr (ExecType == EExecType::Data) {
            if (status != Ydb::StatusIds::SUCCESS) {
                Counters->TxProxyMon->ReportStatusNotOK->Inc();
            } else {
                Counters->TxProxyMon->ReportStatusOK->Inc();
            }
        }

        LWTRACK(KqpBaseExecuterReplyErrorAndDie, ResponseEv->Orbit, TxId);

        ExecuterSpan.EndError(response.DebugString());
        ExecuterStateSpan.EndError(response.DebugString());

        Request.Transactions.crop(0);
        this->Send(Target, ResponseEv.release());
        this->PassAway();
    }

protected:
    template <class TCollection>
    bool ValidateTaskSize(const TCollection& tasks) {
        for (const auto& task : tasks) {
            if (ui32 size = task->ByteSize(); size > MaxTaskSize) {
                LOG_E("Abort execution. Task #" << task->GetId() << " size is too big: " << size << " > " << MaxTaskSize);
                ReplyErrorAndDie(Ydb::StatusIds::ABORTED,
                    MakeIssue(NKikimrIssues::TIssuesIds::SHARD_PROGRAM_SIZE_EXCEEDED, TStringBuilder() <<
                        "Datashard program size limit exceeded (" << size << " > " << MaxTaskSize << ")"));
                return false;
            }
        }
        return true;
    }

    void InitializeChannelProxies() {
        // notice: forward all respones to executer if
        // trailing results are allowed.
        // temporary, will be removed in the next pr.
        if (Request.IsTrailingResultsAllowed())
            return;

        for(const auto& channel: TasksGraph.GetChannels()) {
            if (channel.DstTask) {
                continue;
            }

            CreateChannelProxy(channel);
        }
    }

    const IKqpGateway::TKqpSnapshot& GetSnapshot() const {
        return TasksGraph.GetMeta().Snapshot;
    }

    void SetSnapshot(ui64 step, ui64 txId) {
        TasksGraph.GetMeta().SetSnapshot(step, txId);
    }

    IActor* CreateChannelProxy(const NYql::NDq::TChannel& channel) {
        auto channelIt = ResultChannelProxies.find(channel.Id);
        if (channelIt != ResultChannelProxies.end()) {
            return channelIt->second;
        }

        YQL_ENSURE(channel.DstInputIndex < ResponseEv->ResultsSize());
        const auto& txResult = ResponseEv->TxResults[channel.DstInputIndex];

        IActor* proxy;
        if (txResult.IsStream && txResult.QueryResultIndex.Defined()) {
            proxy = CreateResultStreamChannelProxy(TxId, channel.Id, txResult.MkqlItemType,
                txResult.ColumnOrder, *txResult.QueryResultIndex, Target, this->SelfId(), StatementResultIndex);
        } else {
            proxy = CreateResultDataChannelProxy(TxId, channel.Id, this->SelfId(),
                channel.DstInputIndex, ResponseEv.get());
        }

        this->RegisterWithSameMailbox(proxy);
        ResultChannelProxies.emplace(std::make_pair(channel.Id, proxy));
        TasksGraph.GetMeta().ResultChannelProxies.emplace(channel.Id, proxy->SelfId());

        return proxy;
    }

protected:
    void PassAway() override {
        for (auto channelPair: ResultChannelProxies) {
            LOG_D("terminate result channel " << channelPair.first << " proxy at " << channelPair.second->SelfId());

            TAutoPtr<IEventHandle> ev = new IEventHandle(
                channelPair.second->SelfId(), SelfId(), new TEvents::TEvPoison
            );
            channelPair.second->Receive(ev);
        }

        LOG_D("terminate execution.");
        if (KqpShardsResolverId) {
            this->Send(KqpShardsResolverId, new TEvents::TEvPoison);
        }

        if (Planner) {
            Planner->Unsubscribe();
        }

        if (KqpTableResolverId) {
            this->Send(KqpTableResolverId, new TEvents::TEvPoison);
            this->Send(this->SelfId(), new TEvents::TEvPoison);
            LOG_T("Terminate, become ZombieState");
            this->Become(&TKqpExecuterBase::ZombieState);
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
    virtual TString CurrentStateFuncName() const {
        const auto& func = this->CurrentStateFunc();
        if (func == &TKqpExecuterBase::ZombieState) {
            return "ZombieState";
        } else if (func == &TKqpExecuterBase::ReadyState) {
            return "ReadyState";
        } else {
            return "unknown state";
        }
    }

    std::unordered_map<ui64, IActor*>& GetResultChannelProxies() {
        return ResultChannelProxies;
    }

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

    const TIntrusivePtr<TUserRequestContext>& GetUserRequestContext() const {
        return TasksGraph.GetMeta().UserRequestContext;
    }

    TIntrusivePtr<TUserRequestContext>& MutableUserRequestContext() {
        return TasksGraph.GetMeta().UserRequestContext;
    }

protected:
    IKqpGateway::TExecPhysicalRequest Request;
    const TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TKqpRequestCounters::TPtr Counters;
    std::unique_ptr<TQueryExecutionStats> Stats;
    TInstant LastProgressStats;
    TInstant StartTime;
    TMaybe<TInstant> Deadline;
    TMaybe<TInstant> CancelAt;
    TActorId Target;
    ui64 TxId = 0;

    TKqpTasksGraph TasksGraph;

    TActorId KqpTableResolverId;
    TActorId KqpShardsResolverId;
    THashMap<TActorId, NYql::NDqProto::TComputeActorExtraData> ExtraData;

    TVector<TProgressStat> LastStats;

    TInstant StartResolveTime;
    TInstant LastResourceUsageUpdate;

    std::unordered_map<ui64, IActor*> ResultChannelProxies;
    std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ResponseEv;
    NWilson::TSpan ExecuterSpan;
    NWilson::TSpan ExecuterStateSpan;

    TMap<ui64, ui64> ShardIdToNodeId;
    TMap<ui64, TVector<ui64>> ShardsOnNode;

    ui64 LastTaskId = 0;
    TString LastComputeActorId = "";

    std::unique_ptr<TKqpPlanner> Planner;
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig ExecuterRetriesConfig;

    std::vector<TString> SecretNames;
    std::map<TString, TString> SecureParams;

    const NKikimrConfig::TTableServiceConfig::TAggregationConfig AggregationSettings;
    TVector<NKikimrKqp::TKqpNodeResources> ResourcesSnapshot;
    bool HasOlapTable = false;
    bool StreamResult = false;
    bool HasDatashardSourceScan = false;
    bool UnknownAffectedShardCount = false;

    THashMap<ui64, TActorId> ResultChannelToComputeActor;
    THashMap<NYql::NDq::TStageId, THashMap<ui64, TShardInfo>> SourceScanStageIdToParititions;

    ui32 StatementResultIndex;
    bool AlreadyReplied = false;

private:
    static constexpr TDuration ResourceUsageUpdateInterval = TDuration::MilliSeconds(100);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters, bool streamResult,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregation,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimrConfig::TTableServiceConfig::EChannelTransportVersion chanTransportVersion, const TActorId& creator,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    const bool enableOlapSink, const bool useEvWrite, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings);

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregation,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
    TPreparedQueryHolder::TConstPtr preparedQuery, const NKikimrConfig::TTableServiceConfig::EChannelTransportVersion chanTransportVersion,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex);

} // namespace NKqp
} // namespace NKikimr
