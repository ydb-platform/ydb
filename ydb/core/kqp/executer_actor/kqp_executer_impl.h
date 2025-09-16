#pragma once

#include "kqp_executer.h"
#include "kqp_executer_stats.h"
#include "kqp_planner.h"
#include "kqp_table_resolver.h"

#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/common/kqp_lwtrace_probes.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/core/kqp/executer_actor/kqp_tasks_graph.h>
#include <ydb/core/kqp/executer_actor/shards_resolver/kqp_shards_resolver.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/kqp/opt/kqp_query_plan.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/grpc_services/local_rate_limiter.h>
#include <ydb/core/kqp/common/control.h>

#include <ydb/services/metadata/secret/fetcher.h>
#include <ydb/services/metadata/secret/snapshot.h>

#include <ydb/library/mkql_proto/mkql_proto.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>
#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/dq/common/dq_serialized_batch.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/async/wait_for_event.h>

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
constexpr ui64 PriorityTxShift = 32;

std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const TStageInfo& stageInfo, const TTask& task);

inline bool IsDebugLogEnabled() {
    return TlsActivationContext->LoggerSettings() &&
           TlsActivationContext->LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER);
}

struct TStageScheduleInfo {
    double StageCost = 0.0;
    ui32 TaskCount = 0;
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
class TKqpExecuterBase : public TActor<TDerived> {
    static_assert(ExecType == EExecType::Data || ExecType == EExecType::Scan);

public:
    TKqpExecuterBase(IKqpGateway::TExecPhysicalRequest&& request,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
        const TGUCSettings::TPtr GUCSettings,
        TPartitionPrunerConfig partitionPrunerConfig,
        const TString& database,
        const TIntrusiveConstPtr<NACLib::TUserToken>& userToken,
        TResultSetFormatSettings resultSetFormatSettings,
        TKqpRequestCounters::TPtr counters,
        const TExecuterConfig& executerConfig,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        ui32 statementResultIndex,
        ui64 spanVerbosity = 0, TString spanName = "KqpExecuterBase",
        bool streamResult = false, const TActorId bufferActorId = {}, const IKqpTransactionManagerPtr& txManager = nullptr,
        TMaybe<NBatchOperations::TSettings> batchOperationSettings = Nothing())
        : NActors::TActor<TDerived>(&TDerived::ReadyState)
        , Request(std::move(request))
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FederatedQuerySetup(federatedQuerySetup)
        , GUCSettings(GUCSettings)
        , BufferActorId(bufferActorId)
        , TxManager(txManager)
        , Database(database)
        , UserToken(userToken)
        , ResultSetFormatSettings(std::move(resultSetFormatSettings))
        , Counters(counters)
        , ExecuterSpan(spanVerbosity, std::move(Request.TraceId), spanName)
        , Planner(nullptr)
        , ExecuterRetriesConfig(executerConfig.TableServiceConfig.GetExecuterRetriesConfig())
        , AggregationSettings(executerConfig.TableServiceConfig.GetAggregationConfig())
        , HasOlapTable(false)
        , StreamResult(streamResult)
        , StatementResultIndex(statementResultIndex)
        , BlockTrackingMode(executerConfig.TableServiceConfig.GetBlockTrackingMode())
        , VerboseMemoryLimitException(executerConfig.MutableConfig->VerboseMemoryLimitException.load())
        , BatchOperationSettings(std::move(batchOperationSettings))
        , AccountDefaultPoolInScheduler(executerConfig.TableServiceConfig.GetComputeSchedulerSettings().GetAccountDefaultPool())
        , TasksGraph(Request.TxAlloc, partitionPrunerConfig, AggregationSettings, Counters)
    {
        if (executerConfig.TableServiceConfig.HasArrayBufferMinFillPercentage()) {
            ArrayBufferMinFillPercentage = executerConfig.TableServiceConfig.GetArrayBufferMinFillPercentage();
        }

        if (executerConfig.TableServiceConfig.HasBufferPageAllocSize()) {
            BufferPageAllocSize = executerConfig.TableServiceConfig.GetBufferPageAllocSize();
        }

        EnableReadsMerge = *MergeDatashardReadsControl() == 1;
        TasksGraph.GetMeta().Snapshot = IKqpGateway::TKqpSnapshot(Request.Snapshot.Step, Request.Snapshot.TxId);
        TasksGraph.GetMeta().Arena = MakeIntrusive<NActors::TProtoArenaHolder>();
        TasksGraph.GetMeta().RequestIsolationLevel = Request.IsolationLevel;
        TasksGraph.GetMeta().Database = Database;
        TasksGraph.GetMeta().ChannelTransportVersion = executerConfig.TableServiceConfig.GetChannelTransportVersion();
        TasksGraph.GetMeta().UserRequestContext = userRequestContext;
        TasksGraph.GetMeta().CheckDuplicateRows = executerConfig.MutableConfig->EnableRowsDuplicationCheck.load();
        if (BatchOperationSettings) {
            TasksGraph.GetMeta().MaxBatchSize = BatchOperationSettings->MaxBatchSize;
        }
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(Request.TxAlloc, ExecType);
        ResponseEv->Orbit = std::move(Request.Orbit);
        Stats = std::make_unique<TQueryExecutionStats>(Request.StatsMode, &TasksGraph,
            ResponseEv->Record.MutableResponse()->MutableResult()->MutableStats());

        StartTime = TAppData::TimeProvider->Now();
        if (Request.Timeout) {
            Deadline = StartTime + Request.Timeout;
        }
        if (Request.CancelAfter) {
            CancelAt = StartTime + *Request.CancelAfter;
        }

        LOG_T("Bootstrap done, become ReadyState");
    }

    TActorId SelfId() {
       return TActor<TDerived>::SelfId();
    }

    TString BuildMemoryLimitExceptionMessage() const {
        if (Request.TxAlloc) {
            return TStringBuilder() << "Memory limit exception at " << CurrentStateFuncName()
                << ", current limit is " << Request.TxAlloc->Alloc->GetLimit() << " bytes.";
        }
        return TStringBuilder() << "Memory limit exception at " << CurrentStateFuncName();
    }

    void ReportEventElapsedTime() {
        YQL_ENSURE(Stats);

        ui64 elapsedMicros = TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
        Stats->ExecuterCpuTime += TDuration::MicroSeconds(elapsedMicros);
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
            if (ExecuterStateSpan) {
                ExecuterStateSpan.EndError(TStringBuilder() << Ydb::StatusIds_StatusCode_Name(reply.Status));
            }
            ReplyErrorAndDie(reply.Status, reply.Issues);
            return false;
        }

        if (ExecuterStateSpan) {
            ExecuterStateSpan.EndOk();
        }

        return true;
    }

    [[nodiscard]]
    bool HandleResolve(NShardResolver::TEvShardsResolveStatus::TPtr& ev) {
        auto& reply = *ev->Get();

        KqpShardsResolverId = {};
        TasksGraph.GetMeta().ShardsResolved = true;

        // TODO: count resolve time in CpuTime

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            if (ExecuterStateSpan) {
                ExecuterStateSpan.EndError(Ydb::StatusIds_StatusCode_Name(reply.Status));
            }

            LOG_W("Shards nodes resolve failed, status: " << Ydb::StatusIds_StatusCode_Name(reply.Status)
                << ", issues: " << reply.Issues.ToString());
            ReplyErrorAndDie(reply.Status, reply.Issues);
            return false;
        }
        if (ExecuterStateSpan) {
            ExecuterStateSpan.EndOk();
        }

        LOG_D("Shards nodes resolved, success: " << reply.ShardNodes.size() << ", failed: " << reply.Unresolved);

        ShardIdToNodeId = std::move(reply.ShardNodes);
        for (const auto& [shardId, nodeId] : ShardIdToNodeId) {
            ShardsOnNode[nodeId].push_back(shardId);
            ParticipantNodes.emplace(nodeId);
            if (TxManager) {
                TxManager->AddParticipantNode(nodeId);
            }
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
    };

    void SendStreamData(NKikimr::NKqp::TKqpExecuterTxResult & txResult, TVector<NYql::NDq::TDqSerializedBatch>&& batches,
        ui32 channelId, ui32 seqNo, bool finished)
    {
        auto resultIndex = *txResult.QueryResultIndex + StatementResultIndex;
        auto streamEv = MakeHolder<TEvKqpExecuter::TEvStreamData>();
        streamEv->Record.SetSeqNo(seqNo);
        streamEv->Record.SetQueryResultIndex(resultIndex);
        streamEv->Record.SetChannelId(channelId);
        streamEv->Record.SetFinished(finished);
        const auto &snap = GetSnapshot();
        if (snap.IsValid()) {
            auto vt = streamEv->Record.MutableVirtualTimestamp();
            vt->SetStep(snap.Step);
            vt->SetTxId(snap.TxId);
        }

        bool fillSchema = false;
        if (ResultSetFormatSettings.IsSchemaInclusionAlways()) {
            fillSchema = true;
        } else if (ResultSetFormatSettings.IsSchemaInclusionFirstOnly()) {
            fillSchema =
                (SentResultIndexes.find(resultIndex) == SentResultIndexes.end());
        } else {
            YQL_ENSURE(false, "Unexpected schema inclusion mode");
        }

        TKqpProtoBuilder protoBuilder{*AppData()->FunctionRegistry};
        protoBuilder.BuildYdbResultSet(
            *streamEv->Record.MutableResultSet(), std::move(batches),
            txResult.MkqlItemType, ResultSetFormatSettings, fillSchema,
            txResult.ColumnOrder, txResult.ColumnHints);

        // TODO: Calculate rows/bytes count for the arrow format of result set
        LOG_D("Send TEvStreamData to "
            << Target << ", seqNo: " << streamEv->Record.GetSeqNo()
            << ", nRows: " << streamEv->Record.GetResultSet().rows().size());

        SentResultIndexes.insert(resultIndex);
        this->Send(Target, streamEv.Release());
    }

    void OnEmptyResult() {
        for (ui32 txIdx = 0; txIdx < Request.Transactions.size(); ++txIdx) {
            auto& tx = Request.Transactions[txIdx].Body;
            for (ui32 i = 0; i < tx->ResultsSize(); ++i) {
                const auto& result = tx->GetResults(i);
                const auto& connection = result.GetConnection();
                const auto& inputStageInfo = TasksGraph.GetStageInfo(NYql::NDq::TStageId(txIdx, connection.GetStageIndex()));
                if (inputStageInfo.Tasks.size() >= 1) {
                    continue;
                }

                auto& txResult = ResponseEv->TxResults.at(i);
                TVector<NYql::NDq::TDqSerializedBatch> batches(1);
                if (StreamResult && txResult.IsStream && txResult.QueryResultIndex.Defined()) {
                    SendStreamData(txResult, std::move(batches), std::numeric_limits<ui32>::max(), 1, true);
                }
            }
        }
    }

    void HandleChannelData(NYql::NDq::TEvDqCompute::TEvChannelData::TPtr& ev) {
        auto& record = ev->Get()->Record;
        auto& channelData = record.GetChannelData();
        auto& channel = TasksGraph.GetChannel(channelData.GetChannelId());
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
            batch.Payload = NYql::MakeChunkedBuffer(std::move(computeData.Payload));

            if (!trailingResults) {
                SendStreamData(txResult, std::move(batches), channel.Id, computeData.Proto.GetSeqNo(), channelData.GetFinished());
            } else {
                auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();
                ackEv->Record.SetSeqNo(computeData.Proto.GetSeqNo());
                ackEv->Record.SetChannelId(channel.Id);
                ackEv->Record.SetFreeSpace(50_MB);
                this->Send(channelComputeActorId, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ channel.Id);
                ui64 rowCount = batch.RowCount();
                ResponseEv->TakeResult(channel.DstInputIndex, std::move(batch));
                txResult.HasTrailingResult = true;
                LOG_D("staging TEvStreamData to " << Target << ", seqNo: " << computeData.Proto.GetSeqNo()
                    << ", nRows: " << rowCount);
            }

            return;
        }

        NYql::NDq::TDqSerializedBatch batch;
        batch.Proto = std::move(*record.MutableChannelData()->MutableData());
        if (batch.Proto.HasPayloadId()) {
            batch.Payload = NYql::MakeChunkedBuffer(ev->Get()->GetPayload(batch.Proto.GetPayloadId()));
        }

        YQL_ENSURE(channel.DstTask == 0);
        YQL_ENSURE(Stats);

        Stats->ResultBytes += batch.Size();
        Stats->ResultRows += batch.RowCount();

        LOG_T("Got result, channelId: " << channel.Id
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
        if (ev->Get()->Record.GetChannelId() == std::numeric_limits<ui32>::max())
            return;

        ui64 channelId;
        if (ResponseEv->TxResults.size() == 1) {
            YQL_ENSURE(!ResultChannelToComputeActor.empty());
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

    bool HandleComputeStats(NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
        TActorId computeActor = ev->Sender;
        auto& state = ev->Get()->Record;
        ui64 taskId = state.GetTaskId();

        LOG_D("ActorState: " << CurrentStateFuncName()
            << ", got execution state from compute actor: " << computeActor
            << ", task: " << taskId
            << ", state: " << NYql::NDqProto::EComputeState_Name((NYql::NDqProto::EComputeState) state.GetState())
            << ", stats: " << state.GetStats());

        YQL_ENSURE(Stats);

        if (state.HasStats()) {
            ui64 cycleCount = GetCycleCountFast();

            Stats->UpdateTaskStats(taskId, state.GetStats(), (NYql::NDqProto::EComputeState) state.GetState());

            if (Stats->DeadlockedStageId) {
                NYql::TIssues issues;
                issues.AddIssue(TStringBuilder() << "Deadlock detected: stage " << *Stats->DeadlockedStageId << " waits for input while peer(s) wait for output");
                auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::CANCELLED, issues);
                this->Send(this->SelfId(), abortEv.Release());
            }

            if (Request.ProgressStatsPeriod) {
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
            auto collectBytes = Stats->EstimateCollectMem();
            auto deltaCpuTime = NHPTimer::GetSeconds(GetCycleCountFast() - cycleCount);

            Counters->Counters->QueryStatMemCollectInflightBytes->Add(
                static_cast<i64>(collectBytes) - static_cast<i64>(StatCollectInflightBytes)
            );
            StatCollectInflightBytes = collectBytes;
            Counters->Counters->QueryStatCpuCollectUs->Add(deltaCpuTime * 1'000'000);
        }

        YQL_ENSURE(Planner);
        bool ack = Planner->AcknowledgeCA(taskId, computeActor, &state);

        switch (state.GetState()) {
            case NYql::NDqProto::COMPUTE_STATE_FAILURE:
            case NYql::NDqProto::COMPUTE_STATE_FINISHED:
                // Don't finalize stats twice.
                if (Planner->CompletedCA(taskId, computeActor)) {
                    ui64 cycleCount = GetCycleCountFast();

                    auto& extraData = ExtraData[computeActor];
                    extraData.TaskId = taskId;
                    extraData.Data.Swap(state.MutableExtraData());

                    Stats->AddComputeActorStats(
                        computeActor.NodeId(),
                        std::move(*state.MutableStats()),
                        (NYql::NDqProto::EComputeState) state.GetState(),
                        TDuration::MilliSeconds(AggregationSettings.GetCollectLongTasksStatsTimeoutMs())
                    );

                    LastTaskId = taskId;
                    LastComputeActorId = computeActor.ToString();

                    auto collectBytes = Stats->EstimateFinishMem();
                    auto deltaCpuTime = NHPTimer::GetSeconds(GetCycleCountFast() - cycleCount);

                    Counters->Counters->QueryStatMemFinishInflightBytes->Add(
                        static_cast<i64>(collectBytes) - static_cast<i64>(StatFinishInflightBytes)
                    );
                    StatFinishInflightBytes = collectBytes;
                    Counters->Counters->QueryStatCpuFinishUs->Add(deltaCpuTime * 1'000'000);
                }
            default:
                ; // ignore all other states.
        }

        return ack;
    }

    void HandleComputeState(NYql::NDq::TEvDqCompute::TEvState::TPtr& ev) {
        TActorId computeActor = ev->Sender;
        auto& state = ev->Get()->Record;
        ui64 taskId = state.GetTaskId();

        bool populateChannels = HandleComputeStats(ev);

        switch (state.GetState()) {
            case NYql::NDqProto::COMPUTE_STATE_UNKNOWN: {
                YQL_ENSURE(false, "unexpected state from " << computeActor << ", task: " << taskId);
                return;
            }

            case NYql::NDqProto::COMPUTE_STATE_EXECUTING: {
                if (populateChannels) {
                    auto& task = TasksGraph.GetTask(taskId);
                    THashMap<TActorId, THashSet<ui64>> updates;
                    Planner->CollectTaskChannelsUpdates(task, updates);
                    Planner->PropagateChannelsUpdates(updates);
                }
                break;
            }

            default:
                ; // ignore all other states.
        }

        if (state.GetState() == NYql::NDqProto::COMPUTE_STATE_FAILURE) {
            ReplyErrorAndDie(NYql::NDq::DqStatusToYdbStatus(state.GetStatusCode()), state.MutableIssues());
            return;
        }

        if (CheckpointCoordinatorId) {
            TlsActivationContext->Send(ev->Forward(CheckpointCoordinatorId));
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

    void HandleReady(TEvKqpExecuter::TEvTxRequest::TPtr ev) {
        TasksGraph.GetMeta().ExecuterId = SelfId();

        TxId = ev->Get()->Record.GetRequest().GetTxId();
        Target = ActorIdFromProto(ev->Get()->Record.GetTarget());

        const auto& databaseId = GetUserRequestContext()->DatabaseId;
        const auto& poolId = GetUserRequestContext()->PoolId.empty() ? NResourcePool::DEFAULT_POOL_ID : GetUserRequestContext()->PoolId;

        if (!databaseId.empty() && (poolId != NResourcePool::DEFAULT_POOL_ID || AccountDefaultPoolInScheduler)) {
            const auto schedulerServiceId = MakeKqpSchedulerServiceId(SelfId().NodeId());

            // TODO: deliberately create the database here - since database doesn't have any useful scheduling properties for now.
            //       Replace with more precise database events in the future.
            auto addDatabaseEvent = MakeHolder<NScheduler::TEvAddDatabase>();
            addDatabaseEvent->Id = databaseId;
            this->Send(schedulerServiceId, addDatabaseEvent.Release());

            // TODO: replace with more precise pool events.
            auto addPoolEvent = MakeHolder<NScheduler::TEvAddPool>(databaseId, poolId);
            this->Send(schedulerServiceId, addPoolEvent.Release());

            auto addQueryEvent = MakeHolder<NScheduler::TEvAddQuery>();
            addQueryEvent->DatabaseId = databaseId;
            addQueryEvent->PoolId = poolId;
            addQueryEvent->QueryId = TxId;
            this->Send(schedulerServiceId, addQueryEvent.Release(), 0, TxId);
            Query = (co_await ActorWaitForEvent<NScheduler::TEvQueryResponse>(TxId))->Get()->Query; // TODO: Y_DEFER
        }

        auto lockTxId = Request.AcquireLocksTxId;
        if (lockTxId.Defined() && *lockTxId == 0) {
            lockTxId = TxId;
        }

        TasksGraph.GetMeta().SetLockTxId(lockTxId);
        TasksGraph.GetMeta().SetLockNodeId(SelfId().NodeId());

        switch (Request.IsolationLevel) {
            case NKikimrKqp::ISOLATION_LEVEL_SNAPSHOT_RW:
                TasksGraph.GetMeta().SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
                break;
            default:
                TasksGraph.GetMeta().SetLockMode(NKikimrDataEvents::OPTIMISTIC);
                break;
        }

        LWTRACK(KqpBaseExecuterHandleReady, ResponseEv->Orbit, TxId);
        if (IsDebugLogEnabled()) {
            for (auto& tx : Request.Transactions) {
                LOG_D("Executing physical tx, type: " << (ui32) tx.Body->GetType() << ", stages: " << tx.Body->StagesSize());
            }
        }

        if (BufferActorId && Request.LocksOp == ELocksOp::Rollback) {
            static_cast<TDerived*>(this)->Finalize();
            co_return;
        }

        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterTableResolve, ExecuterSpan.GetTraceId(), "WaitForTableResolve", NWilson::EFlags::AUTO_END);

        TasksGraph.FillKqpTasksGraphStages(Request.Transactions);
        auto kqpTableResolver = CreateKqpTableResolver(this->SelfId(), TxId, UserToken, TasksGraph);
        KqpTableResolverId = this->RegisterWithSameMailbox(kqpTableResolver);

        LOG_T("Got request, become WaitResolveState");
        this->Become(&TDerived::WaitResolveState);

        auto now = TAppData::TimeProvider->Now();
        StartResolveTime = now;

        YQL_ENSURE(Stats);

        Stats->StartTs = now;
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
                switch (reason) {
                    case TEvents::TEvUndelivered::EReason::ReasonActorUnknown: {
                        LOG_D("Schedule a retry by ActorUnknown reason, nodeId:" << ev->Sender.NodeId() << " requestId: " << ev->Cookie);
                        this->Schedule(TDuration::MilliSeconds(Planner->GetCurrentRetryDelay(ev->Cookie)), new typename TEvPrivate::TEvRetry(ev->Cookie, ev->Sender));
                        return;
                    }
                    case TEvents::TEvUndelivered::EReason::Disconnected: {
                        InvalidateNode(ev->Sender.NodeId());
                        ReplyUnavailable(TStringBuilder()
                            << "Failed to send EvStartKqpTasksRequest because node is unavailable: " << ev->Sender.NodeId());
                        return;
                    }
                    case TEvents::TEvUndelivered::EReason::ReasonUnknown: {
                        InvalidateNode(ev->Sender.NodeId());
                        InternalError(TStringBuilder() << "TEvKqpNode::TEvStartKqpTasksRequest lost: " << reason);
                        return;
                    }
                }
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
                case NKikimrKqp::TEvStartKqpTasksResponse::NODE_SHUTTING_DOWN: {
                    for (auto& task : record.GetNotStartedTasks()) {
                        if (task.GetReason() == NKikimrKqp::TEvStartKqpTasksResponse::NODE_SHUTTING_DOWN
                              and ev->Sender.NodeId() != SelfId().NodeId()) {
                            Planner->SendStartKqpTasksRequest(task.GetRequestId(), SelfId());
                        }
                    }
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

            TActorId computeActorId = ActorIdFromProto(startedTask.GetActorId());
            LOG_D("Executing task: " << taskId << " on compute actor: " << computeActorId);
            YQL_ENSURE(Planner);
            bool ack = Planner->AcknowledgeCA(taskId, computeActorId, nullptr);
            if (ack) {
                Planner->CollectTaskChannelsUpdates(task, channelsUpdates);
            }

        }

        Planner->PropagateChannelsUpdates(channelsUpdates);
    }

    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        NYql::TIssues issues = ev->Get()->GetIssues();
        HandleAbortExecution(msg.GetStatusCode(), ev->Get()->GetIssues(), ev->Sender != Target);
    }

    void HandleAbortExecution(
            NYql::NDqProto::StatusIds::StatusCode statusCode,
            const NYql::TIssues& issues,
            const bool sessionSender) {
        LOG_D("Got EvAbortExecution, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode)
            << ", message: " << issues.ToOneLineString());
        auto ydbStatusCode = NYql::NDq::DqStatusToYdbStatus(statusCode);
        if (ydbStatusCode == Ydb::StatusIds::INTERNAL_ERROR) {
            InternalError(issues);
        } else if (ydbStatusCode == Ydb::StatusIds::TIMEOUT) {
            TimeoutError(sessionSender, issues);
        } else {
            RuntimeError(NYql::NDq::DqStatusToYdbStatus(statusCode), issues);
        }
    }

protected:
    void UpdateResourcesUsage(bool force) {
        TInstant now = TActivationContext::Now();
        if ((now - LastResourceUsageUpdate < ResourceUsageUpdateInterval) && !force)
            return;

        LastResourceUsageUpdate = now;

        TProgressStat::TEntry consumption;

        if (Planner) {
            consumption += Planner->CalculateConsumptionUpdate();
        }

        auto ru = NRuCalc::CalcRequestUnit(consumption);

        YQL_ENSURE(consumption.ReadIOStat.Rows < PotentialUnsigned64OverflowLimit);
        YQL_ENSURE(ru < PotentialUnsigned64OverflowLimit);

        // Some heuristic to reduce overprice due to round part stats
        if (ru <= 100 && !force)
            return;

        if (Planner) {
            Planner->ShiftConsumption();
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
            YQL_ENSURE(structuredTokenParser.HasIAMToken(), "only token authentication supported for compute tasks");
            secureParams.emplace(secretName, structuredTokenParser.GetIAMToken());
        }
    }

    std::map<ui32, TStageScheduleInfo> ScheduleByCost(const IKqpGateway::TPhysicalTxData& tx, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot) {
        std::map<ui32, TStageScheduleInfo> result;
        if (!resourceSnapshot.empty()) // can't schedule w/o node count
        {
            // collect costs and schedule stages with external sources only
            double totalCost = 0.0;
            for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
                auto& stage = tx.Body->GetStages(stageIdx);
                if (stage.SourcesSize() > 0 && stage.GetSources(0).GetTypeCase() == NKqpProto::TKqpSource::kExternalSource) {
                    if (stage.GetStageCost() > 0.0 && stage.GetTaskCount() == 0) {
                        totalCost += stage.GetStageCost();
                        result.emplace(stageIdx, TStageScheduleInfo{.StageCost = stage.GetStageCost()});
                    }
                }
            }
            // assign task counts
            if (!result.empty()) {
                // allow use 2/3 of threads in single stage
                ui32 maxStageTaskCount = (TStagePredictor::GetUsableThreads() * 2 + 2) / 3;
                // total limit per mode is x2
                ui32 maxTotalTaskCount = maxStageTaskCount * 2;
                for (auto& [_, stageInfo] : result) {
                    // schedule tasks evenly between nodes
                    stageInfo.TaskCount =
                        std::max<ui32>(
                            std::min(static_cast<ui32>(maxTotalTaskCount * stageInfo.StageCost / totalCost), maxStageTaskCount)
                            , 1
                        ) * resourceSnapshot.size();
                }
            }
        }
        return result;
    }

    template <bool isScan>
    auto BuildAllTasks(bool limitTasksPerNode, std::optional<TLlvmSettings> llvmSettings) {
        size_t sourceScanPartitionsCount = 0;

        for (ui32 txIdx = 0; txIdx < Request.Transactions.size(); ++txIdx) {
            const auto& tx = Request.Transactions[txIdx];
            auto scheduledTaskCount = ScheduleByCost(tx, ResourcesSnapshot);
            for (ui32 stageIdx = 0; stageIdx < tx.Body->StagesSize(); ++stageIdx) {
                const auto& stage = tx.Body->GetStages(stageIdx);
                auto& stageInfo = TasksGraph.GetStageInfo(NYql::NDq::TStageId(txIdx, stageIdx));

                // build task conditions
                const bool buildFromSourceTasks = stage.SourcesSize() > 0;
                const bool buildSysViewTasks = stageInfo.Meta.IsSysView();
                const bool buildComputeTasks = stageInfo.Meta.ShardOperations.empty() || (!isScan && stage.SinksSize() > 0);
                const bool buildScanTasks = isScan
                    ? stageInfo.Meta.IsOlap() || stageInfo.Meta.IsDatashard()
                    : (AllowOlapDataQuery || StreamResult) && stageInfo.Meta.IsOlap() && stage.SinksSize() == 0
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
                            if (EnableReadsMerge) {
                                limitTasksPerNode = true;
                                stageInfo.Introspections.push_back("Using tasks count limit because of enabled reads merge");
                            }
                            if (auto partitionsCount = TasksGraph.BuildScanTasksFromSource(stageInfo, limitTasksPerNode, ShardIdToNodeId, Stats.get())) {
                                sourceScanPartitionsCount += *partitionsCount;
                            } else {
                                UnknownAffectedShardCount = true;
                            }
                        } break;
                        case NKqpProto::TKqpSource::kExternalSource: {
                            YQL_ENSURE(!isScan);
                            auto it = scheduledTaskCount.find(stageIdx);
                            TasksGraph.BuildReadTasksFromSource(stageInfo, ResourcesSnapshot, it != scheduledTaskCount.end() ? it->second.TaskCount : 0, SecureParams);
                        } break;
                        default:
                            YQL_ENSURE(false, "unknown source type");
                    }
                } else if (buildSysViewTasks) {
                    TasksGraph.BuildSysViewScanTasks(stageInfo);
                } else if (buildComputeTasks) {
                    auto nodesCount = ShardsOnNode.size();
                    if constexpr (!isScan) {
                        nodesCount = std::max<ui32>(nodesCount, ResourcesSnapshot.size());
                    }
                    UnknownAffectedShardCount |= TasksGraph.BuildComputeTasks(stageInfo, nodesCount);
                } else if (buildScanTasks) {
                    HasOlapTable = true;
                    TasksGraph.BuildScanTasksFromShards(stageInfo, tx.Body->EnableShuffleElimination(), ShardIdToNodeId,
                        CollectProfileStats(Request.StatsMode) ? Stats.get() : nullptr);
                } else {
                    if constexpr (!isScan) {
                        auto shardsWithEffects = TasksGraph.BuildDatashardTasks(stageInfo, TxManager);
                        ShardsWithEffects.insert(shardsWithEffects.begin(), shardsWithEffects.end());
                    } else {
                        YQL_ENSURE(false, "Unexpected stage type " << (int) stageInfo.Meta.TableKind);
                    }
                }

                if (llvmSettings) {
                    const bool useLlvm = llvmSettings->GetUseLlvm(stage.GetProgram().GetSettings());
                    for (auto& taskId : stageInfo.Tasks) {
                        TasksGraph.GetTask(taskId).SetUseLlvm(useLlvm);
                    }
                    if (Stats && CollectProfileStats(Request.StatsMode)) {
                        Stats->SetUseLlvm(stageInfo.Id.StageId, useLlvm);
                    }
                }

                if (stage.GetIsSinglePartition()) {
                    YQL_ENSURE(stageInfo.Tasks.size() <= 1, "Unexpected multiple tasks in single-partition stage");
                }

                for (const auto& taskId : stageInfo.Tasks) {
                    auto& task = TasksGraph.GetTask(taskId);

                    task.Meta.ExecuterId = this->SelfId();
                    FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                    BuildSinks(stage, stageInfo, task);
                }

                // Not task-related
                TasksGraph.GetMeta().AllowWithSpilling |= stage.GetAllowWithSpilling();
                if (!TasksGraph.GetMeta().IsRestored) {
                    TasksGraph.BuildKqpStageChannels(stageInfo, TxId, /* enableSpilling */ GetTasksGraph().GetMeta().AllowWithSpilling, tx.Body->EnableShuffleElimination());
                }
            }

            // Not task-related
            ResponseEv->InitTxResult(tx.Body);
            TasksGraph.BuildKqpTaskGraphResultChannels(tx.Body, txIdx);
        }

        OnEmptyResult();

        return sourceScanPartitionsCount;
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

    void BuildInternalSinks(const NKqpProto::TKqpSink& sink, const TStageInfo& stageInfo, TKqpTasksGraph::TTaskType& task) {
        const auto& intSink = sink.GetInternalSink();
        auto& output = task.Outputs[sink.GetOutputIndex()];
        output.Type = TTaskOutputType::Sink;
        output.SinkType = intSink.GetType();

        if (intSink.GetSettings().Is<NKikimrKqp::TKqpTableSinkSettings>()) {
            NKikimrKqp::TKqpTableSinkSettings settings;
            if (!stageInfo.Meta.ResolvedSinkSettings) {
                YQL_ENSURE(intSink.GetSettings().UnpackTo(&settings), "Failed to unpack settings");
            } else {
                settings = *stageInfo.Meta.ResolvedSinkSettings;
            }

            auto& lockTxId = TasksGraph.GetMeta().LockTxId;
            if (lockTxId) {
                settings.SetLockTxId(*lockTxId);
                settings.SetLockNodeId(SelfId().NodeId());
            }
            if (!settings.GetInconsistentTx() && !settings.GetIsOlap()) {
                ActorIdToProto(BufferActorId, settings.MutableBufferActorId());
            }
            if (!settings.GetInconsistentTx()
                    && GetSnapshot().IsValid()) {
                settings.MutableMvccSnapshot()->SetStep(GetSnapshot().Step);
                settings.MutableMvccSnapshot()->SetTxId(GetSnapshot().TxId);
            }
            if (!settings.GetInconsistentTx() && TasksGraph.GetMeta().LockMode) {
                settings.SetLockMode(*TasksGraph.GetMeta().LockMode);
            }

            settings.SetPriority((task.StageId.TxId << PriorityTxShift) + settings.GetPriority());

            output.SinkSettings.ConstructInPlace();
            output.SinkSettings->PackFrom(settings);
        } else {
            output.SinkSettings = intSink.GetSettings();
        }
    }

    void BuildSinks(const NKqpProto::TKqpPhyStage& stage, const TStageInfo& stageInfo, TKqpTasksGraph::TTaskType& task) {
        if (stage.SinksSize() > 0) {
            YQL_ENSURE(stage.SinksSize() == 1, "multiple sinks are not supported");
            const auto& sink = stage.GetSinks(0);
            YQL_ENSURE(sink.GetOutputIndex() < task.Outputs.size());

            if (sink.HasInternalSink()) {
                BuildInternalSinks(sink, stageInfo, task);
            } else if (sink.HasExternalSink()) {
                BuildExternalSinks(sink, task);
            } else {
                YQL_ENSURE(false, "unknown sink type");
            }
        }
    }

    void FillReadTaskFromSource(TTask& task, const TString& sourceName, const TString& structuredToken, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui64 nodeOffset) {
        if (structuredToken) {
            task.Meta.SecureParams.emplace(sourceName, structuredToken);
        }

        if (resourceSnapshot.empty()) {
            task.Meta.Type = TTaskMeta::TTaskType::Compute;
        } else {
            task.Meta.NodeId = resourceSnapshot[nodeOffset % resourceSnapshot.size()].GetNodeId();
            task.Meta.Type = TTaskMeta::TTaskType::Scan;
        }
    }

    void FillReadInfo(TTaskMeta& taskMeta, ui64 itemsLimit, const NYql::ERequestSorting sorting) const
    {
        if (taskMeta.Reads && !taskMeta.Reads.GetRef().empty()) {
            // Validate parameters
            YQL_ENSURE(taskMeta.ReadInfo.ItemsLimit == itemsLimit);
            YQL_ENSURE(taskMeta.ReadInfo.GetSorting() == sorting);
            return;
        }

        taskMeta.ReadInfo.ItemsLimit = itemsLimit;
        taskMeta.ReadInfo.SetSorting(sorting);
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
                NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromMiniKQLType(memberType);
                taskMeta.ReadInfo.ResultColumnsTypes.back() = typeInfo;
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
            .Ranges = {},
            .Columns = columns,
        };
        if (keyReadRanges) {
            readInfo.Ranges = std::move(*keyReadRanges); // sorted & non-intersecting
        }

        if (isPersistentScan) {
            readInfo.ShardId = shardId;
        }

        FillReadInfo(meta, readSettings.ItemsLimit, readSettings.GetSorting());
        if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange) {
            FillOlapReadInfo(meta, readSettings.ResultType, op.GetReadOlapRange());
        }

        if (!meta.Reads) {
            meta.Reads.ConstructInPlace();
        }

        meta.Reads->emplace_back(std::move(readInfo));
    }

    ui32 GetScanTasksPerNode(
        TStageInfo& stageInfo,
        const bool isOlapScan,
        const ui64 nodeId,
        bool enableShuffleElimination = false
    ) const {
        auto& intros = stageInfo.Introspections;
        const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);
        if (const auto taskCount = stage.GetTaskCount()) {
            intros.push_back("Manually overridden - " + ToString(taskCount));
            return taskCount;
        }

        ui32 result = 0;
        if (isOlapScan) {
            intros.push_back("This is OLAP scan");
            if (AggregationSettings.HasCSScanThreadsPerNode()) {
                result = AggregationSettings.GetCSScanThreadsPerNode();
                intros.push_back("Using the CSScanThreadsPerNode value - " + ToString(result));
            } else {
                const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
                result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), {}, intros);
                intros.push_back("Predicted value for OLAP scan - " + ToString(result));
            }
        } else {
            result = AggregationSettings.GetDSScanMinimalThreads();
            intros.push_back("Using the DSScanMinimalThreads value - " + ToString(result));
            if (stage.GetProgram().GetSettings().GetHasSort()) {
                result = std::max(result, AggregationSettings.GetDSBaseSortScanThreads());
                intros.push_back("Considering DSBaseSortScanThreads value because program has sort - " + ToString(result));
            }
            if (stage.GetProgram().GetSettings().GetHasMapJoin()) {
                result = std::max(result, AggregationSettings.GetDSBaseJoinScanThreads());
                intros.push_back("Considering DSBaseJoinScanThreads value because program has MapJoin - " + ToString(result));
            }
        }
        result = Max<ui32>(1, result);

        if (enableShuffleElimination) {
            result *= 2;
            intros.push_back("Multiply by 2 because of Shuffle Elimination - " + ToString(result));
        }

        intros.push_back("Predicted number of scan tasks per node " + ToString(nodeId) + " - " + ToString(result));
        return result;
    }

    bool BuildPlannerAndSubmitTasks() {
        Planner = CreateKqpPlanner({
            .TasksGraph = TasksGraph,
            .TxId = TxId,
            .Executer = SelfId(),
            .Database = Database,
            .UserToken = UserToken,
            .Deadline = Deadline.GetOrElse(TInstant::Zero()),
            .StatsMode = Request.StatsMode,
            .WithProgressStats = Request.ProgressStatsPeriod != TDuration::Zero(),
            .RlPath = Request.RlPath,
            .ExecuterSpan =  ExecuterSpan,
            .ResourcesSnapshot = std::move(ResourcesSnapshot),
            .ExecuterRetriesConfig = ExecuterRetriesConfig,
            .MkqlMemoryLimit = Request.MkqlMemoryLimit,
            .AsyncIoFactory = AsyncIoFactory,
            .FederatedQuerySetup = FederatedQuerySetup,
            .OutputChunkMaxSize = Request.OutputChunkMaxSize,
            .GUCSettings = GUCSettings,
            .ResourceManager_ = Request.ResourceManager_,
            .CaFactory_ = Request.CaFactory_,
            .BlockTrackingMode = BlockTrackingMode,
            .ArrayBufferMinFillPercentage = ArrayBufferMinFillPercentage,
            .BufferPageAllocSize = BufferPageAllocSize,
            .VerboseMemoryLimitException = VerboseMemoryLimitException,
            .Query = Query,
            .CheckpointCoordinator = CheckpointCoordinatorId
        });

        auto err = Planner->PlanExecution();
        if (err) {
            TlsActivationContext->Send(err.release());
            return false;
        }

        Planner->Submit();
        return true;
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
        const auto runScriptActorId = GetUserRequestContext()->RunScriptActorId;
        Y_ENSURE(runScriptActorId);
        this->Send(runScriptActorId, scriptEffects.release());
    }

protected:
    void TerminateComputeActors(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        for (const auto& task : this->TasksGraph.GetTasks()) {
            if (task.ComputeActorId && !task.Meta.Completed) {
                LOG_I("aborting compute actor execution, message: " << issues.ToOneLineString()
                    << ", compute actor: " << task.ComputeActorId << ", task: " << task.Id);

                auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDq::YdbStatusToDqStatus(code), issues);
                this->Send(task.ComputeActorId, ev.Release());
            } else {
                LOG_I("task: " << task.Id << ", does not have the CA id yet or is already complete");
            }
        }
    }

    void TerminateComputeActors(Ydb::StatusIds::StatusCode code, const TString& message) {
        TerminateComputeActors(code, NYql::TIssues({NYql::TIssue(message)}));
    }

protected:
    void UnexpectedEvent(const TString& state, ui32 eventType) {
        if (eventType == TEvents::TEvPoison::EventType) {
            LOG_D("TKqpExecuter, TEvPoison event at state:" << state << ", selfID: " << this->SelfId());
            InternalError(TStringBuilder() << "TKqpExecuter got poisoned, state: " << state);
        } else {
            LOG_E("TKqpExecuter, unexpected event: " << eventType << ", at state:" << state << ", selfID: " << this->SelfId());
            InternalError(TStringBuilder() << "Unexpected event at TKqpExecuter, state: " << state << ", event: " << eventType);
        }
    }

    void InternalError(const NYql::TIssues& issues) {
        LOG_E(issues.ToOneLineString());
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
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
        issue.AddSubIssue(new NYql::TIssue(message));
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
    }

    void RuntimeError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        LOG_E(Ydb::StatusIds_StatusCode_Name(code) << ": " << issues.ToOneLineString());
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

    void TimeoutError(bool sessionSender, NYql::TIssues issues) {
        if (AlreadyReplied) {
            LOG_E("Timeout when we already replied - not good" << Endl << TBackTrace().PrintToString() << Endl);
            return;
        }

        const auto status = NYql::NDqProto::StatusIds::TIMEOUT;
        if (issues.Empty()) {
            issues.AddIssue("Request timeout exceeded");
        }

        TerminateComputeActors(Ydb::StatusIds::TIMEOUT, issues);

        AlreadyReplied = true;

        LOG_E("Abort execution: " << NYql::NDqProto::StatusIds_StatusCode_Name(status) << ", " << issues.ToOneLineString());
        if (ExecuterSpan) {
            ExecuterSpan.EndError(TStringBuilder() << NYql::NDqProto::StatusIds_StatusCode_Name(status));
        }

        ResponseEv->Record.MutableResponse()->SetStatus(Ydb::StatusIds::TIMEOUT);
        NYql::IssuesToMessage(issues, ResponseEv->Record.MutableResponse()->MutableIssues());

        // TEvAbortExecution can come from either ComputeActor or SessionActor (== Target).
        if (!sessionSender) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(status, issues);
            this->Send(Target, abortEv.Release());
        }

        LOG_E("Sending timeout response to: " << Target);

        // Pass away immediately, since we already sent response - don't wait for stats.
        this->PassAway();
    }

    virtual void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        if (AlreadyReplied) {
            LOG_E("Error when we already replied - not good" << Endl << TBackTrace().PrintToString() << Endl);
            return;
        }

        TerminateComputeActors(status, "Terminate execution");

        AlreadyReplied = true;
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        if (issues) {
            response.MutableIssues()->Swap(issues);
        }

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

        if (ExecuterSpan) {
            ExecuterSpan.EndError(response.DebugString());
        }
        if (ExecuterStateSpan) {
            ExecuterStateSpan.EndError(response.DebugString());
        }

        this->Shutdown();
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

    const IKqpGateway::TKqpSnapshot& GetSnapshot() const {
        return TasksGraph.GetMeta().Snapshot;
    }

    void SetSnapshot(ui64 step, ui64 txId) {
        TasksGraph.GetMeta().SetSnapshot(step, txId);
    }

protected:
    // Introduced separate method from `PassAway()` - to not get confused with expectations from other actors,
    // that `PassAway()` should kill actor immediately.
    virtual void Shutdown() {
        PassAway();
    }

    void PassAway() override {
        YQL_ENSURE(AlreadyReplied && ResponseEv);

        ResponseEv->ParticipantNodes = std::move(ParticipantNodes);

        // Fill response stats
        {
            auto& response = *ResponseEv->Record.MutableResponse();

            YQL_ENSURE(Stats);

            ReportEventElapsedTime();

            Stats->FinishTs = TInstant::Now();

            Stats->Finish();

            if (Stats->CollectStatsByLongTasks || CollectFullStats(Request.StatsMode)) {

                ui64 jsonSize = 0;
                ui64 cycleCount = GetCycleCountFast();

                response.MutableResult()->MutableStats()->ClearTxPlansWithStats();
                for (ui32 txId = 0; txId < Request.Transactions.size(); ++txId) {
                    const auto& tx = Request.Transactions[txId].Body;
                    auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), response.GetResult().GetStats());
                    jsonSize += planWithStats.size();
                    response.MutableResult()->MutableStats()->AddTxPlansWithStats(planWithStats);
                }

                auto deltaCpuTime = NHPTimer::GetSeconds(GetCycleCountFast() - cycleCount);
                Counters->Counters->QueryStatCpuConvertUs->Add(deltaCpuTime * 1'000'000);
                Counters->Counters->QueryStatMemConvertBytes->Add(jsonSize);
                response.MutableResult()->MutableStats()->SetStatConvertBytes(jsonSize);
            }

            if (Stats->CollectStatsByLongTasks) {
                const auto& txPlansWithStats = response.GetResult().GetStats().GetTxPlansWithStats();
                if (!txPlansWithStats.empty()) {
                    LOG_I("Full stats: " << response.GetResult().GetStats());
                }
            }

            if (!BatchOperationSettings.Empty() && !Stats->TableStats.empty()) {
                auto [_, tableStats] = *Stats->TableStats.begin();
                Counters->Counters->BatchOperationUpdateRows->Add(tableStats->GetWriteRows());
                Counters->Counters->BatchOperationUpdateBytes->Add(tableStats->GetWriteBytes());

                Counters->Counters->BatchOperationDeleteRows->Add(tableStats->GetEraseRows());
                Counters->Counters->BatchOperationDeleteBytes->Add(tableStats->GetEraseBytes());
            }

            auto finishSize = Stats->EstimateFinishMem();
            Counters->Counters->QueryStatMemFinishBytes->Add(finishSize);
            response.MutableResult()->MutableStats()->SetStatFinishBytes(finishSize);
        }

        Counters->Counters->QueryStatMemCollectInflightBytes->Sub(StatCollectInflightBytes);
        StatCollectInflightBytes = 0;
        Counters->Counters->QueryStatMemFinishInflightBytes->Sub(StatFinishInflightBytes);
        StatFinishInflightBytes = 0;

        Request.Transactions.crop(0);
        this->Send(Target, ResponseEv.release());

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
        }

        this->Send(this->SelfId(), new TEvents::TEvPoison);
        LOG_T("Terminate, become ZombieState");
        this->Become(&TKqpExecuterBase::ZombieState);
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

    bool RestoreTasksGraph() {
        if (Request.QueryPhysicalGraph) {
            TasksGraph.RestoreTasksGraphInfo(*Request.QueryPhysicalGraph);
        }

        return TasksGraph.GetMeta().IsRestored;
    }

    NYql::NDqProto::TDqTask* SerializeTaskToProto(const TTask& task, bool serializeAsyncIoSettings) {
        return TasksGraph.ArenaSerializeTaskToProto(task, serializeAsyncIoSettings);
    }

    const TKqpTasksGraph& GetTasksGraph() const {
        return TasksGraph;
    }

    auto& GetMeta() {
        return TasksGraph.GetMeta();
    }

    // TODO: remove this method, so that all task-related stuff is outside executers
    auto& GetTask(ui64 taskId) {
        return TasksGraph.GetTask(taskId);
    }

protected:
    IKqpGateway::TExecPhysicalRequest Request;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const TGUCSettings::TPtr GUCSettings;

    TActorId BufferActorId;
    IKqpTransactionManagerPtr TxManager;
    const TString Database;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TResultSetFormatSettings ResultSetFormatSettings;
    TKqpRequestCounters::TPtr Counters;
    std::unique_ptr<TQueryExecutionStats> Stats;
    TInstant LastProgressStats;
    TInstant StartTime;
    TMaybe<TInstant> Deadline;
    TMaybe<TInstant> CancelAt;
    TActorId Target;
    ui64 TxId = 0;
    NScheduler::NHdrf::NDynamic::TQueryPtr Query;

    TActorId KqpTableResolverId;
    TActorId KqpShardsResolverId;

    struct TExtraData {
        ui64 TaskId;
        NYql::NDqProto::TComputeActorExtraData Data;
    };
    THashMap<TActorId, TExtraData> ExtraData;

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
    bool AllowOlapDataQuery = true; // used by Data executer - always true for Scan executer
    bool HasOlapTable = false;
    bool StreamResult = false;
    bool UnknownAffectedShardCount = false;

    THashMap<ui64, TActorId> ResultChannelToComputeActor;

    ui32 StatementResultIndex;

    // Track which nodes (by shards) have been involved during execution
    THashSet<ui32> ParticipantNodes;

    bool AlreadyReplied = false;
    bool EnableReadsMerge = false;

    const NKikimrConfig::TTableServiceConfig::EBlockTrackingMode BlockTrackingMode;
    const bool VerboseMemoryLimitException;
    TMaybe<ui8> ArrayBufferMinFillPercentage;
    TMaybe<size_t> BufferPageAllocSize;

    ui64 StatCollectInflightBytes = 0;
    ui64 StatFinishInflightBytes = 0;

    TMaybe<NBatchOperations::TSettings> BatchOperationSettings;

    bool AccountDefaultPoolInScheduler = false;

    THashSet<ui32> SentResultIndexes;

    THashSet<ui64> ShardsWithEffects; // tracks which shards are expected to have effects - only Data executer

    TActorId CheckpointCoordinatorId;

private:
    static constexpr TDuration ResourceUsageUpdateInterval = TDuration::MilliSeconds(100);

    TKqpTasksGraph TasksGraph;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TResultSetFormatSettings resultSetFormatSettings,
    TKqpRequestCounters::TPtr counters, bool streamResult, const TExecuterConfig& executerConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const TActorId& creator,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    TPartitionPrunerConfig partitionPrunerConfig, const TShardIdToTableInfoPtr& shardIdToTableInfo,
    const IKqpTransactionManagerPtr& txManager, const TActorId bufferActorId,
    TMaybe<NBatchOperations::TSettings> batchOperationSettings, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, ui64 generation);

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TResultSetFormatSettings resultSetFormatSettings,
    TKqpRequestCounters::TPtr counters, const TExecuterConfig& executerConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    const std::optional<TLlvmSettings>& llvmSettings);

} // namespace NKqp
} // namespace NKikimr
