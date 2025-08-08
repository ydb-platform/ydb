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

struct TShardRangesWithShardId {
    TMaybe<ui64> ShardId;
    const TShardKeyRanges* Ranges;
};



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
        TPartitionPruner::TConfig partitionPrunerConfig,
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
        , PartitionPruner(HolderFactory(), TypeEnv(), std::move(partitionPrunerConfig))
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
        TasksGraph.GetMeta().Database = Database;
        TasksGraph.GetMeta().ChannelTransportVersion = executerConfig.TableServiceConfig.GetChannelTransportVersion();
        TasksGraph.GetMeta().UserRequestContext = userRequestContext;
        ResponseEv = std::make_unique<TEvKqpExecuter::TEvTxResponse>(Request.TxAlloc, ExecType);
        ResponseEv->Orbit = std::move(Request.Orbit);
        Stats = std::make_unique<TQueryExecutionStats>(Request.StatsMode, &TasksGraph,
            ResponseEv->Record.MutableResponse()->MutableResult()->MutableStats());

        CheckDuplicateRows = executerConfig.MutableConfig->EnableRowsDuplicationCheck.load();
        EnableParallelPointReadConsolidation = executerConfig.MutableConfig->EnableParallelPointReadConsolidation.load();

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
        ShardsResolved = true;

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
        for (auto& [shardId, nodeId] : ShardIdToNodeId) {
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
            batch.Payload = NYql::MakeChunkedBuffer(std::move(computeData.Payload));

            if (!trailingResults) {
                auto resultIndex = *txResult.QueryResultIndex + StatementResultIndex;
                auto streamEv = MakeHolder<TEvKqpExecuter::TEvStreamData>();
                streamEv->Record.SetSeqNo(computeData.Proto.GetSeqNo());
                streamEv->Record.SetQueryResultIndex(resultIndex);
                streamEv->Record.SetChannelId(channel.Id);
                streamEv->Record.SetFinished(channelData.GetFinished());
                const auto& snap = GetSnapshot();
                if (snap.IsValid()) {
                    auto vt = streamEv->Record.MutableVirtualTimestamp();
                    vt->SetStep(snap.Step);
                    vt->SetTxId(snap.TxId);
                }

                bool fillSchema = false;
                if (ResultSetFormatSettings.IsSchemaInclusionAlways()) {
                    fillSchema = true;
                } else if (ResultSetFormatSettings.IsSchemaInclusionFirstOnly()) {
                    fillSchema = (SentResultIndexes.find(resultIndex) == SentResultIndexes.end());
                } else {
                    YQL_ENSURE(false, "Unexpected schema inclusion mode");
                }

                TKqpProtoBuilder protoBuilder{*AppData()->FunctionRegistry};
                protoBuilder.BuildYdbResultSet(*streamEv->Record.MutableResultSet(), std::move(batches),
                    txResult.MkqlItemType, ResultSetFormatSettings, fillSchema, txResult.ColumnOrder, txResult.ColumnHints);

                // TODO: Calculate rows/bytes count for the arrow format of result set
                LOG_D("Send TEvStreamData to " << Target << ", seqNo: " << streamEv->Record.GetSeqNo()
                    << ", nRows: " << streamEv->Record.GetResultSet().rows().size());

                SentResultIndexes.insert(resultIndex);
                this->Send(Target, streamEv.Release());
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
        TxId = ev->Get()->Record.GetRequest().GetTxId();
        Target = ActorIdFromProto(ev->Get()->Record.GetTarget());

        const auto& databaseId = GetUserRequestContext()->DatabaseId;
        const auto& poolId = GetUserRequestContext()->PoolId.empty() ? NResourcePool::DEFAULT_POOL_ID : GetUserRequestContext()->PoolId;

        if (!databaseId.empty() && (poolId != NResourcePool::DEFAULT_POOL_ID || AccountDefaultPoolInScheduler)) {
            auto addQueryEvent = MakeHolder<NScheduler::TEvAddQuery>();
            addQueryEvent->DatabaseId = databaseId;
            addQueryEvent->PoolId = poolId;
            addQueryEvent->QueryId = TxId;
            this->Send(MakeKqpSchedulerServiceId(SelfId().NodeId()), addQueryEvent.Release(), 0, TxId);
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

        FillKqpTasksGraphStages(TasksGraph, Request.Transactions);
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

            auto readSettings = ExtractReadSettings(op, stageInfo, HolderFactory(), TypeEnv());
            task.Meta.Reads.ConstructInPlace();
            task.Meta.Reads->emplace_back(std::move(readInfo));
            task.Meta.ReadInfo.SetSorting(readSettings.GetSorting());
            task.Meta.Type = TTaskMeta::TTaskType::Compute;

            FillSecureParamsFromStage(task.Meta.SecureParams, stage);
            BuildSinks(stage, stageInfo, task);

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

    void BuildReadTasksFromSource(TStageInfo& stageInfo, const TVector<NKikimrKqp::TKqpNodeResources>& resourceSnapshot, ui32 scheduledTaskCount) {
        auto& intros = stageInfo.Introspections;
        const auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        YQL_ENSURE(stage.GetSources(0).HasExternalSource());
        YQL_ENSURE(stage.SourcesSize() == 1, "multiple sources in one task are not supported");

        const auto& stageSource = stage.GetSources(0);
        const auto& externalSource = stageSource.GetExternalSource();

        ui32 taskCount = externalSource.GetPartitionedTaskParams().size();
        intros.push_back("Using number of PartitionedTaskParams from external source - " + ToString(taskCount));

        auto taskCountHint = stage.GetTaskCount();
        TString introHint;
        if (taskCountHint == 0) {
            taskCountHint = scheduledTaskCount;
            introHint = "(Using scheduled task count as hint for override - " + ToString(taskCountHint) + ")";
        }

        if (taskCountHint) {
            if (taskCount > taskCountHint) {
                taskCount = taskCountHint;
                if (!introHint.empty()) {
                    intros.push_back(introHint);
                }
                intros.push_back("Manually overridden - " + ToString(taskCount));
            }
        } else if (!resourceSnapshot.empty()) {
            ui32 maxTaskcount = resourceSnapshot.size() * 2;
            if (taskCount > maxTaskcount) {
                taskCount = maxTaskcount;
                intros.push_back("Using less tasks because of resource snapshot size - " + ToString(taskCount));
            }
        }

        auto sourceName = externalSource.GetSourceName();
        TString structuredToken;
        if (sourceName) {
            structuredToken = NYql::CreateStructuredTokenParser(externalSource.GetAuthInfo()).ToBuilder().ReplaceReferences(SecureParams).ToJson();
        }

        ui64 nodeOffset = 0;
        for (size_t i = 0; i < resourceSnapshot.size(); ++i) {
            if (resourceSnapshot[i].GetNodeId() == SelfId().NodeId()) {
                nodeOffset = i;
                break;
            }
        }

        if (Request.QueryPhysicalGraph) {
            for (const auto taskId : stageInfo.Tasks) {
                auto& task = TasksGraph.GetTask(taskId);
                FillReadTaskFromSource(task, sourceName, structuredToken, resourceSnapshot, nodeOffset++);
                FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                BuildSinks(stage, stageInfo, task);
            }
            return;
        }

        TVector<ui64> tasksIds;
        tasksIds.reserve(taskCount);

        // generate all tasks
        for (ui32 i = 0; i < taskCount; i++) {
            auto& task = TasksGraph.AddTask(stageInfo);

            if (!externalSource.GetEmbedded()) {
                auto& input = task.Inputs[stageSource.GetInputIndex()];
                input.ConnectionInfo = NYql::NDq::TSourceInput{};
                input.SourceSettings = externalSource.GetSettings();
                input.SourceType = externalSource.GetType();
            }

            FillReadTaskFromSource(task, sourceName, structuredToken, resourceSnapshot, nodeOffset++);
            FillSecureParamsFromStage(task.Meta.SecureParams, stage);

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
            BuildSinks(stage, stageInfo, TasksGraph.GetTask(taskId));
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
                return CompareBorders<true, true>(
                    lhs.Ranges->GetRightBorder().first->GetCells(),
                    rhs.Ranges->GetRightBorder().first->GetCells(),
                    lhs.Ranges->GetRightBorder().second,
                    rhs.Ranges->GetRightBorder().second,
                    keyTypes) < 0;
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

    TMaybe<size_t> BuildScanTasksFromSource(TStageInfo& stageInfo, bool limitTasksPerNode) {
        auto& intros = stageInfo.Introspections;

        if (EnableReadsMerge) {
            limitTasksPerNode = true;
            intros.push_back("Using tasks count limit because of enabled reads merge");
        }

        THashMap<ui64, std::vector<ui64>> nodeTasks;
        THashMap<ui64, ui64> assignedShardsCount;

        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        bool singlePartitionedStage = stage.GetIsSinglePartition();

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
            }

            if (!nodeId || !ShardsResolved) {
                YQL_ENSURE(!ShardsResolved);
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

            settings->SetIsTableImmutable(source.GetIsTableImmutable());

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

            if (CheckDuplicateRows) {
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

            if (!BatchOperationSettings.Empty()) {
                settings->SetItemsLimit(BatchOperationSettings->MaxBatchSize);
                settings->SetIsBatch(true);
            } else {
                ui64 itemsLimit = ExtractItemsLimit(stageInfo, source.GetItemsLimit(), Request.TxAlloc->HolderFactory,
                Request.TxAlloc->TypeEnv);
                settings->SetItemsLimit(itemsLimit);
            }

            auto self = static_cast<TDerived*>(this)->SelfId();
            auto& lockTxId = TasksGraph.GetMeta().LockTxId;
            if (lockTxId) {
                settings->SetLockTxId(*lockTxId);
                settings->SetLockNodeId(self.NodeId());
            }

            if (TasksGraph.GetMeta().LockMode) {
                settings->SetLockMode(*TasksGraph.GetMeta().LockMode);
            }

            createdTasksIds.push_back(task.Id);
            return task;
        };

        THashMap<ui64, TVector<ui64>> nodeIdToTasks;
        THashMap<ui64, TVector<TShardRangesWithShardId>> nodeIdToShardKeyRanges;
        Y_DEFER {
            intros.push_back("Built scan tasks from source and shards to read for node");
            for (const auto& [nodeId, tasks] : nodeIdToTasks) {
                intros.push_back(ToString(nodeId) + " - " + ToString(tasks.size()) + ", " + ToString(nodeIdToShardKeyRanges.at(nodeId).size()));
            }
            intros.push_back("Total built scan tasks from source - " + ToString(createdTasksIds.size()));
        };

        auto addPartition = [&](
            ui64 taskLocation,
            TMaybe<ui64> nodeId,
            TMaybe<ui64> shardId,
            const TShardInfo& shardInfo,
            TMaybe<ui64> maxInFlightShards = Nothing())
        {
            YQL_ENSURE(!shardInfo.KeyWriteRanges);

            if (!nodeId) {
                const auto nodeIdPtr = ShardIdToNodeId.FindPtr(taskLocation);
                nodeId = nodeIdPtr ? TMaybe<ui64>{*nodeIdPtr} : Nothing();
            }

            YQL_ENSURE(!ShardsResolved || nodeId);
            YQL_ENSURE(Stats);

            if (shardId) {
                Stats->AffectedShards.insert(*shardId);
            }

            if (limitTasksPerNode && ShardsResolved) {
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

        auto buildSinks = [&]() {
            for (const ui64 taskId : createdTasksIds) {
                BuildSinks(stage, stageInfo, TasksGraph.GetTask(taskId));
            }
        };

        bool isFullScan = false;
        const THashMap<ui64, TShardInfo> partitions = SourceScanStageIdToParititions.empty()
            ? PartitionPruner.Prune(source, stageInfo, isFullScan)
            : SourceScanStageIdToParititions.at(stageInfo.Id);

        if (isFullScan && !source.HasItemsLimit()) {
            Counters->Counters->FullScansExecuted->Inc();
        }

        bool isSequentialInFlight = source.GetSequentialInFlightShards() > 0 && partitions.size() > source.GetSequentialInFlightShards();
        bool isParallelPointRead = EnableParallelPointReadConsolidation && !isSequentialInFlight && !source.GetSorted() && IsParallelPointReadPossible(partitions);

        if (partitions.size() > 0 && (isSequentialInFlight || isParallelPointRead || singlePartitionedStage)) {
            auto [startShard, shardInfo] = MakeVirtualTablePartition(source, stageInfo, HolderFactory(), TypeEnv());

            YQL_ENSURE(Stats);

            for (auto& [shardId, _] : partitions) {
                Stats->AffectedShards.insert(shardId);
            }

            TMaybe<ui64> inFlightShards = Nothing();
            if (isSequentialInFlight) {
                inFlightShards = source.GetSequentialInFlightShards();
            }

            if (shardInfo.KeyReadRanges) {
                const TMaybe<ui64> nodeId = (isParallelPointRead || singlePartitionedStage) ? TMaybe<ui64>{SelfId().NodeId()} : Nothing();
                addPartition(startShard, nodeId, {}, shardInfo, inFlightShards);
                fillRangesForTasks();
                buildSinks();
                return (isParallelPointRead || singlePartitionedStage) ? TMaybe<size_t>(partitions.size()) : Nothing();
            } else {
                return 0;
            }
        } else {
            for (auto& [shardId, shardInfo] : partitions) {
                addPartition(shardId, {}, shardId, shardInfo, {});
            }
            fillRangesForTasks();
            buildSinks();
            return partitions.size();
        }
    }

    ui32 GetMaxTasksAggregation(TStageInfo& stageInfo, const ui32 previousTasksCount, const ui32 nodesCount) const {
        auto& intros = stageInfo.Introspections;
        if (AggregationSettings.HasAggregationComputeThreads()) {
            intros.push_back("Considering AggregationComputeThreads value - " + ToString(AggregationSettings.GetAggregationComputeThreads()));
            return std::max<ui32>(1, AggregationSettings.GetAggregationComputeThreads());
        } else if (nodesCount) {
            const TStagePredictor& predictor = stageInfo.Meta.Tx.Body->GetCalculationPredictor(stageInfo.Id.StageId);
            auto result = predictor.CalcTasksOptimalCount(TStagePredictor::GetUsableThreads(), previousTasksCount / nodesCount, intros) * nodesCount;
            intros.push_back("Predicted value for aggregation - " + ToString(result));
            return result;
        } else {
            intros.push_back("Unknown nodes count for aggregation - using value 1");
            return 1;
        }
    }

    void BuildComputeTasks(TStageInfo& stageInfo, const ui32 nodesCount) {
        auto& intros = stageInfo.Introspections;
        auto& stage = stageInfo.Meta.GetStage(stageInfo.Id);

        if (Request.QueryPhysicalGraph) {
            for (const auto taskId : stageInfo.Tasks) {
                auto& task = TasksGraph.GetTask(taskId);
                task.Meta.Type = TTaskMeta::TTaskType::Compute;
                task.Meta.ExecuterId = SelfId();
                FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                BuildSinks(stage, stageInfo, task);
            }
            return;
        }

        ui32 partitionsCount = 1;
        ui32 inputTasks = 0;
        bool isShuffle = false;
        bool forceMapTasks = false;
        bool isParallelUnionAll = false;
        ui32 mapCnt = 0;


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
                case NKqpProto::TKqpPhyConnection::kStreamLookup: {
                    partitionsCount = originStageInfo.Tasks.size();
                    UnknownAffectedShardCount = true;
                    intros.push_back("Resetting compute tasks count because input " + ToString(inputIndex) + " is StreamLookup - " + ToString(partitionsCount));
                    break;
                }
                case NKqpProto::TKqpPhyConnection::kMap: {
                    partitionsCount = originStageInfo.Tasks.size();
                    forceMapTasks = true;
                    ++mapCnt;
                    intros.push_back("Resetting compute tasks count because input " + ToString(inputIndex) + " is Map - " + ToString(partitionsCount));
                    break;
                }
                case NKqpProto::TKqpPhyConnection::kParallelUnionAll: {
                    inputTasks += originStageInfo.Tasks.size();
                    isParallelUnionAll = true;
                    break;
                }
                default:
                    break;
            }

        }

        Y_ENSURE(mapCnt < 2, "There can be only < 2 'Map' connections");

        if ((isShuffle || isParallelUnionAll) && !forceMapTasks) {
            if (stage.GetTaskCount()) {
                partitionsCount = stage.GetTaskCount();
                intros.push_back("Manually overridden - " + ToString(partitionsCount));
            } else {
                partitionsCount = std::max(partitionsCount, GetMaxTasksAggregation(stageInfo, inputTasks, nodesCount));
            }
        }

        intros.push_back("Actual number of compute tasks - " + ToString(partitionsCount));

        for (ui32 i = 0; i < partitionsCount; ++i) {
            auto& task = TasksGraph.AddTask(stageInfo);
            task.Meta.Type = TTaskMeta::TTaskType::Compute;
            task.Meta.ExecuterId = SelfId();
            FillSecureParamsFromStage(task.Meta.SecureParams, stage);
            BuildSinks(stage, stageInfo, task);
            LOG_D("Stage " << stageInfo.Id << " create compute task: " << task.Id);
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
        });

        auto err = Planner->PlanExecution();
        if (err) {
            TlsActivationContext->Send(err.release());
            return false;
        }

        Planner->Submit();
        return true;
    }

    void BuildScanTasksFromShards(TStageInfo& stageInfo, bool enableShuffleElimination) {
        THashMap<ui64, std::vector<ui64>> nodeTasks;
        THashMap<ui64, std::vector<TShardInfoWithId>> nodeShards;
        THashMap<ui64, ui64> assignedShardsCount;
        auto& intros = stageInfo.Introspections;
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

        YQL_ENSURE(Stats);

        const auto& tableInfo = stageInfo.Meta.TableConstInfo;
        const auto& keyTypes = tableInfo->KeyColumnTypes;
        for (auto& op : stage.GetTableOps()) {
            Y_DEBUG_ABORT_UNLESS(stageInfo.Meta.TablePath == op.GetTable().GetPath());

            auto columns = BuildKqpColumns(op, tableInfo);
            bool isFullScan;
            auto partitions = PartitionPruner.Prune(op, stageInfo, isFullScan);
            const bool isOlapScan = (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadOlapRange);
            auto readSettings = ExtractReadSettings(op, stageInfo, HolderFactory(), TypeEnv());

            if (isFullScan && readSettings.ItemsLimit) {
                Counters->Counters->FullScansExecuted->Inc();
            }

            if (op.GetTypeCase() == NKqpProto::TKqpPhyTableOperation::kReadRange) {
                stageInfo.Meta.SkipNullKeys.assign(op.GetReadRange().GetSkipNullKeys().begin(),
                                                   op.GetReadRange().GetSkipNullKeys().end());
                // not supported for scan queries
                YQL_ENSURE(!readSettings.IsReverse());
            }

            for (auto&& i: partitions) {
                const ui64 nodeId = ShardIdToNodeId.at(i.first);
                nodeShards[nodeId].emplace_back(TShardInfoWithId(i.first, std::move(i.second)));
            }

            if (CollectProfileStats(Request.StatsMode)) {
                for (auto&& i : nodeShards) {
                    Stats->AddNodeShardsCount(stageInfo.Id.StageId, i.first, i.second.size());
                }
            }

            if (!AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead() && !shuffleEliminated || (!isOlapScan && readSettings.IsSorted())) {
                THashMap<ui64 /* nodeId */, ui64 /* tasks count */> olapAndSortedTasksCount;

                auto AssignScanTaskToShard = [&](const ui64 shardId, const bool sorted) -> TTask& {
                    ui64 nodeId = ShardIdToNodeId.at(shardId);
                    if (stageInfo.Meta.IsOlap() && sorted) {
                        auto& task = TasksGraph.AddTask(stageInfo);
                        task.Meta.ExecuterId = SelfId();
                        task.Meta.NodeId = nodeId;
                        task.Meta.ScanTask = true;
                        task.Meta.Type = TTaskMeta::TTaskType::Scan;
                        FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                        ++olapAndSortedTasksCount[nodeId];
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
                        intros.push_back("Scan task for node " + ToString(nodeId) + " not created");
                        return TasksGraph.GetTask(tasks[taskIdx]);
                    }
                };

                for (auto&& pair : nodeShards) {
                    auto& shardsInfo = pair.second;
                    for (auto&& shardInfo : shardsInfo) {
                        auto& task = AssignScanTaskToShard(shardInfo.ShardId, readSettings.IsSorted());
                        MergeReadInfoToTaskMeta(task.Meta, shardInfo.ShardId, shardInfo.KeyReadRanges, readSettings,
                            columns, op, /*isPersistentScan*/ true);
                    }
                }

                for (const auto& pair : nodeTasks) {
                    for (const auto& taskIdx : pair.second) {
                        auto& task = TasksGraph.GetTask(taskIdx);
                        task.Meta.SetEnableShardsSequentialScan(readSettings.IsSorted());
                        PrepareScanMetaForUsage(task.Meta, keyTypes);
                        BuildSinks(stage, stageInfo, task);
                    }

                    intros.push_back("Actual number of scan tasks for node " + ToString(pair.first) + " - " + ToString(pair.second.size()));
                }

                for (const auto& [nodeId, count] : olapAndSortedTasksCount) {
                    intros.push_back("Actual number of scan tasks (olap+sorted) for node " + ToString(nodeId) + " - " + ToString(count));
                }
            } else if (shuffleEliminated /* save partitioning for shuffle elimination */) {
                std::size_t stageInternalTaskId = 0;
                columnShardHashV1Params.TaskIndexByHash = std::make_shared<TVector<ui64>>();
                columnShardHashV1Params.TaskIndexByHash->resize(columnShardHashV1Params.SourceShardCount);

                for (auto&& pair : nodeShards) {
                    const auto nodeId = pair.first;
                    auto& shardsInfo = pair.second;
                    std::size_t maxTasksPerNode = std::min<std::size_t>(shardsInfo.size(), GetScanTasksPerNode(stageInfo, isOlapScan, nodeId, true));
                    std::vector<TTaskMeta> metas(maxTasksPerNode, TTaskMeta());
                    {
                        for (std::size_t i = 0; i < shardsInfo.size(); ++i) {
                            auto&& shardInfo = shardsInfo[i];
                            MergeReadInfoToTaskMeta(
                                metas[i % maxTasksPerNode],
                                shardInfo.ShardId,
                                shardInfo.KeyReadRanges,
                                readSettings,
                                columns, op,
                                /*isPersistentScan*/ true
                            );
                        }

                        for (auto& meta: metas) {
                            PrepareScanMetaForUsage(meta, keyTypes);
                            LOG_D("Stage " << stageInfo.Id << " create scan task meta for node: " << nodeId
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

                    intros.push_back("Actual number of scan tasks from shards with shuffle elimination for node " + ToString(nodeId) + " - " + ToString(maxTasksPerNode));

                    for (ui32 t = 0; t < maxTasksPerNode; ++t, ++stageInternalTaskId) {
                        auto& task = TasksGraph.AddTask(stageInfo);
                        task.Meta = metas[t];
                        task.Meta.SetEnableShardsSequentialScan(false);
                        task.Meta.ExecuterId = SelfId();
                        task.Meta.NodeId = nodeId;
                        task.Meta.ScanTask = true;
                        task.Meta.Type = TTaskMeta::TTaskType::Scan;
                        task.SetMetaId(t);
                        FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                        BuildSinks(stage, stageInfo, task);

                        for (const auto& readInfo: *task.Meta.Reads) {
                            Y_ENSURE(hashByShardId.contains(readInfo.ShardId));
                            (*columnShardHashV1Params.TaskIndexByHash)[hashByShardId[readInfo.ShardId]] = stageInternalTaskId;
                        }
                    }
                }

                LOG_DEBUG_S(
                    *TlsActivationContext,
                    NKikimrServices::KQP_EXECUTER,
                    "Stage with scan " << "[" << stageInfo.Id.TxId << ":" << stageInfo.Id.StageId << "]"
                    << " has keys: " << columnShardHashV1Params.KeyTypesToString() << " and task count: " << stageInternalTaskId;
                );
            } else {
                ui32 metaId = 0;
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

                    const auto maxTasksPerNode = GetScanTasksPerNode(stageInfo, isOlapScan, nodeId);
                    intros.push_back("Actual number of scan tasks from shards without shuffle elimination for node " + ToString(nodeId) + " - " + ToString(maxTasksPerNode));

                    for (ui32 t = 0; t < maxTasksPerNode; ++t) {
                        auto& task = TasksGraph.AddTask(stageInfo);
                        task.Meta = meta;
                        task.Meta.SetEnableShardsSequentialScan(false);
                        task.Meta.ExecuterId = SelfId();
                        task.Meta.NodeId = nodeId;
                        task.Meta.ScanTask = true;
                        task.Meta.Type = TTaskMeta::TTaskType::Scan;
                        task.SetMetaId(metaGlueingId);
                        FillSecureParamsFromStage(task.Meta.SecureParams, stage);
                        BuildSinks(stage, stageInfo, task);
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

protected:
    IKqpGateway::TExecPhysicalRequest Request;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    const TGUCSettings::TPtr GUCSettings;
    TPartitionPruner PartitionPruner;

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

    bool ShardsResolved = false;
    TKqpTasksGraph TasksGraph;

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
    bool HasOlapTable = false;
    bool StreamResult = false;
    bool HasDatashardSourceScan = false;
    bool UnknownAffectedShardCount = false;

    THashMap<ui64, TActorId> ResultChannelToComputeActor;
    THashMap<NYql::NDq::TStageId, THashMap<ui64, TShardInfo>> SourceScanStageIdToParititions;

    bool CheckDuplicateRows = false;

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

    bool EnableParallelPointReadConsolidation = false;

    bool AccountDefaultPoolInScheduler = false;
    
    THashSet<ui32> SentResultIndexes;
private:
    static constexpr TDuration ResourceUsageUpdateInterval = TDuration::MilliSeconds(100);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TResultSetFormatSettings resultSetFormatSettings,
    TKqpRequestCounters::TPtr counters, bool streamResult, const TExecuterConfig& executerConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const TActorId& creator,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    TPartitionPruner::TConfig partitionPrunerConfig, const TShardIdToTableInfoPtr& shardIdToTableInfo,
    const IKqpTransactionManagerPtr& txManager, const TActorId bufferActorId,
    TMaybe<NBatchOperations::TSettings> batchOperationSettings = Nothing());

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TResultSetFormatSettings resultSetFormatSettings,
    TKqpRequestCounters::TPtr counters, const TExecuterConfig& executerConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TPreparedQueryHolder::TConstPtr preparedQuery,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings);

} // namespace NKqp
} // namespace NKikimr
