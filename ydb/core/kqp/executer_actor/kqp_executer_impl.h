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
#include <ydb/library/plan2svg/plan2svg.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/library/yql/dq/runtime/dq_channel_service.h>
#include <ydb/core/kqp/executer_actor/kqp_tasks_graph.h>
#include <ydb/core/kqp/executer_actor/shards_resolver/kqp_shards_resolver.h>
#include <ydb/core/kqp/node_service/kqp_node_service.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
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
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/async/wait_for_event.h>

#include <util/generic/size_literals.h>


LWTRACE_USING(KQP_PROVIDER);

namespace NKikimr {
namespace NKqp {
#define KQP_STLOG_T(MARKER, MESSAGE, ...) STLOG(PRI_TRACE, NKikimrServices::KQP_EXECUTER, MARKER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << MESSAGE, __VA_ARGS__)
#define KQP_STLOG_D(MARKER, MESSAGE, ...) STLOG(PRI_DEBUG, NKikimrServices::KQP_EXECUTER, MARKER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << MESSAGE, __VA_ARGS__)
#define KQP_STLOG_I(MARKER, MESSAGE, ...) STLOG(PRI_INFO,  NKikimrServices::KQP_EXECUTER, MARKER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << MESSAGE, __VA_ARGS__)
#define KQP_STLOG_N(MARKER, MESSAGE, ...) STLOG(PRI_NOTICE, NKikimrServices::KQP_EXECUTER, MARKER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << MESSAGE, __VA_ARGS__)
#define KQP_STLOG_W(MARKER, MESSAGE, ...) STLOG(PRI_WARN,  NKikimrServices::KQP_EXECUTER, MARKER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << MESSAGE, __VA_ARGS__)
#define KQP_STLOG_E(MARKER, MESSAGE, ...) STLOG(PRI_ERROR, NKikimrServices::KQP_EXECUTER, MARKER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << MESSAGE, __VA_ARGS__)
#define KQP_STLOG_C(MARKER, MESSAGE, ...) STLOG(PRI_CRIT,  NKikimrServices::KQP_EXECUTER, MARKER, "ActorId: " << SelfId() << " TxId: " << TxId << ". " << "Ctx: " << *GetUserRequestContext() << ". " << MESSAGE, __VA_ARGS__)

using EExecType = TEvKqpExecuter::TEvTxResponse::EExecutionType;

const ui64 MaxTaskSize = 48_MB;
constexpr ui64 PotentialUnsigned64OverflowLimit = (std::numeric_limits<ui64>::max() >> 1);

std::pair<TString, TString> SerializeKqpTasksParametersForOlap(const TStageInfo& stageInfo, const TTask& task);

inline bool IsDebugLogEnabled() {
    return TlsActivationContext->LoggerSettings() &&
           TlsActivationContext->LoggerSettings()->Satisfies(NActors::NLog::PRI_DEBUG, NKikimrServices::KQP_EXECUTER);
}

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
        NFormats::TFormatsSettings formatsSettings,
        TKqpRequestCounters::TPtr counters,
        const TExecuterConfig& executerConfig,
        const TIntrusivePtr<TUserRequestContext>& userRequestContext,
        ui32 statementResultIndex,
        ui64 spanVerbosity = 0, TString spanName = "KqpExecuterBase",
        const TActorId bufferActorId = {}, const IKqpTransactionManagerPtr& txManager = nullptr,
        TMaybe<NBatchOperations::TSettings> batchOperationSettings = Nothing(),
        std::shared_ptr<NYql::NDq::IDqChannelService> channelService = nullptr)
        : NActors::TActor<TDerived>(&TDerived::ReadyState)
        , Request(std::move(request))
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FederatedQuerySetup(federatedQuerySetup)
        , GUCSettings(GUCSettings)
        , BufferActorId(bufferActorId)
        , TxManager(txManager)
        , Database(database)
        , UserToken(userToken)
        , FormatsSettings(std::move(formatsSettings))
        , Counters(counters)
        , ExecuterSpan(spanVerbosity, std::move(Request.TraceId), spanName)
        , Planner(nullptr)
        , ExecuterRetriesConfig(executerConfig.TableServiceConfig.GetExecuterRetriesConfig())
        , AggregationSettings(executerConfig.TableServiceConfig.GetAggregationConfig())
        , HasOlapTable(false)
        , StatementResultIndex(statementResultIndex)
        , BlockTrackingMode(executerConfig.TableServiceConfig.GetBlockTrackingMode())
        , VerboseMemoryLimitException(executerConfig.MutableConfig->VerboseMemoryLimitException.load())
        , BatchOperationSettings(std::move(batchOperationSettings))
        , AccountDefaultPoolInScheduler(executerConfig.TableServiceConfig.GetComputeSchedulerSettings().GetAccountDefaultPool())
        , TasksGraph(Database, Request.Transactions, Request.TxAlloc, partitionPrunerConfig, AggregationSettings, Counters, BufferActorId, UserToken)
        , ChannelService(channelService)
    {
        ArrayBufferMinFillPercentage = executerConfig.TableServiceConfig.GetArrayBufferMinFillPercentage();
        BufferPageAllocSize = executerConfig.TableServiceConfig.GetBufferPageAllocSize();

        TasksGraph.GetMeta().Snapshot = IKqpGateway::TKqpSnapshot(Request.Snapshot.Step, Request.Snapshot.TxId);
        TasksGraph.GetMeta().RequestIsolationLevel = Request.IsolationLevel;
        TasksGraph.GetMeta().ChannelTransportVersion = executerConfig.TableServiceConfig.GetChannelTransportVersion();
        TasksGraph.GetMeta().UserRequestContext = userRequestContext;
        TasksGraph.GetMeta().CheckDuplicateRows = executerConfig.MutableConfig->EnableRowsDuplicationCheck.load();
        TasksGraph.GetMeta().StatsMode = Request.StatsMode;
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

        KQP_STLOG_T(KQPEX, "Bootstrap done, become ReadyState",
            (trace_id, TraceId()));
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

        // TODO: count resolve time in CpuTime

        if (reply.Status != Ydb::StatusIds::SUCCESS) {
            if (ExecuterStateSpan) {
                ExecuterStateSpan.EndError(Ydb::StatusIds_StatusCode_Name(reply.Status));
            }

            KQP_STLOG_W(KQPEX, "Shards nodes resolve failed",
                (Status, Ydb::StatusIds_StatusCode_Name(reply.Status)),
                (Issues, reply.Issues.ToString()),
                (trace_id, TraceId()));
            ReplyErrorAndDie(reply.Status, reply.Issues);
            return false;
        }
        if (ExecuterStateSpan) {
            ExecuterStateSpan.EndOk();
        }

        KQP_STLOG_D(KQPEX, "Shards nodes resolved",
            (SuccessNodes, reply.ShardsToNodes.size()),
            (FailedNodes, reply.Unresolved),
            (trace_id, TraceId()));

        for (const auto& [_, nodeId] : reply.ShardsToNodes) {
            ParticipantNodes.emplace(nodeId);
            if (TxManager) {
                TxManager->AddParticipantNode(nodeId);
            }
        }

        TasksGraph.ResolveShards(std::move(reply.ShardsToNodes));

        if (IsDebugLogEnabled()) {
            TStringBuilder sb;
            sb << "Shards on nodes: " << Endl;
            for (auto& pair : TasksGraph.GetMeta().ShardsOnNode) {
                sb << "  node " << pair.first << ": [";
                if (pair.second.size() <= 20) {
                    sb << JoinSeq(", ", pair.second) << "]" << Endl;
                } else {
                    sb << JoinRange(", ", pair.second.begin(), std::next(pair.second.begin(), 20)) << ", ...] "
                       << "(total " << pair.second.size() << ") " << Endl;
                }
            }
            KQP_STLOG_D(KQPEX, sb,
                (trace_id, TraceId()));
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
        if (FormatsSettings.IsSchemaInclusionAlways()) {
            fillSchema = true;
        } else if (FormatsSettings.IsSchemaInclusionFirstOnly()) {
            fillSchema =
                (SentResultIndexes.find(resultIndex) == SentResultIndexes.end());
        } else {
            YQL_ENSURE(false, "Unexpected schema inclusion mode");
        }

        TKqpProtoBuilder protoBuilder{*AppData()->FunctionRegistry};
        protoBuilder.BuildYdbResultSet(
            *streamEv->Record.MutableResultSet(), std::move(batches),
            txResult.MkqlItemType, FormatsSettings, fillSchema,
            txResult.ColumnOrder, txResult.ColumnHints);

        // TODO: Calculate rows/bytes count for the arrow format of result set
        KQP_STLOG_D(KQPEX, "Send TEvStreamData",
            (Recipient, Target),
            (SeqNo, streamEv->Record.GetSeqNo()),
            (Rows, streamEv->Record.GetResultSet().rows().size()),
            (trace_id, TraceId()));

        SentResultIndexes.insert(resultIndex);
        this->Send(Target, streamEv.Release());
    }

    void OnEmptyResult() {
        for (ui32 txIdx = 0; txIdx < Request.Transactions.size(); ++txIdx) {
            auto& tx = Request.Transactions[txIdx].Body;
            ResponseEv->InitTxResult(tx);
            for (ui32 i = 0; i < tx->ResultsSize(); ++i) {
                const auto& result = tx->GetResults(i);
                const auto& connection = result.GetConnection();
                const auto& inputStageInfo = TasksGraph.GetStageInfo(NYql::NDq::TStageId(txIdx, connection.GetStageIndex()));
                if (inputStageInfo.Tasks.size() >= 1) {
                    continue;
                }

                auto& txResult = ResponseEv->TxResults.at(i);
                TVector<NYql::NDq::TDqSerializedBatch> batches(1);
                if (TasksGraph.GetMeta().StreamResult && txResult.IsStream && txResult.QueryResultIndex.Defined()) {
                    SendStreamData(txResult, std::move(batches), std::numeric_limits<ui32>::max(), 1, true);
                }
            }
        }
    }

    void HandleResultData(NYql::NDq::TEvDqCompute::TEvResumeExecution::TPtr&) {
        for (auto& [channelId, inputBuffer] : ResultInputBuffers) {
            ReadResultFromInputBuffer(channelId, inputBuffer);
        }
    }

    void ReadResultFromTaskOutputs(const TTask& task)
    {
        if (TasksGraph.GetMeta().DqChannelVersion >= 2u) {
            for (auto& output : task.Outputs) {
                for (auto channelId : output.Channels) {
                    auto& channel = TasksGraph.GetChannel(channelId);
                    if (!channel.DstTask) {
                        Y_ENSURE(ChannelService && ResultInputBuffers.find(channelId) == ResultInputBuffers.end());
                        auto inputBuffer = ChannelService->GetInputBuffer(NYql::NDq::TChannelFullInfo(channelId, task.ComputeActorId, SelfId(), task.StageId.StageId, 0,
                            NYql::NDq::StatsModeToCollectStatsLevel(GetDqStatsMode(Request.StatsMode))));
                        ReadResultFromInputBuffer(channelId, inputBuffer);
                        ResultInputBuffers.emplace(channelId, inputBuffer);
                    }
                }
            }
        }
    }

    void ReadResultFromInputBuffer(ui32 channelId, const std::shared_ptr<NYql::NDq::IChannelBuffer>& buffer) {
        auto& channel = TasksGraph.GetChannel(channelId);
        YQL_ENSURE(channel.DstTask == 0);
        auto& txResult = ResponseEv->TxResults[channel.DstInputIndex];
        bool streamingAllowed = TasksGraph.GetMeta().StreamResult && txResult.IsStream && txResult.QueryResultIndex.Defined();

        NYql::NDq::TDataChunk data;
        while (buffer->Pop(data)) {
            YQL_ENSURE(Stats);
            Stats->ResultBytes += data.Bytes;
            Stats->ResultRows += data.Rows;

            const bool trailingResults = data.Finished && Request.IsTrailingResultsAllowed();

            if (!data.Buffer.Empty()) {
                TVector<NYql::NDq::TDqSerializedBatch> batches(1);
                auto& batch = batches.front();

                batch.Payload = std::move(data.Buffer);
                batch.Proto.SetTransportVersion(data.TransportVersion);
                batch.Proto.SetChunks(1);
                batch.Proto.SetRows(data.Rows);
                batch.Proto.SetValuePackerVersion(NYql::NDq::ToProto(data.PackerVersion));

                if (streamingAllowed && !trailingResults) {
                    ui32 seqNo = 1;
                    SendStreamData(txResult, std::move(batches), channel.Id, seqNo, data.Finished);
                } else {
                    ResponseEv->TakeResult(channel.DstInputIndex, std::move(batch));
                    if (streamingAllowed) {
                        txResult.HasTrailingResult = true;
                    }
                }
            } else if (data.Finished) {
                if (streamingAllowed && !trailingResults) {
                    ui32 seqNo = 1;
                    TVector<NYql::NDq::TDqSerializedBatch> batches(1);
                    auto& batch = batches.front();
                    batch.Proto.SetTransportVersion(data.TransportVersion);
                    batch.Proto.SetChunks(0);
                    batch.Proto.SetRows(0);
                    batch.Proto.SetValuePackerVersion(NYql::NDq::ToProto(data.PackerVersion));
                    SendStreamData(txResult, std::move(batches), channel.Id, seqNo, true);
                } else {
                    txResult.HasTrailingResult = true;
                }
            }

            if (data.Finished) {
                break;
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

        if (TasksGraph.GetMeta().StreamResult && txResult.IsStream && txResult.QueryResultIndex.Defined()) {

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
                KQP_STLOG_D(KQPEX, "Staging TEvStreamData",
                    (Recipient, Target),
                    (SeqNo, computeData.Proto.GetSeqNo()),
                    (Rows, rowCount),
                    (trace_id, TraceId()));
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

        KQP_STLOG_T(KQPEX, "Got result",
            (ChannelId, channel.Id),
            (InputIndex, channel.DstInputIndex),
            (Sender, ev->Sender),
            (Finished, channelData.GetFinished()),
            (trace_id, TraceId()));

        ResponseEv->TakeResult(channel.DstInputIndex, std::move(batch));
        KQP_STLOG_T(KQPEX, "Send ack",
            (ChannelId, channel.Id),
            (SeqNo, record.GetSeqNo()),
            (Recipient, ev->Sender),
            (trace_id, TraceId()));

        auto ackEv = MakeHolder<NYql::NDq::TEvDqCompute::TEvChannelDataAck>();
        ackEv->Record.SetSeqNo(record.GetSeqNo());
        ackEv->Record.SetChannelId(channel.Id);
        ackEv->Record.SetFreeSpace(50_MB);
        this->Send(channelComputeActorId, ackEv.Release(), /* TODO: undelivery */ 0, /* cookie */ channel.Id);
    }

    void HandleStreamAck(TEvKqpExecuter::TEvStreamDataAck::TPtr& ev) {
        if (ev->Get()->Record.GetChannelId() == std::numeric_limits<ui32>::max())
            return;

        if (TasksGraph.GetMeta().DqChannelVersion >= 2u) {
            if (ev->Get()->Record.GetEnough()) {
                for (auto& [channelId, inputBuffer] : ResultInputBuffers) {
                    inputBuffer->EarlyFinish();
                }
            }
            return;
        }

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

        KQP_STLOG_D(KQPEX, "Send ack",
            (ChannelId, channelId),
            (SeqNo, seqNo),
            (Enough, ev->Get()->Record.GetEnough()),
            (FreeSpace, freeSpace),
            (Recipient, channelComputeActorId),
            (trace_id, TraceId()));

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

        KQP_STLOG_D(KQPEX, "Got execution state from compute actor",
            (ActorState, CurrentStateFuncName()),
            (ComputeActor, computeActor),
            (TaskId, taskId),
            (State, NYql::NDqProto::EComputeState_Name((NYql::NDqProto::EComputeState) state.GetState())),
            (Stats, state.GetStats()),
            (trace_id, TraceId()));

        YQL_ENSURE(Stats);

        if (state.HasStats()) {
            Stats->UpdateTaskStats(taskId, state.GetStats(), nullptr, (NYql::NDqProto::EComputeState) state.GetState(),
                TDuration::MilliSeconds(AggregationSettings.GetCollectLongTasksStatsTimeoutMs()));

            if (CollectBasicStats(Request.StatsMode)) {
                ui64 cycleCount = GetCycleCountFast();

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
                    ReadResultFromTaskOutputs(task);
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

    void HandleHttpInfo(NMon::TEvHttpInfo::TPtr& ev) {

        TStringStream str;

        const TCgiParameters &cgi = ev->Get()->Request.GetParams();
        auto view = cgi.Get("view");
        if (view == "plan") {
            NYql::NDqProto::TDqExecutionStats execStats;
            Stats->ExportExecStats(execStats);

            for (ui32 txId = 0; txId < Request.Transactions.size(); ++txId) {
                const auto& tx = Request.Transactions[txId].Body;
                auto plans = AddExecStatsToTxPlan(tx->GetPlan(), execStats);
                TPlanVisualizer viz;

                NJson::TJsonReaderConfig jsonConfig;
                NJson::TJsonValue jsonNode;
                if (NJson::ReadJsonTree(plans, &jsonConfig, &jsonNode)) {
                    viz.LoadPlans(jsonNode);
                }

                auto svg = viz.PrintSvgSafe();
                str << svg << Endl;
            }

            this->Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
            return;
        }

        HTML(str) {
            PRE() {
                str << "KQP Executer, SelfId=" << SelfId() << ' ';
                HREF(TStringBuilder() << "/node/" << SelfId().NodeId() << "/actors/kqp_node?ex=" << SelfId() << "&view=plan")  {
                    str << "Plan";
                }
                str << Endl;

                TABLE_SORTABLE_CLASS("table table-condensed") {
                    TABLEHEAD() {
                        TABLER() {
                            TABLEH() {str << "TxId";}
                            TABLEH() {str << "StageId";}
                            TABLEH() {str << "TaskId";}
                            TABLEH() {str << "NodeId";}
                            TABLEH() {str << "ActorId";}
                            TABLEH() {str << "Completed";}
                        }
                    }
                    TABLEBODY() {
                        for (const auto& task : TasksGraph.GetTasks()) {
                            TABLER() {
                                TABLED() {str << task.StageId.TxId;}
                                TABLED() {str << task.StageId.StageId;}
                                TABLED() {str << task.Id;}
                                TABLED() {str << task.Meta.NodeId;}
                                TABLED() {
                                    if (task.ComputeActorId) {
                                        HREF(TStringBuilder() << "/node/" << task.ComputeActorId.NodeId() << "/actors/kqp_node?ca=" << task.ComputeActorId)  {
                                            str << task.ComputeActorId;
                                        }
                                    } else {
                                        str << "N/A";
                                    }
                                    str << Endl;
                                }
                                TABLED() {str << task.Meta.Completed;}
                            }
                        }
                    }
                }
            }
        }

        this->Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
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
        TasksGraph.GetMeta().TxId = TxId = ev->Get()->Record.GetRequest().GetTxId();
        Target = ActorIdFromProto(ev->Get()->Record.GetTarget());

        const auto& databaseId = GetUserRequestContext()->DatabaseId;
        const auto& poolId = GetUserRequestContext()->PoolId.empty() ? NResourcePool::DEFAULT_POOL_ID : GetUserRequestContext()->PoolId;

        LWTRACK(KqpBaseExecuterHandleReady, ResponseEv->Orbit, TxId);

        if (IsSchedulable()) {
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

        if (!ResponseEv) {
            co_return;
        }

        auto lockTxId = Request.AcquireLocksTxId;
        if (lockTxId.Defined() && *lockTxId == 0) {
            lockTxId = TxId;
        }

        TasksGraph.GetMeta().SetLockTxId(lockTxId);
        TasksGraph.GetMeta().SetLockNodeId(SelfId().NodeId());

        switch (Request.IsolationLevel) {
            case NKqpProto::ISOLATION_LEVEL_SNAPSHOT_RW:
                TasksGraph.GetMeta().SetLockMode(NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION);
                break;
            default:
                TasksGraph.GetMeta().SetLockMode(NKikimrDataEvents::OPTIMISTIC);
                break;
        }

        if (IsDebugLogEnabled()) {
            for (auto& tx : Request.Transactions) {
                KQP_STLOG_D(KQPEX, "Executing physical tx",
                    (TxType, (ui32)tx.Body->GetType()),
                    (Stages, tx.Body->StagesSize()),
                    (trace_id, TraceId()));
            }
        }

        if (BufferActorId && Request.LocksOp == ELocksOp::Rollback) {
            static_cast<TDerived*>(this)->Finalize();
            co_return;
        }

        ExecuterStateSpan = NWilson::TSpan(TWilsonKqp::ExecuterTableResolve, ExecuterSpan.GetTraceId(), "WaitForTableResolve", NWilson::EFlags::AUTO_END);

        auto kqpTableResolver = CreateKqpTableResolver(this->SelfId(), TxId, UserToken, TasksGraph, false);
        KqpTableResolverId = this->RegisterWithSameMailbox(kqpTableResolver);

        KQP_STLOG_T(KQPEX, "Got request, become WaitResolveState",
            (trace_id, TraceId()));
        this->Become(&TDerived::WaitResolveState);

        auto now = TAppData::TimeProvider->Now();
        StartResolveTime = now;

        YQL_ENSURE(Stats);

        Stats->StartTs = now;
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
            KQP_STLOG_D(KQPEX, sb,
                (trace_id, TraceId()));
        }

        return false;
    }

    void InvalidateNode(ui64 node) {
        for (auto tablet : TasksGraph.GetMeta().ShardsOnNode[node]) {
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
                        KQP_STLOG_D(KQPEX, "Schedule a retry by ActorUnknown reason",
                            (NodeId, ev->Sender.NodeId()),
                            (RequestId, ev->Cookie),
                            (trace_id, TraceId()));
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
                KQP_STLOG_E(KQPEX, "Event lost",
                    (EventType, eventType),
                    (Reason, reason),
                    (trace_id, TraceId()));
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
        KQP_STLOG_N(KQPEX, "Disconnected node", (node_id, nodeId),
            (trace_id, TraceId()));

        if (Planner) {
            for (auto computeActor : Planner->GetPendingComputeActors()) {
                if (computeActor.first.NodeId() == nodeId) {
                    return ReplyUnavailable(TStringBuilder() << "Connection with node " << nodeId << " lost.");
                }
            }

            for (auto& task : TasksGraph.GetTasks()) {
                if (Planner->GetPendingComputeTasks().contains(task.Id)) {
                    auto actualNodeId = Planner->GetActualNodeIdForTask(task.Id);
                    if (actualNodeId && *actualNodeId == nodeId) {
                        return ReplyUnavailable(TStringBuilder() << "Connection with node " << nodeId << " lost.");
                    }
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

            KQP_STLOG_E(KQPEX, "Stop executing",
                (Reason, NKikimrKqp::TEvStartKqpTasksResponse_ENotStartedTaskReason_Name(reason)),
                (Message, message),
                (trace_id, TraceId()));

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
                    if (!AppData()->FeatureFlags.GetEnableShuttingDownNodeState()) {
                        KQP_STLOG_D(KQPEX, "Received NODE_SHUTTING_DOWN but feature flag EnableShuttingDownNodeState is disabled",
                            (trace_id, TraceId()));
                        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
                            YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE,
                                "Compute node is unavailable"));
                        break;
                    }

                    KQP_STLOG_D(KQPEX, "Received NODE_SHUTTING_DOWN, attempting run tasks locally",
                        (trace_id, TraceId()));

                    ui32 requestId = record.GetNotStartedTasks(0).GetRequestId();
                    auto localNode = MakeKqpNodeServiceID(SelfId().NodeId());

                    // changes requests nodeId when redirect tasks on local node: used to check on disconnected
                    if (!Planner->SendStartKqpTasksRequest(requestId, localNode, true)) {
                        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE,
                            MakeIssue(NKikimrIssues::TIssuesIds::SHARD_NOT_AVAILABLE,
                                "Compute node is unavailable"));
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
            KQP_STLOG_D(KQPEX, "Executing task",
                (TaskId, taskId),
                (ComputeActor, computeActorId),
                (trace_id, TraceId()));
            YQL_ENSURE(Planner);
            bool ack = Planner->AcknowledgeCA(taskId, computeActorId, nullptr);
            if (ack) {
                Planner->CollectTaskChannelsUpdates(task, channelsUpdates);
                ReadResultFromTaskOutputs(task);
            }

        }

        Planner->PropagateChannelsUpdates(channelsUpdates);
    }

    void HandleAbortExecution(TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& msg = ev->Get()->Record;
        NYql::TIssues issues = ev->Get()->GetIssues();

        // If Target is not yet initialized (TEvTxRequest from TxProxy hasn't
        // arrived yet), use the abort sender as the target. Otherwise
        // TEvTxResponse from PassAway() would be sent to a null TActorId and
        // the session actor would stay stuck in ExecuteState forever.
        if (!Target) {
            Target = ev->Sender;
        }

        HandleAbortExecution(msg.GetStatusCode(), ev->Get()->GetIssues(), ev->Sender == Target);
    }

    void HandleAbortExecution(
            NYql::NDqProto::StatusIds::StatusCode statusCode,
            const NYql::TIssues& issues,
            const bool isTargetSender) {
        KQP_STLOG_D(KQPEX, "Got EvAbortExecution",
            (Status, NYql::NDqProto::StatusIds_StatusCode_Name(statusCode)),
            (Issues, issues.ToOneLineString()),
            (trace_id, TraceId()));
        auto ydbStatusCode = NYql::NDq::DqStatusToYdbStatus(statusCode);
        if (ydbStatusCode == Ydb::StatusIds::INTERNAL_ERROR) {
            InternalError(issues);
        } else if (ydbStatusCode == Ydb::StatusIds::TIMEOUT) {
            TimeoutError(isTargetSender, issues);
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

            KQP_STLOG_D(KQPEX, "Resource usage for last stat interval",
                (Consumption, consumption),
                (RequestUnits, ru),
                (RlPath, Request.RlPath.GetRef()),
                (RlActor, actorId),
                (ForceFlag, force),
                (trace_id, TraceId()));
        } else {
            KQP_STLOG_D(KQPEX, "Resource usage for last stat interval, rate limiter was not found",
                (Consumption, consumption),
                (RequestUnits, ru),
                (ForceFlag, force),
                (trace_id, TraceId()));
        }
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
            .CheckpointCoordinator = CheckpointCoordinatorId,
            .EnableWatermarks = Request.QueryPhysicalGraph && Request.QueryPhysicalGraph->GetPreparedQuery().GetPhysicalQuery().GetEnableWatermarks(),
        });

        auto err = Planner->PlanExecution();
        if (err) {
            TlsActivationContext->Send(err.release());
            return false;
        }

        if (TasksGraph.GetMeta().DqChannelVersion >= 2u) {
            Y_ENSURE(ChannelService);
            for (auto& [channelId, outputActorId] : Planner->ResultChannels) {
                auto inputBuffer = ChannelService->GetInputBuffer(NYql::NDq::TChannelFullInfo(channelId, outputActorId, SelfId(), 0, 0,
                    NYql::NDq::StatsModeToCollectStatsLevel(GetDqStatsMode(Request.StatsMode))));
                ReadResultFromInputBuffer(channelId, inputBuffer);
                ResultInputBuffers.emplace(channelId, inputBuffer);
            }
        }

        Planner->Submit();
        return true;
    }

    void GetSecretsSnapshot() {
        RegisterDescribeSecretsActor(this->SelfId(), UserToken, Database, SecretNames, this->ActorContext().ActorSystem());
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
                KQP_STLOG_I(KQPEX, "Aborting compute actor execution",
                    (Issues, issues.ToOneLineString()),
                    (ComputeActor, task.ComputeActorId),
                    (TaskId, task.Id),
                    (trace_id, TraceId()));

                auto ev = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDq::YdbStatusToDqStatus(code), issues);
                this->Send(task.ComputeActorId, ev.Release());
            } else {
                KQP_STLOG_I(KQPEX, "Task does not have the CA id yet or is already complete",
                    (TaskId, task.Id),
                    (trace_id, TraceId()));
            }
        }
    }

    void TerminateComputeActors(Ydb::StatusIds::StatusCode code, const TString& message) {
        TerminateComputeActors(code, NYql::TIssues({NYql::TIssue(message)}));
    }

protected:
    TString TraceId() const {
        if (ExecuterSpan) {
            return ExecuterSpan.GetTraceId().GetHexTraceId();
        }
        return TString();
    }
    void UnexpectedEvent(const TString& state, ui32 eventType) {
        if (eventType == TEvents::TEvPoison::EventType) {
            KQP_STLOG_D(KQPEX, "TKqpExecuter, TEvPoison event",
                (State, state),
                (SelfId, this->SelfId()),
                (trace_id, TraceId()));
            InternalError(TStringBuilder() << "TKqpExecuter got poisoned, state: " << state);
        } else {
            KQP_STLOG_E(KQPEX, "TKqpExecuter, unexpected event",
                (EventType, eventType),
                (State, state),
                (SelfId, this->SelfId()),
                (trace_id, TraceId()));
            InternalError(TStringBuilder() << "Unexpected event at TKqpExecuter, state: " << state << ", event: " << eventType);
        }
    }

    void InternalError(const NYql::TIssues& issues) {
        KQP_STLOG_E(KQPEX, issues.ToOneLineString(),
            (trace_id, TraceId()));
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
        KQP_STLOG_E(KQPEX, "UNAVAILABLE: " << message,
            (trace_id, TraceId()));
        auto issue = NYql::YqlIssue({}, NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE);
        issue.AddSubIssue(new NYql::TIssue(message));
        ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, issue);
    }

    void RuntimeError(Ydb::StatusIds::StatusCode code, const NYql::TIssues& issues) {
        KQP_STLOG_E(KQPEX, "Runtime error",
            (Status, Ydb::StatusIds_StatusCode_Name(code)),
            (Issues, issues.ToOneLineString()),
            (trace_id, TraceId()));
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

    void TimeoutError(bool isTargetSender, NYql::TIssues issues) {
        if (AlreadyReplied) {
            KQP_STLOG_E(KQPEX, "Timeout when we already replied - not good",
                (Backtrace, TBackTrace().PrintToString()),
                (trace_id, TraceId()));
            return;
        }

        const auto status = NYql::NDqProto::StatusIds::TIMEOUT;
        if (issues.Empty()) {
            issues.AddIssue("Request timeout exceeded");
        }

        TerminateComputeActors(Ydb::StatusIds::TIMEOUT, issues);

        AlreadyReplied = true;

        KQP_STLOG_E(KQPEX, "Abort execution",
            (Status, NYql::NDqProto::StatusIds_StatusCode_Name(status)),
            (Issues, issues.ToOneLineString()),
            (trace_id, TraceId()));
        if (ExecuterSpan) {
            ExecuterSpan.EndError(TStringBuilder() << NYql::NDqProto::StatusIds_StatusCode_Name(status));
        }

        ResponseEv->Record.MutableResponse()->SetStatus(Ydb::StatusIds::TIMEOUT);
        NYql::IssuesToMessage(issues, ResponseEv->Record.MutableResponse()->MutableIssues());

        // TEvAbortExecution can come from ComputeActor or SessionActor/PartitionedExecuterActor (== Target).
        if (!isTargetSender) {
            auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(status, issues);
            this->Send(Target, abortEv.Release());
        }

        KQP_STLOG_E(KQPEX, "Sending timeout response", (Recipient, Target),
            (trace_id, TraceId()));

        // Pass away immediately, since we already sent response - don't wait for stats.
        this->PassAway();
    }

    virtual void ReplyErrorAndDie(Ydb::StatusIds::StatusCode status,
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues)
    {
        if (AlreadyReplied) {
            KQP_STLOG_E(KQPEX, "Error when we already replied - not good",
                (Backtrace, TBackTrace().PrintToString()),
                (trace_id, TraceId()));
            return;
        }

        TerminateComputeActors(status, "Terminate execution");

        AlreadyReplied = true;
        auto& response = *ResponseEv->Record.MutableResponse();

        response.SetStatus(status);
        if (issues) {
            response.MutableIssues()->Swap(issues);
        }

        KQP_STLOG_T(KQPEX, "ReplyErrorAndDie",
            (Response, response.DebugString()),
            (TargetActor, Target),
            (trace_id, TraceId()));

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
                KQP_STLOG_E(KQPEX, "Abort execution. Task size is too big",
                    (TaskId, task->GetId()),
                    (Size, size),
                    (MaxSize, MaxTaskSize),
                    (trace_id, TraceId()));
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

            {
                ui64 cycleCount = GetCycleCountFast();
                Stats->ExportExecStats(*response.MutableResult()->MutableStats());

                if (CollectFullStats(Request.StatsMode)) {
                    ui64 jsonSize = 0;

                    response.MutableResult()->MutableStats()->ClearTxPlansWithStats();
                    for (ui32 txId = 0; txId < Request.Transactions.size(); ++txId) {
                        const auto& tx = Request.Transactions[txId].Body;
                        auto planWithStats = AddExecStatsToTxPlan(tx->GetPlan(), response.GetResult().GetStats());
                        jsonSize += planWithStats.size();
                        response.MutableResult()->MutableStats()->AddTxPlansWithStats(planWithStats);
                    }

                    Counters->Counters->QueryStatMemConvertBytes->Add(jsonSize);
                    response.MutableResult()->MutableStats()->SetStatConvertBytes(jsonSize);

                    if (Stats->CollectStatsByLongTasks) {
                        const auto& txPlansWithStats = response.GetResult().GetStats().GetTxPlansWithStats();
                        if (!txPlansWithStats.empty()) {
                            KQP_STLOG_I(KQPEX, "Full stats: " << response.GetResult().GetStats(),
                                (trace_id, TraceId()));
                        }
                    }
                }

                auto deltaCpuTime = NHPTimer::GetSeconds(GetCycleCountFast() - cycleCount);
                Counters->Counters->QueryStatCpuConvertUs->Add(deltaCpuTime * 1'000'000);
            }

            if (!BatchOperationSettings.Empty() && !Stats->TableStats.empty()) {
                auto [_, tableStats] = *Stats->TableStats.begin();
                Counters->Counters->BatchOperationUpdateRows->Add(tableStats->GetWriteRows());
                Counters->Counters->BatchOperationDeleteRows->Add(tableStats->GetEraseRows());
            }

            auto finishSize = Stats->EstimateFinishMem();
            Counters->Counters->QueryStatMemFinishBytes->Add(finishSize);
            response.MutableResult()->MutableStats()->SetStatFinishBytes(finishSize);
        }

        Counters->Counters->QueryStatMemCollectInflightBytes->Sub(StatCollectInflightBytes);
        StatCollectInflightBytes = 0;
        Counters->Counters->QueryStatMemFinishInflightBytes->Sub(StatFinishInflightBytes);
        StatFinishInflightBytes = 0;

        ResponseEv->LocksBrokenAsBreaker = Stats->LocksBrokenAsBreaker;
        ResponseEv->LocksBrokenAsVictim = Stats->LocksBrokenAsVictim;

        Request.Transactions.crop(0);
        this->Send(Target, ResponseEv.release());

        if (IsSchedulable()) {
            auto removeQueryEvent = MakeHolder<NScheduler::TEvRemoveQuery>();
            removeQueryEvent->QueryId = TxId;
            this->Send(MakeKqpSchedulerServiceId(SelfId().NodeId()), removeQueryEvent.Release());
        }

        for (auto channelPair: ResultChannelProxies) {
            KQP_STLOG_D(KQPEX, "Terminate result channel",
                (ChannelId, channelPair.first),
                (ProxyActor, channelPair.second->SelfId()),
                (trace_id, TraceId()));

            TAutoPtr<IEventHandle> ev = new IEventHandle(
                channelPair.second->SelfId(), SelfId(), new TEvents::TEvPoison
            );
            channelPair.second->Receive(ev);
        }

        KQP_STLOG_D(KQPEX, "Terminate execution",
            (trace_id, TraceId()));
        if (KqpShardsResolverId) {
            this->Send(KqpShardsResolverId, new TEvents::TEvPoison);
        }

        if (Planner) {
            Planner->Unsubscribe();
        }

        if (KqpTableResolverId) {
            this->Send(KqpTableResolverId, new TEvents::TEvPoison);
        }

        if (const auto& infoAggregator = TasksGraph.GetMeta().DqInfoAggregator) {
            this->Send(infoAggregator, new TEvents::TEvPoison());
        }

        this->Send(this->SelfId(), new TEvents::TEvPoison);
        KQP_STLOG_T(KQPEX, "Terminate, become ZombieState",
            (trace_id, TraceId()));
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
            TasksGraph.RestoreTasksGraphInfo(ResourcesSnapshot, *Request.QueryPhysicalGraph);
        }

        return TasksGraph.GetMeta().IsRestored;
    }

    NYql::NDqProto::TDqTask* SerializeTaskToProto(const TTask& task, bool serializeAsyncIoSettings) {
        return TasksGraph.ArenaSerializeTaskToProto(task, serializeAsyncIoSettings);
    }

    inline bool IsSchedulable() const {
        const auto& databaseId = GetUserRequestContext()->DatabaseId;
        const auto& poolId = GetUserRequestContext()->PoolId.empty() ? NResourcePool::DEFAULT_POOL_ID : GetUserRequestContext()->PoolId;
        return !databaseId.empty() && (poolId != NResourcePool::DEFAULT_POOL_ID || AccountDefaultPoolInScheduler);
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
    NFormats::TFormatsSettings FormatsSettings;
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
    THashMap<ui32, std::shared_ptr<NYql::NDq::IChannelBuffer>> ResultInputBuffers;

    ui64 LastTaskId = 0;
    TString LastComputeActorId = "";

    std::unique_ptr<TKqpPlanner> Planner;
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig ExecuterRetriesConfig;

    std::vector<TString> SecretNames;

    const NKikimrConfig::TTableServiceConfig::TAggregationConfig AggregationSettings;
    TVector<NKikimrKqp::TKqpNodeResources> ResourcesSnapshot;
    bool HasOlapTable = false;
    bool HasDatashardSourceScan = false;
    bool UnknownAffectedShardCount = false; // used by Data executer

    THashMap<ui64, TActorId> ResultChannelToComputeActor;

    ui32 StatementResultIndex;

    // Track which nodes (by shards) have been involved during execution
    THashSet<ui32> ParticipantNodes;

    bool AlreadyReplied = false;

    const NKikimrConfig::TTableServiceConfig::EBlockTrackingMode BlockTrackingMode;
    const bool VerboseMemoryLimitException;
    TMaybe<ui8> ArrayBufferMinFillPercentage;
    TMaybe<size_t> BufferPageAllocSize;

    ui64 StatCollectInflightBytes = 0;
    ui64 StatFinishInflightBytes = 0;

    TMaybe<NBatchOperations::TSettings> BatchOperationSettings;

    bool AccountDefaultPoolInScheduler = false;

    THashSet<ui32> SentResultIndexes;

    TActorId CheckpointCoordinatorId;

protected:
    TKqpTasksGraph TasksGraph;
    std::shared_ptr<NYql::NDq::IDqChannelService> ChannelService;

private:
    static constexpr TDuration ResourceUsageUpdateInterval = TDuration::MilliSeconds(100);
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpDataExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, NFormats::TFormatsSettings formatsSettings,
    TKqpRequestCounters::TPtr counters, const TExecuterConfig& executerConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, const TActorId& creator,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    TPartitionPrunerConfig partitionPrunerConfig, const TShardIdToTableInfoPtr& shardIdToTableInfo,
    const IKqpTransactionManagerPtr& txManager, const TActorId bufferActorId,
    TMaybe<NBatchOperations::TSettings> batchOperationSettings, const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, ui64 generation,
    std::shared_ptr<NYql::NDq::IDqChannelService> channelService);

IActor* CreateKqpScanExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, NFormats::TFormatsSettings formatsSettings,
    TKqpRequestCounters::TPtr counters, const TExecuterConfig& executerConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings,
    const std::optional<TLlvmSettings>& llvmSettings, std::shared_ptr<NYql::NDq::IDqChannelService> channelService);

} // namespace NKqp
} // namespace NKikimr
