#include "dq_pq_rd_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/common/rope_over_buffer.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor_base.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/log_backend/actor_log_backend.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/utility.h>
#include <util/string/join.h>
#include <ydb/library/actors/core/interconnect.h>

#include <queue>
#include <variant>

#define SRC_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define SRC_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)

namespace NYql::NDq {

using namespace NActors;
using namespace NLog;
using namespace NKikimr::NMiniKQL;

namespace {

LWTRACE_USING(DQ_PQ_PROVIDER);

} // namespace

struct TRowDispatcherReadActorMetrics {
    explicit TRowDispatcherReadActorMetrics(const TTxId& txId, ui64 taskId, const ::NMonitoring::TDynamicCounterPtr& counters)
        : TxId(std::visit([](auto arg) { return ToString(arg); }, txId))
        , Counters(counters) {
        SubGroup = Counters->GetSubgroup("sink", "RdPqRead");
        auto sink = SubGroup->GetSubgroup("tx_id", TxId);
        auto task = sink->GetSubgroup("task_id", ToString(taskId));
        InFlyGetNextBatch = task->GetCounter("InFlyGetNextBatch");
        InFlyAsyncInputData = task->GetCounter("InFlyAsyncInputData");
    }

    ~TRowDispatcherReadActorMetrics() {
        SubGroup->RemoveSubgroup("id", TxId);
    }

    TString TxId;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyGetNextBatch;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
};

struct TEvPrivate {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPrintState = EvBegin + 20,
        EvProcessState = EvBegin + 21,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvPrintState : public NActors::TEventLocal<TEvPrintState, EvPrintState> {};
    struct TEvProcessState : public NActors::TEventLocal<TEvProcessState, EvProcessState> {};
};

class TDqPqRdReadActor : public NActors::TActor<TDqPqRdReadActor>, public NYql::NDq::NInternal::TDqPqReadActorBase {

    const ui64 PrintStatePeriodSec = 300;
    const ui64 ProcessStatePeriodSec = 2;

    using TDebugOffsets = TMaybe<std::pair<ui64, ui64>>;

    struct TReadyBatch {
    public:
        TReadyBatch(ui64 partitionId, ui32 dataCapacity) 
            : PartitionId(partitionId) {
            Data.reserve(dataCapacity);
        }

    public:
        TVector<TRope> Data;
        i64 UsedSpace = 0;
        ui64 NextOffset = 0;
        ui64 PartitionId;
    };

    enum class EState {
        INIT,
        WAIT_COORDINATOR_ID,
        WAIT_PARTITIONS_ADDRES,
        STARTED
    };

    struct TCounters {
        ui64 GetAsyncInputData = 0;
        ui64 CoordinatorChanged = 0;
        ui64 CoordinatorResult = 0;
        ui64 MessageBatch = 0;
        ui64 StartSessionAck = 0;
        ui64 NewDataArrived = 0;
        ui64 SessionError = 0;
        ui64 Statistics = 0;
        ui64 NodeDisconnected = 0;
        ui64 NodeConnected = 0;
        ui64 Undelivered = 0;
        ui64 Retry = 0;
        ui64 PrivateHeartbeat = 0;
        ui64 SessionClosed = 0;
        ui64 Pong = 0;
        ui64 Heartbeat = 0;
        ui64 PrintState = 0;
        ui64 ProcessState = 0;
        ui64 NotifyCA = 0;
    };

private:
    const TString Token;
    TMaybe<NActors::TActorId> CoordinatorActorId;
    NActors::TActorId LocalRowDispatcherActorId;
    std::queue<TReadyBatch> ReadyBuffer;
    EState State = EState::INIT;
    ui64 CoordinatorRequestCookie = 0;
    TRowDispatcherReadActorMetrics Metrics;
    bool SchedulePrintStatePeriod = false;
    bool ProcessStateScheduled = false;
    bool InFlyAsyncInputData = false;
    TCounters Counters;

    // Parsing info
    std::vector<std::optional<ui64>> ColumnIndexes;  // Output column index in schema passed into RowDispatcher
    const TType* InputDataType = nullptr;  // Multi type (comes from Row Dispatcher)
    std::unique_ptr<NKikimr::NMiniKQL::TValuePackerTransport<true>> DataUnpacker;

    struct SessionInfo {
        enum class ESessionStatus {
            NoSession,
            Started,
        };
        SessionInfo(
            const TTxId& txId,
            const NActors::TActorId selfId,
            TActorId rowDispatcherActorId,
            ui64 partitionId,
            ui64 eventQueueId,
            ui64 generation)
            : RowDispatcherActorId(rowDispatcherActorId)
            , PartitionId(partitionId)
            , Generation(generation) {
            EventsQueue.Init(txId, selfId, selfId, eventQueueId, /* KeepAlive */ true);
            EventsQueue.OnNewRecipientId(rowDispatcherActorId);
        }

        ESessionStatus Status = ESessionStatus::NoSession;
        ui64 NextOffset = 0;
        bool IsWaitingStartSessionAck = false;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        bool HasPendingData = false;
        bool IsWaitingMessageBatch = false;
        TActorId RowDispatcherActorId;
        ui64 PartitionId;
        ui64 Generation;
    };

    TMap<ui64, SessionInfo> Sessions;
    const THolderFactory& HolderFactory;
    const i64 MaxBufferSize;
    i64 ReadyBufferSizeBytes = 0;
    ui64 NextGeneration = 0;

public:
    TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        const TTypeEnvironment& typeEnv,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        NPq::NProto::TDqReadTaskParams&& readParams,
        const NActors::TActorId& computeActorId,
        const NActors::TActorId& localRowDispatcherActorId,
        const TString& token,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        i64 bufferSize);

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStartSessionAck::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvStatistics::TPtr& ev);
    void Handle(NFq::TEvRowDispatcher::TEvGetInternalStateRequest::TPtr& ev);

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr&);
    void Handle(NActors::TEvents::TEvPong::TPtr& ev);
    void Handle(const NFq::TEvRowDispatcher::TEvHeartbeat::TPtr&);
    void Handle(TEvPrivate::TEvPrintState::TPtr&);
    void Handle(TEvPrivate::TEvProcessState::TPtr&);

    STRICT_STFUNC(StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorResult, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStartSessionAck, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvSessionError, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvStatistics, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvGetInternalStateRequest, Handle);

        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvHeartbeat, Handle);
        hFunc(TEvPrivate::TEvPrintState, Handle);
        hFunc(TEvPrivate::TEvProcessState, Handle);
    })

    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override;
    void PassAway() override;
    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override;
    std::vector<ui64> GetPartitionsToRead() const;
    void AddMessageBatch(TRope&& serializedBatch, NKikimr::NMiniKQL::TUnboxedValueBatch& buffer);
    void ProcessState();
    void Stop(NDqProto::StatusIds::StatusCode status, TIssues issues);
    void StopSessions();
    void ReInit(const TString& reason);
    void PrintInternalState();
    TString GetInternalState();
    void TrySendGetNextBatch(SessionInfo& sessionInfo);
    template <class TEventPtr>
    bool CheckSession(SessionInfo& session, const TEventPtr& ev, ui64 partitionId);
    void SendStopSession(const NActors::TActorId& recipient, ui64 partitionId, ui64 cookie);
    void NotifyCA();
};

TDqPqRdReadActor::TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        const TTypeEnvironment& typeEnv,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        NPq::NProto::TDqReadTaskParams&& readParams,
        const NActors::TActorId& computeActorId,
        const NActors::TActorId& localRowDispatcherActorId,
        const TString& token,
        const ::NMonitoring::TDynamicCounterPtr& counters,
        i64 bufferSize)
        : TActor<TDqPqRdReadActor>(&TDqPqRdReadActor::StateFunc)
        , TDqPqReadActorBase(inputIndex, taskId, this->SelfId(), txId, std::move(sourceParams), std::move(readParams), computeActorId)
        , Token(token)
        , LocalRowDispatcherActorId(localRowDispatcherActorId)
        , Metrics(txId, taskId, counters)
        , HolderFactory(holderFactory)
        , MaxBufferSize(bufferSize)
{
    const auto programBuilder = std::make_unique<TProgramBuilder>(typeEnv, *holderFactory.GetFunctionRegistry());

    // Parse output schema (expected struct output type)
    const auto& outputTypeYson = SourceParams.GetRowType();
    const auto outputItemType = NCommon::ParseTypeFromYson(TStringBuf(outputTypeYson), *programBuilder, Cerr);
    YQL_ENSURE(outputItemType, "Failed to parse output type: " << outputTypeYson);
    YQL_ENSURE(outputItemType->IsStruct(), "Output type " << outputTypeYson << " is not struct");
    const auto structType = static_cast<TStructType*>(outputItemType);

    // Build input schema and unpacker (for data comes from RowDispatcher)
    TVector<TType* const> inputTypeParts;
    inputTypeParts.reserve(SourceParams.ColumnsSize());
    ColumnIndexes.resize(structType->GetMembersCount());
    for (size_t i = 0; i < SourceParams.ColumnsSize(); ++i) {
        const auto index = structType->GetMemberIndex(SourceParams.GetColumns().Get(i));
        inputTypeParts.emplace_back(structType->GetMemberType(index));
        ColumnIndexes[index] = i;
    }
    InputDataType = programBuilder->NewMultiType(inputTypeParts);
    DataUnpacker = std::make_unique<NKikimr::NMiniKQL::TValuePackerTransport<true>>(InputDataType);

    IngressStats.Level = statsLevel;
    SRC_LOG_I("Start read actor, local row dispatcher " << LocalRowDispatcherActorId.ToString() << ", metadatafields: " << JoinSeq(',', SourceParams.GetMetadataFields()));
}

void TDqPqRdReadActor::ProcessState() {
    switch (State) {
    case EState::INIT:
        LogPrefix = (TStringBuilder() << "SelfId: " << SelfId() << ", TxId: " << TxId << ", task: " << TaskId << ". PQ source. ");

        if (!ReadyBuffer.empty()) {
            return;
        }
        if (!ProcessStateScheduled) {
            ProcessStateScheduled = true;
            Schedule(TDuration::Seconds(ProcessStatePeriodSec), new TEvPrivate::TEvProcessState());
        }
        if (!CoordinatorActorId) {
            SRC_LOG_I("Send TEvCoordinatorChangesSubscribe to local row dispatcher, self id " << SelfId());
            Send(LocalRowDispatcherActorId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
            if (!SchedulePrintStatePeriod) {
                SchedulePrintStatePeriod = true;
                Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());
            }
        }
        State = EState::WAIT_COORDINATOR_ID; 
        [[fallthrough]];
    case EState::WAIT_COORDINATOR_ID: {
        if (!CoordinatorActorId) {
            return;
        }
        State = EState::WAIT_PARTITIONS_ADDRES;
        auto partitionToRead = GetPartitionsToRead();
        auto cookie = ++CoordinatorRequestCookie;
        SRC_LOG_I("Send TEvCoordinatorRequest to coordinator " << CoordinatorActorId->ToString() << ", partIds: "
            << JoinSeq(", ", partitionToRead) << " cookie " << cookie);
        Send(
            *CoordinatorActorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorRequest(SourceParams, partitionToRead),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            cookie);
        return;
    }
    case EState::WAIT_PARTITIONS_ADDRES:
        if (Sessions.empty()) {
            return;
        }

        for (auto& [partitionId, sessionInfo] : Sessions) {
            if (sessionInfo.Status == SessionInfo::ESessionStatus::NoSession) {
                TMaybe<ui64> readOffset;
                TPartitionKey partitionKey{TString{}, partitionId};
                const auto offsetIt = PartitionToOffset.find(partitionKey);
                if (offsetIt != PartitionToOffset.end()) {
                    SRC_LOG_D("ReadOffset found" );
                    readOffset = offsetIt->second;
                }

                SRC_LOG_I("Send TEvStartSession to " << sessionInfo.RowDispatcherActorId 
                        << ", offset " << readOffset 
                        << ", partitionId " << partitionId
                        << ", connection id " << sessionInfo.Generation);

                auto event = new NFq::TEvRowDispatcher::TEvStartSession(
                    SourceParams,
                    partitionId,
                    Token,
                    readOffset,
                    StartingMessageTimestamp.MilliSeconds(),
                    std::visit([](auto arg) { return ToString(arg); }, TxId));
                sessionInfo.EventsQueue.Send(event, sessionInfo.Generation);
                sessionInfo.IsWaitingStartSessionAck = true;
                sessionInfo.Status = SessionInfo::ESessionStatus::Started;
            }
        }
        State = EState::STARTED;
        return;
    case EState::STARTED:
        return;
    }
}


void TDqPqRdReadActor::CommitState(const NDqProto::TCheckpoint& /*checkpoint*/) {
}

void TDqPqRdReadActor::StopSessions() {
    SRC_LOG_I("Stop all session");
    for (auto& [partitionId, sessionInfo] : Sessions) {
        if (sessionInfo.Status == SessionInfo::ESessionStatus::NoSession) {
            continue;
        }
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
        *event->Record.MutableSource() = SourceParams;
        event->Record.SetPartitionId(partitionId);
        SRC_LOG_I("Send StopSession to " << sessionInfo.RowDispatcherActorId);
        sessionInfo.EventsQueue.Send(event.release(), sessionInfo.Generation);
    }
}

// IActor & IDqComputeActorAsyncInput
void TDqPqRdReadActor::PassAway() { // Is called from Compute Actor
    SRC_LOG_I("PassAway");
    PrintInternalState();
    StopSessions();
    TActor<TDqPqRdReadActor>::PassAway();
    
    // TODO: RetryQueue::Unsubscribe()
}

i64 TDqPqRdReadActor::GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& /*watermark*/, bool&, i64 freeSpace) {
    SRC_LOG_T("GetAsyncInputData freeSpace = " << freeSpace);
    Metrics.InFlyAsyncInputData->Set(0);
    InFlyAsyncInputData = false;

    ProcessState();
    if (ReadyBuffer.empty() || !freeSpace) {
        return 0;
    }
    i64 usedSpace = 0;
    buffer.clear();
    do {
        auto readyBatch = std::move(ReadyBuffer.front());
        ReadyBuffer.pop();

        for (auto& messageBatch : readyBatch.Data) {
            AddMessageBatch(std::move(messageBatch), buffer);
        }

        usedSpace += readyBatch.UsedSpace;
        freeSpace -= readyBatch.UsedSpace;
        TPartitionKey partitionKey{TString{}, readyBatch.PartitionId};
        PartitionToOffset[partitionKey] = readyBatch.NextOffset;
        SRC_LOG_T("NextOffset " << readyBatch.NextOffset);
    } while (freeSpace > 0 && !ReadyBuffer.empty());

    ReadyBufferSizeBytes -= usedSpace;
    SRC_LOG_T("Return " << buffer.RowCount() << " rows, buffer size " << ReadyBufferSizeBytes << ", free space " << freeSpace << ", result size " << usedSpace);

    if (!ReadyBuffer.empty()) {
        NotifyCA();
    }
    for (auto& [partitionId, sessionInfo] : Sessions) {
        TrySendGetNextBatch(sessionInfo);
    }
    ProcessState();
    return usedSpace;
}

std::vector<ui64> TDqPqRdReadActor::GetPartitionsToRead() const {
    std::vector<ui64> res;

    ui64 currentPartition = ReadParams.GetPartitioningParams().GetEachTopicPartitionGroupId();
    do {
        res.emplace_back(currentPartition); // 0-based in topic API
        currentPartition += ReadParams.GetPartitioningParams().GetDqPartitionsCount();
    } while (currentPartition < ReadParams.GetPartitioningParams().GetTopicPartitionsCount());
    return res;
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvStartSessionAck::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_I("TEvStartSessionAck from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    Counters.StartSessionAck++;

    ui64 partitionId = ev->Get()->Record.GetConsumer().GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvStartSessionAck from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId << ", cookie " << ev->Cookie);
        YQL_ENSURE(State != EState::STARTED);
        SendStopSession(ev->Sender, partitionId, ev->Cookie);
        return;
    }
    auto& sessionInfo = sessionIt->second;
    if (!CheckSession(sessionInfo, ev, partitionId)) {
        return;
    }
    sessionInfo.IsWaitingStartSessionAck = false;
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_I("TEvSessionError from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    Counters.SessionError++;

    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvSessionError from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId << ", cookie " << ev->Cookie);
        YQL_ENSURE(State != EState::STARTED);
        SendStopSession(ev->Sender, partitionId, ev->Cookie);
        return;
    }

    auto& sessionInfo = sessionIt->second;
    if (!CheckSession(sessionInfo, ev, partitionId)) {
        return;
    }

    NYql::TIssues issues;
    IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
    Stop(ev->Get()->Record.GetStatusCode(), issues);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvStatistics::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("TEvStatistics from " << ev->Sender << ", offset " << ev->Get()->Record.GetNextMessageOffset() << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    Counters.Statistics++;

    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvStatistics from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId);
        YQL_ENSURE(State != EState::STARTED);
        SendStopSession(ev->Sender, partitionId, ev->Cookie);
        return;
    }
    auto& sessionInfo = sessionIt->second;
    IngressStats.Bytes += ev->Get()->Record.GetReadBytes();

    if (!CheckSession(sessionInfo, ev, partitionId)) {
        return;
    }

    if (ReadyBuffer.empty()) {
        TPartitionKey partitionKey{TString{}, partitionId};
        PartitionToOffset[partitionKey] = ev->Get()->Record.GetNextMessageOffset();
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvGetInternalStateRequest::TPtr& ev) {
    auto response = std::make_unique<NFq::TEvRowDispatcher::TEvGetInternalStateResponse>();
    response->Record.SetInternalState(GetInternalState());
    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("TEvNewDataArrived from " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId() << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    Counters.NewDataArrived++;

    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvNewDataArrived from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId);
        YQL_ENSURE(State != EState::STARTED);
        SendStopSession(ev->Sender, partitionId, ev->Cookie);
        return;
    }

    auto& sessionInfo = sessionIt->second;
    if (!CheckSession(sessionInfo, ev, partitionId)) {
        return;
    }
    sessionInfo.HasPendingData = true;
    TrySendGetNextBatch(sessionInfo);
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    SRC_LOG_D("TEvRetry");
    Counters.Retry++;
    ui64 partitionId = ev->Get()->EventQueueId;

    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Unknown partition id " << partitionId << ", skip TEvRetry");
        return;
    }
    sessionIt->second.EventsQueue.Retry();
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr& ev) {
    SRC_LOG_T("TEvRetryQueuePrivate::TEvEvHeartbeat");
    Counters.PrivateHeartbeat++;
    ui64 partitionId = ev->Get()->EventQueueId;

    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Unknown partition id " << partitionId << ", skip TEvPing");
        return;
    }
    auto& sessionInfo = sessionIt->second;
    bool needSend = sessionInfo.EventsQueue.Heartbeat();
    if (needSend) {
        SRC_LOG_T("Send TEvEvHeartbeat");
        sessionInfo.EventsQueue.Send(new NFq::TEvRowDispatcher::TEvHeartbeat(sessionInfo.PartitionId), sessionInfo.Generation);
    }
}

void TDqPqRdReadActor::Handle(const NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev) {
    SRC_LOG_T("Received TEvHeartbeat from " << ev->Sender);
    Counters.Heartbeat++;
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();

    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvHeartbeat from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId << ", cookie " << ev->Cookie);
        SendStopSession(ev->Sender, partitionId, ev->Cookie);
        return;
    }
    CheckSession(sessionIt->second, ev, partitionId);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    SRC_LOG_D("TEvCoordinatorChanged, new coordinator " << ev->Get()->CoordinatorActorId);
    Counters.GetAsyncInputData++;

    if (CoordinatorActorId
        && CoordinatorActorId == ev->Get()->CoordinatorActorId) {
        return;
    }

    if (!CoordinatorActorId) {
        CoordinatorActorId = ev->Get()->CoordinatorActorId;
        ProcessState();
        return;
    }

    CoordinatorActorId = ev->Get()->CoordinatorActorId;
    ReInit("Coordinator is changed");
    ProcessState();
}

void TDqPqRdReadActor::ReInit(const TString& reason) {
    SRC_LOG_I("ReInit state, reason " << reason);
    StopSessions();
    Sessions.clear();
    State = EState::INIT;
    if (!ReadyBuffer.empty()) {
        NotifyCA();
    }
    PrintInternalState();
}

void TDqPqRdReadActor::Stop(NDqProto::StatusIds::StatusCode status, TIssues issues) {
    SRC_LOG_E("Stop read actor, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(status) << ", issues: " << issues.ToOneLineString());
    Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), status));
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr& ev) {
    SRC_LOG_I("TEvCoordinatorResult from " << ev->Sender.ToString() << ", cookie " << ev->Cookie);
    Counters.CoordinatorChanged++;
    if (ev->Cookie != CoordinatorRequestCookie) {
        SRC_LOG_W("Ignore TEvCoordinatorResult. wrong cookie");
        return;
    }
    if (State != EState::WAIT_PARTITIONS_ADDRES) {
        SRC_LOG_W("Ignore TEvCoordinatorResult. wrong state " << static_cast<ui64>(EState::WAIT_PARTITIONS_ADDRES));
        return;
    }
    for (auto& p : ev->Get()->Record.GetPartitions()) {
        TActorId rowDispatcherActorId = ActorIdFromProto(p.GetActorId());
        for (auto partitionId : p.GetPartitionId()) {
            if (Sessions.contains(partitionId)) {
                Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue("Session already exists")});
                return;
            }
            SRC_LOG_I("Create session to RD (" << rowDispatcherActorId << "), partitionId " << partitionId);
            Sessions.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(partitionId),
                std::forward_as_tuple(TxId, SelfId(), rowDispatcherActorId, partitionId, partitionId, ++NextGeneration));
        }
    }
    ProcessState();
}

void TDqPqRdReadActor::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    SRC_LOG_D("EvNodeConnected " << ev->Get()->NodeId);
    Counters.NodeConnected++;
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TDqPqRdReadActor::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    SRC_LOG_D("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    Counters.NodeDisconnected++;
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
    // In case of row dispatcher disconnection: wait connected or SessionClosed(). TODO: Stop actor after timeout.
    // In case of row dispatcher disconnection: wait CoordinatorChanged().
    //Stop(TString{"Node disconnected, nodeId "} + ToString(ev->Get()->NodeId));
}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    SRC_LOG_D("TEvUndelivered, " << ev->Get()->ToString() << " from " << ev->Sender.ToString());
    Counters.Undelivered++;
    for (auto& [partitionId, sessionInfo] : Sessions) {
        if (sessionInfo.EventsQueue.HandleUndelivered(ev) == NYql::NDq::TRetryEventsQueue::ESessionState::SessionClosed) {
            ReInit(TStringBuilder() << "Session closed, partition id " << sessionInfo.PartitionId);
            break;
        }
    }

    if (CoordinatorActorId && *CoordinatorActorId == ev->Sender) {
        ReInit("TEvUndelivered to coordinator");
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("TEvMessageBatch from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    Counters.MessageBatch++;
    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvMessageBatch from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId << ", cookie " << ev->Cookie);
        YQL_ENSURE(State != EState::STARTED);
        SendStopSession(ev->Sender, partitionId, ev->Cookie);
        return;
    }

    Metrics.InFlyGetNextBatch->Set(0);
    auto& sessionInfo = sessionIt->second;
    if (!CheckSession(sessionInfo, ev, partitionId)) {
        return;
    }
    ReadyBuffer.emplace(partitionId, ev->Get()->Record.MessagesSize());
    TReadyBatch& activeBatch = ReadyBuffer.back();

    ui64 bytes = 0;
    for (const auto& message : ev->Get()->Record.GetMessages()) {
        const auto& offsets = message.GetOffsets();
        if (offsets.empty()) {
            Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue("Got unexpected empty batch from row dispatcher")});
            return;
        }

        activeBatch.Data.emplace_back(ev->Get()->GetPayload(message.GetPayloadId()));
        bytes += activeBatch.Data.back().GetSize();

        sessionInfo.NextOffset = *offsets.rbegin() + 1;
        SRC_LOG_T("TEvMessageBatch NextOffset " << sessionInfo.NextOffset);
    }
    activeBatch.UsedSpace = bytes;
    ReadyBufferSizeBytes += bytes;
    activeBatch.NextOffset = ev->Get()->Record.GetNextMessageOffset();
    sessionInfo.IsWaitingMessageBatch = false;
    NotifyCA();
}

void TDqPqRdReadActor::AddMessageBatch(TRope&& messageBatch, NKikimr::NMiniKQL::TUnboxedValueBatch& buffer) {
    // TDOD: pass multi type directly to CA, without transforming it into struct

    NKikimr::NMiniKQL::TUnboxedValueBatch parsedData(InputDataType);
    DataUnpacker->UnpackBatch(MakeChunkedBuffer(std::move(messageBatch)), HolderFactory, parsedData);

    while (!parsedData.empty()) {
        const auto* parsedRow = parsedData.Head();

        NUdf::TUnboxedValue* itemPtr;
        NUdf::TUnboxedValuePod item = HolderFactory.CreateDirectArrayHolder(ColumnIndexes.size(), itemPtr);
        for (const auto index : ColumnIndexes) {
            if (index) {
                YQL_ENSURE(*index < parsedData.Width(), "Unexpected data width " << parsedData.Width() << ", failed to extract column by index " << index);
                *(itemPtr++) = parsedRow[*index];
            } else {
                // TODO: support metadata fields here
                *(itemPtr++) = NUdf::TUnboxedValue();
            }
        }

        buffer.emplace_back(std::move(item));
        parsedData.Pop();
    }
}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvPong::TPtr& ev) {
    SRC_LOG_T("TEvPong from " << ev->Sender);
    Counters.Pong++;
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvPrintState::TPtr&) {
    Counters.PrintState++;
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());
    PrintInternalState();
}

void TDqPqRdReadActor::PrintInternalState() {
    SRC_LOG_I(GetInternalState());
}

TString TDqPqRdReadActor::GetInternalState() {
    TStringStream str;
    str << "State: used buffer size " << ReadyBufferSizeBytes << " ready buffer event size " << ReadyBuffer.size()  << " state " << static_cast<ui64>(State) << " InFlyAsyncInputData " << InFlyAsyncInputData << "\n";
    str << "Counters: GetAsyncInputData " << Counters.GetAsyncInputData << " CoordinatorChanged " << Counters.CoordinatorChanged << " CoordinatorResult " << Counters.CoordinatorResult
        << " MessageBatch " << Counters.MessageBatch << " StartSessionAck " << Counters.StartSessionAck << " NewDataArrived " << Counters.NewDataArrived
        << " SessionError " << Counters.SessionError << " Statistics " << Counters.Statistics << " NodeDisconnected " << Counters.NodeDisconnected
        << " NodeConnected " << Counters.NodeConnected << " Undelivered " << Counters.Undelivered << " Retry " << Counters.Retry
        << " PrivateHeartbeat " << Counters.PrivateHeartbeat << " SessionClosed " << Counters.SessionClosed << " Pong " << Counters.Pong
        << " Heartbeat " << Counters.Heartbeat << " PrintState " << Counters.PrintState << " ProcessState " << Counters.ProcessState
        << " NotifyCA " << Counters.NotifyCA << "\n";
    
    for (auto& [partitionId, sessionInfo] : Sessions) {
        str << "   partId " << partitionId << " status " << static_cast<ui64>(sessionInfo.Status)
            << " next offset " << sessionInfo.NextOffset
            << " is waiting ack " << sessionInfo.IsWaitingStartSessionAck << " is waiting batch " << sessionInfo.IsWaitingMessageBatch
            << " has pending data " << sessionInfo.HasPendingData << " connection id " << sessionInfo.Generation << " ";
        sessionInfo.EventsQueue.PrintInternalState(str);
    }
    return str.Str();
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvProcessState::TPtr&) {
    Counters.ProcessState++;
    Schedule(TDuration::Seconds(ProcessStatePeriodSec), new TEvPrivate::TEvProcessState());
    ProcessState();
}

void TDqPqRdReadActor::TrySendGetNextBatch(SessionInfo& sessionInfo) {
    if (!sessionInfo.HasPendingData) {
        return;
    }
    if (ReadyBufferSizeBytes > MaxBufferSize) {
        return;
    }
    Metrics.InFlyGetNextBatch->Inc();
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvGetNextBatch>();
    sessionInfo.HasPendingData = false;
    sessionInfo.IsWaitingMessageBatch = true; 
    event->Record.SetPartitionId(sessionInfo.PartitionId);
    sessionInfo.EventsQueue.Send(event.release(), sessionInfo.Generation);
}

template <class TEventPtr>
bool TDqPqRdReadActor::CheckSession(SessionInfo& session, const TEventPtr& ev, ui64 partitionId) {
    if (ev->Cookie != session.Generation) {
        SRC_LOG_W("Wrong message generation (" << typeid(TEventPtr).name()  << "), sender " << ev->Sender << " cookie " << ev->Cookie << ", session generation " << session.Generation << ", send TEvStopSession");
        SendStopSession(ev->Sender, partitionId, ev->Cookie);
        return false;
    }
    if (!session.EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        SRC_LOG_W("Wrong seq num ignore message (" << typeid(TEventPtr).name() << ") seqNo " << meta.GetSeqNo() << " from " << ev->Sender.ToString());
        return false;
    }
    return true;
}

void TDqPqRdReadActor::SendStopSession(const NActors::TActorId& recipient, ui64 partitionId, ui64 cookie) {
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
    *event->Record.MutableSource() = SourceParams;
    event->Record.SetPartitionId(partitionId);
    Send(recipient, event.release(), 0, cookie);
}

void TDqPqRdReadActor::NotifyCA() {
    Metrics.InFlyAsyncInputData->Set(1);
    InFlyAsyncInputData = true;
    Counters.NotifyCA++;
    Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
}

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqPqRdReadActor(
    const TTypeEnvironment& typeEnv,
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    const NActors::TActorId& computeActorId,
    const NActors::TActorId& localRowDispatcherActorId,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const ::NMonitoring::TDynamicCounterPtr& counters,
    i64 bufferSize)
{
    auto taskParamsIt = taskParams.find("pq");
    YQL_ENSURE(taskParamsIt != taskParams.end(), "Failed to get pq task params");

    NPq::NProto::TDqReadTaskParams readTaskParamsMsg;
    YQL_ENSURE(readTaskParamsMsg.ParseFromString(taskParamsIt->second), "Failed to parse DqPqRead task params");

    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());

    TDqPqRdReadActor* actor = new TDqPqRdReadActor(
        inputIndex,
        statsLevel,
        txId,
        taskId,
        holderFactory,
        typeEnv,
        std::move(settings),
        std::move(readTaskParamsMsg),
        computeActorId,
        localRowDispatcherActorId,
        token,
        counters,
        bufferSize
    );

    return {actor, actor};
}

} // namespace NYql::NDq
