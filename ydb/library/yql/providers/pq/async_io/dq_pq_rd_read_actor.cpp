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

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

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
        SubGroup = Counters->GetSubgroup("source", "RdPqRead");
        auto source = SubGroup->GetSubgroup("tx_id", TxId);
        auto task = source->GetSubgroup("task_id", ToString(taskId));
        InFlyGetNextBatch = task->GetCounter("InFlyGetNextBatch");
        InFlyAsyncInputData = task->GetCounter("InFlyAsyncInputData");
        ReInit = task->GetCounter("ReInit", true);
    }

    ~TRowDispatcherReadActorMetrics() {
        SubGroup->RemoveSubgroup("tx_id", TxId);
    }

    TString TxId;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyGetNextBatch;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyAsyncInputData;
    ::NMonitoring::TDynamicCounters::TCounterPtr ReInit;
};

struct TEvPrivate {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPrintState = EvBegin + 20,
        EvProcessState = EvBegin + 21,
        EvNotifyCA = EvBegin + 22,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvPrintState : public NActors::TEventLocal<TEvPrintState, EvPrintState> {};
    struct TEvProcessState : public NActors::TEventLocal<TEvProcessState, EvProcessState> {};
    struct TEvNotifyCA : public NActors::TEventLocal<TEvNotifyCA, EvNotifyCA> {};
};

class TDqPqRdReadActor : public NActors::TActor<TDqPqRdReadActor>, public NYql::NDq::NInternal::TDqPqReadActorBase {

    const ui64 PrintStatePeriodSec = 300;
    const ui64 ProcessStatePeriodSec = 1;
    const ui64 PrintStateToLogSplitSize = 64000;
    const ui64 NotifyCAPeriodSec = 10;

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
    bool Inited = false;
    ui64 CoordinatorRequestCookie = 0;
    TRowDispatcherReadActorMetrics Metrics;
    bool ProcessStateScheduled = false;
    bool InFlyAsyncInputData = false;
    TCounters Counters;
    // Parsing info
    std::vector<std::optional<ui64>> ColumnIndexes;  // Output column index in schema passed into RowDispatcher
    const TType* InputDataType = nullptr;  // Multi type (comes from Row Dispatcher)
    std::unique_ptr<NKikimr::NMiniKQL::TValuePackerTransport<true>> DataUnpacker;
    ui64 CpuMicrosec = 0;

    THashMap<ui32, TMaybe<ui64>> NextOffsetFromRD;

    struct TPartition {
        bool HasPendingData = false;
        bool IsWaitingMessageBatch = false;
    };

    struct TSession {
        enum class ESessionStatus {
            INIT,
            WAIT_START_SESSION_ACK,
            STARTED,
        };

        TSession(
            const TTxId& txId,
            const NActors::TActorId selfId,
            TActorId rowDispatcherActorId,
            ui64 eventQueueId,
            ui64 generation)
            : TxId(txId)
            , SelfId(selfId)
            , EventQueueId(eventQueueId)
            , RowDispatcherActorId(rowDispatcherActorId)
            , Generation(generation)
        {
            EventsQueue.Init(TxId, SelfId, SelfId, EventQueueId, /* KeepAlive */ true);
            EventsQueue.OnNewRecipientId(rowDispatcherActorId);
        }

        ESessionStatus Status = ESessionStatus::INIT;
        const TTxId TxId;
        const NActors::TActorId SelfId;
        const ui64 EventQueueId;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        TActorId RowDispatcherActorId;
        ui64 Generation = std::numeric_limits<ui64>::max();
        THashMap<ui32, TPartition> Partitions;
        bool IsWaitingStartSessionAck = false;
        ui64 QueuedBytes = 0;
        ui64 QueuedRows = 0;
    };
    
    TMap<NActors::TActorId, TSession> Sessions;
    THashMap<ui64, NActors::TActorId> ReadActorByEventQueueId;
    const THolderFactory& HolderFactory;
    const i64 MaxBufferSize;
    i64 ReadyBufferSizeBytes = 0;
    ui64 NextGeneration = 0;
    ui64 NextEventQueueId = 0;

    TMap<NActors::TActorId, TSet<ui32>> LastUsedPartitionDistribution;
    TMap<NActors::TActorId, TSet<ui32>> LastReceivedPartitionDistribution;

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
    void Handle(TEvPrivate::TEvNotifyCA::TPtr&);

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
        hFunc(TEvPrivate::TEvNotifyCA, Handle);
    })

    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override;
    void PassAway() override;
    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override;
    TDuration GetCpuTime() override;
    std::vector<ui64> GetPartitionsToRead() const;
    void AddMessageBatch(TRope&& serializedBatch, NKikimr::NMiniKQL::TUnboxedValueBatch& buffer);
    void ProcessState();
    void StopSession(TSession& sessionInfo);
    void Stop(NDqProto::StatusIds::StatusCode status, TIssues issues);
    void ReInit(const TString& reason);
    void PrintInternalState();
    void TrySendGetNextBatch(TSession& sessionInfo);
    TString GetInternalState();
    template <class TEventPtr>
    TSession* FindAndUpdateSession(const TEventPtr& ev);
    void SendNoSession(const NActors::TActorId& recipient, ui64 cookie);
    void NotifyCA();
    void SendStartSession(TSession& sessionInfo);
    void Init();
    void ScheduleProcessState();
    void ProcessGlobalState();
    void ProcessSessionsState();
    void UpdateSessions();
    void UpdateQueuedSize();
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
    SRC_LOG_I("Start read actor, local row dispatcher " << LocalRowDispatcherActorId.ToString() << ", metadatafields: " << JoinSeq(',', SourceParams.GetMetadataFields())
        << ", partitions: " << JoinSeq(',', GetPartitionsToRead()));
}

void TDqPqRdReadActor::Init() {
    if (Inited) {
        return;
    }
    LogPrefix = (TStringBuilder() << "SelfId: " << SelfId() << ", TxId: " << TxId << ", task: " << TaskId << ". PQ source. ");

    auto partitionToRead = GetPartitionsToRead();
    for (auto partitionId : partitionToRead) {
        TPartitionKey partitionKey{TString{}, partitionId};
        const auto offsetIt = PartitionToOffset.find(partitionKey);
        auto& nextOffset = NextOffsetFromRD[partitionId];
        if (offsetIt != PartitionToOffset.end()) {
            nextOffset = offsetIt->second;
        }
    }
    SRC_LOG_I("Send TEvCoordinatorChangesSubscribe to local RD (" << LocalRowDispatcherActorId << ")");
    Send(LocalRowDispatcherActorId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());

    Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());
    Schedule(TDuration::Seconds(NotifyCAPeriodSec), new TEvPrivate::TEvNotifyCA());
    Inited = true;
}

void TDqPqRdReadActor::ProcessGlobalState() {
    switch (State) {
    case EState::INIT:
        if (!ReadyBuffer.empty()) {
            return;
        }
        if (!CoordinatorActorId) {
            SRC_LOG_I("Send TEvCoordinatorChangesSubscribe to local row dispatcher, self id " << SelfId());
            Send(LocalRowDispatcherActorId, new NFq::TEvRowDispatcher::TEvCoordinatorChangesSubscribe());
            State = EState::WAIT_COORDINATOR_ID;
        }
        [[fallthrough]];
    case EState::WAIT_COORDINATOR_ID: {
        if (!CoordinatorActorId) {
            return;
        }
        auto partitionToRead = GetPartitionsToRead();
        auto cookie = ++CoordinatorRequestCookie;
        SRC_LOG_I("Send TEvCoordinatorRequest to coordinator " << CoordinatorActorId->ToString() << ", partIds: "
            << JoinSeq(", ", partitionToRead) << " cookie " << cookie);
        Send(
            *CoordinatorActorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorRequest(SourceParams, partitionToRead),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            cookie);
        LastReceivedPartitionDistribution.clear();
        State = EState::WAIT_PARTITIONS_ADDRES;
        [[fallthrough]];
    }
    case EState::WAIT_PARTITIONS_ADDRES:
        if (LastReceivedPartitionDistribution.empty()) {
            break;
        }
        UpdateSessions();
        State = EState::STARTED;
        [[fallthrough]];
    case EState::STARTED:
        break;
    }
}

void TDqPqRdReadActor::ProcessSessionsState() {
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        switch (sessionInfo.Status) {
        case TSession::ESessionStatus::INIT:
            SendStartSession(sessionInfo);
            sessionInfo.IsWaitingStartSessionAck = true;
            sessionInfo.Status = TSession::ESessionStatus::WAIT_START_SESSION_ACK;
            [[fallthrough]];
        case TSession::ESessionStatus::WAIT_START_SESSION_ACK:
            if (sessionInfo.IsWaitingStartSessionAck) {
                break;
            }
            sessionInfo.Status = TSession::ESessionStatus::STARTED;
            [[fallthrough]];
        case TSession::ESessionStatus::STARTED:
            break;
        }
    }
}

void TDqPqRdReadActor::ProcessState() {
    ProcessGlobalState();
    ProcessSessionsState();   
}

void TDqPqRdReadActor::SendStartSession(TSession& sessionInfo) {
    auto str = TStringBuilder() << "Send TEvStartSession to " << sessionInfo.RowDispatcherActorId 
        << ", connection id " << sessionInfo.Generation << " partitions offsets ";

    std::set<ui32> partitions;
    std::map<ui32, ui64> partitionOffsets;
    for (auto& [partitionId, partition] : sessionInfo.Partitions) {
        partitions.insert(partitionId);
        auto nextOffset = NextOffsetFromRD[partitionId];
        str << "(" << partitionId << " / ";
        if (!nextOffset) {
            str << "<empty>),";
            continue;
        }
        partitionOffsets[partitionId] = *nextOffset;
        str << nextOffset << "),";
    }
    SRC_LOG_I(str);

    auto event = new NFq::TEvRowDispatcher::TEvStartSession(
        SourceParams,
        partitions,
        Token,
        partitionOffsets,
        StartingMessageTimestamp.MilliSeconds(),
        std::visit([](auto arg) { return ToString(arg); }, TxId));
    sessionInfo.EventsQueue.Send(event, sessionInfo.Generation);
}

void TDqPqRdReadActor::CommitState(const NDqProto::TCheckpoint& /*checkpoint*/) {
}

void TDqPqRdReadActor::StopSession(TSession& sessionInfo) {
    SRC_LOG_I("Send StopSession to " << sessionInfo.RowDispatcherActorId
        << " generation " << sessionInfo.Generation);
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
    *event->Record.MutableSource() = SourceParams;
    sessionInfo.EventsQueue.Send(event.release(), sessionInfo.Generation);
}

// IActor & IDqComputeActorAsyncInput
void TDqPqRdReadActor::PassAway() { // Is called from Compute Actor
    SRC_LOG_I("PassAway");
    PrintInternalState();
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        StopSession(sessionInfo);
    }
    TActor<TDqPqRdReadActor>::PassAway();

    // TODO: RetryQueue::Unsubscribe()
}

i64 TDqPqRdReadActor::GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& /*watermark*/, bool&, i64 freeSpace) {
    SRC_LOG_T("GetAsyncInputData freeSpace = " << freeSpace);
    Init();
    Metrics.InFlyAsyncInputData->Set(0);
    InFlyAsyncInputData = false;

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
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        TrySendGetNextBatch(sessionInfo);
    }
    return usedSpace;
}

TDuration TDqPqRdReadActor::GetCpuTime() {
    return TDuration::MicroSeconds(CpuMicrosec);
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
    SRC_LOG_I("Received TEvStartSessionAck from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() <<  ", generation " << ev->Cookie);
    Counters.StartSessionAck++;
    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    session->IsWaitingStartSessionAck = false;
    ProcessState();
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_I("Received TEvSessionError from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    Counters.SessionError++;

    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    NYql::TIssues issues;
    IssuesFromMessage(ev->Get()->Record.GetIssues(), issues);
    Stop(ev->Get()->Record.GetStatusCode(), issues);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvStatistics::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("Received TEvStatistics from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << " generation " << ev->Cookie);
    Counters.Statistics++;
    CpuMicrosec += ev->Get()->Record.GetCpuMicrosec();
    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    IngressStats.Bytes += ev->Get()->Record.GetReadBytes();
    IngressStats.FilteredBytes += ev->Get()->Record.GetFilteredBytes();
    IngressStats.FilteredRows += ev->Get()->Record.GetFilteredRows();
    session->QueuedBytes = ev->Get()->Record.GetQueuedBytes();
    session->QueuedRows = ev->Get()->Record.GetQueuedRows();
    UpdateQueuedSize();

    for (auto partition : ev->Get()->Record.GetPartition()) {
        ui64 partitionId = partition.GetPartitionId();
        auto& nextOffset = NextOffsetFromRD[partitionId];
        if (!nextOffset) {
            nextOffset = partition.GetNextMessageOffset();
        } else {
            nextOffset = std::max(*nextOffset, partition.GetNextMessageOffset());
        }
        SRC_LOG_T("NextOffsetFromRD [" << partitionId << "]= " << nextOffset);
        if (ReadyBuffer.empty()) {
            TPartitionKey partitionKey{TString{}, partitionId};
            PartitionToOffset[partitionKey] = *nextOffset;
        }
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvGetInternalStateRequest::TPtr& ev) {
    auto response = std::make_unique<NFq::TEvRowDispatcher::TEvGetInternalStateResponse>();
    response->Record.SetInternalState(GetInternalState());
    Send(ev->Sender, response.release(), 0, ev->Cookie);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("Received TEvNewDataArrived from " << ev->Sender << ", partition " << ev->Get()->Record.GetPartitionId() << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << " generation " << ev->Cookie);
    Counters.NewDataArrived++;
 
    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    auto partitionIt = session->Partitions.find(ev->Get()->Record.GetPartitionId());
    if (partitionIt == session->Partitions.end()) {
        SRC_LOG_E("Received TEvNewDataArrived  from " << ev->Sender << " with wrong partition id " << ev->Get()->Record.GetPartitionId());
        Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue(TStringBuilder() << LogPrefix << "No partition with id " << ev->Get()->Record.GetPartitionId())});
        return;
    }
    partitionIt->second.HasPendingData = true;
    TrySendGetNextBatch(*session);
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    SRC_LOG_T("Received TEvRetry, EventQueueId " << ev->Get()->EventQueueId);
    Counters.Retry++;

    auto readActorIt = ReadActorByEventQueueId.find(ev->Get()->EventQueueId);
    if (readActorIt == ReadActorByEventQueueId.end()) {
        SRC_LOG_D("Ignore TEvRetry, wrong EventQueueId " << ev->Get()->EventQueueId);
        return;
    }

    auto sessionIt = Sessions.find(readActorIt->second);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_D("Ignore TEvRetry, wrong read actor id " << readActorIt->second);
        return;
    }
    sessionIt->second.EventsQueue.Retry();
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr& ev) {
    SRC_LOG_T("TEvRetryQueuePrivate::TEvEvHeartbeat");
    Counters.PrivateHeartbeat++;
    auto readActorIt = ReadActorByEventQueueId.find(ev->Get()->EventQueueId);
    if (readActorIt == ReadActorByEventQueueId.end()) {
        SRC_LOG_D("Ignore TEvEvHeartbeat, wrong EventQueueId " << ev->Get()->EventQueueId);
        return;
    }

    auto sessionIt = Sessions.find(readActorIt->second);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_D("Ignore TEvEvHeartbeat, wrong read actor id " << readActorIt->second);
        return;
    }
    auto& sessionInfo = sessionIt->second;
    bool needSend = sessionInfo.EventsQueue.Heartbeat();
    if (needSend) {
        SRC_LOG_T("Send TEvEvHeartbeat");
        sessionInfo.EventsQueue.Send(new NFq::TEvRowDispatcher::TEvHeartbeat(), sessionInfo.Generation);
    }
}

void TDqPqRdReadActor::Handle(const NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev) {
    SRC_LOG_T("Received TEvHeartbeat from " << ev->Sender << ", generation " << ev->Cookie);
    Counters.Heartbeat++;
    FindAndUpdateSession(ev);
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
    ScheduleProcessState();
}

void TDqPqRdReadActor::ScheduleProcessState() {
    if (ProcessStateScheduled) {
        return;
    }
    ProcessStateScheduled = true;
    Schedule(TDuration::Seconds(ProcessStatePeriodSec), new TEvPrivate::TEvProcessState());
}

void TDqPqRdReadActor::ReInit(const TString& reason) {
    SRC_LOG_I("ReInit state, reason " << reason);
    Metrics.ReInit->Inc();

    State = EState::WAIT_COORDINATOR_ID;
    if (!ReadyBuffer.empty()) {
        NotifyCA();
    }
}

void TDqPqRdReadActor::Stop(NDqProto::StatusIds::StatusCode status, TIssues issues) {
    SRC_LOG_E("Stop read actor, status: " << NYql::NDqProto::StatusIds_StatusCode_Name(status) << ", issues: " << issues.ToOneLineString());
    Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), status));
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr& ev) {
    SRC_LOG_I("Received TEvCoordinatorResult from " << ev->Sender.ToString() << ", cookie " << ev->Cookie);
    Counters.CoordinatorChanged++;
    if (ev->Cookie != CoordinatorRequestCookie) {
        SRC_LOG_W("Ignore TEvCoordinatorResult. wrong cookie");
        return;
    }
    LastReceivedPartitionDistribution.clear();
    TMap<NActors::TActorId, TSet<ui32>> distribution;
    for (auto& p : ev->Get()->Record.GetPartitions()) {
        TActorId rowDispatcherActorId = ActorIdFromProto(p.GetActorId());
        for (auto partitionId : p.GetPartitionIds()) {
            LastReceivedPartitionDistribution[rowDispatcherActorId].insert(partitionId);
        }
    }
    ProcessState();
}

void TDqPqRdReadActor::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    SRC_LOG_D("EvNodeConnected " << ev->Get()->NodeId);
    Counters.NodeConnected++;
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TDqPqRdReadActor::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    SRC_LOG_D("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    Counters.NodeDisconnected++;
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
    // In case of row dispatcher disconnection: wait connected or SessionClosed(). TODO: Stop actor after timeout.
    // In case of row dispatcher disconnection: wait CoordinatorChanged().
    //Stop(TString{"Node disconnected, nodeId "} + ToString(ev->Get()->NodeId));
}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    SRC_LOG_D("Received TEvUndelivered, " << ev->Get()->ToString() << " from " << ev->Sender.ToString() << ", reason " << ev->Get()->Reason << ", cookie " << ev->Cookie);
    Counters.Undelivered++;
    
    auto sessionIt = Sessions.find(ev->Sender);
    if (sessionIt != Sessions.end()) {
        auto& sessionInfo = sessionIt->second;
        if (sessionInfo.EventsQueue.HandleUndelivered(ev) == NYql::NDq::TRetryEventsQueue::ESessionState::SessionClosed) {
            if (sessionInfo.Generation == ev->Cookie) {
                SRC_LOG_D("Erase session to " << ev->Sender.ToString());
                ReadActorByEventQueueId.erase(sessionInfo.EventQueueId);
                Sessions.erase(sessionIt);
                ReInit("Reset session state (by TEvUndelivered)");
                ScheduleProcessState();
            }
        }
    }

    if (CoordinatorActorId && *CoordinatorActorId == ev->Sender) {
        ReInit("TEvUndelivered to coordinator");
        ScheduleProcessState();
        return;
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("Received TEvMessageBatch from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << " generation " << ev->Cookie);
    Counters.MessageBatch++;
    auto* session = FindAndUpdateSession(ev);
    if (!session) {
        return;
    }
    auto partitionId = ev->Get()->Record.GetPartitionId();
    auto partitionIt = session->Partitions.find(partitionId);
    if (partitionIt == session->Partitions.end()) {
        SRC_LOG_E("TEvMessageBatch: wrong partition id " << partitionId);
        Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue(TStringBuilder() << LogPrefix << "No partition with id " << partitionId)});
        return;
    }
    auto& partirtion = partitionIt->second;
    Metrics.InFlyGetNextBatch->Set(0);
    ReadyBuffer.emplace(partitionId, ev->Get()->Record.MessagesSize());
    TReadyBatch& activeBatch = ReadyBuffer.back();

    auto& nextOffset = NextOffsetFromRD[partitionId];

    ui64 bytes = 0;
    for (const auto& message : ev->Get()->Record.GetMessages()) {
        const auto& offsets = message.GetOffsets();
        if (offsets.empty()) {
            Stop(NDqProto::StatusIds::INTERNAL_ERROR, {TIssue(TStringBuilder() << LogPrefix << "Got unexpected empty batch from row dispatcher")});
            return;
        }

        activeBatch.Data.emplace_back(ev->Get()->GetPayload(message.GetPayloadId()));
        bytes += activeBatch.Data.back().GetSize();

        nextOffset  = *offsets.rbegin() + 1;
        SRC_LOG_T("TEvMessageBatch NextOffset " << nextOffset);
    }
    activeBatch.UsedSpace = bytes;
    ReadyBufferSizeBytes += bytes;
    activeBatch.NextOffset = ev->Get()->Record.GetNextMessageOffset();
    partirtion.IsWaitingMessageBatch = false;
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
                *(itemPtr++) = NUdf::TUnboxedValuePod::Zero();
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
    auto str = GetInternalState();
    auto buf = TStringBuf(str);
    for (ui64 offset = 0; offset < buf.size(); offset += PrintStateToLogSplitSize) {
        SRC_LOG_I(buf.SubString(offset, PrintStateToLogSplitSize));
    }
}

TString TDqPqRdReadActor::GetInternalState() {
    TStringStream str;
    str << LogPrefix << "State: used buffer size " << ReadyBufferSizeBytes << " ready buffer event size " << ReadyBuffer.size()  << " state " << static_cast<ui64>(State) << " InFlyAsyncInputData " << InFlyAsyncInputData << "\n";
    str << "Counters: GetAsyncInputData " << Counters.GetAsyncInputData << " CoordinatorChanged " << Counters.CoordinatorChanged << " CoordinatorResult " << Counters.CoordinatorResult
        << " MessageBatch " << Counters.MessageBatch << " StartSessionAck " << Counters.StartSessionAck << " NewDataArrived " << Counters.NewDataArrived
        << " SessionError " << Counters.SessionError << " Statistics " << Counters.Statistics << " NodeDisconnected " << Counters.NodeDisconnected
        << " NodeConnected " << Counters.NodeConnected << " Undelivered " << Counters.Undelivered << " Retry " << Counters.Retry
        << " PrivateHeartbeat " << Counters.PrivateHeartbeat << " SessionClosed " << Counters.SessionClosed << " Pong " << Counters.Pong
        << " Heartbeat " << Counters.Heartbeat << " PrintState " << Counters.PrintState << " ProcessState " << Counters.ProcessState
        << " NotifyCA " << Counters.NotifyCA << "\n";
    
    for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
        str << " " << rowDispatcherActorId << " status " << static_cast<ui64>(sessionInfo.Status)
            << " is waiting ack " << sessionInfo.IsWaitingStartSessionAck << " connection id " << sessionInfo.Generation << " ";
        sessionInfo.EventsQueue.PrintInternalState(str);
        for (const auto& [partitionId, partition] : sessionInfo.Partitions) {
            const auto offsetIt = NextOffsetFromRD.find(partitionId);
            str << "   partId " << partitionId 
                << " next offset " << ((offsetIt != NextOffsetFromRD.end()) ? ToString(offsetIt->second) : TString("<empty>"))
                << " is waiting batch " << partition.IsWaitingMessageBatch
                << " has pending data " << partition.HasPendingData << "\n";
        }
        str << "\n";
    }
    return str.Str();
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvProcessState::TPtr&) {
    ProcessStateScheduled = false;
    Counters.ProcessState++;
    ProcessState();
}

void TDqPqRdReadActor::TrySendGetNextBatch(TSession& sessionInfo) {
    if (ReadyBufferSizeBytes > MaxBufferSize) {
        return;
    }
    for (auto& [partitionId, partition] : sessionInfo.Partitions) {
        if (!partition.HasPendingData) {
            continue;
        }
        Metrics.InFlyGetNextBatch->Inc();
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvGetNextBatch>();
        partition.HasPendingData = false;
        partition.IsWaitingMessageBatch = true; 
        event->Record.SetPartitionId(partitionId);
        sessionInfo.EventsQueue.Send(event.release(), sessionInfo.Generation);
    }
}

template <class TEventPtr>
TDqPqRdReadActor::TSession* TDqPqRdReadActor::FindAndUpdateSession(const TEventPtr& ev) {
    auto sessionIt = Sessions.find(ev->Sender);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore " << typeid(TEventPtr).name() << " from " << ev->Sender);
        SendNoSession(ev->Sender, ev->Cookie);
        return nullptr;
    }
    auto& session = sessionIt->second;

    if (ev->Cookie != session.Generation) {
        SRC_LOG_W("Wrong message generation (" << typeid(TEventPtr).name()  << "), sender " << ev->Sender << " cookie " << ev->Cookie << ", session generation " << session.Generation << ", send TEvNoSession");
        SendNoSession(ev->Sender, ev->Cookie);
        return nullptr;
    }
    if (!session.EventsQueue.OnEventReceived(ev)) {
        const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
        SRC_LOG_W("Ignore " << typeid(TEventPtr).name() << " from " << ev->Sender << ", wrong seq num, seqNo " << meta.GetSeqNo());
        return nullptr;
    }

    auto expectedStatus = TSession::ESessionStatus::STARTED;
    if constexpr (std::is_same_v<TEventPtr, NFq::TEvRowDispatcher::TEvStartSessionAck::TPtr>) {
        expectedStatus = TSession::ESessionStatus::WAIT_START_SESSION_ACK;
    }

    if (session.Status != expectedStatus) {
        SRC_LOG_E("Wrong " << typeid(TEventPtr).name() << " from " << ev->Sender << " session status " << static_cast<ui64>(session.Status) << " expected " << static_cast<ui64>(expectedStatus));
        return nullptr;
    }
    return &session;
}

void TDqPqRdReadActor::SendNoSession(const NActors::TActorId& recipient, ui64 cookie) {
    auto event = std::make_unique<NFq::TEvRowDispatcher::TEvNoSession>();
    Send(recipient, event.release(), 0, cookie);
}

void TDqPqRdReadActor::NotifyCA() {
    Metrics.InFlyAsyncInputData->Set(1);
    InFlyAsyncInputData = true;
    Counters.NotifyCA++;
    Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
}

void TDqPqRdReadActor::UpdateSessions() {
    SRC_LOG_I("UpdateSessions, Sessions size " << Sessions.size());

    if (LastUsedPartitionDistribution != LastReceivedPartitionDistribution) {
        SRC_LOG_I("Distribution is changed, remove sessions");
        for (auto& [rowDispatcherActorId, sessionInfo] : Sessions) {
            StopSession(sessionInfo);
            ReadActorByEventQueueId.erase(sessionInfo.EventQueueId);
        }
        Sessions.clear();
    }

    for (const auto& [rowDispatcherActorId, partitions] : LastReceivedPartitionDistribution) {
        if (Sessions.contains(rowDispatcherActorId)) {
            continue;
        }

        auto queueId = ++NextEventQueueId;
        Sessions.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(rowDispatcherActorId),
            std::forward_as_tuple(TxId, SelfId(), rowDispatcherActorId, queueId, ++NextGeneration));
        auto& session = Sessions.at(rowDispatcherActorId);
        SRC_LOG_I("Create session to " << rowDispatcherActorId << ", generation " << session.Generation);
        for (auto partitionId : partitions) {
            session.Partitions[partitionId];
        }
        ReadActorByEventQueueId[queueId] = rowDispatcherActorId;
    }
    LastUsedPartitionDistribution = LastReceivedPartitionDistribution;
}

void TDqPqRdReadActor::UpdateQueuedSize() {
    ui64 queuedBytes = 0;
    ui64 queuedRows = 0;
    for (auto& [_, sessionInfo] : Sessions) {
        queuedBytes += sessionInfo.QueuedBytes;
        queuedRows += sessionInfo.QueuedRows;
    }
    IngressStats.QueuedBytes = queuedBytes;
    IngressStats.QueuedRows = queuedRows;
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvNotifyCA::TPtr&) {
    Schedule(TDuration::Seconds(NotifyCAPeriodSec), new TEvPrivate::TEvNotifyCA());
    NotifyCA();
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
