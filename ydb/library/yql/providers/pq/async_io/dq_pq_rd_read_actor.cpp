#include "dq_pq_rd_read_actor.h"
#include "probes.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>

#include <yql/essentials/minikql/comp_nodes/mkql_saveload.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor_base.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
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
    }

    ~TRowDispatcherReadActorMetrics() {
        SubGroup->RemoveSubgroup("id", TxId);
    }

    TString TxId;
    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounterPtr SubGroup;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFlyGetNextBatch;
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
        TVector<TString> Data;
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
private:
    std::vector<std::tuple<TString, TPqMetaExtractor::TPqMetaExtractorLambda>> MetadataFields;
    const TString Token;
    TMaybe<NActors::TActorId> CoordinatorActorId;
    NActors::TActorId LocalRowDispatcherActorId;
    std::queue<TReadyBatch> ReadyBuffer;
    EState State = EState::INIT;
    ui64 CoordinatorRequestCookie = 0;
    TRowDispatcherReadActorMetrics Metrics;
    bool SchedulePrintStatePeriod = false;
    bool ProcessStateScheduled = false;

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
            ui64 eventQueueId)
            : RowDispatcherActorId(rowDispatcherActorId)
            , PartitionId(partitionId) {
            EventsQueue.Init(txId, selfId, selfId, eventQueueId, /* KeepAlive */ true);
            EventsQueue.OnNewRecipientId(rowDispatcherActorId);
        }

        ESessionStatus Status = ESessionStatus::NoSession;
        ui64 NextOffset = 0;
        bool IsWaitingRowDispatcherResponse = false;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        bool HasPendingData = false;
        TActorId RowDispatcherActorId;
        ui64 PartitionId;
    };
    
    TMap<ui64, SessionInfo> Sessions;
    const THolderFactory& HolderFactory;
    const i64 MaxBufferSize;
    i64 ReadyBufferSizeBytes = 0;

public:
    TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
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
    void Handle(NFq::TEvRowDispatcher::TEvStatus::TPtr& ev);

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed::TPtr&);
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
        hFunc(NFq::TEvRowDispatcher::TEvStatus, Handle);

        hFunc(NActors::TEvents::TEvPong, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvHeartbeat, Handle);
        hFunc(TEvPrivate::TEvPrintState, Handle);
        hFunc(TEvPrivate::TEvProcessState, Handle);
    })

    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override;
    void PassAway() override;
    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override;
    std::vector<ui64> GetPartitionsToRead() const;
    std::pair<NUdf::TUnboxedValuePod, i64> CreateItem(const TString& data);
    void ProcessState();
    void Stop(const TString& message);
    void StopSessions();
    void ReInit(const TString& reason);
    void PrintInternalState();
    void TrySendGetNextBatch(SessionInfo& sessionInfo);
};

TDqPqRdReadActor::TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
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
    MetadataFields.reserve(SourceParams.MetadataFieldsSize());
    TPqMetaExtractor fieldsExtractor;
    for (const auto& fieldName : SourceParams.GetMetadataFields()) {
        MetadataFields.emplace_back(fieldName, fieldsExtractor.FindExtractorLambda(fieldName));
    }

    IngressStats.Level = statsLevel;
    SRC_LOG_D("Start read actor, local row dispatcher " << LocalRowDispatcherActorId.ToString() << ", metadatafields: " << JoinSeq(',', SourceParams.GetMetadataFields()));
}

void TDqPqRdReadActor::ProcessState() {
    switch (State) {
    case EState::INIT:
        if (!ReadyBuffer.empty()) {
            return;
        }
        if (!ProcessStateScheduled) {
            ProcessStateScheduled = true;
            Schedule(TDuration::Seconds(ProcessStatePeriodSec), new TEvPrivate::TEvProcessState());
        }
        if (!CoordinatorActorId) {
            SRC_LOG_D("Send TEvCoordinatorChangesSubscribe to local row dispatcher, self id " << SelfId());
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
        SRC_LOG_D("Send TEvCoordinatorRequest to coordinator " << CoordinatorActorId->ToString() << ", partIds: " << JoinSeq(", ", partitionToRead));
        Send(
            *CoordinatorActorId,
            new NFq::TEvRowDispatcher::TEvCoordinatorRequest(SourceParams, partitionToRead),
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
            ++CoordinatorRequestCookie);
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
                    SRC_LOG_D("readOffset found" );
                    readOffset = offsetIt->second;
                }

                SRC_LOG_D("Send TEvStartSession to " << sessionInfo.RowDispatcherActorId 
                        << ", offset " << readOffset 
                        << ", partitionId " << partitionId);

                auto event = new NFq::TEvRowDispatcher::TEvStartSession(
                    SourceParams,
                    partitionId,
                    Token,
                    readOffset,
                    StartingMessageTimestamp.MilliSeconds(),
                    std::visit([](auto arg) { return ToString(arg); }, TxId));
                sessionInfo.EventsQueue.Send(event);
                sessionInfo.IsWaitingRowDispatcherResponse = true;
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
        SRC_LOG_D("Send StopSession to " << sessionInfo.RowDispatcherActorId);
        sessionInfo.EventsQueue.Send(event.release());
    }
}

// IActor & IDqComputeActorAsyncInput
void TDqPqRdReadActor::PassAway() { // Is called from Compute Actor
    SRC_LOG_D("PassAway");
    PrintInternalState();
    StopSessions();
    TActor<TDqPqRdReadActor>::PassAway();
    
    // TODO: RetryQueue::Unsubscribe()
}

i64 TDqPqRdReadActor::GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& /*watermark*/, bool&, i64 freeSpace) {
    SRC_LOG_T("GetAsyncInputData freeSpace = " << freeSpace);

    ProcessState();
    if (ReadyBuffer.empty() || !freeSpace) {
        return 0;
    }
    i64 usedSpace = 0;
    buffer.clear();
    do {
        auto& readyBatch = ReadyBuffer.front();

        for (const auto& message : readyBatch.Data) {
            auto [item, size] = CreateItem(message);
            buffer.push_back(std::move(item));
        }
        usedSpace += readyBatch.UsedSpace;
        freeSpace -= readyBatch.UsedSpace;
        TPartitionKey partitionKey{TString{}, readyBatch.PartitionId};
        PartitionToOffset[partitionKey] = readyBatch.NextOffset;
        SRC_LOG_T("NextOffset " << readyBatch.NextOffset);
        ReadyBuffer.pop();
    } while (freeSpace > 0 && !ReadyBuffer.empty());

    ReadyBufferSizeBytes -= usedSpace;
    SRC_LOG_T("Return " << buffer.RowCount() << " rows, buffer size " << ReadyBufferSizeBytes << ", free space " << freeSpace << ", result size " << usedSpace);

    if (!ReadyBuffer.empty()) {
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
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
    SRC_LOG_D("TEvStartSessionAck from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());

    ui64 partitionId = ev->Get()->Record.GetConsumer().GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvStartSessionAck from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId);
        YQL_ENSURE(State != EState::STARTED);
        return;
    }
    auto& sessionInfo = sessionIt->second;
    if (!sessionInfo.EventsQueue.OnEventReceived(ev)) {
        SRC_LOG_W("Wrong seq num ignore message, seqNo " << meta.GetSeqNo());
        return;
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvSessionError::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_D("TEvSessionError from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());

    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvSessionError from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId);
        YQL_ENSURE(State != EState::STARTED);
        return;
    }

    auto& sessionInfo = sessionIt->second;
    if (!sessionInfo.EventsQueue.OnEventReceived(ev)) {
        SRC_LOG_W("Wrong seq num ignore message, seqNo " << meta.GetSeqNo());
        return;
    }
    Stop(ev->Get()->Record.GetMessage());
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvStatus::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_D("TEvStatus from " << ev->Sender << ", offset " << ev->Get()->Record.GetNextMessageOffset() << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());

    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvStatus from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId);
        YQL_ENSURE(State != EState::STARTED);
        return;
    }
    auto& sessionInfo = sessionIt->second;

    if (!sessionInfo.EventsQueue.OnEventReceived(ev)) {
        SRC_LOG_W("Wrong seq num ignore message, seqNo " << meta.GetSeqNo());
        return;
    }

    if (ReadyBuffer.empty()) {
        TPartitionKey partitionKey{TString{}, partitionId};
        PartitionToOffset[partitionKey] = ev->Get()->Record.GetNextMessageOffset();
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("TEvNewDataArrived from " << ev->Sender << ", part id " << ev->Get()->Record.GetPartitionId() << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());

    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvNewDataArrived from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId);
        YQL_ENSURE(State != EState::STARTED);
        return;
    }

    auto& sessionInfo = sessionIt->second;
    if (!sessionInfo.EventsQueue.OnEventReceived(ev)) {
        SRC_LOG_W("Wrong seq num ignore message, seqNo " << meta.GetSeqNo());
        return;
    }
    sessionInfo.HasPendingData = true;
    TrySendGetNextBatch(sessionInfo);
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    SRC_LOG_D("TEvRetry");
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
    ui64 partitionId = ev->Get()->EventQueueId;

    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Unknown partition id " << partitionId << ", skip TEvPing");
        return;
    }
    auto& sessionInfo = sessionIt->second;
    bool needSend = sessionInfo.EventsQueue.Heartbeat();
    if (needSend) {
        sessionInfo.EventsQueue.Send(new NFq::TEvRowDispatcher::TEvHeartbeat(sessionInfo.PartitionId));
    }
}

void TDqPqRdReadActor::Handle(const NFq::TEvRowDispatcher::TEvHeartbeat::TPtr& ev) {
    SRC_LOG_T("TEvHeartbeat");
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();

    auto sessionIt = Sessions.find(ev->Get()->Record.GetPartitionId());
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvMessageBatch from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << ev->Get()->Record.GetPartitionId());
        return;
    }
    sessionIt->second.EventsQueue.OnEventReceived(ev);
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr& ev) {
    SRC_LOG_D("TEvCoordinatorChanged, new coordinator " << ev->Get()->CoordinatorActorId);

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
        Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
    }
}

void TDqPqRdReadActor::Stop(const TString& message) {
    NYql::TIssues issues;
    issues.AddIssue(NYql::TIssue{message});
    SRC_LOG_E("Stop read actor, error: " << message);
    Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::BAD_REQUEST)); // TODO: use UNAVAILABLE ?
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr& ev) {
    SRC_LOG_D("TEvCoordinatorResult from " << ev->Sender.ToString() << ", cookie " << ev->Cookie);
    if (ev->Cookie != CoordinatorRequestCookie) {
        SRC_LOG_W("Ignore TEvCoordinatorResult. wrong cookie");
        return;
    }
    for (auto& p : ev->Get()->Record.GetPartitions()) {
        TActorId rowDispatcherActorId = ActorIdFromProto(p.GetActorId());
        SRC_LOG_D("   rowDispatcherActorId:" << rowDispatcherActorId);

        for (auto partitionId : p.GetPartitionId()) {
            SRC_LOG_D("   partitionId:" << partitionId);
            if (!Sessions.contains(partitionId)) { // TODO
                Sessions.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(partitionId),
                    std::forward_as_tuple(TxId, SelfId(), rowDispatcherActorId, partitionId, partitionId));
            }
        }
    }
    ProcessState();
}

void TDqPqRdReadActor::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    SRC_LOG_D("EvNodeConnected " << ev->Get()->NodeId);
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TDqPqRdReadActor::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    SRC_LOG_D("TEvNodeDisconnected, node id " << ev->Get()->NodeId);
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
    // In case of row dispatcher disconnection: wait connected or SessionClosed(). TODO: Stop actor after timeout.
    // In case of row dispatcher disconnection: wait CoordinatorChanged().
    //Stop(TString{"Node disconnected, nodeId "} + ToString(ev->Get()->NodeId));
}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    SRC_LOG_D("TEvUndelivered,  " << ev->Get()->ToString() << " from " << ev->Sender.ToString());
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleUndelivered(ev);
    }

    if (CoordinatorActorId && *CoordinatorActorId == ev->Sender) {
        ReInit("TEvUndelivered to coordinator");
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr& ev) {
    const NYql::NDqProto::TMessageTransportMeta& meta = ev->Get()->Record.GetTransportMeta();
    SRC_LOG_T("TEvMessageBatch from " << ev->Sender << ", seqNo " << meta.GetSeqNo() << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo());
    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    if (sessionIt == Sessions.end()) {
        SRC_LOG_W("Ignore TEvMessageBatch from " << ev->Sender << ", seqNo " << meta.GetSeqNo() 
            << ", ConfirmedSeqNo " << meta.GetConfirmedSeqNo() << ", PartitionId " << partitionId);
        YQL_ENSURE(State != EState::STARTED);
        return;
    }

    Metrics.InFlyGetNextBatch->Set(0);
    auto& sessionInfo = sessionIt->second;
    if (!sessionInfo.EventsQueue.OnEventReceived(ev)) {
        SRC_LOG_W("Wrong seq num ignore message, seqNo " << meta.GetSeqNo());
        return;
    }
    ReadyBuffer.emplace(partitionId, ev->Get()->Record.MessagesSize());
    TReadyBatch& activeBatch = ReadyBuffer.back();

    ui64 bytes = 0;
    for (const auto& message : ev->Get()->Record.GetMessages()) {
        SRC_LOG_T("Json: " << message.GetJson());    
        activeBatch.Data.emplace_back(message.GetJson());
        sessionInfo.NextOffset = message.GetOffset() + 1;
        bytes += message.GetJson().size();
        SRC_LOG_T("TEvMessageBatch NextOffset " << sessionInfo.NextOffset);
    }
    activeBatch.UsedSpace = bytes;
    ReadyBufferSizeBytes += bytes;
    IngressStats.Bytes += bytes;
    IngressStats.Chunks++;
    activeBatch.NextOffset = ev->Get()->Record.GetNextMessageOffset();
    Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
}

std::pair<NUdf::TUnboxedValuePod, i64> TDqPqRdReadActor::CreateItem(const TString& data) {
    i64 usedSpace = 0;
    NUdf::TUnboxedValuePod item;
    if (MetadataFields.empty()) {
        item = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
        usedSpace += data.size();
        return std::make_pair(item, usedSpace);
    } 

    NUdf::TUnboxedValue* itemPtr;
    item = HolderFactory.CreateDirectArrayHolder(MetadataFields.size() + 1, itemPtr);
    *(itemPtr++) = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.data(), data.size()));
    usedSpace += data.size();

    for ([[maybe_unused]] const auto& [name, extractor] : MetadataFields) {
        auto ub = NYql::NUdf::TUnboxedValuePod(0);  // TODO: use real values
        *(itemPtr++) = std::move(ub);
    }
    return std::make_pair(item, usedSpace);
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvSessionClosed::TPtr& ev) {
    ReInit(TStringBuilder() << "Session closed, event queue id " << ev->Get()->EventQueueId);
}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvPong::TPtr& ev) {
    SRC_LOG_T("TEvPong from " << ev->Sender);
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvPrintState::TPtr&) {
    Schedule(TDuration::Seconds(PrintStatePeriodSec), new TEvPrivate::TEvPrintState());
    PrintInternalState();
}

void TDqPqRdReadActor::PrintInternalState() {
    TStringStream str;
    str << "State:\n";
    for (auto& [partitionId, sessionInfo] : Sessions) {
        
        str << "   partId " << partitionId << " status " << static_cast<ui64>(sessionInfo.Status)
            << " next offset " << sessionInfo.NextOffset << " is waiting " << sessionInfo.IsWaitingRowDispatcherResponse
            << " has pending data " << sessionInfo.HasPendingData << " ";
        sessionInfo.EventsQueue.PrintInternalState(str);
    }
    SRC_LOG_I(str.Str());
}

void TDqPqRdReadActor::Handle(TEvPrivate::TEvProcessState::TPtr&) {
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
    event->Record.SetPartitionId(sessionInfo.PartitionId);
    sessionInfo.EventsQueue.Send(event.release());
}

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqPqRdReadActor(
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
