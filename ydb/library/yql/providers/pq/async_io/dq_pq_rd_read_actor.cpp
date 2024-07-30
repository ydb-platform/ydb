#include "dq_pq_rd_read_actor.h"
#include "dq_pq_rd_session.h"
#include "probes.h"

#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/dq/actors/compute/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>

#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_meta_extractor.h>
#include <ydb/library/yql/providers/pq/common/pq_meta_fields.h>
#include <ydb/library/yql/providers/pq/proto/dq_io_state.pb.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>
//#include <ydb/core/fq/libs/row_dispatcher/leader_detector.h>
#include <ydb/core/fq/libs/events/events.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
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

constexpr ui32 StateVersion = 1;

namespace {

LWTRACE_USING(DQ_PQ_PROVIDER);

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvSourceDataReady = EvBegin,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvSourceDataReady : public TEventLocal<TEvSourceDataReady, EvSourceDataReady> {};
};

} // namespace

class TDqPqRdReadActor : public NActors::TActorBootstrapped<TDqPqRdReadActor>, public IDqComputeActorAsyncInput, public NYql::NDq::TRetryEventsQueue::ICallbacks {
public:
    using TPartitionKey = std::pair<TString, ui64>; // Cluster, partition id.
    using TDebugOffsets = TMaybe<std::pair<ui64, ui64>>;

private:
 //   NKikimr::NMiniKQL::TScopedAlloc Alloc;
    const ui64 InputIndex;
    TDqAsyncStats IngressStats;
    const TTxId TxId;
    [[maybe_unused]] const THolderFactory& HolderFactory;
    const TString LogPrefix;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;
    const NPq::NProto::TDqPqTopicSource SourceParams;
    const NPq::NProto::TDqReadTaskParams ReadParams;
    NThreading::TFuture<void> EventFuture;
    THashMap<TPartitionKey, ui64> PartitionToOffset; // {cluster, partition} -> offset of next event.
    TInstant StartingMessageTimestamp;
    const NActors::TActorId ComputeActorId;
    std::queue<std::pair<ui64, NYdb::NTopic::TDeferredCommit>> DeferredCommits;
    NYdb::NTopic::TDeferredCommit CurrentDeferredCommit;
    std::vector<std::tuple<TString, TPqMetaExtractor::TPqMetaExtractorLambda>> MetadataFields;
    TMaybe<TDqSourceWatermarkTracker<TPartitionKey>> WatermarkTracker;
    TMaybe<TInstant> NextIdlenesCheckAt;
    NKikimr::TYdbCredentialsProviderFactory CredentialsProviderFactory2;
    const TString Token;
    bool AddBearerToToken;
    TMaybe<NActors::TActorId> CoordinatorActorId;
   // TMap<ui64, NActors::TActorId> RowDispatcherByPartitionId;


    struct SessionInfo {
        enum class ESessionStatus {
            NoSession,
            Started,
        };
        SessionInfo(
            TDqPqRdReadActor* ptr,
            const TTxId& txId,
            const NActors::TActorId selfId,
            TActorId rowDispatcherActorId,
            ui64 eventQueueId)
            : EventsQueue(ptr)
            , RowDispatcherActorId(rowDispatcherActorId) {
            EventsQueue.Init(txId, selfId, selfId, eventQueueId, /* KeepAlive */ true);
            EventsQueue.OnNewRecipientId(rowDispatcherActorId);
        }

        ESessionStatus Status = ESessionStatus::NoSession;
        ui64 LastOffset = 0;
        bool IsWaitingRowDispatcherResponse = false;
        TVector<TString> Data;
        NYql::NDq::TRetryEventsQueue EventsQueue;
        bool NewDataArrived = false;
        TActorId RowDispatcherActorId;
    };
    
    TMap<ui64, SessionInfo> Sessions;

public:
    TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        NPq::NProto::TDqReadTaskParams&& readParams,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const NActors::TActorId& computeActorId,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory2,
        const TString& token,
        bool addBearerToToken);

    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvAck::TPtr &ev);
    void Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr &ev);
    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev);
    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev);
    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr&);
    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr&);
    
    void SessionClosed(ui64 eventQueueId) override;

    STRICT_STFUNC(
        StateFunc, {
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorChanged, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvCoordinatorResult, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvNewDataArrived, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvMessageBatch, Handle);
        hFunc(NFq::TEvRowDispatcher::TEvAck, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvPing, Handle);

        // hFunc(NActors::TEvents::TEvWakeup, Handle)
    })
    static constexpr char ActorName[] = "DQ_PQ_READ_ACTOR";

    void Bootstrap();
    void SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) override;
    void LoadState(const TSourceState& state) override;
    void CommitState(const NDqProto::TCheckpoint& checkpoint) override;
    ui64 GetInputIndex() const override;
    const TDqAsyncStats& GetIngressStats() const override;
    void PassAway() override;
    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) override;
    std::vector<ui64> GetPartitionsToRead() const;
    bool MaybeReturnReadyBatch(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, i64& usedSpace);
    std::pair<NUdf::TUnboxedValuePod, i64> CreateItem(const TString& data);
    void ProcessState();
    void Stop(const TString& message);
};

TDqPqRdReadActor::TDqPqRdReadActor(
        ui64 inputIndex,
        TCollectStatsLevel statsLevel,
        const TTxId& txId,
        ui64 taskId,
        const THolderFactory& holderFactory,
        NPq::NProto::TDqPqTopicSource&& sourceParams,
        NPq::NProto::TDqReadTaskParams&& readParams,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory,
        const NActors::TActorId& computeActorId,
        const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory2,
        const TString& token,
        bool addBearerToToken)
       // : Alloc(__LOCATION__)
        : InputIndex(inputIndex)
        , TxId(txId)
        , HolderFactory(holderFactory)
        , LogPrefix(TStringBuilder() << "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << taskId << ". PQ source. ")
        , CredentialsProviderFactory(std::move(credentialsProviderFactory))
        , SourceParams(std::move(sourceParams))
        , ReadParams(std::move(readParams))
        , StartingMessageTimestamp(TInstant::MilliSeconds(TInstant::Now().MilliSeconds())) // this field is serialized as milliseconds, so drop microseconds part to be consistent with storage
        , ComputeActorId(computeActorId)
        , CredentialsProviderFactory2(credentialsProviderFactory2)
        , Token(token)
        , AddBearerToToken(addBearerToToken)
{
    MetadataFields.reserve(SourceParams.MetadataFieldsSize());
    TPqMetaExtractor fieldsExtractor;
    for (const auto& fieldName : SourceParams.GetMetadataFields()) {
        MetadataFields.emplace_back(fieldName, fieldsExtractor.FindExtractorLambda(fieldName));
    }

    IngressStats.Level = statsLevel;
    SRC_LOG_D("TDqPqRdReadActor");
}

void TDqPqRdReadActor::ProcessState() {
    if (!CoordinatorActorId) {
        Send(NFq::RowDispatcherServiceActorId(), new NFq::TEvRowDispatcher::TEvRowDispatcherRequest());
        return;
    }
    if (Sessions.empty()) {
        Send(*CoordinatorActorId, new NFq::TEvRowDispatcher::TEvCoordinatorRequest(SourceParams, GetPartitionsToRead()));
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

            SRC_LOG_D("readOffset " << readOffset << " partitionId " << partitionId );

            auto event = new NFq::TEvRowDispatcher::TEvAddConsumer(
                SourceParams,
                partitionId,
                Token,
                AddBearerToToken,
                readOffset,
                StartingMessageTimestamp.MilliSeconds());
            sessionInfo.EventsQueue.Send(event);
            sessionInfo.IsWaitingRowDispatcherResponse = true;
            sessionInfo.Status = SessionInfo::ESessionStatus::Started;
        }
    }
}

void TDqPqRdReadActor::Bootstrap() {
    Become(&TDqPqRdReadActor::StateFunc);
    SRC_LOG_D("TDqPqRdReadActor::Bootstrap");
   // ProcessState();
}

void TDqPqRdReadActor::SaveState(const NDqProto::TCheckpoint& checkpoint, TSourceState& state) {

    SRC_LOG_D("TDqPqRdReadActor::SaveState");
    NPq::NProto::TDqPqTopicSourceState stateProto;

    NPq::NProto::TDqPqTopicSourceState::TTopicDescription* topic = stateProto.AddTopics();
    topic->SetDatabaseId(SourceParams.GetDatabaseId());
    topic->SetEndpoint(SourceParams.GetEndpoint());
    topic->SetDatabase(SourceParams.GetDatabase());
    topic->SetTopicPath(SourceParams.GetTopicPath());

    for (const auto& [clusterAndPartition, offset] : PartitionToOffset) {
        const auto& [cluster, partition] = clusterAndPartition;
        NPq::NProto::TDqPqTopicSourceState::TPartitionReadState* partitionState = stateProto.AddPartitions();
        partitionState->SetTopicIndex(0); // Now we are supporting only one topic per source.
        partitionState->SetCluster(cluster);
        partitionState->SetPartition(partition);
        partitionState->SetOffset(offset + 1);
        SRC_LOG_D("TDqPqRdReadActor::SaveState offset " << offset);
    }

    stateProto.SetStartingMessageTimestampMs(StartingMessageTimestamp.MilliSeconds());
    stateProto.SetIngressBytes(IngressStats.Bytes);

    TString stateBlob;
    YQL_ENSURE(stateProto.SerializeToString(&stateBlob));

    state.Data.emplace_back(stateBlob, StateVersion);

    DeferredCommits.emplace(checkpoint.GetId(), std::move(CurrentDeferredCommit));
    CurrentDeferredCommit = NYdb::NTopic::TDeferredCommit();
}

void TDqPqRdReadActor::LoadState(const TSourceState& state) {
    TInstant minStartingMessageTs = state.DataSize() ? TInstant::Max() : StartingMessageTimestamp;
    ui64 ingressBytes = 0;
    for (const auto& data : state.Data) {
        if (data.Version == StateVersion) { // Current version
        NPq::NProto::TDqPqTopicSourceState stateProto;
            YQL_ENSURE(stateProto.ParseFromString(data.Blob), "Serialized state is corrupted");
            YQL_ENSURE(stateProto.TopicsSize() == 1, "One topic per source is expected");
            PartitionToOffset.reserve(PartitionToOffset.size() + stateProto.PartitionsSize());
            for (const NPq::NProto::TDqPqTopicSourceState::TPartitionReadState& partitionProto : stateProto.GetPartitions()) {
                ui64& offset = PartitionToOffset[TPartitionKey{partitionProto.GetCluster(), partitionProto.GetPartition()}];
                if (offset) {
                    offset = Min(offset, partitionProto.GetOffset());
                } else {
                    offset = partitionProto.GetOffset();
                }
            }
            minStartingMessageTs = Min(minStartingMessageTs, TInstant::MilliSeconds(stateProto.GetStartingMessageTimestampMs()));
            ingressBytes += stateProto.GetIngressBytes();
        } else {
            ythrow yexception() << "Invalid state version " << data.Version;
        }
    }
    for (const auto& [key, value] : PartitionToOffset) {
        SRC_LOG_D("SessionId: " << " Restoring offset: cluster " << key.first << ", partition id " << key.second << ", offset: " << value);
    }
    StartingMessageTimestamp = minStartingMessageTs;
    IngressStats.Bytes += ingressBytes;
    IngressStats.Chunks++;
}

void TDqPqRdReadActor::CommitState(const NDqProto::TCheckpoint& checkpoint) {
    const auto checkpointId = checkpoint.GetId();
    while (!DeferredCommits.empty() && DeferredCommits.front().first <= checkpointId) {
        auto& deferredCommit = DeferredCommits.front().second;
        deferredCommit.Commit();
        DeferredCommits.pop();
    }
}

ui64 TDqPqRdReadActor::GetInputIndex() const {
    return InputIndex;
}

const TDqAsyncStats& TDqPqRdReadActor::GetIngressStats() const {
    return IngressStats;
}

// IActor & IDqComputeActorAsyncInput
void TDqPqRdReadActor::PassAway() { // Is called from Compute Actor
    SRC_LOG_D("PassAway");

    for (auto& [partitionId, sessionInfo] : Sessions) {
        if (sessionInfo.Status == SessionInfo::ESessionStatus::NoSession) {
            continue;
        }

        //SRC_LOG_D("Send TEvStopSession to " << sessionInfo.RowDispatcherActorId);
        auto event = std::make_unique<NFq::TEvRowDispatcher::TEvStopSession>();
        event->Record.MutableSource()->CopyFrom(SourceParams);
        event->Record.SetPartitionId(partitionId);
        //Send(sessionInfo.RowDispatcherActorId, event.release());
        sessionInfo.EventsQueue.Send(event.release());
    }
    TActorBootstrapped<TDqPqRdReadActor>::PassAway();
}

i64 TDqPqRdReadActor::GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& watermark, bool&, i64 freeSpace) {
    SRC_LOG_D("SessionId: " << " GetAsyncInputData freeSpace = " << freeSpace);
    ProcessState();

    i64 usedSpace = 0;
    if (MaybeReturnReadyBatch(buffer, watermark, usedSpace)) {
        return usedSpace;
    }
    return 0;
}

bool TDqPqRdReadActor::MaybeReturnReadyBatch(NKikimr::NMiniKQL::TUnboxedValueBatch& buffer, TMaybe<TInstant>& /*watermark*/, i64& /*usedSpace*/) {

    buffer.clear();
    for (auto& [partitionId, sessionInfo] : Sessions) {
        if (sessionInfo.Data.empty())
            continue;
        for (const auto& blob : sessionInfo.Data) {
            auto [value, size] = CreateItem(blob);
            buffer.push_back(value);
            //std::move(sessionInfo.Data.begin(), sessionInfo.Data.end(), std::back_inserter(buffer));
        }
        sessionInfo.Data.clear();
        TPartitionKey partitionKey{TString{}, partitionId};
        PartitionToOffset[partitionKey] = sessionInfo.LastOffset;
        return true;

      //  usedSpace = readyBatch.UsedSpace;
      
    }
    return false;

//     if (ReadyBuffer.empty()) {
//         SubscribeOnNextEvent();
//         return false;
//     }

//     auto& readyBatch = ReadyBuffer.front();
//     buffer.clear();
//     std::move(readyBatch.Data.begin(), readyBatch.Data.end(), std::back_inserter(buffer));
//    // watermark = readyBatch.Watermark;
//     usedSpace = readyBatch.UsedSpace;

//     for (const auto& [PartitionSession, ranges] : readyBatch.OffsetRanges) {
//         for (const auto& [start, end] : ranges) {
//             CurrentDeferredCommit.Add(PartitionSession, start, end);
//         }
//         PartitionToOffset[MakePartitionKey(PartitionSession)] = ranges.back().second;
//     }

//     ReadyBuffer.pop();

//     if (ReadyBuffer.empty()) {
//         SubscribeOnNextEvent();
//     } else {
//         Send(SelfId(), new TEvPrivate::TEvSourceDataReady());
//     }

//     SRC_LOG_T("SessionId: " << GetSessionId() << " Return ready batch."
//         << " DataCount = " << buffer.RowCount()
//        // << " Watermark = " << (watermark ? ToString(*watermark) : "none")
//         << " Used space = " << usedSpace);
//     return true;
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

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvAck::TPtr &ev) {
    SRC_LOG_D("TEvAck " << ev->Sender);

    //  TODO 
    ui64 partitionId = ev->Get()->Record.GetConsumer().GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    YQL_ENSURE(sessionIt != Sessions.end(), "Unknown partition id");
    auto& sessionInfo = sessionIt->second;
    if (!sessionInfo.EventsQueue.OnEventReceived(ev)) {
        SRC_LOG_D("TEvAck failed");
    }
    sessionInfo.EventsQueue.ChangeRecipientId(ev->Sender);
    //sessionInfo.EventsQueue.Send(new NFq::TEvRowDispatcher::TEvGetNextBatch());
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvNewDataArrived::TPtr &ev) {
    SRC_LOG_D("TEvNewDataArrived");
    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    auto sessionIt = Sessions.find(partitionId);
    YQL_ENSURE(sessionIt != Sessions.end(), "Unknown partition id");
    auto& sessionInfo = sessionIt->second;
    sessionInfo.NewDataArrived = true;
    sessionInfo.EventsQueue.Send(new NFq::TEvRowDispatcher::TEvGetNextBatch());
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ev) {
    SRC_LOG_D("TEvRetry");
    ui64 partitionId = ev->Get()->EventQueueId;

    auto sessionIt = Sessions.find(partitionId);
    YQL_ENSURE(sessionIt != Sessions.end(), "Unknown partition id");
    sessionIt->second.EventsQueue.Retry();
}

void TDqPqRdReadActor::Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr& ev) {
    SRC_LOG_D("TEvPing");
    ui64 partitionId = ev->Get()->EventQueueId;

    auto sessionIt = Sessions.find(partitionId);
    YQL_ENSURE(sessionIt != Sessions.end(), "Unknown partition id");
    sessionIt->second.EventsQueue.Ping();
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorChanged::TPtr &ev) {
    SRC_LOG_D("TEvCoordinatorChanged = " << ev->Get()->CoordinatorActorId);

    if (CoordinatorActorId
        && CoordinatorActorId != ev->Get()->CoordinatorActorId)  
    {
        SRC_LOG_W("Coordinator changed, pass away");
        Stop("Coordinator changed, pass away");
        // TODO
        return;
    }

    CoordinatorActorId = ev->Get()->CoordinatorActorId;
    ProcessState();
}

void TDqPqRdReadActor::Stop(const TString& message) {
    NYql::TIssues issues;
    issues.AddIssue(NYql::TIssue{message});
    Send(ComputeActorId, new TEvAsyncInputError(InputIndex, issues, NYql::NDqProto::StatusIds::UNAVAILABLE));
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvCoordinatorResult::TPtr &ev) {
    SRC_LOG_D("TEvCoordinatorResult:");
    for (auto& p : ev->Get()->Record.GetPartitions()) {
        TActorId rowDispatcherActorId = ActorIdFromProto(p.GetActorId());
        SRC_LOG_D("   rowDispatcherActorId:" << rowDispatcherActorId);

        for (auto partitionId : p.GetPartitionId()) {
            SRC_LOG_D("   partitionId:" << partitionId);
            if (!Sessions.contains(partitionId)) { // TODO
                Sessions.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(partitionId),
                    std::forward_as_tuple(this, TxId, SelfId(), rowDispatcherActorId, partitionId));
            }
        }
    }
    ProcessState();
}

void TDqPqRdReadActor::HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
    SRC_LOG_D("EvNodeConnected " << ev->Get()->NodeId);
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }
}

void TDqPqRdReadActor::HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
    SRC_LOG_D("TEvNodeDisconnected " << ev->Get()->NodeId);
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }
    Stop("TEvNodeDisconnected"); // TODO

}

void TDqPqRdReadActor::Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
    SRC_LOG_D("TEvUndelivered, ev: " << ev->Get()->ToString());
    for (auto& [partitionId, sessionInfo] : Sessions) {
        sessionInfo.EventsQueue.HandleUndelivered(ev);
    }
}

void TDqPqRdReadActor::Handle(NFq::TEvRowDispatcher::TEvMessageBatch::TPtr &ev) {
    SRC_LOG_D("TEvMessageBatch, ev: " << ev->Sender);
    ui64 partitionId = ev->Get()->Record.GetPartitionId();
    YQL_ENSURE(Sessions.count(partitionId), "Unknown partition id");
    auto it = Sessions.find(partitionId);
    if (it == Sessions.end()) {
        Stop("Wrong session data");
        
        return;
    }
    auto& sessionInfo = it->second;
    if (!sessionInfo.EventsQueue.OnEventReceived(ev)) {
        SRC_LOG_D("OnEventReceived failed ");
        return;
    }
    for (const auto& message : ev->Get()->Record.GetMessages()) {
        SRC_LOG_D("Json: " << message.GetJson());    
        sessionInfo.Data.emplace_back(message.GetJson());
        sessionInfo.LastOffset = message.GetOffset();
    }

    Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
}
std::pair<NUdf::TUnboxedValuePod, i64> TDqPqRdReadActor::CreateItem(const TString& data) {
    i64 usedSpace = 0;
    NUdf::TUnboxedValuePod item;
    if (MetadataFields.empty()) {
        item = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.Data(), data.Size()));
        usedSpace += data.Size();
    } else {
  /*      NUdf::TUnboxedValue* itemPtr;
        item = HolderFactory.CreateDirectArrayHolder(MetadataFields.size() + 1, itemPtr);
        *(itemPtr++) = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.Data(), data.Size()));
        usedSpace += data.Size();

        for (const auto& [name, extractor] : MetadataFields) {
            auto [ub, size] = extractor(data);
            *(itemPtr++) = std::move(ub);
            usedSpace += size;
        }*/
    }

    return std::make_pair(item, usedSpace);
}

void TDqPqRdReadActor::SessionClosed(ui64 eventQueueId) {
    SRC_LOG_D("Session closed to " << eventQueueId);
    Stop("SessionClosed");
}

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> CreateDqPqRdReadActor(
    NPq::NProto::TDqPqTopicSource&& settings,
    ui64 inputIndex,
    TCollectStatsLevel statsLevel,
    TTxId txId,
    ui64 taskId,
    const THashMap<TString, TString>& secureParams,
    const THashMap<TString, TString>& taskParams,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NActors::TActorId& computeActorId,
    const NKikimr::NMiniKQL::THolderFactory& holderFactory,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory)
{
    auto taskParamsIt = taskParams.find("pq");
    YQL_ENSURE(taskParamsIt != taskParams.end(), "Failed to get pq task params");

    NPq::NProto::TDqReadTaskParams readTaskParamsMsg;
    YQL_ENSURE(readTaskParamsMsg.ParseFromString(taskParamsIt->second), "Failed to parse DqPqRead task params");

    const TString& tokenName = settings.GetToken().GetName();
    const TString token = secureParams.Value(tokenName, TString());
    const bool addBearerToToken = settings.GetAddBearerToToken();

    TDqPqRdReadActor* actor = new TDqPqRdReadActor(
        inputIndex,
        statsLevel,
        txId,
        taskId,
        holderFactory,
        std::move(settings),
        std::move(readTaskParamsMsg),
        CreateCredentialsProviderFactoryForStructuredToken(credentialsFactory, token, addBearerToToken),
        computeActorId,
        credentialsProviderFactory,
        token,
        addBearerToToken
    );

    return {actor, actor};
}

void RegisterDqPqRdReadActorFactory(
    TDqAsyncIoFactory& factory,
    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory) {
    factory.RegisterSource<NPq::NProto::TDqPqTopicSource>("PqRdSource",
        [credentialsFactory = std::move(credentialsFactory), credentialsProviderFactory = std::move(credentialsProviderFactory)](
            NPq::NProto::TDqPqTopicSource&& settings,
            IDqAsyncIoFactory::TSourceArguments&& args)
    {
        NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(DQ_PQ_PROVIDER));
        return CreateDqPqRdReadActor(
            std::move(settings),
            args.InputIndex,
            args.StatsLevel,
            args.TxId,
            args.TaskId,
            args.SecureParams,
            args.TaskParams,
            credentialsFactory,
            args.ComputeActorId,
            args.HolderFactory,
            credentialsProviderFactory);
    });
}

} // namespace NYql::NDq
