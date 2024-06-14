#include "row_dispatcher.h"
#include "coordinator.h"
#include "leader_detector.h"

#include <ydb/core/fq/libs/config/protos/storage.pb.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

//#include <ydb/core/fq/libs/row_dispatcher/events/events.h>
#include <ydb/library/actors/core/interconnect.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/strip.h>

// #define LOG_D(s) LOG_YQ_ROW_DISPATCHER_DEBUG(LogPrefix << s)
// #define LOG_I(s) LOG_YQ_ROW_DISPATCHER_DEBUG(LogPrefix << s)


namespace NFq {

using namespace NActors;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvPqEventsReady = EvBegin,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvPqEventsReady : public TEventLocal<TEvPqEventsReady, EvPqEventsReady> {};
};



////////////////////////////////////////////////////////////////////////////////

struct TTopicEventProcessor;

class TTopicSession : public TActorBootstrapped<TTopicSession> {
private:
    const NYql::NPq::NProto::TDqPqTopicSource SourceParams;
    ui32 PartitionId;
    TMaybe<ui64> ReadOffset;
    NYdb::TDriver Driver;
    std::shared_ptr<NYdb::ICredentialsProviderFactory> CredentialsProviderFactory;

    std::unique_ptr<NYdb::NTopic::TTopicClient> TopicClient;
    std::shared_ptr<NYdb::NTopic::IReadSession> ReadSession;
    const i64 BufferSize;
    const TString LogPrefix;
    NYql::NDq::TDqAsyncStats IngressStats;

public:
    TInstant StartingMessageTimestamp;


public:
    explicit TTopicSession(
        const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
        ui32 partitionId,
        TMaybe<ui64> readOffset,
        NYdb::TDriver driver,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory);

    void Bootstrap();
    NYdb::NTopic::TTopicClientSettings GetTopicClientSettings() const;
    NYdb::NTopic::TTopicClient& GetTopicClient();
    NYdb::NTopic::TReadSessionSettings GetReadSessionSettings() const;
    NYdb::NTopic::IReadSession& GetReadSession();
    void SubscribeOnNextEvent();

    void Handle(TEvPrivate::TEvPqEventsReady::TPtr&);
    TString GetSessionId() const;
    void HandleNewEvents();

    std::optional<NYql::TIssues> ProcessDataReceivedEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event);
    std::optional<NYql::TIssues> ProcessSessionClosedEvent(NYdb::NTopic::TSessionClosedEvent& ev);
    std::optional<NYql::TIssues> ProcessStartPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event);
    std::optional<NYql::TIssues> ProcessStopPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event);

    static constexpr char ActorName[] = "YQ_ROW_DISPATCHER";

private:

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvPqEventsReady, Handle);
    )


};

TTopicSession::TTopicSession(
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
    ui32 partitionId,
    TMaybe<ui64> readOffset,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory)
    : SourceParams(sourceParams)
    , PartitionId(partitionId)
    , ReadOffset(ReadOffset)
    , Driver(driver)
    , CredentialsProviderFactory(credentialsProviderFactory)
    , BufferSize(16_MB)
    , LogPrefix("TopicSession: ")
    , StartingMessageTimestamp(TInstant::MilliSeconds(TInstant::Now().MilliSeconds())) // this field is serialized as milliseconds, so drop microseconds part to be consistent with storage
{
}

void TTopicSession::Bootstrap() {
    Become(&TTopicSession::StateFunc);
    LOG_ROW_DISPATCHER_DEBUG("id " << SelfId());
    GetReadSession();
    SubscribeOnNextEvent();
}

void TTopicSession::SubscribeOnNextEvent() {
    if (!ReadSession) {
        return;
    }

    LOG_ROW_DISPATCHER_DEBUG("SubscribeOnNextEvent");

    NActors::TActorSystem* actorSystem = NActors::TActivationContext::ActorSystem();
    ReadSession->WaitEvent().Subscribe([actorSystem, selfId = SelfId()](const auto&){
        actorSystem->Send(selfId, new TEvPrivate::TEvPqEventsReady());
    });
}


 NYdb::NTopic::TTopicClientSettings TTopicSession::GetTopicClientSettings() const {
    NYdb::NTopic::TTopicClientSettings opts;
    opts.Database(SourceParams.GetDatabase())
        .DiscoveryEndpoint(SourceParams.GetEndpoint())
        .SslCredentials(NYdb::TSslCredentials(SourceParams.GetUseSsl()))
        .CredentialsProviderFactory(CredentialsProviderFactory);

    return opts;
}

NYdb::NTopic::TTopicClient& TTopicSession::GetTopicClient() {
    if (!TopicClient) {
        TopicClient = std::make_unique<NYdb::NTopic::TTopicClient>(Driver, GetTopicClientSettings());
    }
    return *TopicClient;
}

NYdb::NTopic::TReadSessionSettings TTopicSession::GetReadSessionSettings() const {
    NYdb::NTopic::TTopicReadSettings topicReadSettings;
    topicReadSettings.Path(SourceParams.GetTopicPath());
    topicReadSettings.AppendPartitionIds(PartitionId);

    return NYdb::NTopic::TReadSessionSettings()
        .AppendTopics(topicReadSettings)
        .ConsumerName(SourceParams.GetConsumerName())
        .MaxMemoryUsageBytes(BufferSize)
        .ReadFromTimestamp(StartingMessageTimestamp);
}

NYdb::NTopic::IReadSession& TTopicSession::GetReadSession() {
    if (!ReadSession) {
        ReadSession = GetTopicClient().CreateReadSession(GetReadSessionSettings());
        LOG_ROW_DISPATCHER_DEBUG("CreateReadSession");
    }
    return *ReadSession;
}

void TTopicSession::Handle(TEvPrivate::TEvPqEventsReady::TPtr&) {
    LOG_ROW_DISPATCHER_DEBUG("TEvPqEventsReady");
    HandleNewEvents();
    SubscribeOnNextEvent();
}

void TTopicSession::HandleNewEvents() {
    if (!ReadSession) {
        return;
    }

    while (true) {
        TMaybe<NYdb::NTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
        if (!event) {
            break;
        }
        std::optional<NYql::TIssues> issues;
      //  ui32 batchItemsEstimatedCount = 0;

        if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            issues = ProcessDataReceivedEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
            issues = ProcessSessionClosedEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            issues = ProcessStartPartitionSessionEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
            issues = ProcessStopPartitionSessionEvent(*e);
        } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {

        }

         // if NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent
        // auto issues = std::visit(TTopicEventProcessor{*this, batchItemsEstimatedCount, LogPrefix}, event);
        if (issues) {
            LOG_ROW_DISPATCHER_DEBUG("Close session");
            ReadSession->Close(TDuration::Zero());
            ReadSession.reset();
            // Callbacks->OnAsyncOutputError(OutputIndex, *issues, NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
            break;
        }
    }
}

std::optional<NYql::TIssues> TTopicSession::ProcessDataReceivedEvent(NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent& event) {
    // const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
    // const auto partitionKeyStr = ToString(partitionKey);
    for (const auto& message : event.GetMessages()) {
        const TString& data = message.GetData();
        IngressStats.Bytes += data.size();
        // LWPROBE(PqReadDataReceived, TString(TStringBuilder() << Self.TxId), Self.SourceParams.GetTopicPath(), data);
        LOG_ROW_DISPATCHER_DEBUG("Data received: " << message.DebugString(true));

        if (message.GetWriteTime() < StartingMessageTimestamp) {
            LOG_ROW_DISPATCHER_DEBUG("Skip data. StartingMessageTimestamp: " << StartingMessageTimestamp << ". Write time: " << message.GetWriteTime());
            continue;
        }

        //   auto [item, size] = CreateItem(message);

        // auto& curBatch = GetActiveBatch(partitionKey, message.GetWriteTime());
        // curBatch.Data.emplace_back(std::move(item));
        // curBatch.UsedSpace += size;

        // auto& offsets = curBatch.OffsetRanges[message.GetPartitionSession()];
        // if (!offsets.empty() && offsets.back().second == message.GetOffset()) {
        //     offsets.back().second = message.GetOffset() + 1;
        // } else {
        //     offsets.emplace_back(message.GetOffset(), message.GetOffset() + 1);
        // }
    }
    return std::nullopt;
}

std::optional<NYql::TIssues> TTopicSession::ProcessSessionClosedEvent(NYdb::NTopic::TSessionClosedEvent& ev) {
    TString message = (TStringBuilder() << "Read session to topic \"" << "\" was closed");
    LOG_ROW_DISPATCHER_DEBUG(message << ": " << ev.DebugString());

    NYql::TIssues issues;
    issues.AddIssue(TStringBuilder() << "Read session to topic \"" << "\" was closed: " << ev.DebugString());
    return issues;
}


std::optional<NYql::TIssues> TTopicSession::ProcessStartPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent& event) {
    LOG_ROW_DISPATCHER_DEBUG(" StartPartitionSessionEvent received");

    TMaybe<ui64> readOffset;
    // const auto offsetIt = Self.PartitionToOffset.find(partitionKey);
    // if (offsetIt != Self.PartitionToOffset.end()) {
    //     readOffset = offsetIt->second;
    // }
    // LOG_ROW_DISPATCHER_DEBUG("SessionId: " << Self.GetSessionId() << " Key: " << partitionKeyStr << " Confirm StartPartitionSession with offset " << readOffset);
    event.Confirm(readOffset);
    return std::nullopt;
}

std::optional<NYql::TIssues> TTopicSession::ProcessStopPartitionSessionEvent(NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent& event) {
    //  const auto partitionKey = MakePartitionKey(event.GetPartitionSession());
    // const auto partitionKeyStr = ToString(partitionKey);
    LOG_ROW_DISPATCHER_DEBUG("SessionId: " << GetSessionId() << " StopPartitionSessionEvent received");
    event.Confirm();
    return std::nullopt;
}

// std::optional<NYql::TIssues> operator()(NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent& /*event*/) {
//     LOG_ROW_DISPATCHER_DEBUG("SessionId: " << Self.GetSessionId() << " EndPartitionSessionEvent received");
//     return std::nullopt;
// }

// std::optional<NYql::TIssues> operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent&) {
//     return std::nullopt;
// }

// std::optional<NYql::TIssues> operator()(NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent& /*event*/) {
//     LOG_ROW_DISPATCHER_DEBUG("SessionId: " << Self.GetSessionId()  << " PartitionSessionClosedEvent received");
//     return std::nullopt;
// }

    // TReadyBatch& GetActiveBatch(const TPartitionKey& partitionKey, TInstant time) {
    //     if (Y_UNLIKELY(Self.ReadyBuffer.empty() || Self.ReadyBuffer.back().Watermark.Defined())) {
    //         Self.ReadyBuffer.emplace(Nothing(), BatchCapacity);
    //     }

    //     TReadyBatch& activeBatch = Self.ReadyBuffer.back();

    //     if (!Self.WatermarkTracker) {
    //         // Watermark tracker disabled => there is no way more than one batch will be used
    //         return activeBatch;
    //     }

    //     const auto maybeNewWatermark = Self.WatermarkTracker->NotifyNewPartitionTime(
    //         partitionKey,
    //         time,
    //         TInstant::Now());
    //     if (!maybeNewWatermark) {
    //         // Watermark wasn't moved => use current active batch
    //         return activeBatch;
    //     }

    //     Self.PushWatermarkToReady(*maybeNewWatermark);
    //     return Self.ReadyBuffer.emplace(Nothing(), BatchCapacity); // And open new batch
    // }

    // std::pair<NUdf::TUnboxedValuePod, i64> CreateItem(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message) {
    //     const TString& data = message.GetData();

    //     i64 usedSpace = 0;
    //     NUdf::TUnboxedValuePod item;
    //     if (Self.MetadataFields.empty()) {
    //         item = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.Data(), data.Size()));
    //         usedSpace += data.Size();
    //     } else {
    //         NUdf::TUnboxedValue* itemPtr;
    //         item = Self.HolderFactory.CreateDirectArrayHolder(Self.MetadataFields.size() + 1, itemPtr);
    //         *(itemPtr++) = NKikimr::NMiniKQL::MakeString(NUdf::TStringRef(data.Data(), data.Size()));
    //         usedSpace += data.Size();

    //         for (const auto& [name, extractor] : Self.MetadataFields) {
    //             auto [ub, size] = extractor(message);
    //             *(itemPtr++) = std::move(ub);
    //             usedSpace += size;
    //         }
    //     }

    //     return std::make_pair(item, usedSpace);
    // }

TString TTopicSession::GetSessionId() const {
    return ReadSession ? ReadSession->GetSessionId() : TString{"empty"};
}


} // namespace

////////////////////////////////////////////////////////////////////////////////
    
std::unique_ptr<NActors::IActor> NewTopicSession(
    const NYql::NPq::NProto::TDqPqTopicSource& sourceParams,
    ui32 partitionId,
    TMaybe<ui64> readOffset,
    NYdb::TDriver driver,
    std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory) {
    return std::unique_ptr<NActors::IActor>(new TTopicSession(sourceParams, partitionId, readOffset, driver, credentialsProviderFactory));
}

} // namespace NFq
