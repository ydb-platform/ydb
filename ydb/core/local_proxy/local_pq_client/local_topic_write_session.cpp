#include "local_topic_write_session.h"
#include "local_topic_client_helpers.h"
#include "local_topic_io_session_common.h"

#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/services/persqueue_v1/actors/write_session_actor.h>

#include <library/cpp/protobuf/interop/cast.h>

namespace NKikimr::NKqp {

namespace {

using namespace NGRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

class TLocalTopicWriteSessionActor final
    : public TLocalTopicIoSessionActor<TLocalTopicWriteSessionActor, Ydb::Topic::StreamWriteMessage::FromClient, Ydb::Topic::StreamWriteMessage::FromServer, TWriteSessionEvent::TEvent, TWriterCounters>
    , private TContinuationTokenIssuer
{
    using TBase = TLocalTopicIoSessionActor<TLocalTopicWriteSessionActor, Ydb::Topic::StreamWriteMessage::FromClient, Ydb::Topic::StreamWriteMessage::FromServer, TWriteSessionEvent::TEvent, TWriterCounters>;

    struct TEvWriteSession {
        enum EEv {
            EvWriteMessage = TEvPrivate::EvEnd,
            EvGetInitSeqNo,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
    };

    struct TWriteSettings {
        TString ProducerId;
        TString MessageGroupId;
        std::optional<ui32> PartitionId;
        std::unordered_map<std::string, std::string> Meta;
    };

public:
    struct TSessionEvents final : public TBase::TSessionEvents {
        struct TEvWriteMessage : public TEventLocal<TEvWriteMessage, TEvWriteSession::EvWriteMessage> {
            TEvWriteMessage(TContinuationToken&& continuationToken, TWriteMessage&& message)
                : ContinuationToken(std::move(continuationToken))
                , Message(std::move(message))
                , Data(Message.Data)
            {}

            TContinuationToken ContinuationToken;
            TWriteMessage Message;
            TString Data;
        };

        struct TEvGetInitSeqNo : public TEventLocal<TEvGetInitSeqNo, TEvWriteSession::EvGetInitSeqNo> {
            explicit TEvGetInitSeqNo(NThreading::TPromise<uint64_t> seqNoPromise)
                : SeqNoPromise(std::move(seqNoPromise))
            {}

            NThreading::TPromise<uint64_t> SeqNoPromise;
        };
    };

    TLocalTopicWriteSessionActor(const TSettings& actorSettings, const TWriteSessionSettings& sessionSettings)
        : TBase(__func__, actorSettings, TString(sessionSettings.Path_), sessionSettings.MaxMemoryUsage_)
        , Settings(sessionSettings)
        , WriteSettings(GetWriteSettings(sessionSettings))
        , MaxInflightCount(sessionSettings.MaxInflightCount_)
    {
        Y_VALIDATE(MaxInflightCount > 0, "MaxInflightCount must be greater than 0");
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TSessionEvents::TEvWriteMessage, Handle);
        hFunc(TSessionEvents::TEvGetInitSeqNo, Handle);
        hFunc(TSessionEvents::TEvExtractReadyEvents, TBase::Handle);
        hFunc(TSessionEvents::TEvEventsConsumed, TBase::Handle);
        hFunc(TSessionEvents::TEvSessionFinished, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvActorAttached, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvReadRequest, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvWriteRequest, TBase::Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvFinishRequest, TBase::Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
        sFunc(TEvents::TEvWakeup, HandleWakeup);
    );

protected:
    void StartSession() final {
        auto ev = CreateRpcBiStreamingEvent<TEvStreamTopicWriteRequest>(Settings, "StreamWrite", NJaegerTracing::ERequestType::TOPIC_STREAMWRITE);
        Send(NGRpcProxy::V1::GetPQWriteServiceActorID(), ev.release(), IEventHandle::FlagTrackDelivery);
    }

    void SendInitMessage() final {
        LOG_I("Sending init message"
            << ", ProducerId: " << WriteSettings.ProducerId
            << ", MessageGroupId: " << WriteSettings.MessageGroupId
            << ", PartitionId: " << (WriteSettings.PartitionId ? ToString(*WriteSettings.PartitionId) : "null"));

        TRpcIn message;

        auto& initRequest = *message.mutable_init_request();
        initRequest.set_path(Topic);
        initRequest.set_producer_id(WriteSettings.ProducerId);
        initRequest.mutable_write_session_meta()->insert(WriteSettings.Meta.begin(), WriteSettings.Meta.end());

        if (const auto partition = WriteSettings.PartitionId) {
            initRequest.set_partition_id(*partition);
        } else if (const auto& group = WriteSettings.MessageGroupId) {
            initRequest.set_message_group_id(group);
        }

        AddSessionEvent(std::move(message));
    }

    void HandleRpcMessage(TRpcOut& message) final {
        switch (message.server_message_case()) {
            case Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse:
                ComputeSessionMessage(message.init_response());
                break;
            case Ydb::Topic::StreamWriteMessage::FromServer::kWriteResponse:
                ComputeSessionMessage(message.write_response());
                break;
            case Ydb::Topic::StreamWriteMessage::FromServer::kUpdateTokenResponse:
                TBase::ComputeSessionMessage(message.update_token_response());
                break;
            case Ydb::Topic::StreamWriteMessage::FromServer::SERVER_MESSAGE_NOT_SET:
                CloseSession(EStatus::INTERNAL_ERROR, "Unknown server message");
                break;
        }
    }

private:
    static TWriteSettings GetWriteSettings(const TWriteSessionSettings& sessionSettings) {
        TWriteSettings settings;
        settings.ProducerId = sessionSettings.ProducerId_;
        settings.Meta = sessionSettings.Meta_.Fields;

        if (sessionSettings.DeduplicationEnabled_ && !settings.ProducerId) {
            settings.ProducerId = CreateGuidAsString();
        }

        settings.MessageGroupId = sessionSettings.MessageGroupId_;
        settings.PartitionId = sessionSettings.PartitionId_;
        Y_VALIDATE(!settings.MessageGroupId || !settings.PartitionId, "MessageGroupId and PartitionId cannot be set at the same time");

        return settings;
    }

    void SendInitSeqNo() {
        if (SeqNoPromise && !SeqNoPromise->IsReady() && InitSeqNo) {
            SeqNoPromise->SetValue(*InitSeqNo);
        }
    }

    // Events from TLocalTopicWriteSession

    void Handle(TSessionEvents::TEvWriteMessage::TPtr& ev) {
        auto& message = ev->Get()->Message;
        auto& data = ev->Get()->Data;
        const auto seqNo = message.SeqNo_.value_or(MessageSeqNo++);
        const auto size = data.size();
        LOG_T("Got write message event with seq no: " << seqNo << " and size: " << size);

        InflightMemory += size;
        Y_VALIDATE(InflightMessages.emplace(seqNo, size).second, "Got duplicated message seq no: " << seqNo);
        Counters->BytesInflightTotal->Add(size);

        ContinuationEventInflight = false;
        AddContinuationEvent();

        TRpcIn rpcMessage;

        auto& writeRequest = *rpcMessage.mutable_write_request();
        writeRequest.set_codec(static_cast<i32>(message.Codec.value_or(ECodec::RAW)));

        auto& messageData = *writeRequest.add_messages();
        messageData.set_seq_no(seqNo);
        messageData.set_data(std::move(data));
        messageData.set_uncompressed_size(message.OriginalSize);
        *messageData.mutable_created_at() = NProtoInterop::CastToProto(message.CreateTimestamp_.value_or(TInstant::Now()));

        for (auto& [key, value] : message.MessageMeta_) {
            auto& item = *messageData.add_metadata_items();
            item.set_key(std::move(key));
            item.set_value(std::move(value));
        }

        AddSessionEvent(std::move(rpcMessage));
    }

    void Handle(TSessionEvents::TEvGetInitSeqNo::TPtr& ev) {
        LOG_I("Got get init seq no event");

        Y_VALIDATE(!SeqNoPromise, "Can not handle get init seq no twice");
        SeqNoPromise = std::move(ev->Get()->SeqNoPromise);
        SendInitSeqNo();
    }

    // Events from local RPC session

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto sourceType = ev->Get()->SourceType;
        const auto reason = ev->Get()->Reason;
        Y_VALIDATE(sourceType == TEvStreamTopicWriteRequest::EventType, "Unexpected undelivered event: " << sourceType << ", reason: " << reason);

        LOG_E("PQ write service is unavailable, reason: " << reason);
        CloseSession(EStatus::INTERNAL_ERROR, "PQ write service is unavailable, please contact internal support");
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamWriteMessage::InitResponse& message) {
        LOG_I("Session initialized with id: " << message.session_id() << ", used partition: " << message.partition_id());

        if (!InitSeqNo.has_value()) {
            InitSeqNo = message.last_seq_no();
            MessageSeqNo = *InitSeqNo + 1;
            SendInitSeqNo();
        }

        SessionStartedAt = TInstant::Now();
        AddContinuationEvent();
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamWriteMessage::WriteResponse& message) {
        const auto partitionId = message.partition_id();
        LOG_T("Got write response from partition: " << partitionId);

        const auto& protoStats = message.write_statistics();
        const auto writeStats = MakeIntrusive<TWriteStat>();
        writeStats->WriteTime = NProtoInterop::CastFromProto(protoStats.persisting_time());
        writeStats->MinTimeInPartitionQueue = NProtoInterop::CastFromProto(protoStats.min_queue_wait_time());
        writeStats->MaxTimeInPartitionQueue = NProtoInterop::CastFromProto(protoStats.max_queue_wait_time());
        writeStats->PartitionQuotedTime = NProtoInterop::CastFromProto(protoStats.partition_quota_wait_time());
        writeStats->TopicQuotedTime = NProtoInterop::CastFromProto(protoStats.topic_quota_wait_time());

        TWriteSessionEvent::TAcksEvent event;
        event.Acks.reserve(message.acks_size());
        for (const auto& ackProto : message.acks()) {
            auto& ack = event.Acks.emplace_back();
            ack.SeqNo = ackProto.seq_no();
            ack.Stat = writeStats;

            const auto inflightIt = InflightMessages.find(ack.SeqNo);
            if (inflightIt != InflightMessages.end()) {
                const auto size = inflightIt->second;
                InflightMemory -= size;
                InflightMessages.erase(inflightIt);

                Counters->MessagesWritten->Inc();
                Counters->BytesWritten->Add(size);
                Counters->BytesInflightTotal->Sub(size);
            }

            switch (ackProto.message_write_status_case()) {
                case Ydb::Topic::StreamWriteMessage::WriteResponse::WriteAck::kWritten: {
                    ack.State = TWriteSessionEvent::TWriteAck::EES_WRITTEN;

                    auto& details = ack.Details.emplace();
                    details.Offset = ackProto.written().offset();
                    details.PartitionId = partitionId;
                    break;
                }
                case Ydb::Topic::StreamWriteMessage::WriteResponse::WriteAck::kSkipped: {
                    switch (ackProto.skipped().reason()) {
                        case Ydb::Topic::StreamWriteMessage::WriteResponse::WriteAck::Skipped::REASON_ALREADY_WRITTEN:
                            ack.State = TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN;
                            break;
                        default:
                            ack.State = TWriteSessionEvent::TWriteAck::EES_DISCARDED;
                            break;
                    }
                    break;
                }
                case Ydb::Topic::StreamWriteMessage::WriteResponse::WriteAck::kWrittenInTx: {
                    ack.State = TWriteSessionEvent::TWriteAck::EES_WRITTEN_IN_TX;
                    break;
                }
                case Ydb::Topic::StreamWriteMessage::WriteResponse::WriteAck::MESSAGE_WRITE_STATUS_NOT_SET: {
                    CloseSession(EStatus::INTERNAL_ERROR, "Unknown message write status");
                    break;
                }
            }
        }

        AddOutgoingEvent(std::move(event));
        AddContinuationEvent();
    }

    // Events to TLocalTopicWriteSession

    void AddContinuationEvent() {
        if (ContinuationEventInflight) {
            LOG_T("Continuation event is already inflight, skipping adding");
            return;
        }

        if (InflightMemory >= MaxMemoryUsage) {
            LOG_T("Max memory usage reached, skipping adding, InflightMemory: " << InflightMemory << ", MaxMemoryUsage: " << MaxMemoryUsage);
            return;
        }

        if (InflightMessages.size() >= MaxInflightCount) {
            LOG_T("Max inflight count reached, skipping adding, InflightMessages: " << InflightMessages.size() << ", MaxInflightCount: " << MaxInflightCount);
            return;
        }

        LOG_T("Adding continuation event, InflightMemory: " << InflightMemory << ", InflightMessages: " << InflightMessages.size());
        ContinuationEventInflight = true;
        AddOutgoingEvent(TWriteSessionEvent::TReadyToAcceptEvent(IssueContinuationToken()));
    }

    const TRequestSettings<TWriteSessionSettings> Settings;
    const TWriteSettings WriteSettings;
    const ui64 MaxInflightCount = 0;

    std::unordered_map<ui64, i64> InflightMessages;
    ui64 MessageSeqNo = 0;
    bool ContinuationEventInflight = false;

    // Events to TLocalTopicWriteSession
    std::optional<ui64> InitSeqNo;
    std::optional<NThreading::TPromise<uint64_t>> SeqNoPromise;
};

// Supposed to be used from actor system, so all blocking methods are not supported.
// Write session is not thread safe and MUST be used from single actor.
// NOTICE: data compression is not supported.
class TLocalTopicWriteSession final : public TLocalTopicSessionBase<TWriteSessionEvent::TEvent>, public IWriteSession {
    using TBase = TLocalTopicSessionBase<TWriteSessionEvent::TEvent>;
    using TWriteEvents = TLocalTopicWriteSessionActor::TSessionEvents;

public:
    TLocalTopicWriteSession(const TLocalTopicSessionSettings& localSettings, const TWriteSessionSettings& sessionSettings)
        : TBase(localSettings)
        , Counters(SetupCounters(sessionSettings))
        , DeduplicationEnabled(sessionSettings.DeduplicationEnabled_.value_or(true))
        , ValidateSeqNo(sessionSettings.ValidateSeqNo_)
    {
        ValidateSettings(sessionSettings);
        Start(sessionSettings);
    }

    ~TLocalTopicWriteSession() {
        try {
            if (!Close(TDuration::Zero()) && ActorSystem && WriteSessionActor) {
                ActorSystem->Send(WriteSessionActor, new TWriteEvents::TEvSessionFinished(/* force */ true));
            }
        } catch (...) {
            // ¯\_(ツ)_/¯
        }
    }

    NThreading::TFuture<void> WaitEvent() final {
        return TBase::WaitEvent();
    }

    std::optional<TWriteSessionEvent::TEvent> GetEvent(bool block) final {
        auto events = GetEvents(block, 1);
        if (events.empty()) {
            return std::nullopt;
        }
        return std::move(events[0]);
    }

    std::vector<TWriteSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount) final {
        Y_VALIDATE(!block, "Blocking is not supported for local topic write session");

        ExtractEvents();

        std::vector<TWriteSessionEvent::TEvent> result;
        const auto maxCount = maxEventsCount.value_or(std::numeric_limits<size_t>::max());
        result.reserve(std::min(Events.size(), maxCount));

        while (!Events.empty() && result.size() < maxCount) {
            auto& event = Events.front();
            if (std::holds_alternative<TSessionClosedEvent>(event.Event)) {
                // Skip all events after session closed event
                result.emplace_back(event.Event);
                break;
            }

            result.emplace_back(std::move(event.Event));
            Events.pop();
        }

        if (WriteSessionActor && !result.empty()) {
            ActorSystem->Send(WriteSessionActor, new TWriteEvents::TEvEventsConsumed(result.size()));
        }

        WaitEvent(); // Request next event batch

        return result;
    }

    NThreading::TFuture<uint64_t> GetInitSeqNo() final {
        Y_VALIDATE(DeduplicationEnabled, "Can not get init seq no, deduplication is not enabled");

        UseManualSeqNo();

        if (!InitSeqNoPromise) {
            InitSeqNoPromise = NThreading::NewPromise<uint64_t>();

            Y_VALIDATE(WriteSessionActor, "Can not get init seq no, session already closed");
            ActorSystem->Send(WriteSessionActor, new TWriteEvents::TEvGetInitSeqNo(*InitSeqNoPromise));
        }

        return InitSeqNoPromise->GetFuture();
    }

    void Write(TContinuationToken&& continuationToken, TWriteMessage&& message, TTransactionBase* tx) final {
        Y_VALIDATE(!tx && !message.Tx_, "Transaction is not supported for local topic write session");
        Y_VALIDATE(WriteSessionActor, "Can not write message, session already closed");

        if (message.SeqNo_) {
            UseManualSeqNo();
        } else {
            UseAutoSeqNo();
        }

        ActorSystem->Send(WriteSessionActor, new TWriteEvents::TEvWriteMessage(std::move(continuationToken), std::move(message)));
    }

    void Write(TContinuationToken&& continuationToken, std::string_view data, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) final {
        TWriteMessage message(data);
        message.SeqNo(seqNo);
        message.CreateTimestamp(createTimestamp);
        Write(std::move(continuationToken), std::move(message), nullptr);
    }

    void WriteEncoded(TContinuationToken&& continuationToken, TWriteMessage&& params, TTransactionBase* tx) final {
        Write(std::move(continuationToken), std::move(params), tx);
    }

    void WriteEncoded(TContinuationToken&& continuationToken, std::string_view data, ECodec codec, uint32_t originalSize, std::optional<uint64_t> seqNo, std::optional<TInstant> createTimestamp) final {
        auto message = TWriteMessage::CompressedMessage(data, codec, originalSize);
        message.SeqNo(seqNo);
        message.CreateTimestamp(createTimestamp);
        Write(std::move(continuationToken), std::move(message), nullptr);
    }

    bool Close(TDuration closeTimeout) final {
        Y_VALIDATE(!closeTimeout, "Timeout on close is not allowed for local topic write session");

        ExtractEvents(); // Refresh events and maybe accept TSessionClosedEvent
        WaitEvent(); // Request next event batch if there is no TSessionClosedEvent 

        if (!WriteSessionActor) {
            // TSessionClosedEvent accepted
            return true;
        }

        // Wait for TSessionClosedEvent
        ActorSystem->Send(WriteSessionActor, new TWriteEvents::TEvSessionFinished(/* force */ false));
        return false;
    }

    TWriterCounters::TPtr GetCounters() final {
        return Counters;
    }

protected:
    void RequestEvents(NThreading::TPromise<std::vector<TEvent>> promise) final {
        Y_VALIDATE(WriteSessionActor, "Write session actor unexpectedly finished");
        ActorSystem->Send(WriteSessionActor, new TWriteEvents::TEvExtractReadyEvents(std::move(promise)));
    }

    void OnCloseReceived() final {
        WriteSessionActor = {};
    }

private:
    static void ValidateSettings(const TWriteSessionSettings& settings) {
        TBase::ValidateSettings(settings);

        Y_VALIDATE(settings.Codec_ == ECodec::RAW, "Compression is not supported for local topic write session");
        Y_VALIDATE(!settings.BatchFlushInterval_, "BatchFlushInterval is not supported for local topic write session");
        Y_VALIDATE(!settings.BatchFlushSizeBytes_, "BatchFlushSizeBytes is not supported for local topic write session");

        const auto& eventHandlers = settings.EventHandlers_;
        Y_VALIDATE(!eventHandlers.AcksHandler_, "Event handlers are not supported for local topic write session");
        Y_VALIDATE(!eventHandlers.ReadyToAcceptHandler_, "Event handlers are not supported for local topic write session");
        Y_VALIDATE(!eventHandlers.SessionClosedHandler_, "Event handlers are not supported for local topic write session");
        Y_VALIDATE(!eventHandlers.CommonHandler_, "Event handlers are not supported for local topic write session");
    }

    static TWriterCounters::TPtr SetupCounters(const TWriteSessionSettings& settings) {
        TWriterCounters::TPtr result = settings.Counters_.value_or(nullptr);
        if (!result) {
            result = MakeIntrusive<TWriterCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
        }
        return result;
    }

    void Start(const TWriteSessionSettings& sessionSettings) {
        Y_VALIDATE(!WriteSessionActor, "Write session is already started");
        WriteSessionActor = ActorSystem->Register(new TLocalTopicWriteSessionActor({
            .Database = Database,
            .CredentialsProvider = CredentialsProvider,
            .Counters = Counters,
        }, sessionSettings), TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    }

    void UseAutoSeqNo() {
        if (!ValidateSeqNo) {
            return;
        }

        if (!AutoSeqNo) {
            AutoSeqNo = true;
            return;
        }

        Y_VALIDATE(*AutoSeqNo, "Got mixed auto seq no and manual seq no modes");
    }

    void UseManualSeqNo() {
        if (!ValidateSeqNo) {
            return;
        }

        if (!AutoSeqNo) {
            AutoSeqNo = false;
            return;
        }

        Y_VALIDATE(!*AutoSeqNo, "Got mixed auto seq no and manual seq no modes");
    }

    const TWriterCounters::TPtr Counters;
    const bool DeduplicationEnabled = true;
    const bool ValidateSeqNo = true;
    std::optional<bool> AutoSeqNo;
    TActorId WriteSessionActor;
    std::optional<NThreading::TPromise<uint64_t>> InitSeqNoPromise;
};

} // anonymous namespace

std::shared_ptr<IWriteSession> CreateLocalTopicWriteSession(const TLocalTopicSessionSettings& localSettings, const TWriteSessionSettings& sessionSettings) {
    return std::make_shared<TLocalTopicWriteSession>(localSettings, sessionSettings);
}

} // namespace NKikimr::NKqp
