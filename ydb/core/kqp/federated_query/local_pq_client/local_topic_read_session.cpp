#include "local_topic_read_session.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc_bi_streaming.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/services/persqueue_v1/actors/read_session_actor.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <util/generic/guid.h>

#include <queue>

namespace NKikimr::NKqp {

namespace {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::YDB_SDK, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YDB_SDK, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::YDB_SDK, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::YDB_SDK, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::YDB_SDK, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YDB_SDK, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::YDB_SDK, "[LocalTopicReadSession] " << LogPrefix() << stream)

using namespace NActors;
using namespace NGRpcService;
using namespace NRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

struct TLocalTopicReadyEvent {
    TLocalTopicReadyEvent(TReadSessionEvent::TEvent&& event, i64 size)
        : Event(std::move(event))
        , Size(size)
    {}

    TReadSessionEvent::TEvent Event;
    i64 Size = 0;
};

class TLocalTopicReadSessionActor final : public TActorBootstrapped<TLocalTopicReadSessionActor>, public NActors::IActorExceptionHandler {
    static constexpr TDuration COUNTERS_REFRESH_INTERVAL = TDuration::Seconds(1);

    using TRpcIn = Ydb::Topic::StreamReadMessage::FromClient;
    using TRpcOut = Ydb::Topic::StreamReadMessage::FromServer;
    using TLocalRpcCtx = TLocalRpcBiStreamingCtx<TRpcIn, TRpcOut>;

    struct TEvPrivate {
        enum EEv {
            EvPartitionStatusRequest = TLocalRpcCtx::TRpcEvents::EvEnd,
            EvPartitionOffsetsCommitRequest,
            EvPartitionConfirmCreate,
            EvPartitionConfirmDestroy,
            EvExtractReadyEvents,
            EvEventsConsumed,
            EvSessionFinished,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        // Events from partition session callbacks

        struct TEvPartitionStatusRequest : public TEventLocal<TEvPartitionStatusRequest, EvPartitionStatusRequest> {
            explicit TEvPartitionStatusRequest(i64 partitionSessionId)
                : PartitionSessionId(partitionSessionId)
            {}

            const i64 PartitionSessionId;
        };

        struct TEvPartitionOffsetsCommitRequest : public TEventLocal<TEvPartitionOffsetsCommitRequest, EvPartitionOffsetsCommitRequest> {
            TEvPartitionOffsetsCommitRequest(i64 partitionSessionId, ui64 startOffset, ui64 endOffset)
                : PartitionSessionId(partitionSessionId)
                , StartOffset(startOffset)
                , EndOffset(endOffset)
            {}

            const i64 PartitionSessionId;
            const ui64 StartOffset;
            const ui64 EndOffset;
        };

        struct TEvPartitionConfirmCreate : public TEventLocal<TEvPartitionConfirmCreate, EvPartitionConfirmCreate> {
            TEvPartitionConfirmCreate(i64 partitionSessionId, std::optional<ui64> readOffset, std::optional<ui64> commitOffset)
                : PartitionSessionId(partitionSessionId)
                , ReadOffset(readOffset)
                , CommitOffset(commitOffset)
            {}

            const i64 PartitionSessionId;
            const std::optional<ui64> ReadOffset;
            const std::optional<ui64> CommitOffset;
        };

        struct TEvPartitionConfirmDestroy : public TEventLocal<TEvPartitionConfirmDestroy, EvPartitionConfirmDestroy> {
            explicit TEvPartitionConfirmDestroy(i64 partitionSessionId)
                : PartitionSessionId(partitionSessionId)
            {}

            const i64 PartitionSessionId;
        };
    };

    class TLocalPartitionSession final : public TPartitionSession {
    public:
        struct TSettings {
            i64 PartitionSessionId = 0;
            i64 PartitionId = 0;
            TString TopicPath;
            TString ReadSessionId;
        };

        TLocalPartitionSession(const TActorSystem* actorSystem, const TActorId& selfId, const TSettings& settings)
            : ActorSystem(actorSystem)
            , SelfId(selfId)
        {
            Y_VALIDATE(settings.PartitionSessionId >= 0, "PartitionSessionId must be non negative");
            PartitionSessionId = settings.PartitionSessionId;

            Y_VALIDATE(settings.PartitionId >= 0, "PartitionId must be non negative");
            PartitionId = settings.PartitionId;

            Y_VALIDATE(settings.TopicPath, "Topic path is not set");
            TopicPath = settings.TopicPath;

            Y_VALIDATE(settings.ReadSessionId, "Read session is not started");
            ReadSessionId = settings.ReadSessionId;
        }

        void RequestStatus() final {
            ActorSystem->Send(SelfId, new TEvPrivate::TEvPartitionStatusRequest(PartitionSessionId));
        }

        void Commit(ui64 startOffset, ui64 endOffset) final {
            ActorSystem->Send(SelfId, new TEvPrivate::TEvPartitionOffsetsCommitRequest(PartitionSessionId, startOffset, endOffset));
        }

        void ConfirmCreate(std::optional<ui64> readOffset, std::optional<ui64> commitOffset) final {
            ActorSystem->Send(SelfId, new TEvPrivate::TEvPartitionConfirmCreate(PartitionSessionId, readOffset, commitOffset));
        }

        void ConfirmDestroy() final {
            ActorSystem->Send(SelfId, new TEvPrivate::TEvPartitionConfirmDestroy(PartitionSessionId));
        }

        void ConfirmEnd(const std::vector<ui32>& childIds) final {
            Y_UNUSED(childIds);
        }

    private:
        const TActorSystem* const ActorSystem = nullptr;
        const TActorId SelfId;
    };

    struct TReadSettings {
        TString Consumer;
        TString Topic;
        std::optional<TInstant> ReadFrom;
        std::optional<TDuration> MaxLag;
        std::vector<i64> PartitionIds;
    };

public:
    struct TSessionEvents : private TEvPrivate {
        // Events from TLocalTopicReadSession

        struct TEvExtractReadyEvents : public TEventLocal<TEvExtractReadyEvents, EvExtractReadyEvents> {
            explicit TEvExtractReadyEvents(NThreading::TPromise<std::vector<TLocalTopicReadyEvent>> eventsPromise)
                : EventsPromise(std::move(eventsPromise))
            {}

            NThreading::TPromise<std::vector<TLocalTopicReadyEvent>> EventsPromise;
        };

        struct TEvEventsConsumed : public TEventLocal<TEvEventsConsumed, EvEventsConsumed> {
            TEvEventsConsumed(i64 size, ui64 eventsCount)
                : Size(size)
                , EventsCount(eventsCount)
            {}

            const i64 Size = 0; // Return this size to free memory
            const ui64 EventsCount = 0;
        };

        struct TEvSessionFinished : public TEventLocal<TEvSessionFinished, EvSessionFinished> {
            explicit TEvSessionFinished(bool force)
                : Force(force)
            {}

            const bool Force = false;
        };
    };

    struct TSettings {
        TString Database;
        std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
        TReaderCounters::TPtr Counters;
    };

    TLocalTopicReadSessionActor(const TSettings& actorSettings, const TReadSessionSettings& sessionSettings)
        : Settings(sessionSettings)
        , ReadSettings(GetReadSettings(sessionSettings))
        , Database(actorSettings.Database)
        , CredentialsProvider(actorSettings.CredentialsProvider)
        , MaxMemoryUsage(sessionSettings.MaxMemoryUsageBytes_)
        , Counters(actorSettings.Counters)
    {
        Y_VALIDATE(Database, "Missing database");
        Y_VALIDATE(Counters, "Missing counters");
    }

    void Bootstrap() {
        Become(&TThis::StateFunc);

        LOG_I("Start local topic read session, MaxMemoryUsage: " << MaxMemoryUsage);
        StartSession();

        Schedule(COUNTERS_REFRESH_INTERVAL, new TEvents::TEvWakeup());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TSessionEvents::TEvExtractReadyEvents, Handle);
        hFunc(TSessionEvents::TEvEventsConsumed, Handle);
        hFunc(TSessionEvents::TEvSessionFinished, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvActorAttached, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvReadRequest, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvWriteRequest, Handle);
        hFunc(TLocalRpcCtx::TRpcEvents::TEvFinishRequest, Handle);
        hFunc(TEvPrivate::TEvPartitionStatusRequest, Handle);
        hFunc(TEvPrivate::TEvPartitionOffsetsCommitRequest, Handle);
        hFunc(TEvPrivate::TEvPartitionConfirmCreate, Handle);
        hFunc(TEvPrivate::TEvPartitionConfirmDestroy, Handle);
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvUndelivered, Handle);
    );

    bool OnUnhandledException(const std::exception& e) final {
        LOG_E("Got unexpected exception: " << e.what());
        CloseSession(EStatus::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << e.what());
        return true;
    }

private:
    static TReadSettings GetReadSettings(const TReadSessionSettings& sessionSettings) {
        TReadSettings settings;
        settings.Consumer = sessionSettings.ConsumerName_;

        Y_VALIDATE(sessionSettings.Topics_.size() == 1, "Only one topic is supported per read session");
        const auto& topic = sessionSettings.Topics_[0];
        settings.Topic = topic.Path_;
        settings.ReadFrom = topic.ReadFromTimestamp_ ? topic.ReadFromTimestamp_ : sessionSettings.ReadFromTimestamp_;
        settings.MaxLag = topic.MaxLag_ ? topic.MaxLag_ : sessionSettings.MaxLag_;

        settings.PartitionIds.reserve(topic.PartitionIds_.size());
        for (auto partitionId : topic.PartitionIds_) {
            Y_VALIDATE(partitionId <= std::numeric_limits<i64>::max(), "PartitionId is too large");
            settings.PartitionIds.emplace_back(partitionId);
        }

        return settings;
    }

    // Events from TLocalTopicReadSession

    void Handle(TSessionEvents::TEvExtractReadyEvents::TPtr& ev) {
        LOG_T("Got extract ready events request, OutgoingEvents #" << OutgoingEvents.size());

        Y_VALIDATE(!EventsPromise, "Can not handle extract event in parallel");
        EventsPromise = std::move(ev->Get()->EventsPromise);
        SendOutgoingEvents();
    }

    void Handle(TSessionEvents::TEvEventsConsumed::TPtr& ev) {
        const auto size = std::min(ev->Get()->Size, InflightMemory);
        InflightMemory -= size;
        Counters->BytesInflightTotal->Sub(size);

        const auto eventsCount = ev->Get()->EventsCount;
        Counters->MessagesInflight->Sub(eventsCount);

        LOG_T("Handled #" << eventsCount << " events with amount size: " << size << ", new InflightMemory: " << InflightMemory << ", MaxMemoryUsage: " << MaxMemoryUsage);
        ContinueReading();
    }

    void Handle(TSessionEvents::TEvSessionFinished::TPtr& ev) {
        const bool force = ev->Get()->Force;
        LOG_I("Local topic read session finished from client side, force: " << force);

        CloseSession(EStatus::SUCCESS);

        if (force) {
            PassAway();
        }
    }

    // Events from local RPC session

    void Handle(TLocalRpcCtx::TRpcEvents::TEvActorAttached::TPtr& ev) {
        Y_VALIDATE(!RpcActor, "RpcActor is already set");
        RpcActor = ev->Get()->RpcActor;

        LOG_I("RpcActor attached: " << RpcActor);
        SendInitMessage();
    }

    void Handle(TLocalRpcCtx::TRpcEvents::TEvReadRequest::TPtr&) {
        PendingRpcResponses++;

        if (SessionClosed) {
            LOG_D("Rpc read request skipped, session is closed");
            SendSessionEventFail();
            return;
        }

        LOG_T("Rpc read request");
        SendSessionEvents();
    }

    void Handle(TLocalRpcCtx::TRpcEvents::TEvWriteRequest::TPtr& ev) {
        Y_VALIDATE(RpcActor, "RpcActor is not set before write request");
        auto response = std::make_unique<TLocalRpcCtx::TEvWriteFinished>();

        if (SessionClosed) {
            LOG_D("Rpc write request skipped, session is closed");
            response->Success = false;
            Send(RpcActor, response.release());
            return;
        }

        response->Success = true;
        Send(RpcActor, response.release());

        auto& message = ev->Get()->Message;
        const auto status = message.status();
        if (status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            IssuesFromMessage(message.issues(), issues);
            LOG_E("Rpc write request, got error " << status << ", reason: " << issues.ToOneLineString());
            return CloseSession(status, issues);
        }

        const auto messageCase = message.server_message_case();
        LOG_T("Rpc write request: " << static_cast<i64>(messageCase));

        switch (messageCase) {
            case Ydb::Topic::StreamReadMessage::FromServer::kInitResponse:
                ComputeSessionMessage(message.init_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kReadResponse:
                ComputeSessionMessage(*message.mutable_read_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kCommitOffsetResponse:
                ComputeSessionMessage(message.commit_offset_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kPartitionSessionStatusResponse:
                ComputeSessionMessage(message.partition_session_status_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kUpdateTokenResponse:
                ComputeSessionMessage(message.update_token_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest:
                ComputeSessionMessage(message.start_partition_session_request());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest:
                ComputeSessionMessage(message.stop_partition_session_request());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kUpdatePartitionSession:
                ComputeSessionMessage(message.update_partition_session());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kEndPartitionSession:
                ComputeSessionMessage(message.end_partition_session());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::SERVER_MESSAGE_NOT_SET:
                CloseSession(EStatus::INTERNAL_ERROR, "Unknown server message");
                break;
        }
    }

    void Handle(TLocalRpcCtx::TRpcEvents::TEvFinishRequest::TPtr& ev) {
        const auto& status = ev->Get()->Status;
        if (!status.ok()) {
            LOG_E("Rpc session finished with error status code: " << static_cast<ui64>(status.error_code()) << ", message: " << status.error_message());
        } else {
            LOG_I("Rpc session successfully finished");
        }

        CloseSession(status, "Read session closed");
    }

    // Events from topic partition session callbacks

    void Handle(TEvPrivate::TEvPartitionStatusRequest::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        LOG_D("Partition session #" << partitionSessionId << " status request");

        TRpcIn message;
        message.mutable_partition_session_status_request()->set_partition_session_id(partitionSessionId);

        AddSessionEvent(std::move(message));
    }

    void Handle(TEvPrivate::TEvPartitionOffsetsCommitRequest::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        const auto start = ev->Get()->StartOffset;
        const auto end = ev->Get()->EndOffset;
        LOG_D("Partition session #" << partitionSessionId << " offsets [" << start << ", " << end << "] commit request");

        TRpcIn message;

        auto& commitRequest = *message.mutable_commit_offset_request()->add_commit_offsets();
        commitRequest.set_partition_session_id(partitionSessionId);

        auto& offsets = *commitRequest.add_offsets();
        offsets.set_start(start);
        offsets.set_end(end);

        AddSessionEvent(std::move(message));
    }

    void Handle(TEvPrivate::TEvPartitionConfirmCreate::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        const auto readOffset = ev->Get()->ReadOffset;
        const auto commitOffset = ev->Get()->CommitOffset;
        LOG_D("Partition session #" << partitionSessionId << " confirmed"
            << ", read offset: " << (readOffset ? ToString(*readOffset) : "null")
            << ", commit offset: " << (commitOffset ? ToString(*commitOffset) : "null"));

        TRpcIn message;

        auto& startResponse = *message.mutable_start_partition_session_response();
        startResponse.set_partition_session_id(partitionSessionId);
        if (readOffset) {
            startResponse.set_read_offset(*readOffset);
        }
        if (commitOffset) {
            startResponse.set_commit_offset(*commitOffset);
        }

        AddSessionEvent(std::move(message));
    }

    void Handle(TEvPrivate::TEvPartitionConfirmDestroy::TPtr& ev) {
        const auto partitionSessionId = ev->Get()->PartitionSessionId;
        LOG_D("Partition session #" << partitionSessionId << " destroyed");

        TRpcIn message;
        message.mutable_stop_partition_session_response()->set_partition_session_id(partitionSessionId);
        AddSessionEvent(std::move(message));

        AddOutgoingSessionClosedEvent(partitionSessionId, TReadSessionEvent::TPartitionSessionClosedEvent::EReason::StopConfirmedByUser);
    }

    void Handle(TEvents::TEvWakeup::TPtr&) {
        const auto now = TInstant::Now();

        if (SessionStartedAt) {
            *Counters->CurrentSessionLifetimeMs = (now - SessionStartedAt).MilliSeconds();
        }

        if (LastCountersUpdateAt) {
            const auto delta = (now - LastCountersUpdateAt).MilliSeconds();
            const double percent = 100.0 / MaxMemoryUsage;
            Counters->TotalBytesInflightUsageByTime->Collect(InflightMemory * percent, delta);
        }

        LastCountersUpdateAt = now;
        Schedule(COUNTERS_REFRESH_INTERVAL, new TEvents::TEvWakeup());
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto sourceType = ev->Get()->SourceType;
        const auto reason = ev->Get()->Reason;
        Y_VALIDATE(sourceType == TEvStreamTopicReadRequest::EventType, "Unexpected undelivered event: " << sourceType << ", reason: " << reason);

        LOG_E("PQ read service is unavailable, reason: " << reason);
        CloseSession(EStatus::INTERNAL_ERROR, "PQ read service is unavailable, please contact internal support");
    }

    void StartSession() const {
        const auto& token = CredentialsProvider ? std::optional<TString>(CredentialsProvider->GetAuthInfo()) : std::nullopt;
        auto ctx = MakeIntrusive<TLocalRpcCtx>(ActorContext().ActorSystem(), SelfId(), TLocalRpcCtx::TSettings{
            .Database = Database,
            .Token = token,
            .PeerName = "localhost/local_topic_rpc_read",
            .RequestType = Settings.RequestType_.empty() ? std::nullopt : std::optional<TString>(Settings.RequestType_),
            .ParentTraceId = TString(Settings.TraceParent_),
            .TraceId = TString(Settings.TraceId_),
            .RpcMethodName = "TopicService.StreamRead",
        });

        for (const auto& [key, value] : Settings.Header_) {
            ctx->PutPeerMeta(TString(key), TString(value));
        }

        auto ev = std::make_unique<TEvStreamTopicReadRequest>(std::move(ctx), TRequestAuxSettings{.RequestType = NJaegerTracing::ERequestType::TOPIC_STREAMREAD});

        if (token) {
            ev->SetInternalToken(MakeIntrusive<NACLib::TUserToken>(*token));
        }

        Send(NGRpcProxy::V1::GetPQReadServiceActorID(), ev.release(), IEventHandle::FlagTrackDelivery);
    }

    void SendInitMessage() {
        LOG_I("Sending init message"
            << ", Consumer: " << ReadSettings.Consumer
            << ", ReadFrom: " << (ReadSettings.ReadFrom ? ToString(*ReadSettings.ReadFrom) : "null")
            << ", MaxLag: " << (ReadSettings.MaxLag ? ToString(*ReadSettings.MaxLag) : "null"));

        TRpcIn message;

        auto& initRequest = *message.mutable_init_request();
        initRequest.set_consumer(ReadSettings.Consumer);

        auto& topic = *initRequest.add_topics_read_settings();
        topic.set_path(ReadSettings.Topic);
        if (ReadSettings.ReadFrom) {
            *topic.mutable_read_from() = NProtoInterop::CastToProto(*ReadSettings.ReadFrom);
        }
        if (ReadSettings.MaxLag) {
            *topic.mutable_max_lag() = NProtoInterop::CastToProto(*ReadSettings.MaxLag);
        }

        AddSessionEvent(std::move(message));
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::InitResponse& message) {
        SessionStartedAt = TInstant::Now();
        SessionId = message.session_id();
        LOG_I("Session initialized with id: " << SessionId);
        ContinueReading();
    }

    void ComputeSessionMessage(Ydb::Topic::StreamReadMessage::ReadResponse& message) {
        const auto responseSize = message.bytes_size();
        ServerMemoryDelta -= responseSize;
        Counters->BytesReadCompressed->Add(responseSize);
        LOG_T("Received read response with size: " << responseSize << ", new ServerMemoryDelta: " << ServerMemoryDelta);

        for (auto& partitionData : *message.mutable_partition_data()) {
            const auto partitionSessionId = partitionData.partition_session_id();
            LOG_T("Partition session #" << partitionSessionId << " data received, batches #" << partitionData.batches_size());

            i64 messagesSize = 0;
            std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
            auto partitionSession = GetPartitionSession(partitionSessionId);

            for (auto& batch : *partitionData.mutable_batches()) {
                const auto& producerId = batch.producer_id();
                const auto writtenAt = NProtoInterop::CastFromProto(batch.written_at());

                const auto writeSessionMeta = MakeIntrusive<TWriteSessionMeta>();
                writeSessionMeta->Fields.reserve(batch.write_session_meta_size());
                for (auto& [key, value] : *batch.mutable_write_session_meta()) {
                    messagesSize += key.size() + value.size();
                    writeSessionMeta->Fields.emplace(key, std::move(value));
                }
                messagesSize += sizeof(TWriteSessionMeta);

                for (auto& event : *batch.mutable_message_data()) {
                    std::string decompressedData;
                    std::exception_ptr decompressionException;
                    if (!IsIn({Ydb::Topic::CODEC_RAW, Ydb::Topic::CODEC_UNSPECIFIED}, static_cast<Ydb::Topic::Codec>(batch.codec()))) {
                        try {
                            const ICodec* codecImpl = TCodecMap::GetTheCodecMap().GetOrThrow(static_cast<ui32>(batch.codec()));
                            decompressedData = codecImpl->Decompress(event.data());
                        } catch (...) {
                            decompressionException = std::current_exception();
                        }
                    } else {
                        decompressedData = std::move(*event.mutable_data());
                    }

                    auto messageMeta = MakeIntrusive<TMessageMeta>();
                    messageMeta->Fields.reserve(event.metadata_items_size());
                    for (auto& item : *event.mutable_metadata_items()) {
                        messagesSize += item.key().size() + item.value().size();
                        messageMeta->Fields.emplace_back(std::move(*item.mutable_key()), std::move(*item.mutable_value()));
                    }

                    messagesSize += decompressedData.size() + producerId.size() + sizeof(TMessageMeta) + event.message_group_id().size();
                    Counters->BytesRead->Add(decompressedData.size());

                    messages.emplace_back(std::move(decompressedData), std::move(decompressionException), TReadSessionEvent::TDataReceivedEvent::TMessageInformation(
                        event.offset(),
                        producerId,
                        event.seq_no(),
                        NProtoInterop::CastFromProto(event.created_at()),
                        writtenAt,
                        writeSessionMeta,
                        std::move(messageMeta),
                        event.uncompressed_size(),
                        std::move(*event.mutable_message_group_id())
                    ), partitionSession);
                }
            }

            messagesSize += sizeof(TReadSessionEvent::TDataReceivedEvent::TMessage) * messages.size();
            Counters->MessagesRead->Add(messages.size());

            AddOutgoingEvent(TReadSessionEvent::TDataReceivedEvent(
                std::move(messages),
                {},
                std::move(partitionSession)
            ), messagesSize);
        }
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::CommitOffsetResponse& message) {
        for (const auto& commitOffset : message.partitions_committed_offsets()) {
            const auto partitionSessionId = commitOffset.partition_session_id();
            const auto offset = commitOffset.committed_offset();
            LOG_D("Partition session #" << partitionSessionId << " offset " << offset << " commited");

            AddOutgoingEvent(TReadSessionEvent::TCommitOffsetAcknowledgementEvent(
                GetPartitionSession(partitionSessionId),
                offset
            ));
        }
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::PartitionSessionStatusResponse& message) {
        const auto partitionSessionId = message.partition_session_id();
        LOG_D("Partition session #" << partitionSessionId << " status response: " << message.ShortDebugString());

        AddOutgoingEvent(TReadSessionEvent::TPartitionSessionStatusEvent(
            GetPartitionSession(partitionSessionId),
            message.committed_offset(),
            message.read_offset(),
            message.partition_offsets().end(),
            NProtoInterop::CastFromProto(message.write_time_high_watermark())
        ));
    }

    void ComputeSessionMessage(const Ydb::Topic::UpdateTokenResponse&) {
        Y_VALIDATE(false, "Unexpected message type: UpdateTokenResponse");
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::StartPartitionSessionRequest& message) {
        const auto& info = message.partition_session();
        const auto partitionId = info.partition_id();
        const auto partitionSessionId = info.partition_session_id();
        const auto committedOffset = message.committed_offset();
        const auto& offsets = message.partition_offsets();
        LOG_D("Start partition #" << partitionId << " session with id " << partitionSessionId
            << ", commited offset: " << committedOffset
            << ", offsets range: " << offsets.ShortDebugString());

        auto partitionSession = MakeIntrusive<TLocalPartitionSession>(ActorContext().ActorSystem(), SelfId(), TLocalPartitionSession::TSettings{
            .PartitionSessionId = partitionSessionId,
            .PartitionId = partitionId,
            .TopicPath = info.path(),
            .ReadSessionId = SessionId,
        });
        Y_VALIDATE(PartitionSessions.emplace(partitionSessionId, partitionSession).second, "Partition session #" << partitionSessionId << " already exists");

        AddOutgoingEvent(TReadSessionEvent::TStartPartitionSessionEvent(
            std::move(partitionSession),
            committedOffset,
            offsets.end()
        ));
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::StopPartitionSessionRequest& message) {
        const auto partitionSessionId = message.partition_session_id();
        const auto committedOffset = message.committed_offset();
        LOG_D("Partition session #" << partitionSessionId << " received stop event, commited offset: " << committedOffset);

        if (!message.graceful()) {
            return AddOutgoingSessionClosedEvent(partitionSessionId, TReadSessionEvent::TPartitionSessionClosedEvent::EReason::Lost);
        }

        return AddOutgoingEvent(TReadSessionEvent::TStopPartitionSessionEvent(
            GetPartitionSession(partitionSessionId),
            committedOffset
        ));
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::UpdatePartitionSession& message) {
        LOG_D("Partition session #" << message.partition_session_id() << " received update event: " << message.ShortDebugString());
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::EndPartitionSession& message) {
        const auto partitionSessionId = message.partition_session_id();
        const auto& adjacentIds = message.adjacent_partition_ids();
        const auto& childIds = message.child_partition_ids();
        LOG_D("Partition session #" << partitionSessionId << " received end event"
            << ", adjacent ids #" << adjacentIds.size()
            << ", child ids #" << childIds.size());

        AddOutgoingEvent(TReadSessionEvent::TEndPartitionSessionEvent(
            GetPartitionSession(partitionSessionId),
            std::vector<uint32_t>(adjacentIds.begin(), adjacentIds.end()),
            std::vector<uint32_t>(childIds.begin(), childIds.end())
        ), sizeof(uint32_t) * (adjacentIds.size() + childIds.size()));
    }

    TPartitionSession::TPtr GetPartitionSession(i64 partitionSessionId) const {
        const auto it = PartitionSessions.find(partitionSessionId);
        Y_VALIDATE(it != PartitionSessions.end(), "Unknown partition session: " << partitionSessionId);
        return it->second;
    }

    void ContinueReading() {
        if (!SessionId) {
            LOG_D("Session not started yet, skip reading");
            return;
        }

        if (InflightMemory >= MaxMemoryUsage) {
            LOG_T("Max memory usage reached, skip reading, InflightMemory: " << InflightMemory << ", MaxMemoryUsage: " << MaxMemoryUsage);
            return;
        }

        const auto readMemoryBudget = MaxMemoryUsage - InflightMemory;
        if (ServerMemoryDelta >= readMemoryBudget) {
            LOG_T("Server already has enough memory, skip reading, ServerMemoryDelta: " << ServerMemoryDelta << ", read memory budget: " << readMemoryBudget);
            return;
        }

        const auto bytesToRead = readMemoryBudget - ServerMemoryDelta;
        ServerMemoryDelta = readMemoryBudget;
        LOG_T("Reading " << bytesToRead << " bytes, new ServerMemoryDelta: " << ServerMemoryDelta << ", InflightMemory: " << InflightMemory << ", MaxMemoryUsage: " << MaxMemoryUsage);

        TRpcIn message;
        message.mutable_read_request()->set_bytes_size(bytesToRead);
        AddSessionEvent(std::move(message));
    }

    // Events to PQ read service

    void AddSessionEvent(TRpcIn&& message) {
        if (SessionClosed) {
            LOG_D("Session already closed, skip session event");
            return;
        }

        RpcResponses.push(message);
        LOG_T("Added session event: " << static_cast<i64>(message.client_message_case()));

        if (RpcActor) {
            SendSessionEvents();            
        }
    }

    void SendSessionEvents() {
        Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
        LOG_T("Going to send session events, PendingRpcResponses: " << PendingRpcResponses << ", RpcResponses #" << RpcResponses.size());

        while (PendingRpcResponses > 0 && !RpcResponses.empty()) {
            SendSessionEvent(std::move(RpcResponses.front()));
            RpcResponses.pop();
        }
    }

    void SendSessionEvent(TRpcIn&& message, bool success = true) {
        LOG_T("Sending session event: " << static_cast<i64>(message.client_message_case()) << ", success: " << success);
        Y_VALIDATE(PendingRpcResponses > 0, "Rpc read is not expected");
        PendingRpcResponses--;

        auto ev = std::make_unique<TLocalRpcCtx::TEvReadFinished>();
        ev->Success = success;
        ev->Record = std::move(message);

        Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
        Send(RpcActor, ev.release());
    }

    void SendSessionEventFail() {
        SendSessionEvent({}, /* success */ false);
    }

    // Events to TLocalTopicReadSession

    void AddOutgoingSessionClosedEvent(i64 partitionSessionId, TReadSessionEvent::TPartitionSessionClosedEvent::EReason reason) {
        AddOutgoingEvent(TReadSessionEvent::TPartitionSessionClosedEvent(
            GetPartitionSession(partitionSessionId),
            reason
        ));
    }

    template <typename TEvent>
    void AddOutgoingEvent(TEvent&& event, i64 internalSize = 0) {
        OutgoingEvents.emplace(std::move(event), sizeof(TEvent) + internalSize);
        Counters->MessagesInflight->Inc();

        const auto size = OutgoingEvents.back().Size;
        InflightMemory += size;
        Counters->BytesInflightTotal->Add(size);
        LOG_T("Added outgoing event: " << OutgoingEvents.back().Event.index() << ", size: " << size << ", InflightMemory: " << InflightMemory);

        SendOutgoingEvents();
    }

    void SendOutgoingEvents() {
        if (!EventsPromise || OutgoingEvents.empty()) {
            return;
        }

        LOG_T("Going to send outgoing events, OutgoingEvents #" << OutgoingEvents.size());

        bool closeEventSent = false;
        std::vector<TLocalTopicReadyEvent> result;
        result.reserve(OutgoingEvents.size());
        while (!OutgoingEvents.empty()) {
            result.push_back(std::move(OutgoingEvents.front()));
            OutgoingEvents.pop();

            if (std::holds_alternative<TSessionClosedEvent>(result.back().Event)) {
                LOG_I("Sent close session event, finishing");
                closeEventSent = true;
            }
        }

        EventsPromise->SetValue(std::move(result));
        EventsPromise.reset();

        if (closeEventSent) {
            PassAway();
        }
    }

    void CloseSession(EStatus status, const NYql::TIssues& issues = {}) {
        const bool success = status == EStatus::SUCCESS;
        if (!success) {
            Counters->Errors->Inc();
            LOG_E("Closing session with status " << status << " and issues: " << issues.ToOneLineString());
        } else {
            LOG_I("Closing session with success status");
        }

        if (SessionClosed) {
            LOG_W("Session already closed, but got status " << status << " and issues: " << issues.ToOneLineString());
            return;
        }
        SessionClosed = true;

        // Close session on client side
        AddOutgoingEvent(TSessionClosedEvent(status, NAdapters::ToSdkIssues(issues)));

        // Close session on server side
        while (PendingRpcResponses) {
            SendSessionEventFail();
        }
        Send(RpcActor, new TLocalRpcCtx::TEvNotifiedWhenDone(success));
    }

    void CloseSession(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
        CloseSession(static_cast<EStatus>(status), issues);
    }

    void CloseSession(EStatus status, const TString& message) {
        CloseSession(status, {NYql::TIssue(message)});
    }

    void CloseSession(const grpc::Status& status, const TString& message = "") {
        NYql::TIssues issues;
        if (const auto& errorMessage = status.error_message(); !errorMessage.empty()) {
            issues.AddIssue(errorMessage);
        }
        if (message) {
            issues = AddRootIssue(message, issues);
        }

        switch (status.error_code()) {
            case grpc::OK:
                return CloseSession(EStatus::SUCCESS, issues);
            case grpc::CANCELLED:
                return CloseSession(EStatus::CANCELLED, issues);
            case grpc::UNKNOWN:
                return CloseSession(EStatus::UNDETERMINED, issues);
            case grpc::DEADLINE_EXCEEDED:
                return CloseSession(EStatus::TIMEOUT, issues);
            case grpc::NOT_FOUND:
                return CloseSession(EStatus::NOT_FOUND, issues);
            case grpc::ALREADY_EXISTS:
                return CloseSession(EStatus::ALREADY_EXISTS, issues);
            case grpc::RESOURCE_EXHAUSTED:
                return CloseSession(EStatus::OVERLOADED, issues);
            case grpc::ABORTED:
                return CloseSession(EStatus::ABORTED, issues);
            case grpc::OUT_OF_RANGE:
                return CloseSession(EStatus::CLIENT_OUT_OF_RANGE, issues);
            case grpc::UNIMPLEMENTED:
                return CloseSession(EStatus::UNSUPPORTED, issues);
            case grpc::UNAVAILABLE:
                return CloseSession(EStatus::UNAVAILABLE, issues);
            case grpc::INVALID_ARGUMENT:
            case grpc::FAILED_PRECONDITION:
                return CloseSession(EStatus::PRECONDITION_FAILED, issues);
            case grpc::UNAUTHENTICATED:
            case grpc::PERMISSION_DENIED:
                return CloseSession(EStatus::UNAUTHORIZED, issues);
            default:
                return CloseSession(EStatus::INTERNAL_ERROR, issues);
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "Database: " << Database << ", Topic: " << ReadSettings.Topic << ", SelfId: " << SelfId() << ". ";
    }

    const TRequestSettings<TReadSessionSettings> Settings;
    const TReadSettings ReadSettings;
    const TString Database;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
    const i64 MaxMemoryUsage = 0;
    const TReaderCounters::TPtr Counters;
    TInstant SessionStartedAt;
    TInstant LastCountersUpdateAt;

    TActorId RpcActor;
    i64 InflightMemory = 0;
    i64 ServerMemoryDelta = 0;
    TString SessionId;
    std::unordered_map<i64, TPartitionSession::TPtr> PartitionSessions;
    bool SessionClosed = false;

    // Outgoing messages to PQ
    ui64 PendingRpcResponses = 0;
    std::queue<TRpcIn> RpcResponses;

    // Outgoing messages to TLocalTopicReadSession
    std::queue<TLocalTopicReadyEvent> OutgoingEvents;
    std::optional<NThreading::TPromise<std::vector<TLocalTopicReadyEvent>>> EventsPromise;
};

// Supposed to be used from actor system, so all blocking methods are not supported.
// Read session is not thread safe and MUST be used from single actor.
// NOTICE: data is decompressed in one thread inside TLocalTopicReadSessionActor for simplicity.
class TLocalTopicReadSession final : public IReadSession {
public:
    TLocalTopicReadSession(const TLocalTopicReadSettings& localSettings, const TReadSessionSettings& sessionSettings)
        : ActorSystem(localSettings.ActorSystem)
        , Counters(SetupCounters(sessionSettings))
        , SessionId(CreateGuidAsString())
    {
        Y_VALIDATE(ActorSystem, "Actor system is required for local topic read session");
        ValidateSettings(sessionSettings);
        Start(localSettings.Database, localSettings.CredentialsProvider, sessionSettings);
    }

    ~TLocalTopicReadSession() {
        try {
            if (!Close(TDuration::Zero()) && ActorSystem && ReadSessionActor) {
                ActorSystem->Send(ReadSessionActor, new TLocalTopicReadSessionActor::TSessionEvents::TEvSessionFinished(/* force */ true));
            }
        } catch (...) {
            // ¯\_(ツ)_/¯
        }
    }

    NThreading::TFuture<void> WaitEvent() final {
        if (!Events.empty() || ExtractEvents()) {
            return NThreading::MakeFuture();
        }

        if (!EventFuture) {
            auto eventsPromise = NThreading::NewPromise<std::vector<TLocalTopicReadyEvent>>();
            EventFuture = eventsPromise.GetFuture();

            Y_VALIDATE(ReadSessionActor, "Read session actor unexpectedly finished");
            ActorSystem->Send(ReadSessionActor, new TLocalTopicReadSessionActor::TSessionEvents::TEvExtractReadyEvents(std::move(eventsPromise)));
        }

        return EventFuture->IgnoreResult();
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(bool block, std::optional<size_t> maxEventsCount, size_t maxByteSize) final {
        Y_VALIDATE(!block, "Blocking is not supported for local topic read session");
        Y_VALIDATE(maxByteSize > 0, "MaxByteSize must be greater than 0");

        ExtractEvents();

        std::vector<TReadSessionEvent::TEvent> result;
        const auto maxCount = maxEventsCount.value_or(std::numeric_limits<size_t>::max());
        result.reserve(std::min(Events.size(), maxCount));

        i64 totalSize = 0;
        while (!Events.empty() && result.size() < maxCount && static_cast<size_t>(totalSize) < maxByteSize) {
            auto& event = Events.front();
            result.emplace_back(std::move(event.Event));
            totalSize += event.Size;

            if (std::holds_alternative<TSessionClosedEvent>(result.back())) {
                // Skip all events after session closed event
                break;
            }

            Events.pop();
        }

        if (ReadSessionActor && !result.empty()) {
            ActorSystem->Send(ReadSessionActor, new TLocalTopicReadSessionActor::TSessionEvents::TEvEventsConsumed(totalSize, result.size()));
        }

        WaitEvent(); // Request next event batch

        return result;
    }

    std::vector<TReadSessionEvent::TEvent> GetEvents(const TReadSessionGetEventSettings& settings) final {
        Y_VALIDATE(!settings.Tx_, "Transaction is not supported for local topic read session");
        return GetEvents(settings.Block_, settings.MaxEventsCount_, settings.MaxByteSize_);
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(bool block, size_t maxByteSize) final {
        const auto& events = GetEvents(block, 1, maxByteSize);
        if (events.empty()) {
            return std::nullopt;
        }
        return events[0];
    }

    std::optional<TReadSessionEvent::TEvent> GetEvent(const TReadSessionGetEventSettings& settings) final {
        Y_VALIDATE(!settings.MaxEventsCount_, "MaxEventsCount is not allowed for GetEvent");
        Y_VALIDATE(!settings.Tx_, "Transaction is not supported for local topic read session");
        return GetEvent(settings.Block_, settings.MaxByteSize_);
    }

    bool Close(TDuration timeout) final {
        Y_VALIDATE(!timeout, "Timeout on close is not allowed for local topic read session");

        ExtractEvents(); // Refresh events and maybe accept TSessionClosedEvent
        WaitEvent(); // Request next event batch if there is no TSessionClosedEvent 

        if (!ReadSessionActor) {
            // TSessionClosedEvent accepted
            return true;
        }

        // Wait for TSessionClosedEvent
        ActorSystem->Send(ReadSessionActor, new TLocalTopicReadSessionActor::TSessionEvents::TEvSessionFinished(/* force */ false));
        return false;
    }

    TReaderCounters::TPtr GetCounters() const final {
        return Counters;
    }

    std::string GetSessionId() const final {
        return SessionId;
    }

private:
    static void ValidateSettings(const TReadSessionSettings& settings) {
        Y_VALIDATE(!settings.AutoPartitioningSupport_, "AutoPartitioningSupport is not supported for local topic read session");
        Y_VALIDATE(settings.ClientTimeout_ == TDuration::Max(), "Timeout is not allowed for local topic read session");
        Y_VALIDATE(settings.Deadline_ == TDeadline::Max(), "Timeout is not allowed for local topic read session");
        Y_VALIDATE(settings.Decompress_, "Read session without decompression is not supported");

        const auto& eventHandlers = settings.EventHandlers_;
        Y_VALIDATE(!eventHandlers.DataReceivedHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.CommitOffsetAcknowledgementHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.StartPartitionSessionHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.StopPartitionSessionHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.EndPartitionSessionHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.PartitionSessionStatusHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.PartitionSessionClosedHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.SessionClosedHandler_, "Event handlers are not supported for local topic read session");
        Y_VALIDATE(!eventHandlers.CommonHandler_, "Event handlers are not supported for local topic read session");
    }

    static TReaderCounters::TPtr SetupCounters(const TReadSessionSettings& settings) {
        auto result = settings.Counters_;
        if (!result || HasNullCounters(*result)) {
            result = result ? result : MakeIntrusive<TReaderCounters>();
            MakeCountersNotNull(*result);
        }
        return result;
    }

    void Start(const TString& database, std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider, const TReadSessionSettings& sessionSettings) {
        Y_VALIDATE(!ReadSessionActor, "Read session is already started");
        ReadSessionActor = ActorSystem->Register(new TLocalTopicReadSessionActor({
            .Database = database,
            .CredentialsProvider = std::move(credentialsProvider),
            .Counters = Counters,
        }, sessionSettings), TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    }

    bool ExtractEvents() {
        if (!EventFuture || !EventFuture->IsReady()) {
            return false;
        }

        auto events = EventFuture->ExtractValue();
        EventFuture.reset();

        for (auto& event : events) {
            Events.emplace(std::move(event));

            if (std::holds_alternative<TSessionClosedEvent>(Events.back().Event)) {
                // Session was finished, skip all other events
                ReadSessionActor = {};
                break;
            }
        }

        return true;
    }

    TActorSystem* const ActorSystem = nullptr;
    const TReaderCounters::TPtr Counters;
    const TString SessionId;
    TActorId ReadSessionActor;
    std::optional<NThreading::TFuture<std::vector<TLocalTopicReadyEvent>>> EventFuture;
    std::queue<TLocalTopicReadyEvent> Events;
};

} // anonymous namespace

std::shared_ptr<IReadSession> CreateLocalTopicReadSession(const TLocalTopicReadSettings& localSettings, const TReadSessionSettings& sessionSettings) {
    return std::make_shared<TLocalTopicReadSession>(localSettings, sessionSettings);
}

} // namespace NKikimr::NKqp
