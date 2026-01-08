#include "local_topic_read_session.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc_bi_streaming.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/services/persqueue_v1/actors/read_session_actor.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <util/generic/guid.h>

#include <queue>

namespace NKikimr::NKqp {

namespace {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[LocalTopicReadSession] " << LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_COMPUTE, "[LocalTopicReadSession] " << LogPrefix() << stream)

using namespace NActors;
using namespace NGRpcService;
using namespace NRpcService;
using namespace NYdb;
using namespace NYdb::NTopic;

struct TLocalTopicReadyEvent {
    TReadSessionEvent::TEvent Event;
    i64 Size = 0;
};

// TODO:
// Decompress_
// DecompressionExecutor_

class TLocalTopicReadSessionActor final : public TActorBootstrapped<TLocalTopicReadSessionActor>, public NActors::IActorExceptionHandler {
    using TRpcIn = Ydb::Topic::StreamReadMessage::FromClient;
    using TRpcOut = Ydb::Topic::StreamReadMessage::FromServer;

    struct TEvPrivate {
        enum EEv {
            EvRpcActorAttached = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvRpcReadRequest,
            EvRpcWriteRequest,
            EvRpcFinishRequest,
            EvExtractReadyEvents,
            EvEventsConsumed,
            EvSessionFinished,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvRpcActorAttached : public TEventLocal<TEvRpcActorAttached, EvRpcActorAttached> {
            explicit TEvRpcActorAttached(const TActorId& rpcActor)
                : RpcActor(rpcActor)
            {}

            const TActorId RpcActor;
        };

        struct TEvRpcReadRequest : public TEventLocal<TEvRpcReadRequest, EvRpcReadRequest> {
        };

        struct TEvRpcWriteRequest : public TEventLocal<TEvRpcWriteRequest, EvRpcWriteRequest> {
            explicit TEvRpcWriteRequest(TRpcOut&& message)
                : Message(std::move(message))
            {}

            TRpcOut Message;
        };

        struct TEvRpcFinishRequest : public TEventLocal<TEvRpcFinishRequest, EvRpcFinishRequest> {
            explicit TEvRpcFinishRequest(const grpc::Status& status)
                : Status(status)
            {}

            const grpc::Status Status;
        };
    };

    class TLocalRpcCtx final : public TLocalRpcStreamingCtxBase<TRpcIn, TRpcOut> {
        using TBase = TLocalRpcStreamingCtxBase<TRpcIn, TRpcOut>;

    public:
        TLocalRpcCtx(const TActorSystem* actorSystem, const TActorId& selfId, const TSettings& settings)
            : TBase(settings)
            , ActorSystem(actorSystem)
            , SelfId(selfId)
        {
            Y_VALIDATE(ActorSystem, "ActorSystem is not set");
            Y_VALIDATE(SelfId, "SelfId is not set");
        }

    protected:
        void DoAttach(TActorId actor) final {
            RpcActor = actor;
            ActorSystem->Send(actor, new TEvPrivate::TEvRpcActorAttached(SelfId));
        }

        void DoRead() final {
            ActorSystem->Send(SelfId, new TEvPrivate::TEvRpcReadRequest());
        }

        void DoWrite(TRpcOut&& message, const grpc::WriteOptions& options) final {
            Y_UNUSED(options);
            ActorSystem->Send(SelfId, new TEvPrivate::TEvRpcWriteRequest(std::move(message)));

            Y_VALIDATE(RpcActor, "RpcActor is not set before write request");
            auto ev = std::make_unique<TEvWriteFinished>();
            ev->Success = true;
            ActorSystem->Send(RpcActor, ev.release());
        }

        void DoFinish(const grpc::Status& status) final {
            ActorSystem->Send(SelfId, new TEvPrivate::TEvRpcFinishRequest(status));

            if (RpcActor) {
                ActorSystem->Send(RpcActor, new TEvNotifiedWhenDone(/* success */ true));
            }
        }

    private:
        const TActorSystem* const ActorSystem = nullptr;
        const TActorId SelfId;
        TActorId RpcActor;
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
        struct TEvExtractReadyEvents : public TEventLocal<TEvExtractReadyEvents, EvExtractReadyEvents> {
            explicit TEvExtractReadyEvents(NThreading::TPromise<std::vector<TLocalTopicReadyEvent>> eventsPromise)
                : EventsPromise(std::move(eventsPromise))
            {}

            NThreading::TPromise<std::vector<TLocalTopicReadyEvent>> EventsPromise;
        };

        struct TEvEventsConsumed : public TEventLocal<TEvEventsConsumed, EvEventsConsumed> {
            explicit TEvEventsConsumed(i64 size)
                : Size(size)
            {}

            const i64 Size = 0;
        };

        struct TEvSessionFinished : public TEventLocal<TEvSessionFinished, EvSessionFinished> {
        };
    };

    struct TSettings {
        TString Database;
        TReaderCounters::TPtr Counters;
    };

    explicit TLocalTopicReadSessionActor(const TSettings& actorSettings, const TReadSessionSettings& sessionSettings)
        : Settings(sessionSettings)
        , ReadSettings(GetReadSettings(sessionSettings))
        , Database(actorSettings.Database)
        , MaxMemoryUsage(sessionSettings.MaxMemoryUsageBytes_)
        , Counters(actorSettings.Counters)
    {
        Y_VALIDATE(Database, "Missing database");
        Y_VALIDATE(Counters, "Missing counters");
    }

    void Bootstrap() {
        Become(&TThis::StateFunc);

        LOG_D("Start local topic read session");
        StartSession();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TSessionEvents::TEvExtractReadyEvents, Handle);
        hFunc(TSessionEvents::TEvEventsConsumed, Handle);
        hFunc(TSessionEvents::TEvSessionFinished, Handle);
        hFunc(TEvPrivate::TEvRpcActorAttached, Handle);
        hFunc(TEvPrivate::TEvRpcReadRequest, Handle);
        hFunc(TEvPrivate::TEvRpcWriteRequest, Handle);
        hFunc(TEvPrivate::TEvRpcFinishRequest, Handle);
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

        Y_VALIDATE(sessionSettings.Topics_.size() == 1, "Only one topic is supported");
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

    void Handle(TSessionEvents::TEvExtractReadyEvents::TPtr& ev) {
        // TODO
        Y_UNUSED(ev);
    }

    void Handle(TSessionEvents::TEvEventsConsumed::TPtr& ev) {
        const auto size = ev->Get()->Size;
        LOG_T("Handled events with size: " << size);
        InflightMemory = std::max(InflightMemory - size, static_cast<i64>(0));
        ContinueReading();
    }

    void Handle(TSessionEvents::TEvSessionFinished::TPtr&) {
        LOG_D("Local topic read session finished from client side");
        CloseSession(EStatus::SUCCESS);
    }

    void Handle(TEvPrivate::TEvRpcActorAttached::TPtr& ev) {
        Y_VALIDATE(!RpcActor, "RpcActor is already set");

        RpcActor = ev->Get()->RpcActor;
        LOG_D("RpcActor attached: " << RpcActor);

        SendInitMessage();
    }

    void Handle(TEvPrivate::TEvRpcReadRequest::TPtr&) {
        PendingRpcResponses++;
        LOG_D("Rpc read request, PendingRpcResponses: " << PendingRpcResponses << ", RpcResponses #" << RpcResponses.size());
        SendSessionEvents();
    }

    void Handle(TEvPrivate::TEvRpcWriteRequest::TPtr& ev) {
        const auto& message = ev->Get()->Message;
        const auto status = message.status();
        if (status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            IssuesFromMessage(message.issues(), issues);
            LOG_E("Rpc write request, got error " << status << ", reason: " << issues.ToOneLineString());
            return CloseSession(status, issues);
        }

        LOG_D("Rpc write request");

        switch (message.server_message_case()) {
            case Ydb::Topic::StreamReadMessage::FromServer::kInitResponse:
                ComputeSessionMessage(message.init_response());
                break;
            case Ydb::Topic::StreamReadMessage::FromServer::kReadResponse:
                ComputeSessionMessage(message.read_response());
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

    void Handle(TEvPrivate::TEvRpcFinishRequest::TPtr& ev) {
        const auto& status = ev->Get()->Status;
        if (!status.ok()) {
            LOG_E("Rpc session finished with error status code: " << static_cast<ui64>(status.error_code()) << ", message: " << status.error_message());
        } else {
            LOG_D("Rpc session successfully finished");
        }

        CloseSession(status, "Read session closed");
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        const auto sourceType = ev->Get()->SourceType;
        const auto reason = ev->Get()->Reason;
        Y_VALIDATE(sourceType == TEvStreamTopicReadRequest::EventType, "Unexpected undelivered event: " << sourceType << ", reason: " << reason);

        LOG_E("PQ read service is unavailable, reason: " << reason);
        CloseSession(EStatus::INTERNAL_ERROR, "PQ read service is unavailable, please contact internal support");
    }

    void StartSession() const {
        auto ctx = MakeIntrusive<TLocalRpcCtx>(ActorContext().ActorSystem(), SelfId(), TLocalRpcCtx::TSettings{
            .Database = Database,
            .PeerName = "localhost/local_topic_rpc_read",
            .RequestType = Settings.RequestType_.empty() ? std::nullopt : std::optional<TString>(Settings.RequestType_),
            .ParentTraceId = TString(Settings.TraceParent_),
            .TraceId = TString(Settings.TraceId_),
            .RpcMethodName = "TopicService.StreamRead",
        });

        for (const auto& [key, value] : Settings.Header_) {
            ctx->PutPeerMeta(TString(key), TString(value));
        }

        Send(NGRpcProxy::V1::GetPQReadServiceActorID(), new TEvStreamTopicReadRequest(std::move(ctx), {.RequestType = NJaegerTracing::ERequestType::TOPIC_STREAMREAD}), IEventHandle::FlagTrackDelivery);
    }

    void SendInitMessage() {
        LOG_I("Sending init message, Consumer: " << ReadSettings.Consumer);

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
        LOG_D("Session initialized with id: " << message.session_id());
        SessionStarted = true;
        ContinueReading();
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::ReadResponse& message) {
        // TODO
        Y_UNUSED(message);
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::CommitOffsetResponse& message) {
        // TODO
        Y_UNUSED(message);
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::PartitionSessionStatusResponse& message) {
        // TODO
        Y_UNUSED(message);
    }

    void ComputeSessionMessage(const Ydb::Topic::UpdateTokenResponse&) {
        Y_VALIDATE(false, "Unexpected message type: UpdateTokenResponse");
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::StartPartitionSessionRequest& message) {
        // TODO
        Y_UNUSED(message);
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::StopPartitionSessionRequest& message) {
        // TODO
        Y_UNUSED(message);
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::UpdatePartitionSession& message) {
        // TODO
        Y_UNUSED(message);
    }

    void ComputeSessionMessage(const Ydb::Topic::StreamReadMessage::EndPartitionSession& message) {
        // TODO
        Y_UNUSED(message);
    }

    void ContinueReading() {
        if (!SessionStarted) {
            LOG_D("Session not started yet, skip reading");
            return;
        }

        if (InflightMemory >= MaxMemoryUsage) {
            LOG_D("Max memory usage reached, skip reading");
            return;
        }

        const auto readMemoryBudget = MaxMemoryUsage - InflightMemory;
        if (ServerMemoryDelta >= readMemoryBudget) {
            LOG_D("Server already has enough memory, skip reading");
            return;
        }

        const auto bytesToRead = readMemoryBudget - ServerMemoryDelta;
        LOG_T("Reading " << bytesToRead << " bytes");

        TRpcIn message;
        message.mutable_read_request()->set_bytes_size(bytesToRead);
        AddSessionEvent(std::move(message));

        ServerMemoryDelta = readMemoryBudget;
    }

    void AddSessionEvent(TRpcIn&& message) {
        RpcResponses.push(message);
        LOG_T("Added session event, RpcResponses #" << RpcResponses.size());

        if (RpcActor) {
            SendSessionEvents();            
        }
    }

    void SendSessionEvents() {
        Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
        LOG_T("Going to send session events, PendingRpcResponses: " << PendingRpcResponses << ", RpcResponses #" << RpcResponses.size());

        while (PendingRpcResponses > 0 && !RpcResponses.empty()) {
            auto ev = std::make_unique<TLocalRpcCtx::TEvReadFinished>();
            ev->Success = true;
            ev->Record = std::move(RpcResponses.front());
            Send(RpcActor, ev.release());

            RpcResponses.pop();
            PendingRpcResponses--;
        }
    }

    void CloseSession(EStatus status, const NYql::TIssues& issues = {}) {
        // TODO
        Y_UNUSED(status, issues);
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
    const i64 MaxMemoryUsage = 0;
    const TReaderCounters::TPtr Counters;  // TODO: report counters

    TActorId RpcActor;
    bool SessionStarted = false;
    i64 InflightMemory = 0;
    i64 ServerMemoryDelta = 0;

    // Outgoing messages to PQ
    ui64 PendingRpcResponses = 0;
    std::queue<TRpcIn> RpcResponses;
};

// Supposed to be used from actor system, so all blocking methods are not supported.
// Read session is not thread safe and MUST be used from single actor.
class TLocalTopicReadSession final : public IReadSession {
public:
    TLocalTopicReadSession(const TLocalTopicReadSettings& localSettings, const TReadSessionSettings& sessionSettings)
        : ActorSystem(localSettings.ActorSystem)
        , Counters(SetupCounters(sessionSettings))
        , SessionId(CreateGuidAsString())
    {
        Y_VALIDATE(ActorSystem, "Actor system is required for local topic read session");
        ValidateSettings(sessionSettings);
        Start(localSettings.Database, sessionSettings);
    }

    ~TLocalTopicReadSession() {
        try {
            Close(TDuration::Zero());
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

        if (ReadSessionActor) {
            ActorSystem->Send(ReadSessionActor, new TLocalTopicReadSessionActor::TSessionEvents::TEvEventsConsumed(totalSize));
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
        WaitEvent();

        if (!ReadSessionActor) {
            return true;
        }

        ActorSystem->Send(ReadSessionActor, new TLocalTopicReadSessionActor::TSessionEvents::TEvSessionFinished());
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
        Y_VALIDATE(settings.DecompressionExecutor_, "DecompressionExecutor is required for local topic read session");
        Y_VALIDATE(settings.ClientTimeout_ == TDuration::Max(), "Timeout is not allowed for local topic read session");
        Y_VALIDATE(settings.Deadline_ == TDeadline::Max(), "Timeout is not allowed for local topic read session");

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

    void Start(const TString& database, const TReadSessionSettings& sessionSettings) {
        Y_VALIDATE(!ReadSessionActor, "Read session is already started");
        ReadSessionActor = ActorSystem->Register(new TLocalTopicReadSessionActor({
            .Database = database,
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

    TActorSystem* ActorSystem = nullptr;
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
