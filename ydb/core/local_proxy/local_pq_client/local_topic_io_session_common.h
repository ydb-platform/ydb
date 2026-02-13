#pragma once

#include "local_topic_client_helpers.h"

#include <ydb/core/grpc_services/local_rpc/local_rpc_bi_streaming.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <contrib/libs/grpc/include/grpcpp/support/status.h>

#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <queue>

namespace NKikimr::NKqp {

#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::YDB_SDK, LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::YDB_SDK, LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::YDB_SDK, LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::YDB_SDK, LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::YDB_SDK, LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::YDB_SDK, LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*NActors::TlsActivationContext, NKikimrServices::YDB_SDK, LogPrefix() << stream)

template <typename TDerived, typename TRpcProtoIn, typename TRpcProtoOut, typename TEventVariant, typename TCounters>
class TLocalTopicIoSessionActor : public NActors::TActorBootstrapped<TDerived>, public NActors::IActorExceptionHandler {
    static constexpr TDuration COUNTERS_REFRESH_INTERVAL = TDuration::Seconds(1);

    using TBase = NActors::TActorBootstrapped<TDerived>;

protected:
    using TEvent = typename TLocalTopicSessionBase<TEventVariant>::TEvent;
    using TRpcIn = TRpcProtoIn;
    using TRpcOut = TRpcProtoOut;
    using TLocalRpcCtx = NRpcService::TLocalRpcBiStreamingCtx<TRpcIn, TRpcOut>;

    struct TEvPrivate {
        enum EEv {
            EvExtractReadyEvents = TLocalRpcCtx::TRpcEvents::EvEnd,
            EvEventsConsumed,
            EvSessionFinished,
            EvEnd,
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
    };

public:
    struct TSessionEvents : private TEvPrivate {
        // Events from local sdk topic session

        struct TEvExtractReadyEvents : public TEventLocal<TEvExtractReadyEvents, TEvPrivate::EvExtractReadyEvents> {
            explicit TEvExtractReadyEvents(NThreading::TPromise<std::vector<TEvent>> eventsPromise)
                : EventsPromise(std::move(eventsPromise))
            {}

            NThreading::TPromise<std::vector<TEvent>> EventsPromise;
        };

        struct TEvEventsConsumed : public TEventLocal<TEvEventsConsumed, TEvPrivate::EvEventsConsumed> {
            explicit TEvEventsConsumed(ui64 eventsCount)
                : EventsCount(eventsCount)
            {}

            TEvEventsConsumed(i64 size, ui64 eventsCount)
                : Size(size)
                , EventsCount(eventsCount)
            {}

            const i64 Size = 0; // Return this size to free memory
            const ui64 EventsCount = 0;
        };

        struct TEvSessionFinished : public TEventLocal<TEvSessionFinished, TEvPrivate::EvSessionFinished> {
            explicit TEvSessionFinished(bool force)
                : Force(force)
            {}

            const bool Force = false;
        };
    };

    struct TSettings {
        TString Database;
        std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;
        TCounters::TPtr Counters;
    };

    TLocalTopicIoSessionActor(const TString& logPrefix, const TSettings& actorSettings, const TString& topic, i64 maxMemoryUsage)
        : Topic(topic)
        , MaxMemoryUsage(maxMemoryUsage)
        , Counters(actorSettings.Counters)
        , LogPrefixStr(logPrefix)
        , Database(actorSettings.Database)
        , CredentialsProvider(actorSettings.CredentialsProvider)
    {
        Y_VALIDATE(Topic, "Missing topic path");
        Y_VALIDATE(MaxMemoryUsage > 0, "MaxMemoryUsage must be positive");
        Y_VALIDATE(Counters, "Missing counters");
        Y_VALIDATE(Database, "Missing database");
    }

    void Bootstrap() {
        LOG_I("Start local topic session, MaxMemoryUsage: " << MaxMemoryUsage);
        StartSession();
        TBase::Become(&TDerived::StateFunc);
        TBase::Schedule(COUNTERS_REFRESH_INTERVAL, new TEvents::TEvWakeup());
    }

    bool OnUnhandledException(const std::exception& e) final {
        LOG_E("Got unexpected exception: " << e.what());
        CloseSession(NYdb::EStatus::INTERNAL_ERROR, TStringBuilder() << "Got unexpected exception: " << e.what());
        return true;
    }

protected:
    virtual void StartSession() = 0;

    virtual void SendInitMessage() = 0;

    virtual void HandleRpcMessage(TRpcOut& message) = 0;

    TString LogPrefix() const {
        return TStringBuilder() << "[" << LogPrefixStr << "] Database: " << Database << ", Topic: " << Topic << ", SelfId: " << TBase::SelfId() << ". ";
    }

    template <typename TRpcEvent, typename TSettings>
    std::unique_ptr<TRpcEvent> CreateRpcBiStreamingEvent(const NYdb::TRequestSettings<TSettings>& settings, const TString& methodName, NJaegerTracing::ERequestType requestType) const {
        const auto& token = CredentialsProvider ? std::optional<TString>(CredentialsProvider->GetAuthInfo()) : std::nullopt;
        auto ctx = MakeIntrusive<TLocalRpcCtx>(TBase::ActorContext().ActorSystem(), TBase::SelfId(), typename TLocalRpcCtx::TSettings{
            .Database = Database,
            .Token = token,
            .PeerName = TStringBuilder() << "localhost/LocalTopicRpc" << methodName,
            .RequestType = settings.RequestType_.empty() ? std::nullopt : std::optional<TString>(settings.RequestType_),
            .ParentTraceId = TString(settings.TraceParent_),
            .TraceId = TString(settings.TraceId_),
            .RpcMethodName = TStringBuilder() << "TopicService." << methodName,
        });

        for (const auto& [key, value] : settings.Header_) {
            ctx->PutPeerMeta(TString(key), TString(value));
        }

        auto ev = std::make_unique<TRpcEvent>(std::move(ctx), NGRpcService::TRequestAuxSettings{.RequestType = requestType});

        if (token) {
            ev->SetInternalToken(MakeIntrusive<NACLib::TUserToken>(*token));
        }

        return ev;
    }

    void CloseSession(NYdb::EStatus status, const NYql::TIssues& issues = {}) {
        const bool success = status == NYdb::EStatus::SUCCESS;
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
        AddOutgoingEvent(NYdb::NTopic::TSessionClosedEvent(status, NYdb::NAdapters::ToSdkIssues(issues)));

        // Close session on server side
        while (PendingRpcResponses) {
            SendSessionEventFail();
        }
        if (RpcActor) {
            TBase::Send(RpcActor, new TLocalRpcCtx::TEvNotifiedWhenDone(success));
        }
    }

    void CloseSession(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues = {}) {
        CloseSession(static_cast<NYdb::EStatus>(status), issues);
    }

    void CloseSession(NYdb::EStatus status, const TString& message) {
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
                return CloseSession(NYdb::EStatus::SUCCESS, issues);
            case grpc::CANCELLED:
                return CloseSession(NYdb::EStatus::CANCELLED, issues);
            case grpc::UNKNOWN:
                return CloseSession(NYdb::EStatus::UNDETERMINED, issues);
            case grpc::DEADLINE_EXCEEDED:
                return CloseSession(NYdb::EStatus::TIMEOUT, issues);
            case grpc::NOT_FOUND:
                return CloseSession(NYdb::EStatus::NOT_FOUND, issues);
            case grpc::ALREADY_EXISTS:
                return CloseSession(NYdb::EStatus::ALREADY_EXISTS, issues);
            case grpc::RESOURCE_EXHAUSTED:
                return CloseSession(NYdb::EStatus::OVERLOADED, issues);
            case grpc::ABORTED:
                return CloseSession(NYdb::EStatus::ABORTED, issues);
            case grpc::OUT_OF_RANGE:
                return CloseSession(NYdb::EStatus::CLIENT_OUT_OF_RANGE, issues);
            case grpc::UNIMPLEMENTED:
                return CloseSession(NYdb::EStatus::UNSUPPORTED, issues);
            case grpc::UNAVAILABLE:
                return CloseSession(NYdb::EStatus::UNAVAILABLE, issues);
            case grpc::INVALID_ARGUMENT:
            case grpc::FAILED_PRECONDITION:
                return CloseSession(NYdb::EStatus::PRECONDITION_FAILED, issues);
            case grpc::UNAUTHENTICATED:
            case grpc::PERMISSION_DENIED:
                return CloseSession(NYdb::EStatus::UNAUTHORIZED, issues);
            default:
                return CloseSession(NYdb::EStatus::INTERNAL_ERROR, issues);
        }
    }

    void HandleWakeup() {
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
        TBase::Schedule(COUNTERS_REFRESH_INTERVAL, new TEvents::TEvWakeup());
    }

    // Events from local sdk topic session

    void Handle(TSessionEvents::TEvExtractReadyEvents::TPtr& ev) {
        LOG_T("Got extract ready events request, OutgoingEvents #" << OutgoingEvents.size());

        Y_VALIDATE(!EventsPromise, "Can not handle extract event in parallel");
        EventsPromise = std::move(ev->Get()->EventsPromise);
        SendOutgoingEvents();
    }

    void Handle(TSessionEvents::TEvEventsConsumed::TPtr& ev) {
        const auto eventsCount = ev->Get()->EventsCount;
        Counters->MessagesInflight->Sub(eventsCount);
        LOG_T("Handled #" << eventsCount << " events");
    }

    void Handle(TSessionEvents::TEvSessionFinished::TPtr& ev) {
        const bool force = ev->Get()->Force;
        LOG_I("Local topic session finished from client side, force: " << force);

        CloseSession(NYdb::EStatus::SUCCESS);

        if (force) {
            TBase::PassAway();
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
        auto response = std::make_unique<typename TLocalRpcCtx::TEvWriteFinished>();

        if (SessionClosed) {
            LOG_D("Rpc write request skipped, session is closed");
            response->Success = false;
            TBase::Send(RpcActor, response.release());
            return;
        }

        response->Success = true;
        TBase::Send(RpcActor, response.release());

        auto& message = ev->Get()->Message;
        const auto status = message.status();
        if (status != Ydb::StatusIds::SUCCESS) {
            NYql::TIssues issues;
            IssuesFromMessage(message.issues(), issues);
            LOG_E("Rpc write request, got error " << status << ", reason: " << issues.ToOneLineString());
            return CloseSession(status, issues);
        }

        LOG_T("Rpc write request: " << static_cast<i64>(message.server_message_case()));
        HandleRpcMessage(message);
    }

    void Handle(TLocalRpcCtx::TRpcEvents::TEvFinishRequest::TPtr& ev) {
        const auto& status = ev->Get()->Status;
        if (!status.ok()) {
            LOG_E("Rpc session finished with error status code: " << static_cast<ui64>(status.error_code()) << ", message: " << status.error_message());
        } else {
            LOG_I("Rpc session successfully finished");
        }

        CloseSession(status, "Session closed");
    }

    void ComputeSessionMessage(const Ydb::Topic::UpdateTokenResponse& message) {
        Y_UNUSED(message);
        Y_VALIDATE(false, "Unexpected message type: UpdateTokenResponse");
    }

    // Events to PQ read service

    void AddSessionEvent(TRpcIn&& message) {
        if (SessionClosed) {
            LOG_D("Session already closed, skip session event");
            return;
        }

        RpcResponses.emplace(std::move(message));
        LOG_T("Added session event: " << static_cast<i64>(RpcResponses.back().client_message_case()));

        if (RpcActor) {
            SendSessionEvents();            
        }
    }

    // Events to local sdk topic session

    void AddOutgoingEvent(TEventVariant&& event, i64 size = 0) {
        Counters->MessagesInflight->Inc();

        OutgoingEvents.emplace(std::move(event), size);
        LOG_T("Added outgoing event: " << OutgoingEvents.back().Event.index());

        SendOutgoingEvents();
    }

    const TString Topic;
    const i64 MaxMemoryUsage = 0;
    const TCounters::TPtr Counters;
    TInstant SessionStartedAt;
    i64 InflightMemory = 0;

private:
    void SendSessionEvents() {
        Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
        LOG_T("Going to send session events, PendingRpcResponses: " << PendingRpcResponses << ", RpcResponses #" << RpcResponses.size());

        while (PendingRpcResponses && !RpcResponses.empty()) {
            SendSessionEvent(std::move(RpcResponses.front()));
            RpcResponses.pop();
        }
    }

    void SendSessionEvent(TRpcIn&& message, bool success = true) {
        LOG_T("Sending session event: " << static_cast<i64>(message.client_message_case()) << ", success: " << success);
        Y_VALIDATE(PendingRpcResponses > 0, "Rpc read is not expected");
        PendingRpcResponses--;

        auto ev = std::make_unique<typename TLocalRpcCtx::TEvReadFinished>();
        ev->Success = success;
        ev->Record = std::move(message);

        Y_VALIDATE(RpcActor, "RpcActor is not set before read request");
        TBase::Send(RpcActor, ev.release());
    }

    void SendSessionEventFail() {
        SendSessionEvent({}, /* success */ false);
    }

    void SendOutgoingEvents() {
        if (!EventsPromise || OutgoingEvents.empty()) {
            return;
        }

        LOG_T("Going to send outgoing events, OutgoingEvents #" << OutgoingEvents.size());

        bool closeEventSent = false;
        std::vector<TEvent> result;
        result.reserve(OutgoingEvents.size());
        while (!OutgoingEvents.empty()) {
            result.push_back(std::move(OutgoingEvents.front()));
            OutgoingEvents.pop();

            if (std::holds_alternative<NYdb::NTopic::TSessionClosedEvent>(result.back().Event)) {
                LOG_I("Sent close session event, finishing");
                closeEventSent = true;
            }
        }

        EventsPromise->SetValue(std::move(result));
        EventsPromise.reset();

        if (closeEventSent) {
            TBase::PassAway();
        }
    }

    const TString LogPrefixStr;
    const TString Database;
    const std::shared_ptr<NYdb::ICredentialsProvider> CredentialsProvider;

    bool SessionClosed = false;
    TInstant LastCountersUpdateAt;

    // Outgoing messages to PQ
    TActorId RpcActor;
    ui64 PendingRpcResponses = 0;
    std::queue<TRpcIn> RpcResponses;

    // Outgoing messages to local sdk topic session
    std::queue<TEvent> OutgoingEvents;
    std::optional<NThreading::TPromise<std::vector<TEvent>>> EventsPromise;
};

} // namespace NKikimr::NKqp
