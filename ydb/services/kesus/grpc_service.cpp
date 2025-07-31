#include "grpc_service.h"

#include <ydb/core/kesus/proxy/proxy.h>
#include <ydb/core/kesus/proxy/events.h>
#include <ydb/core/kesus/tablet/events.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/service_coordination.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/core/base/ticket_parser.h>

#include <ydb/library/grpc/server/event_callback.h>
#include <ydb/library/grpc/server/grpc_async_ctx_base.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NKesus {

////////////////////////////////////////////////////////////////////////////////

class TGRpcSessionActor
    : public TActorBootstrapped<TGRpcSessionActor>
{
    static constexpr TDuration MinPingPeriod = TDuration::MilliSeconds(10);
    static constexpr TDuration MaxPingPeriod = TDuration::Seconds(5);

public:
    using TRequest = Ydb::Coordination::SessionRequest;
    using TResponse = Ydb::Coordination::SessionResponse;
    using IContext = NGRpcServer::IGRpcStreamingContext<TRequest, TResponse>;
    using TGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
        TRequest,
        TResponse,
        TKesusGRpcService,
        NKikimrServices::GRPC_SERVER>;

    TGRpcSessionActor(THolder<NGRpcService::TEvCoordinationSessionRequest> requestEvent)
        : RequestEvent(std::move(requestEvent))
    { }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KESUS_REQ;
    }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        RequestEvent->Attach(SelfId());
        BeginAuth();
    }

private:
    void AbandonSession() {
        Send(ProxyActor, new TEvKesusProxy::TEvCancelRequest());
    }

    void PassAway() override {
        if (ProxyActor && AttachSessionSent) {
            AbandonSession();
        }

        if (RequestEvent) {
            // Write to audit log if it is needed and we have not written yet.
            RequestEvent->AuditLogRequestEnd(Ydb::StatusIds::SUCCESS);
        }

        TActorBootstrapped::PassAway();
    }

private:
    struct TEvPrivate {
        enum EEv {
            EvPingScheduled,
        };

        struct TEvPingScheduled : public TEventLocal<TEvPingScheduled, EvPingScheduled> {
            TEvPingScheduled() = default;
        };
    };

    void Reply(TResponse&& response) {
        RequestEvent->Write(std::move(response));
    }

    void ReplyLast(TResponse&& response, Ydb::StatusIds::StatusCode status, const grpc::Status& grpcStatus = grpc::Status::OK) {
        RequestEvent->WriteAndFinish(std::move(response), status, grpcStatus);
        PassAway();
    }

    void ReplyError(const NKikimrKesus::TKesusError& error) {
        TResponse response;
        response.mutable_failure()->set_status(error.GetStatus());
        response.mutable_failure()->mutable_issues()->CopyFrom(error.GetIssues());
        ReplyLast(std::move(response), error.GetStatus());
    }

    void ReplyError(Ydb::StatusIds::StatusCode status, const TString& reason) {
        TResponse response;
        response.mutable_failure()->set_status(status);
        response.mutable_failure()->add_issues()->set_message(reason);
        ReplyLast(std::move(response), status);
    }

    void Handle(const TEvKesusProxy::TEvProxyError::TPtr& ev) {
        ReplyError(ev->Get()->Error);
    }

private:
    void BeginAuth() {
        if (const auto& token = RequestEvent->GetSerializedToken()) {
            UserToken.Reset(new TUserToken(token));
        }
        ReadyToStart();
    }

private:
    void ReadyToStart() {
        RequestEvent->Read();
        Become(&TThis::StateWaitStart);
    }

    void HandleStart(IContext::TEvReadFinished::TPtr& ev) {
        const auto* msg = ev->Get();
        if (!msg->Success) {
            // We die on any read failure
            return PassAway();
        }

        Y_ABORT_UNLESS(!StartRequest);
        StartRequest.Reset(ev->Release());
        if (StartRequest->Record.request_case() != TRequest::kSessionStart) {
            RequestEvent->Finish(Ydb::StatusIds::BAD_REQUEST, grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                "First message must be a SessionStart"));
            return PassAway();
        }

        PingPeriod = TDuration::MilliSeconds(StartRequest->Record.session_start().timeout_millis() / 3);
        if (PingPeriod > MaxPingPeriod) {
            PingPeriod = MaxPingPeriod;
        } else if (PingPeriod < MinPingPeriod) {
            PingPeriod = MinPingPeriod;
        }

        KesusPath = StartRequest->Record.session_start().path();

        auto resolve = MakeHolder<TEvKesusProxy::TEvResolveKesusProxy>(KesusPath);
        if (!Send(MakeKesusProxyServiceId(), resolve.Release())) {
            RequestEvent->Finish(Ydb::StatusIds::UNSUPPORTED, grpc::Status(grpc::StatusCode::UNIMPLEMENTED,
                "Coordination service not implemented on this server"));
            return PassAway();
        }

        Become(&TThis::StateResolve);
    }

    STFUNC(StateWaitStart) {
        switch (ev->GetTypeRewrite()) {
            hFunc(IContext::TEvReadFinished, HandleStart);
            cFunc(IContext::TEvNotifiedWhenDone::EventType, PassAway);

            default:
                Y_ABORT("Unexpected event 0x%x for TGRpcSessionActor", ev->GetTypeRewrite());
        }
    }

private:
    bool CheckAccess(ui32 access) const {
        if (UserToken) {
            if (!SecurityObject) {
                // FIXME: is it possible to not have a security object?
                return true;
            }

            return SecurityObject->CheckAccess(access, *UserToken);
        } else {
            const auto& ctx = TActivationContext::AsActorContext();

            // Anonymous users have all access unless token is enforced
            if (AppData(ctx)->EnforceUserTokenRequirement) {
                return false;
            }

            return true;
        }
    }

    void HandleResolve(const TEvKesusProxy::TEvAttachProxyActor::TPtr& ev) {
        Y_ABORT_UNLESS(!ProxyActor);
        ProxyActor = ev->Get()->ProxyActor;
        SecurityObject = ev->Get()->SecurityObject;

        ReadAllowed = CheckAccess(NACLib::EAccessRights::SelectRow);
        WriteAllowed = CheckAccess(NACLib::EAccessRights::UpdateRow);
        EraseAllowed = CheckAccess(NACLib::EAccessRights::EraseRow);

        if (!(ReadAllowed || WriteAllowed || EraseAllowed)) {
            // Don't allow session attach if nothing is allowed
            return ReplyError(Ydb::StatusIds::UNAUTHORIZED,
                "User has no access to the coordination node");
        }

        Y_ABORT_UNLESS(StartRequest);
        Y_ABORT_UNLESS(!AttachSessionSent);
        const auto& source = StartRequest->Record.session_start();
        Send(ProxyActor,
            new TEvKesus::TEvAttachSession(
                KesusPath,
                0,
                source.session_id(),
                source.timeout_millis(),
                source.description(),
                source.seq_no(),
                source.protection_key()));
        AttachSessionSent = true;
        StartRequest.Reset();

        Become(&TThis::StateAttach);
    }

    STFUNC(StateResolve) {
        switch (ev->GetTypeRewrite()) {
            cFunc(IContext::TEvNotifiedWhenDone::EventType, PassAway);
            hFunc(TEvKesusProxy::TEvProxyError, Handle);
            hFunc(TEvKesusProxy::TEvAttachProxyActor, HandleResolve);

            default:
                Y_ABORT("Unexpected event 0x%x for TGRpcSessionActor", ev->GetTypeRewrite());
        }
    }

private:
    void HandleAttach(const TEvKesus::TEvAttachSessionResult::TPtr& ev) {
        Y_ABORT_UNLESS(AttachSessionSent);
        const auto& record = ev->Get()->Record;
        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            return ReplyError(record.GetError());
        }

        SessionId = record.GetSessionId();
        SessionEstablished = true;

        TResponse response;
        auto started = response.mutable_session_started();
        started->set_session_id(SessionId);
        Reply(std::move(response));

        SendPing();

        RequestEvent->Read();
        Become(&TThis::StateWork);
    }

    STFUNC(StateAttach) {
        switch (ev->GetTypeRewrite()) {
            cFunc(IContext::TEvNotifiedWhenDone::EventType, PassAway);
            hFunc(TEvKesusProxy::TEvProxyError, Handle);
            hFunc(TEvKesus::TEvAttachSessionResult, HandleAttach);

            default:
                Y_ABORT("Unexpected event 0x%x for TGRpcSessionActor", ev->GetTypeRewrite());
        }
    }

private:
    void Handle(IContext::TEvReadFinished::TPtr& ev) {
        const auto* msg = ev->Get();
        if (!msg->Success) {
            // We die on any read failure
            return PassAway();
        }

        Process(msg->Record);

        // Start reading the next message
        // It will be ignored if Finish was called
        RequestEvent->Read();
    }

    void Handle(IContext::TEvWriteFinished::TPtr& ev) {
        const auto* msg = ev->Get();
        if (!msg->Success) {
            // We die on any write failure
            return PassAway();
        }
    }

    void Process(const TRequest& request) {
        switch (request.request_case()) {
            case TRequest::REQUEST_NOT_SET:
                return ReplyError(Ydb::StatusIds::BAD_REQUEST, "Empty request");
            case TRequest::kPing: {
                const auto& source = request.ping();
                TResponse response;
                response.mutable_pong()->set_opaque(source.opaque());
                return Reply(std::move(response));
            }
            case TRequest::kPong: {
                const auto& source = request.pong();
                if (source.opaque() != CurrentPingData) {
                    return ReplyError(Ydb::StatusIds::BAD_REQUEST, "Pong data mismatch");
                }
                // Just mark ping as inactive
                CurrentPingData = 0;
                return;
            }
            case TRequest::kSessionStart: {
                return ReplyError(Ydb::StatusIds::BAD_REQUEST, "Session cannot be started twice");
            }
            case TRequest::kSessionStop: {
                Send(ProxyActor, new TEvKesus::TEvDestroySession(KesusPath, 0, SessionId));
                return;
            }
            case TRequest::kAcquireSemaphore: {
                const auto& source = request.acquire_semaphore();
                if (!WriteAllowed) {
                    TResponse response;
                    auto* result = response.mutable_acquire_semaphore_result();
                    result->set_req_id(source.req_id());
                    result->set_status(Ydb::StatusIds::UNAUTHORIZED);
                    result->add_issues()->set_message("Write permission denied");
                    return Reply(std::move(response));
                }
                auto event = MakeHolder<TEvKesus::TEvAcquireSemaphore>();
                event->Record.SetKesusPath(KesusPath);
                event->Record.SetName(source.name());
                event->Record.SetTimeoutMillis(source.timeout_millis());
                event->Record.SetCount(source.count());
                event->Record.SetData(source.data());
                event->Record.SetEphemeral(source.ephemeral());
                Send(ProxyActor, event.Release(), 0, source.req_id());
                return;
            }
            case TRequest::kReleaseSemaphore: {
                const auto& source = request.release_semaphore();
                if (!WriteAllowed) {
                    TResponse response;
                    auto* result = response.mutable_release_semaphore_result();
                    result->set_req_id(source.req_id());
                    result->set_status(Ydb::StatusIds::UNAUTHORIZED);
                    result->add_issues()->set_message("Write permission denied");
                    return Reply(std::move(response));
                }
                auto event = MakeHolder<TEvKesus::TEvReleaseSemaphore>();
                event->Record.SetKesusPath(KesusPath);
                event->Record.SetName(source.name());
                Send(ProxyActor, event.Release(), 0, source.req_id());
                return;
            }
            case TRequest::kDescribeSemaphore: {
                const auto& source = request.describe_semaphore();
                if (!ReadAllowed) {
                    TResponse response;
                    auto* result = response.mutable_describe_semaphore_result();
                    result->set_req_id(source.req_id());
                    result->set_status(Ydb::StatusIds::UNAUTHORIZED);
                    result->add_issues()->set_message("Read permission denied");
                    return Reply(std::move(response));
                }
                auto event = MakeHolder<TEvKesus::TEvDescribeSemaphore>();
                event->Record.SetKesusPath(KesusPath);
                event->Record.SetName(source.name());
                event->Record.SetIncludeOwners(source.include_owners());
                event->Record.SetIncludeWaiters(source.include_waiters());
                event->Record.SetWatchData(source.watch_data());
                event->Record.SetWatchOwners(source.watch_owners());
                event->Record.SetSessionId(SessionId);
                Send(ProxyActor, event.Release(), 0, source.req_id());
                return;
            }
            case TRequest::kCreateSemaphore: {
                const auto& source = request.create_semaphore();
                if (!WriteAllowed) {
                    TResponse response;
                    auto* result = response.mutable_create_semaphore_result();
                    result->set_req_id(source.req_id());
                    result->set_status(Ydb::StatusIds::UNAUTHORIZED);
                    result->add_issues()->set_message("Create permission denied");
                    return Reply(std::move(response));
                }
                auto event = MakeHolder<TEvKesus::TEvCreateSemaphore>();
                event->Record.SetKesusPath(KesusPath);
                event->Record.SetName(source.name());
                event->Record.SetLimit(source.limit());
                event->Record.SetData(source.data());
                event->Record.SetSessionId(SessionId);
                Send(ProxyActor, event.Release(), 0, source.req_id());
                return;
            }
            case TRequest::kUpdateSemaphore: {
                const auto& source = request.update_semaphore();
                if (!WriteAllowed) {
                    TResponse response;
                    auto* result = response.mutable_update_semaphore_result();
                    result->set_req_id(source.req_id());
                    result->set_status(Ydb::StatusIds::UNAUTHORIZED);
                    result->add_issues()->set_message("Update permission denied");
                    return Reply(std::move(response));
                }
                auto event = MakeHolder<TEvKesus::TEvUpdateSemaphore>();
                event->Record.SetKesusPath(KesusPath);
                event->Record.SetName(source.name());
                event->Record.SetData(source.data());
                event->Record.SetSessionId(SessionId);
                Send(ProxyActor, event.Release(), 0, source.req_id());
                return;
            }
            case TRequest::kDeleteSemaphore: {
                const auto& source = request.delete_semaphore();
                if (!EraseAllowed) {
                    TResponse response;
                    auto* result = response.mutable_delete_semaphore_result();
                    result->set_req_id(source.req_id());
                    result->set_status(Ydb::StatusIds::UNAUTHORIZED);
                    result->add_issues()->set_message("Delete permission denied");
                    return Reply(std::move(response));
                }
                auto event = MakeHolder<TEvKesus::TEvDeleteSemaphore>();
                event->Record.SetKesusPath(KesusPath);
                event->Record.SetName(source.name());
                event->Record.SetForce(source.force());
                event->Record.SetSessionId(SessionId);
                Send(ProxyActor, event.Release(), 0, source.req_id());
                return;
            }
            case TRequest::kUnsupported5:
            case TRequest::kUnsupported6:
            case TRequest::kUnsupported13:
            case TRequest::kUnsupported14:
            case TRequest::kUnsupported15:
                break;
        }

        return ReplyError(Ydb::StatusIds::BAD_REQUEST, "Unimplemented request");
    }

    void Handle(const TEvKesus::TEvDestroySessionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            ReplyError(record.GetError());
            return;
        }

        TResponse response;
        auto stopped = response.mutable_session_stopped();
        stopped->set_session_id(SessionId);
        ReplyLast(std::move(response), Ydb::StatusIds::BAD_SESSION);
    }

    void Handle(const TEvKesus::TEvAcquireSemaphorePending::TPtr& ev) {
        Y_UNUSED(ev);

        TResponse response;
        auto result = response.mutable_acquire_semaphore_pending();
        result->set_req_id(ev->Cookie);
        Reply(std::move(response));
    }

    void Handle(const TEvKesus::TEvAcquireSemaphoreResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TResponse response;
        auto result = response.mutable_acquire_semaphore_result();
        result->set_req_id(ev->Cookie);
        result->set_status(record.GetError().GetStatus());
        result->mutable_issues()->CopyFrom(record.GetError().GetIssues());
        result->set_acquired(record.GetAcquired());
        Reply(std::move(response));
    }

    void Handle(const TEvKesus::TEvReleaseSemaphoreResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TResponse response;
        auto result = response.mutable_release_semaphore_result();
        result->set_req_id(ev->Cookie);
        result->set_status(record.GetError().GetStatus());
        result->mutable_issues()->CopyFrom(record.GetError().GetIssues());
        result->set_released(record.GetReleased());
        Reply(std::move(response));
    }

    void Handle(const TEvKesus::TEvDescribeSemaphoreResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TResponse response;
        auto result = response.mutable_describe_semaphore_result();
        result->set_req_id(ev->Cookie);
        result->set_status(record.GetError().GetStatus());
        result->mutable_issues()->CopyFrom(record.GetError().GetIssues());
        if (record.HasSemaphoreDescription()) {
            result->mutable_semaphore_description()->CopyFrom(record.GetSemaphoreDescription());
        }
        result->set_watch_added(record.GetWatchAdded());
        Reply(std::move(response));
    }

    void Handle(const TEvKesus::TEvDescribeSemaphoreChanged::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TResponse response;
        auto result = response.mutable_describe_semaphore_changed();
        result->set_req_id(ev->Cookie);
        result->set_data_changed(record.GetDataChanged());
        result->set_owners_changed(record.GetOwnersChanged());
        Reply(std::move(response));
    }

    void Handle(const TEvKesus::TEvCreateSemaphoreResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TResponse response;
        auto result = response.mutable_create_semaphore_result();
        result->set_req_id(ev->Cookie);
        result->set_status(record.GetError().GetStatus());
        result->mutable_issues()->CopyFrom(record.GetError().GetIssues());
        Reply(std::move(response));
    }

    void Handle(const TEvKesus::TEvUpdateSemaphoreResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TResponse response;
        auto result = response.mutable_update_semaphore_result();
        result->set_req_id(ev->Cookie);
        result->set_status(record.GetError().GetStatus());
        result->mutable_issues()->CopyFrom(record.GetError().GetIssues());
        Reply(std::move(response));
    }

    void Handle(const TEvKesus::TEvDeleteSemaphoreResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        TResponse response;
        auto result = response.mutable_delete_semaphore_result();
        result->set_req_id(ev->Cookie);
        result->set_status(record.GetError().GetStatus());
        result->mutable_issues()->CopyFrom(record.GetError().GetIssues());
        Reply(std::move(response));
    }

    void SendPing() {
        Y_ABORT_UNLESS(CurrentPingData == 0);
        while (CurrentPingData == 0) {
            CurrentPingData = RandomNumber<ui64>();
        }

        TResponse response;
        auto ping = response.mutable_ping();
        ping->set_opaque(CurrentPingData);
        Reply(std::move(response));

        Schedule(PingPeriod, new TEvPrivate::TEvPingScheduled());
    }

    void Handle(const TEvPrivate::TEvPingScheduled::TPtr& ev) {
        Y_UNUSED(ev);

        if (CurrentPingData != 0) {
            ReplyError(Ydb::StatusIds::TIMEOUT, "Ping timeout");
            return;
        }

        SendPing();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(IContext::TEvReadFinished, Handle);
            hFunc(IContext::TEvWriteFinished, Handle);
            cFunc(IContext::TEvNotifiedWhenDone::EventType, PassAway);

            hFunc(TEvKesusProxy::TEvProxyError, Handle);
            hFunc(TEvKesus::TEvDestroySessionResult, Handle);
            hFunc(TEvKesus::TEvAcquireSemaphorePending, Handle);
            hFunc(TEvKesus::TEvAcquireSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvReleaseSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvDescribeSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvDescribeSemaphoreChanged, Handle);
            hFunc(TEvKesus::TEvCreateSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvUpdateSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvDeleteSemaphoreResult, Handle);
            hFunc(TEvPrivate::TEvPingScheduled, Handle);

            default:
                Y_ABORT("Unexpected event 0x%x for TGRpcSessionActor", ev->GetTypeRewrite());
        }
    }

private:
    using TUserToken = NACLib::TUserToken;

private:
    THolder<NGRpcService::TEvCoordinationSessionRequest> RequestEvent;
    TIntrusivePtr<TUserToken> UserToken;
    TIntrusivePtr<TSecurityObject> SecurityObject;

    THolder<IContext::TEvReadFinished> StartRequest;
    TString KesusPath;

    TActorId ProxyActor;
    bool ReadAllowed = false;
    bool WriteAllowed = false;
    bool EraseAllowed = false;
    bool AttachSessionSent = false;

    ui64 SessionId = 0;
    bool SessionEstablished = false;
    ui64 CurrentPingData = 0;
    TDuration PingPeriod;
};

////////////////////////////////////////////////////////////////////////////////

void TKesusGRpcService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using NGRpcService::TAuditMode;
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters_, ActorSystem_);
    using NGRpcService::TRateLimiterMode;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro is already defined
#endif

#define ADD_REQUEST(NAME, IN, OUT, CB, AUDIT_MODE) \
    MakeIntrusive<NGRpcService::TGRpcRequest<Ydb::Coordination::IN, Ydb::Coordination::OUT, TKesusGRpcService>>( \
        this, \
        &Service_, \
        CQ_, \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestOperationCall<Ydb::Coordination::IN, Ydb::Coordination::OUT> \
                    (reqCtx, &CB, NGRpcService::TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr, AUDIT_MODE})); \
        }, \
        &Ydb::Coordination::V1::CoordinationService::AsyncService::Request ## NAME, \
        "Coordination/" #NAME,             \
        logger, \
        getCounterBlock("coordination", #NAME))->Run();

    ADD_REQUEST(CreateNode, CreateNodeRequest, CreateNodeResponse, NGRpcService::DoCreateCoordinationNode, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    ADD_REQUEST(AlterNode, AlterNodeRequest, AlterNodeResponse, NGRpcService::DoAlterCoordinationNode, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    ADD_REQUEST(DropNode, DropNodeRequest, DropNodeResponse, NGRpcService::DoDropCoordinationNode, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Ddl));
    ADD_REQUEST(DescribeNode, DescribeNodeRequest, DescribeNodeResponse, NGRpcService::DoDescribeCoordinationNode, TAuditMode::NonModifying());

#undef ADD_REQUEST

    TGRpcSessionActor::TGRpcRequest::Start(
        this,
        this->GetService(),
        CQ_,
        &Ydb::Coordination::V1::CoordinationService::AsyncService::RequestSession,
        [this](TIntrusivePtr<TGRpcSessionActor::IContext> context) {
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, context->GetPeerName());
            ActorSystem_->Send(GRpcRequestProxyId_, new NGRpcService::TEvCoordinationSessionRequest(context));
        },
        *ActorSystem_,
        "Coordination/Session",
        getCounterBlock("coordination", "Session", true),
        /* TODO: limiter */ nullptr);
}

} // namespace NKesus
} // namespace NKikimr

void NKikimr::NGRpcService::TGRpcRequestProxyHandleMethods::Handle(
        NKikimr::NGRpcService::TEvCoordinationSessionRequest::TPtr& ev,
        const NActors::TActorContext& ctx)
{
    ctx.Register(new NKikimr::NKesus::TGRpcSessionActor(std::move(ev->Release())));
}
