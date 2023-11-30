#include "proxy_actor.h"

#include "events.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/generic/hash_set.h>

#define KPROXY_LOG_DEBUG_S(stream) \
    LOG_DEBUG_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_PROXY, \
        "[" << SelfId() << " " << KesusPath.Quote() << "] " << stream)

#define KPROXY_LOG_TRACE_S(stream) \
    LOG_TRACE_S(TActivationContext::AsActorContext(), NKikimrServices::KESUS_PROXY, \
        "[" << SelfId() << " " << KesusPath.Quote() << "] " << stream)

namespace NKikimr {
namespace NKesus {

class TKesusProxyActor : public TActorBootstrapped<TKesusProxyActor> {
    enum EState {
        STATE_IDLE,
        STATE_CONNECTING,
        STATE_CONNECTED,
        STATE_REGISTERING,
        STATE_REGISTERED,
    };

    struct TDirectRequest {
        TActorId Sender;
        ui64 Cookie;
        THolder<IEventBase> Event;
    };

    enum class ESessionState {
        ATTACHING,
        ATTACHED,
        DESTROYING,
    };

    struct TSessionData {
        ui64 SeqNo;
        ui64 SessionId;
        ui64 ClientSeqNo;
        TActorId Owner;
        ui64 OwnerCookie;
        ESessionState State = ESessionState::ATTACHING;
        THolder<TEvKesus::TEvAttachSession> AttachEvent;
        bool Destroy = false;

        // Request SeqNo -> Cookie
        THashMap<ui64, ui64> CookieByRequest;
    };

private:
    const TActorId MetaProxy;
    const ui64 TabletId;
    const TString KesusPath;
    EState State = STATE_IDLE;
    TActorId TabletPipe;
    ui64 ProxyGeneration = 0;

    ui64 SeqNo = 0;

    // SeqNo -> direct request
    THashMap<ui64, TDirectRequest> DirectRequests;
    THashMap<TActorId, ui64> DirectRequestBySender;

    // SeqNo -> session data
    THashMap<ui64, TSessionData> Sessions;
    // SessionId -> SeqNo
    THashMap<ui64, TSessionData*> SessionById;
    // Owner -> SeqNo
    THashMap<TActorId, TSessionData*> SessionByOwner;
    // Request SeqNo -> Session SeqNo
    THashMap<ui64, TSessionData*> SessionByRequest;

public:
    TKesusProxyActor(const TActorId& meta, ui64 tabletId, const TString& kesusPath)
        : MetaProxy(meta)
        , TabletId(tabletId)
        , KesusPath(kesusPath)
    {}

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        KPROXY_LOG_DEBUG_S("Bootstrapped proxy actor");
        Become(&TThis::StateWork);
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KESUS_PROXY_ACTOR;
    }

private:
    TSessionData* FindSession(ui64 seqNo) {
        return Sessions.FindPtr(seqNo);
    }

    TSessionData* FindSessionById(ui64 sessionId) {
        return SessionById.Value(sessionId, nullptr);
    }

    TSessionData* FindSessionByOwner(const TActorId& owner) {
        return SessionByOwner.Value(owner, nullptr);
    }

    TSessionData* FindSessionByRequest(ui64 cookie) {
        return SessionByRequest.Value(cookie, nullptr);
    }

    ui64 AddSessionRequest(TSessionData* data, ui64 cookie) {
        ui64 seqNo = ++SeqNo;
        data->CookieByRequest[seqNo] = cookie;
        SessionByRequest[seqNo] = data;
        return seqNo;
    }

    ui64 GetSessionRequest(TSessionData* data, ui64 seqNo) {
        ui64 *pCookie = data->CookieByRequest.FindPtr(seqNo);
        Y_ABORT_UNLESS(pCookie);
        return *pCookie;
    }

    ui64 RemoveSessionRequest(TSessionData* data, ui64 seqNo) {
        ui64 cookie = GetSessionRequest(data, seqNo);
        data->CookieByRequest.erase(seqNo);
        SessionByRequest.erase(seqNo);
        return cookie;
    }

    void ClearSessionRequests(TSessionData* data) {
        for (const auto& kv : data->CookieByRequest) {
            SessionByRequest.erase(kv.first);
        }
        data->CookieByRequest.clear();
    }

    void ClearSessionOwner(TSessionData* data) {
        if (data->Owner) {
            ClearSessionRequests(data);
            SessionByOwner.erase(data->Owner);
            data->Owner = {};
        }
    }

    void ClearSessionId(TSessionData* data) {
        if (data->SessionId) {
            SessionById.erase(data->SessionId);
            data->SessionId = 0;
        }
    }

    void RemoveSession(TSessionData* data) {
        ClearSessionOwner(data);
        ClearSessionId(data);
        Sessions.erase(data->SeqNo);
    }

    bool PipeNeeded() {
        return !DirectRequests.empty() || SessionNeeded();
    }

    bool EstablishPipe() {
        if (State >= STATE_CONNECTED) {
            return true;
        }
        if (State == STATE_CONNECTING) {
            return false;
        }

        Y_ABORT_UNLESS(State == STATE_IDLE);
        Y_ABORT_UNLESS(!TabletPipe);
        State = STATE_CONNECTING;
        KPROXY_LOG_DEBUG_S("Connecting to kesus");

        // TODO: add some retry policy?
        TabletPipe = Register(NTabletPipe::CreateClient(SelfId(), TabletId));
        return false;
    }

    bool SessionNeeded() {
        return !Sessions.empty();
    }

    bool EstablishSession() {
        if (State >= STATE_REGISTERED) {
            return true;
        }
        if (!EstablishPipe()) {
            return false;
        }
        if (State == STATE_REGISTERING) {
            return false;
        }

        Y_ABORT_UNLESS(State == STATE_CONNECTED);
        State = STATE_REGISTERING;

        ++ProxyGeneration;
        KPROXY_LOG_DEBUG_S("Registering with ProxyGeneration=" << ProxyGeneration);
        NTabletPipe::SendData(SelfId(), TabletPipe, new TEvKesus::TEvRegisterProxy(KesusPath, ProxyGeneration));
        return false;
    }

    void Handle(const TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        KPROXY_LOG_DEBUG_S("Destroying proxy");
        HandleLinkFailure();
        Die(ctx);
    }

    void Handle(const TEvKesusProxy::TEvCancelRequest::TPtr& ev) {
        KPROXY_LOG_TRACE_S("Received TEvCancelRequest from " << ev->Sender);
        CancelDirectRequest(ev->Sender);
        CancelSessionRequest(ev->Sender);
    }

    void CancelDirectRequest(const TActorId& owner) {
        auto it = DirectRequestBySender.find(owner);
        if (it != DirectRequestBySender.end()) {
            DirectRequests.erase(it->second);
            DirectRequestBySender.erase(it);
        }
    }

    void CancelSessionRequest(const TActorId& owner) {
        if (auto* data = FindSessionByOwner(owner)) {
            ClearSessionOwner(data);

            if (data->SessionId) {
                if (SessionDetachNeeded(data->State)) {
                    // Send detach if there was an attach
                    SendDetachSession(data);
                }
                RemoveSession(data);
            } else {
                // We don't know session id yet, wait and then destroy
                Y_ABORT_UNLESS(data->State == ESessionState::ATTACHING);
                data->Destroy = true;
                if (State < STATE_REGISTERED) {
                    // Attach request is not out yet, remove directly
                    RemoveSession(data);
                }
            }
        }
    }

    void HandleLinkFailure() {
        NKikimrKesus::TKesusError error;
        error.SetStatus(Ydb::StatusIds::UNAVAILABLE);
        error.AddIssues()->set_message("Link failure");
        HandleLinkError(error);
    }

    void HandleLinkError(const NKikimrKesus::TKesusError& error) {
        State = STATE_IDLE;
        if (TabletPipe) {
            NTabletPipe::CloseClient(TActivationContext::AsActorContext(), TabletPipe);
            TabletPipe = {};
        }

        // Fail all pending requests
        for (const auto& kv : DirectRequests) {
            Send(kv.second.Sender, new TEvKesusProxy::TEvProxyError(error), 0, kv.second.Cookie);
        }
        DirectRequests.clear();
        DirectRequestBySender.clear();

        HandleSessionError(error);
    }

    void HandleSessionError(const NKikimrKesus::TKesusError& error) {
        if (State >= STATE_REGISTERING) {
            // Downgrade state to simple connection
            State = STATE_CONNECTED;
        }

        // Fail all active sessions
        auto it = Sessions.begin();
        while (it != Sessions.end()) {
            auto* data = &it->second;
            if (data->Owner) {
                Send(data->Owner, new TEvKesusProxy::TEvProxyError(error), 0, data->OwnerCookie);
                ClearSessionOwner(data);
            }
            if (data->SessionId && data->Destroy) {
                // Keep this session until destroy is finished
                data->State = ESessionState::DESTROYING;
                ++it;
                continue;
            }
            // Finish removing the session
            ClearSessionId(data);
            Sessions.erase(it++);
        }
    }

    void Handle(const TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        const auto* msg = ev->Get();

        Y_ABORT_UNLESS(msg->TabletId == TabletId);
        Y_ABORT_UNLESS(msg->ClientId == TabletPipe);

        if (msg->Status != NKikimrProto::OK) {
            // Tablet pipe failed, report link failure
            KPROXY_LOG_DEBUG_S("Pipe to kesus failed to connect");
            HandleLinkFailure();
            if (PipeNeeded()) {
                // TODO: maybe add some delay
                EstablishPipe();
            }
            return;
        }

        Y_ABORT_UNLESS(State == STATE_CONNECTING);
        State = STATE_CONNECTED;
        KPROXY_LOG_DEBUG_S("Pipe to kesus connected");

        // Send all waiting direct requests
        for (auto& kv : DirectRequests) {
            const ui64 seqNo = kv.first;
            auto& req = kv.second;
            Y_ABORT_UNLESS(req.Event);
            NTabletPipe::SendData(SelfId(), TabletPipe, req.Event.Release(), seqNo);
        }

        if (SessionNeeded()) {
            EstablishSession();
        }
    }

    void Handle(const TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        const auto* msg = ev->Get();

        Y_ABORT_UNLESS(msg->TabletId == TabletId);
        Y_ABORT_UNLESS(msg->ClientId == TabletPipe);

        KPROXY_LOG_DEBUG_S("Pipe to kesus disconnected");
        HandleLinkFailure();
        if (PipeNeeded()) {
            // TODO: maybe add some delay
            EstablishPipe();
        }
    }

    void Handle(const TEvKesus::TEvRegisterProxyResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetProxyGeneration() != ProxyGeneration) {
            // Ignore outdated responses
            KPROXY_LOG_TRACE_S("Ignoring outdated TEvRegisterProxyResult with ProxyGeneration="
                << record.GetProxyGeneration());
            return;
        }

        if (record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS) {
            Y_ABORT("Unexpected proxy registration error");
        }

        Y_ABORT_UNLESS(State == STATE_REGISTERING);
        State = STATE_REGISTERED;
        KPROXY_LOG_DEBUG_S("Registered with ProxyGeneration=" << ProxyGeneration);

        for (auto& kv : Sessions) {
            auto* data = &kv.second;
            switch (data->State) {
                case ESessionState::ATTACHING:
                    SendAttachSession(data);
                    break;
                case ESessionState::ATTACHED:
                    Y_ABORT("Unexpected ATTACHED session during registration");
                    break;
                case ESessionState::DESTROYING:
                    SendDestroySession(data);
                    break;
            }
        }
    }

    void SendAttachSession(TSessionData* data) {
        Y_ABORT_UNLESS(data->AttachEvent);
        data->AttachEvent->Record.SetProxyGeneration(ProxyGeneration);
        KPROXY_LOG_DEBUG_S("Sending attach request for session "
            << data->AttachEvent->Record.GetSessionId()
            << " (cookie=" << data->SeqNo << ")");
        NTabletPipe::SendData(SelfId(), TabletPipe, data->AttachEvent.Release(), data->SeqNo);
    }

    void SendDetachSession(TSessionData* data) {
        KPROXY_LOG_DEBUG_S("Sending detach request for session "
            << data->SessionId
            << " (cookie=" << data->SeqNo << ")");
        NTabletPipe::SendData(
            SelfId(),
            TabletPipe,
            new TEvKesus::TEvDetachSession(KesusPath, ProxyGeneration, data->SessionId),
            data->SeqNo);
    }

    void SendDestroySession(const TSessionData* data) {
        KPROXY_LOG_DEBUG_S("Sending destroy request for session "
            << data->SessionId
            << " (cookie=" << data->SeqNo << ")");
        NTabletPipe::SendData(
            SelfId(),
            TabletPipe,
            new TEvKesus::TEvDestroySession(KesusPath, ProxyGeneration, data->SessionId),
            data->SeqNo);
    }

    bool SessionDetachNeeded(ESessionState sessionState) {
        switch (sessionState) {
            case ESessionState::ATTACHING:
            case ESessionState::ATTACHED:
                return State >= STATE_REGISTERED;
            default:
                return false;
        }
    }

    void HandleDirectRequest(const TActorId& sender, ui64 cookie, THolder<IEventBase> event) {
        KPROXY_LOG_TRACE_S("Received " << event->ToStringHeader() << " from " << sender);
        Y_ABORT_UNLESS(!DirectRequestBySender.contains(sender), "Only one outgoing request per sender is allowed");
        const ui64 seqNo = ++SeqNo;
        auto& req = DirectRequests[seqNo];
        req.Sender = sender;
        req.Cookie = cookie;
        DirectRequestBySender[sender] = seqNo;

        if (EstablishPipe()) {
            // Pipe already established
            NTabletPipe::SendData(SelfId(), TabletPipe, event.Release(), seqNo);
        } else {
            // Store event until pipe is established
            req.Event = std::move(event);
        }
    }

    void HandleDirectResponse(ui64 seqNo, THolder<IEventBase> event) {
        if (const auto* req = DirectRequests.FindPtr(seqNo)) {
            KPROXY_LOG_TRACE_S("Relaying " << event->ToStringHeader() << " to " << req->Sender);
            Send(req->Sender, event.Release(), 0, req->Cookie);
            DirectRequestBySender.erase(req->Sender);
            DirectRequests.erase(seqNo);
        } else {
            KPROXY_LOG_TRACE_S("Ignoring " << event->ToStringHeader());
        }
    }

    void Handle(TEvKesus::TEvDescribeSemaphore::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0 && msg->Record.GetSessionId() == 0) {
            HandleDirectRequest(ev->Sender, ev->Cookie, std::move(msg));
        } else {
            HandleSessionRequest(ev->Sender, ev->Cookie, std::move(msg));
        }
    }

    void Handle(TEvKesus::TEvDescribeSemaphoreResult::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0) {
            HandleDirectResponse(ev->Cookie, std::move(msg));
        } else {
            bool finished = !msg->Record.GetWatchAdded();
            HandleSessionResponse(ev->Cookie, std::move(msg), finished);
        }
    }

    void Handle(TEvKesus::TEvDescribeSemaphoreChanged::TPtr& ev) {
        HandleSessionResponse(ev->Cookie, ev->Release());
    }

    void Handle(TEvKesus::TEvCreateSemaphore::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0 && msg->Record.GetSessionId() == 0) {
            HandleDirectRequest(ev->Sender, ev->Cookie, std::move(msg));
        } else {
            HandleSessionRequest(ev->Sender, ev->Cookie, std::move(msg));
        }
    }

    void Handle(TEvKesus::TEvCreateSemaphoreResult::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0) {
            HandleDirectResponse(ev->Cookie, std::move(msg));
        } else {
            HandleSessionResponse(ev->Cookie, std::move(msg));
        }
    }

    void Handle(TEvKesus::TEvUpdateSemaphore::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0 && msg->Record.GetSessionId() == 0) {
            HandleDirectRequest(ev->Sender, ev->Cookie, std::move(msg));
        } else {
            HandleSessionRequest(ev->Sender, ev->Cookie, std::move(msg));
        }
    }

    void Handle(TEvKesus::TEvUpdateSemaphoreResult::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0) {
            HandleDirectResponse(ev->Cookie, std::move(msg));
        } else {
            HandleSessionResponse(ev->Cookie, std::move(msg));
        }
    }

    void Handle(TEvKesus::TEvDeleteSemaphore::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0 && msg->Record.GetSessionId() == 0) {
            HandleDirectRequest(ev->Sender, ev->Cookie, std::move(msg));
        } else {
            HandleSessionRequest(ev->Sender, ev->Cookie, std::move(msg));
        }
    }

    void Handle(TEvKesus::TEvDeleteSemaphoreResult::TPtr& ev) {
        auto msg = ev->Release();
        if (msg->Record.GetProxyGeneration() == 0) {
            HandleDirectResponse(ev->Cookie, std::move(msg));
        } else {
            HandleSessionResponse(ev->Cookie, std::move(msg));
        }
    }

    void Handle(TEvKesus::TEvAttachSession::TPtr& ev) {
        KPROXY_LOG_TRACE_S("Received TEvAttachSession from " << ev->Sender);
        Y_ABORT_UNLESS(ev->Sender);
        Y_ABORT_UNLESS(!SessionByOwner.contains(ev->Sender), "Only one outgoing request per sender is allowed");

        auto& record = ev->Get()->Record;
        const ui64 sessionId = record.GetSessionId();
        if (auto* data = FindSessionById(sessionId)) {
            // this is an attempt to restore session
            if (record.GetSeqNo() < data->ClientSeqNo) {
                KPROXY_LOG_TRACE_S("Replying with BAD_SESSION to " << ev->Sender);
                Send(ev->Sender,
                    new TEvKesus::TEvAttachSessionResult(
                        ProxyGeneration,
                        record.GetSessionId(),
                        Ydb::StatusIds::BAD_SESSION,
                        "Attach request is out of sequence"),
                    0, ev->Cookie);
                return;
            }
            if (data->Destroy) {
                KPROXY_LOG_TRACE_S("Replying with SESSION_EXPIRED to " << ev->Sender);
                Send(ev->Sender,
                    new TEvKesus::TEvAttachSessionResult(
                        ProxyGeneration,
                        record.GetSessionId(),
                        Ydb::StatusIds::SESSION_EXPIRED,
                        "Session is already being destroyed"),
                    0, ev->Cookie);
                return;
            }
            if (data->Owner) {
                KPROXY_LOG_TRACE_S("Sending BAD_SESSION to " << data->Owner);
                Send(data->Owner,
                    new TEvKesusProxy::TEvProxyError(
                        Ydb::StatusIds::BAD_SESSION,
                        "Session stolen by another local client"),
                    0, data->OwnerCookie);
            }
            RemoveSession(data);
        }

        const ui64 seqNo = ++SeqNo;
        TSessionData* data = &Sessions[seqNo];
        data->SeqNo = seqNo;
        data->SessionId = sessionId;
        data->ClientSeqNo = record.GetSeqNo();
        data->Owner = ev->Sender;
        data->OwnerCookie = ev->Cookie;
        if (sessionId != 0) {
            SessionById[sessionId] = data;
        }
        SessionByOwner[data->Owner] = data;
        data->AttachEvent.Reset(ev->Release().Release());
        KPROXY_LOG_TRACE_S("Allocated new session (cookie=" << data->SeqNo << ")");

        if (EstablishSession()) {
            // Already registered, send attach immediately
            SendAttachSession(data);
        }
    }

    void Handle(const TEvKesus::TEvAttachSessionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetProxyGeneration() != ProxyGeneration) {
            // Ignore outdated responses
            KPROXY_LOG_TRACE_S("Ignoring TEvAttachSessionResult with ProxyGeneration="
                << record.GetProxyGeneration());
            return;
        }

        bool isError = record.GetError().GetStatus() != Ydb::StatusIds::SUCCESS;
        if (auto* data = FindSession(ev->Cookie)) {
            if (!data->SessionId) {
                // New session and we finally know its id
                data->SessionId = record.GetSessionId();
                if (data->SessionId) {
                    SessionById[data->SessionId] = data;
                    KPROXY_LOG_DEBUG_S("Created new session " << data->SessionId
                        << " (cookie=" << data->SeqNo << ")");
                }
            } else if (!isError) {
                KPROXY_LOG_DEBUG_S("Attached to session " << data->SessionId
                    << " (cookie=" << data->SeqNo << ")");
            }
            Y_ABORT_UNLESS(data->SessionId == record.GetSessionId());

            if (data->Owner) {
                KPROXY_LOG_TRACE_S("Relaying TEvAttachSessionResult to " << data->Owner);
                Send(data->Owner, ev->Release().Release(), 0, data->OwnerCookie);
            }

            if (isError) {
                // If attach failed, then just forget this session
                RemoveSession(data);
                return;
            }

            if (data->State == ESessionState::ATTACHING) {
                data->State = ESessionState::ATTACHED;
                if (data->Destroy) {
                    data->State = ESessionState::DESTROYING;
                    if (EstablishSession()) {
                        SendDestroySession(data);
                    }
                }
            }
        }
    }

    void Handle(const TEvKesus::TEvProxyExpired::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetProxyGeneration() != ProxyGeneration) {
            // Ignore outdated notifications
            return;
        }

        KPROXY_LOG_TRACE_S("Received TEvProxyExpired with ProxyGeneration=" << ProxyGeneration);

        NKikimrKesus::TKesusError error;
        error.SetStatus(Ydb::StatusIds::BAD_SESSION);
        error.AddIssues()->set_message("Proxy session expired");
        HandleSessionError(error);

        if (SessionNeeded()) {
            EstablishSession();
        }
    }

    void Handle(const TEvKesus::TEvSessionExpired::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        const ui64 sessionId = record.GetSessionId();
        KPROXY_LOG_TRACE_S("Received TEvSessionExpired with SessionId=" << sessionId);

        if (auto* data = FindSessionById(sessionId)) {
            if (data->Owner) {
                KPROXY_LOG_TRACE_S("Sending SESSION_EXPIRED to " << data->Owner);
                Send(data->Owner,
                    new TEvKesusProxy::TEvProxyError(
                        Ydb::StatusIds::SESSION_EXPIRED,
                        "Session has expired"),
                    0, data->OwnerCookie);
            }
            RemoveSession(data);
        }
    }

    void Handle(const TEvKesus::TEvSessionStolen::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetProxyGeneration() != ProxyGeneration) {
            // Ignore outdated notifications
            return;
        }

        if (auto* data = FindSession(ev->Cookie)) {
            if (data->Owner) {
                KPROXY_LOG_TRACE_S("Sending BAD_SESSION to " << data->Owner);
                Send(data->Owner,
                    new TEvKesusProxy::TEvProxyError(
                        Ydb::StatusIds::BAD_SESSION,
                        "Session stolen by another remote client"),
                    0, data->OwnerCookie);
            }
            // This session is not our problem anymore
            RemoveSession(data);
        }
    }

    void Handle(const TEvKesus::TEvDetachSessionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetProxyGeneration() != ProxyGeneration) {
            // Ignore outdated notifications
            return;
        }

        // We actually don't care if detach is successful or not
    }

    void Handle(const TEvKesus::TEvDestroySession::TPtr& ev) {
        KPROXY_LOG_TRACE_S("Received TEvDestroySession from " << ev->Sender);
        Y_ABORT_UNLESS(ev->Sender);

        if (auto* data = FindSessionByOwner(ev->Sender)) {
            if (data->State == ESessionState::ATTACHED) {
                data->Destroy = true;
                data->State = ESessionState::DESTROYING;
                SendDestroySession(data);
                return;
            }
        }

        KPROXY_LOG_TRACE_S("Sending BAD_SESSION to " << ev->Sender);
        Send(ev->Sender,
            new TEvKesusProxy::TEvProxyError(
                Ydb::StatusIds::BAD_SESSION,
                "Session is not attached"),
            0, ev->Cookie);
    }

    void Handle(const TEvKesus::TEvDestroySessionResult::TPtr& ev) {
        const auto& record = ev->Get()->Record;
        if (record.GetProxyGeneration() != ProxyGeneration) {
            // Ignore responses from the wrong generation
            return;
        }

        if (auto* data = FindSession(ev->Cookie)) {
            Y_ABORT_UNLESS(data->State == ESessionState::DESTROYING);

            if (data->Owner) {
                KPROXY_LOG_TRACE_S("Relaying TEvDestroySessionResult to " << data->Owner);
                Send(data->Owner, ev->Release().Release(), 0, data->OwnerCookie);
            }

            RemoveSession(data);
        }
    }

    template<class TRequest>
    void HandleSessionRequest(const TActorId& sender, ui64 cookie, TAutoPtr<TRequest> event) {
        KPROXY_LOG_TRACE_S("Received " << event->ToStringHeader() << " from " << sender);
        Y_ABORT_UNLESS(sender);
        auto& record = event->Record;

        if (auto* data = FindSessionByOwner(sender)) {
            if (data->State == ESessionState::ATTACHED) {
                Y_ABORT_UNLESS(data->Owner == sender);
                record.SetProxyGeneration(ProxyGeneration);
                record.SetSessionId(data->SessionId);
                ui64 seqNo = AddSessionRequest(data, cookie);
                KPROXY_LOG_TRACE_S("Forwarding " << event->ToStringHeader()
                    << " to kesus (session=" << data->SessionId << ", cookie=" << seqNo << ")");
                NTabletPipe::SendData(SelfId(), TabletPipe, event.Release(), seqNo);
                return;
            }
        }

        KPROXY_LOG_TRACE_S("Sending BAD_SESSION to " << sender);
        Send(sender,
            new TEvKesusProxy::TEvProxyError(
                Ydb::StatusIds::BAD_SESSION,
                "Session is not attached"),
            0, cookie);
    }

    template<class TResponse>
    void HandleSessionResponse(ui64 seqNo, TAutoPtr<TResponse> event, bool finished = true) {
        if (auto* data = FindSessionByRequest(seqNo)) {
            Y_ABORT_UNLESS(data->Owner);
            const ui64 replyCookie = finished ? RemoveSessionRequest(data, seqNo) : GetSessionRequest(data, seqNo);
            KPROXY_LOG_TRACE_S("Relaying " << event->ToStringHeader() << " to " << data->Owner
                << " (session=" << data->SessionId << ", cookie=" << seqNo << ")");
            Send(data->Owner, event.Release(), 0, replyCookie);
            return;
        }
    }

    void Handle(const TEvKesus::TEvAcquireSemaphore::TPtr& ev) {
        HandleSessionRequest(ev->Sender, ev->Cookie, ev->Release());
    }

    void Handle(const TEvKesus::TEvAcquireSemaphorePending::TPtr& ev) {
        HandleSessionResponse(ev->Cookie, ev->Release(), false);
    }

    void Handle(const TEvKesus::TEvAcquireSemaphoreResult::TPtr& ev) {
        HandleSessionResponse(ev->Cookie, ev->Release());
    }

    void Handle(const TEvKesus::TEvReleaseSemaphore::TPtr& ev) {
        HandleSessionRequest(ev->Sender, ev->Cookie, ev->Release());
    }

    void Handle(const TEvKesus::TEvReleaseSemaphoreResult::TPtr& ev) {
        HandleSessionResponse(ev->Cookie, ev->Release());
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, Handle);
            hFunc(TEvKesusProxy::TEvCancelRequest, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvKesus::TEvRegisterProxyResult, Handle);
            hFunc(TEvKesus::TEvDescribeSemaphore, Handle);
            hFunc(TEvKesus::TEvDescribeSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvDescribeSemaphoreChanged, Handle);
            hFunc(TEvKesus::TEvCreateSemaphore, Handle);
            hFunc(TEvKesus::TEvCreateSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvUpdateSemaphore, Handle);
            hFunc(TEvKesus::TEvUpdateSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvDeleteSemaphore, Handle);
            hFunc(TEvKesus::TEvDeleteSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvAttachSession, Handle);
            hFunc(TEvKesus::TEvAttachSessionResult, Handle);
            hFunc(TEvKesus::TEvProxyExpired, Handle);
            hFunc(TEvKesus::TEvSessionExpired, Handle);
            hFunc(TEvKesus::TEvSessionStolen, Handle);
            hFunc(TEvKesus::TEvDetachSessionResult, Handle);
            hFunc(TEvKesus::TEvDestroySession, Handle);
            hFunc(TEvKesus::TEvDestroySessionResult, Handle);
            hFunc(TEvKesus::TEvAcquireSemaphore, Handle);
            hFunc(TEvKesus::TEvAcquireSemaphorePending, Handle);
            hFunc(TEvKesus::TEvAcquireSemaphoreResult, Handle);
            hFunc(TEvKesus::TEvReleaseSemaphore, Handle);
            hFunc(TEvKesus::TEvReleaseSemaphoreResult, Handle);

            default:
                Y_ABORT("Unexpected event 0x%x for TKesusProxyActor", ev->GetTypeRewrite());
        }
    }
};

IActor* CreateKesusProxyActor(const TActorId& meta, ui64 tabletId, const TString& kesusPath) {
    return new TKesusProxyActor(meta, tabletId, kesusPath);
}

}
}
