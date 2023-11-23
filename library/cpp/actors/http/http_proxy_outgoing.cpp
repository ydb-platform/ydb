#include "http_proxy.h"
#include "http_proxy_sock_impl.h"

namespace NHttp {

template <typename TSocketImpl>
class TOutgoingConnectionActor : public NActors::TActor<TOutgoingConnectionActor<TSocketImpl>>, public TSocketImpl, virtual public THttpConfig {
public:
    using TBase = NActors::TActor<TOutgoingConnectionActor<TSocketImpl>>;
    using TSelf = TOutgoingConnectionActor<TSocketImpl>;
    const TActorId Owner;
    const TActorId Poller;
    SocketAddressType Address;
    TActorId RequestOwner;
    THttpOutgoingRequestPtr Request;
    THttpIncomingResponsePtr Response;
    TInstant LastActivity;
    TDuration ConnectionTimeout = CONNECTION_TIMEOUT;
    NActors::TPollerToken::TPtr PollerToken;

    TOutgoingConnectionActor(const TActorId& owner, const TActorId& poller)
        : TBase(&TSelf::StateWaiting)
        , Owner(owner)
        , Poller(poller)
    {
    }

    static constexpr char ActorName[] = "OUT_CONNECTION_ACTOR";

    void Die(const NActors::TActorContext& ctx) override {
        ctx.Send(Owner, new TEvHttpProxy::TEvHttpConnectionClosed(ctx.SelfID));
        TSocketImpl::Shutdown(); // to avoid errors when connection already closed
        TBase::Die(ctx);
    }

    TString GetSocketName() {
        TStringBuilder builder;
        if (TSocketImpl::Socket) {
            builder << "(#" << TSocketImpl::GetRawSocket();
            if (Address && Address->SockAddr()->sa_family) {
                builder << "," << Address;
            }
            builder << ") ";
        }
        return builder;
    }

    void ReplyAndDie(const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, HttpLog, GetSocketName() << "-> (" << Response->Status << " " << Response->Message << ")");
        ctx.Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response));
        RequestOwner = TActorId();
        THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildOutgoingRequestSensors(Request, Response));
        ctx.Send(Owner, sensors.Release());
        LOG_DEBUG_S(ctx, HttpLog, GetSocketName() << "connection closed");
        Die(ctx);
    }

    void ReplyErrorAndDie(const NActors::TActorContext& ctx, const TString& error) {
        LOG_ERROR_S(ctx, HttpLog, GetSocketName() << "connection closed with error: " << error);
        if (RequestOwner) {
            ctx.Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response, error));
            RequestOwner = TActorId();
            THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildOutgoingRequestSensors(Request, Response));
            ctx.Send(Owner, sensors.Release());
            Die(ctx);
        }
    }

protected:
    void FailConnection(const NActors::TActorContext& ctx, const TString& error) {
        if (Request) {
            return ReplyErrorAndDie(ctx, error);
        }
        return TBase::Become(&TOutgoingConnectionActor::StateFailed);
    }

    void Connect(const NActors::TActorContext& ctx) {
        LOG_DEBUG_S(ctx, HttpLog, GetSocketName() << "connecting");
        TSocketImpl::Create(Address->SockAddr()->sa_family);
        TSocketImpl::SetNonBlock();
        TSocketImpl::SetTimeout(ConnectionTimeout);
        int res = TSocketImpl::Connect(Address);
        RegisterPoller(ctx);
        switch (-res) {
        case 0:
            return OnConnect(ctx);
        case EINPROGRESS:
        case EAGAIN:
            return TBase::Become(&TOutgoingConnectionActor::StateConnecting);
        default:
            return ReplyErrorAndDie(ctx, strerror(-res));
        }
    }

    void FlushOutput(const NActors::TActorContext& ctx) {
        if (Request != nullptr) {
            Request->Finish();
            while (auto size = Request->Size()) {
                bool read = false, write = false;
                ssize_t res = TSocketImpl::Send(Request->Data(), size, read, write);
                if (res > 0) {
                   Request->ChopHead(res);
                } else if (-res == EINTR) {
                    continue;
                } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                    if (PollerToken) {
                        if (!read && !write) {
                            write = true;
                        }
                        if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                            continue;
                        }
                    }
                    break;
                } else {
                    if (!res) {
                        ReplyAndDie(ctx);
                    } else {
                        ReplyErrorAndDie(ctx, strerror(-res));
                    }
                    break;
                }
            }
        }
    }

    void PullInput(const NActors::TActorContext& ctx) {
        for (;;) {
            if (Response == nullptr) {
                Response = new THttpIncomingResponse(Request);
            }
            if (!Response->EnsureEnoughSpaceAvailable()) {
                return ReplyErrorAndDie(ctx, "Not enough space in socket buffer");
            }
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(Response->Pos(), Response->Avail(), read, write);
            if (res > 0) {
                Response->Advance(res);
                if (Response->IsDone() && Response->IsReady()) {
                    return ReplyAndDie(ctx);
                }
            } else if (-res == EINTR) {
                continue;
            } else if (-res == EAGAIN || -res == EWOULDBLOCK) {
                if (PollerToken) {
                    if (!read && !write) {
                        read = true;
                    }
                    if (PollerToken->RequestNotificationAfterWouldBlock(read, write)) {
                        continue;
                    }
                }
                return;
            } else {
                if (!res) {
                    Response->ConnectionClosed();
                }
                if (Response->IsDone() && Response->IsReady()) {
                    return ReplyAndDie(ctx);
                }
                return ReplyErrorAndDie(ctx, strerror(-res));
            }
        }
    }

    void RegisterPoller(const NActors::TActorContext& ctx) {
        ctx.Send(Poller, new NActors::TEvPollerRegister(TSocketImpl::Socket, ctx.SelfID, ctx.SelfID));
    }

    void OnConnect(const NActors::TActorContext& ctx) {
        bool read = false, write = false;
        if (int res = TSocketImpl::OnConnect(read, write); res != 1) {
            if (-res == EAGAIN) {
                if (PollerToken) {
                    PollerToken->Request(read, write);
                }
                return;
            } else {
                return ReplyErrorAndDie(ctx, strerror(-res));
            }
        }
        LOG_DEBUG_S(ctx, HttpLog, GetSocketName() << "outgoing connection opened");
        TBase::Become(&TOutgoingConnectionActor::StateConnected);
        LOG_DEBUG_S(ctx, HttpLog, GetSocketName() << "<- (" << Request->Method << " " << Request->URL << ")");
        ctx.Send(ctx.SelfID, new NActors::TEvPollerReady(nullptr, true, true));
    }

    static int GetPort(SocketAddressType address) {
        switch (address->SockAddr()->sa_family) {
            case AF_INET:
                return ntohs(reinterpret_cast<sockaddr_in*>(address->SockAddr())->sin_port);
            case AF_INET6:
                return ntohs(reinterpret_cast<sockaddr_in6*>(address->SockAddr())->sin6_port);
        }
        return {};
    }

    static void SetPort(SocketAddressType address, int port) {
        switch (address->SockAddr()->sa_family) {
            case AF_INET:
                reinterpret_cast<sockaddr_in*>(address->SockAddr())->sin_port = htons(port);
                break;
            case AF_INET6:
                reinterpret_cast<sockaddr_in6*>(address->SockAddr())->sin6_port = htons(port);
                break;
        }
    }

    void HandleResolving(TEvHttpProxy::TEvResolveHostResponse::TPtr event, const NActors::TActorContext& ctx) {
        LastActivity = ctx.Now();
        if (!event->Get()->Error.empty()) {
            return FailConnection(ctx, event->Get()->Error);
        }
        Address = event->Get()->Address;
        if (GetPort(Address) == 0) {
            SetPort(Address, Request->Secure ? 443 : 80);
        }
        Connect(ctx);
    }

    void HandleConnecting(NActors::TEvPollerReady::TPtr, const NActors::TActorContext& ctx) {
        LastActivity = ctx.Now();
        int res = TSocketImpl::GetError();
        if (res == 0) {
            OnConnect(ctx);
        } else {
            FailConnection(ctx, TStringBuilder() << strerror(res));
        }
    }

    void HandleConnecting(NActors::TEvPollerRegisterResult::TPtr ev, const NActors::TActorContext& ctx) {
        PollerToken = std::move(ev->Get()->PollerToken);
        LastActivity = ctx.Now();
        int res = TSocketImpl::GetError();
        if (res == 0) {
            OnConnect(ctx);
        } else {
            FailConnection(ctx, TStringBuilder() << strerror(res));
        }
    }

    void HandleWaiting(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event, const NActors::TActorContext& ctx) {
        LastActivity = ctx.Now();
        Request = std::move(event->Get()->Request);
        TSocketImpl::SetHost(TString(Request->Host));
        LOG_DEBUG_S(ctx, HttpLog, GetSocketName() << "resolving " << TSocketImpl::Host);
        Request->Timer.Reset();
        RequestOwner = event->Sender;
        ctx.Send(Owner, new TEvHttpProxy::TEvResolveHostRequest(TSocketImpl::Host));
        if (event->Get()->Timeout) {
            ConnectionTimeout = event->Get()->Timeout;
        }
        ctx.Schedule(ConnectionTimeout, new NActors::TEvents::TEvWakeup());
        LastActivity = ctx.Now();
        TBase::Become(&TOutgoingConnectionActor::StateResolving);
    }

    void HandleConnected(NActors::TEvPollerReady::TPtr event, const NActors::TActorContext& ctx) {
        LastActivity = ctx.Now();
        if (event->Get()->Write && RequestOwner) {
            FlushOutput(ctx);
        }
        if (event->Get()->Read && RequestOwner) {
            PullInput(ctx);
        }
    }

    void HandleConnected(NActors::TEvPollerRegisterResult::TPtr ev, const NActors::TActorContext& ctx) {
        PollerToken = std::move(ev->Get()->PollerToken);
        LastActivity = ctx.Now();
        PullInput(ctx);
        FlushOutput(ctx);
    }

    void HandleFailed(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr event, const NActors::TActorContext& ctx) {
        Request = std::move(event->Get()->Request);
        RequestOwner = event->Sender;
        ReplyErrorAndDie(ctx, "Failed");
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        TDuration inactivityTime = ctx.Now() - LastActivity;
        if (inactivityTime >= ConnectionTimeout) {
            FailConnection(ctx, "Connection timed out");
        } else {
            ctx.Schedule(Min(ConnectionTimeout - inactivityTime, TDuration::MilliSeconds(100)), new NActors::TEvents::TEvWakeup());
        }
    }

    STFUNC(StateWaiting) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHttpProxy::TEvHttpOutgoingRequest, HandleWaiting);
            CFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
        }
    }

    STFUNC(StateResolving) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHttpProxy::TEvResolveHostResponse, HandleResolving);
            CFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
        }
    }

    STFUNC(StateConnecting) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvPollerReady, HandleConnecting);
            CFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
            HFunc(NActors::TEvPollerRegisterResult, HandleConnecting);
        }
    }

    STFUNC(StateConnected) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvPollerReady, HandleConnected);
            CFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
            HFunc(NActors::TEvPollerRegisterResult, HandleConnected);
        }
    }

    STFUNC(StateFailed) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHttpProxy::TEvHttpOutgoingRequest, HandleFailed);
        }
    }
};

NActors::IActor* CreateOutgoingConnectionActor(const TActorId& owner, bool secure, const TActorId& poller) {
    if (secure) {
        return new TOutgoingConnectionActor<TSecureSocketImpl>(owner, poller);
    } else {
        return new TOutgoingConnectionActor<TPlainSocketImpl>(owner, poller);
    }
}

}
