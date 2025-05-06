#include "http_proxy.h"
#include "http_proxy_sock_impl.h"

namespace NHttp {

template <typename TSocketImpl>
class TOutgoingConnectionActor : public NActors::TActorBootstrapped<TOutgoingConnectionActor<TSocketImpl>>, public TSocketImpl, virtual public THttpConfig {
public:
    using TBase = NActors::TActorBootstrapped<TOutgoingConnectionActor<TSocketImpl>>;
    using TSelf = TOutgoingConnectionActor<TSocketImpl>;
    using TBase::Become;
    using TBase::Send;
    using TBase::Schedule;
    using TBase::SelfId;

    const TActorId Owner;
    SocketAddressType Address;
    TString Destination;
    TActorId RequestOwner;
    THttpOutgoingRequestPtr Request;
    THttpIncomingResponsePtr Response;
    TInstant LastActivity;
    TDuration ConnectionTimeout = CONNECTION_TIMEOUT;
    bool AllowConnectionReuse = false;
    NActors::TPollerToken::TPtr PollerToken;

    TOutgoingConnectionActor(const TActorId& owner, TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event)
        : Owner(owner)
    {
        InitiateRequest(event);
    }

    static constexpr char ActorName[] = "OUT_CONNECTION_ACTOR";

    void Bootstrap() {
        PerformRequest();
    }

    void PassAway() override {
        Send(Owner, new TEvHttpProxy::TEvHttpOutgoingConnectionClosed(SelfId(), Destination));
        TSocketImpl::Shutdown(); // to avoid errors when connection already closed
        TBase::PassAway();
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

    void ReplyAndPassAway() {
        ALOG_DEBUG(HttpLog, GetSocketName() << "-> (" << Response->Status << " " << Response->Message << ")");
        Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response));
        RequestOwner = TActorId();
        THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildOutgoingRequestSensors(Request, Response));
        Send(Owner, sensors.Release());
        if (!AllowConnectionReuse || Response->IsConnectionClose()) {
            ALOG_DEBUG(HttpLog, GetSocketName() << "connection closed");
            PassAway();
        } else {
            ALOG_DEBUG(HttpLog, GetSocketName() << "connection available for reuse");
            CheckClose();
            Send(Owner, new TEvHttpProxy::TEvHttpOutgoingConnectionAvailable(SelfId(), Destination));
        }
    }

    void ReplyErrorAndPassAway(const TString& error) {
        if (error) {
            ALOG_ERROR(HttpLog, GetSocketName() << "connection closed with error: " << error);
        } else {
            ALOG_DEBUG(HttpLog, GetSocketName() << "connection closed");
        }
        if (RequestOwner) {
            if (!error && Response && !Response->IsReady()) {
                Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response, "ConnectionClosed")); // connection closed prematurely
            } else {
                Send(RequestOwner, new TEvHttpProxy::TEvHttpIncomingResponse(Request, Response, error));
            }
            RequestOwner = TActorId();
            THolder<TEvHttpProxy::TEvReportSensors> sensors(BuildOutgoingRequestSensors(Request, Response));
            Send(Owner, sensors.Release());
        }
        PassAway();
    }

protected:
    void FailConnection(const TString& error) {
        if (Request) {
            return ReplyErrorAndPassAway(error);
        }
        return TBase::Become(&TOutgoingConnectionActor::StateFailed);
    }

    void Connect() {
        ALOG_DEBUG(HttpLog, GetSocketName() << "connecting to " << Address->ToString());
        TSocketImpl::Create(Address->SockAddr()->sa_family);
        TSocketImpl::SetNonBlock();
        TSocketImpl::SetTimeout(ConnectionTimeout);
        int res = TSocketImpl::Connect(Address);
        RegisterPoller();
        switch (-res) {
        case 0:
            return OnConnect();
        case EINPROGRESS:
        case EAGAIN:
            return TBase::Become(&TOutgoingConnectionActor::StateConnecting);
        default:
            return ReplyErrorAndPassAway(strerror(-res));
        }
    }

    void InitiateRequest(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        Request = std::move(event->Get()->Request);
        Destination = Request->GetDestination();
        TSocketImpl::SetHost(TString(Request->Host));
        RequestOwner = event->Sender;
        if (event->Get()->Timeout) {
            ConnectionTimeout = event->Get()->Timeout;
        }
        AllowConnectionReuse = event->Get()->AllowConnectionReuse;
    }

    void PerformRequest() {
        Request->Timer.Reset();
        ALOG_DEBUG(HttpLog, GetSocketName() << "resolving " << TSocketImpl::Host);
        Send(Owner, new TEvHttpProxy::TEvResolveHostRequest(TSocketImpl::Host));
        Schedule(ConnectionTimeout, new NActors::TEvents::TEvWakeup());
        LastActivity = NActors::TActivationContext::Now();
        TBase::Become(&TOutgoingConnectionActor::StateResolving);
    }

    void FlushOutput() {
        if (Request != nullptr) {
            Request->Finish();
            while (auto size = Request->Size()) {
                bool read = false, write = false;
                ssize_t res = TSocketImpl::Send(Request->Data(), size, read, write);
                if (res > 0) {
                    LastActivity = NActors::TActivationContext::Now();
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
                    ReplyErrorAndPassAway(res == 0 ? "" : strerror(-res));
                    break;
                }
            }
        }
    }

    void CheckClose() {
        char buf[8];
        for (;;) {
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(&buf, 0, read, write);
            if (res > 0) {
                return ReplyErrorAndPassAway("Unexpected data received");
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
                return ReplyErrorAndPassAway(res == 0 ? "" : strerror(-res));
            }
        }
    }

    void PullInput() {
        for (;;) {
            if (Response == nullptr) {
                Response = new THttpIncomingResponse(Request);
            }
            if (!Response->EnsureEnoughSpaceAvailable()) {
                return ReplyErrorAndPassAway("Not enough space in socket buffer");
            }
            bool read = false, write = false;
            ssize_t res = TSocketImpl::Recv(Response->Pos(), Response->Avail(), read, write);
            if (res > 0) {
                LastActivity = NActors::TActivationContext::Now();
                Response->Advance(res);
                if (Response->IsDone() && Response->IsReady()) {
                    return ReplyAndPassAway();
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
                if (res == 0) {
                    Response->ConnectionClosed();
                }
                if (Response->IsDone() && Response->IsReady()) {
                    return ReplyAndPassAway();
                }
                return ReplyErrorAndPassAway(res == 0 ? "" : strerror(-res));
            }
        }
    }

    void RegisterPoller() {
        Send(NActors::MakePollerActorId(), new NActors::TEvPollerRegister(TSocketImpl::Socket, SelfId(), SelfId()));
    }

    void OnConnect() {
        bool read = false, write = false;
        if (int res = TSocketImpl::OnConnect(read, write); res != 1) {
            if (-res == EAGAIN) {
                if (PollerToken) {
                    PollerToken->Request(read, write);
                }
                return;
            } else {
                return ReplyErrorAndPassAway(strerror(-res));
            }
        }
        ALOG_DEBUG(HttpLog, GetSocketName() << "outgoing connection opened");
        TBase::Become(&TOutgoingConnectionActor::StateConnected);
        ALOG_DEBUG(HttpLog, GetSocketName() << "<- (" << Request->Method << " " << Request->URL << ")");
        Send(SelfId(), new NActors::TEvPollerReady(nullptr, true, true));
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

    void HandleResolving(TEvHttpProxy::TEvResolveHostResponse::TPtr& event) {
        LastActivity = NActors::TActivationContext::Now();
        if (!event->Get()->Error.empty()) {
            return FailConnection(event->Get()->Error);
        }
        Address = event->Get()->Address;
        if (GetPort(Address) == 0) {
            SetPort(Address, Request->Secure ? 443 : 80);
        }
        Connect();
    }

    void HandleConnecting(NActors::TEvPollerReady::TPtr&) {
        LastActivity = NActors::TActivationContext::Now();
        int res = TSocketImpl::GetError();
        if (res == 0) {
            OnConnect();
        } else {
            FailConnection(TStringBuilder() << strerror(res));
        }
    }

    void HandleConnecting(NActors::TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        LastActivity = NActors::TActivationContext::Now();
        int res = TSocketImpl::GetError();
        if (res == 0) {
            OnConnect();
        } else {
            FailConnection(TStringBuilder() << strerror(res));
        }
    }

    void HandleWaiting(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        InitiateRequest(event);
        PerformRequest();
    }

    void HandleConnected(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        Request = std::move(event->Get()->Request);
        Request->Timer.Reset();
        Response = nullptr;
        RequestOwner = event->Sender;
        if (event->Get()->Timeout) {
            ConnectionTimeout = event->Get()->Timeout;
        }
        AllowConnectionReuse = event->Get()->AllowConnectionReuse;
        Schedule(ConnectionTimeout, new NActors::TEvents::TEvWakeup());
        LastActivity = NActors::TActivationContext::Now();
        ALOG_DEBUG(HttpLog, GetSocketName() << "<- (" << Request->Method << " " << Request->URL << ")");
        FlushOutput();
        PullInput();
    }

    void HandleConnected(NActors::TEvPollerReady::TPtr& event) {
        LastActivity = NActors::TActivationContext::Now();
        if (event->Get()->Write && RequestOwner) {
            FlushOutput();
        }
        if (event->Get()->Read) {
            if (RequestOwner) {
                PullInput();
            } else {
                CheckClose();
            }
        }
    }

    void HandleConnected(NActors::TEvPollerRegisterResult::TPtr& ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        LastActivity = NActors::TActivationContext::Now();
        PullInput();
        FlushOutput();
    }

    void HandleFailed(TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
        Request = std::move(event->Get()->Request);
        RequestOwner = event->Sender;
        ReplyErrorAndPassAway("Failed");
    }

    void HandleTimeout() {
        TDuration inactivityTime = NActors::TActivationContext::Now() - LastActivity;
        if (inactivityTime >= ConnectionTimeout) {
            FailConnection("Connection timed out");
        } else {
            Schedule(Min(ConnectionTimeout - inactivityTime, TDuration::MilliSeconds(100)), new NActors::TEvents::TEvWakeup());
        }
    }

    STATEFN(StateResolving) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHttpProxy::TEvResolveHostResponse, HandleResolving);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
        }
    }

    STATEFN(StateConnecting) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerReady, HandleConnecting);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
            hFunc(NActors::TEvPollerRegisterResult, HandleConnecting);
        }
    }

    STATEFN(StateConnected) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerReady, HandleConnected);
            cFunc(NActors::TEvents::TEvWakeup::EventType, HandleTimeout);
            hFunc(NActors::TEvPollerRegisterResult, HandleConnected);
            hFunc(TEvHttpProxy::TEvHttpOutgoingRequest, HandleConnected);
        }
    }

    STATEFN(StateFailed) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHttpProxy::TEvHttpOutgoingRequest, HandleFailed);
        }
    }
};

NActors::IActor* CreateOutgoingConnectionActor(const TActorId& owner, TEvHttpProxy::TEvHttpOutgoingRequest::TPtr& event) {
    if (event->Get()->Request->Secure) {
        return new TOutgoingConnectionActor<TSecureSocketImpl>(owner, event);
    } else {
        return new TOutgoingConnectionActor<TPlainSocketImpl>(owner, event);
    }
}

}
