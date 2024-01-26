#include <util/network/sock.h>
#include "http_proxy.h"
#include "http_proxy_ssl.h"

namespace NHttp {

class TAcceptorActor : public NActors::TActor<TAcceptorActor>, public THttpConfig {
public:
    using TBase = NActors::TActor<TAcceptorActor>;
    const TActorId Owner;
    const TActorId Poller;
    TIntrusivePtr<TSocketDescriptor> Socket;
    NActors::TPollerToken::TPtr PollerToken;
    THashSet<TActorId> Connections;
    TDeque<THttpIncomingRequestPtr> RecycledRequests;
    ui32 MaxRecycledRequestsCount = 0;
    std::shared_ptr<TPrivateEndpointInfo> Endpoint;

    TAcceptorActor(const TActorId& owner, const TActorId& poller)
        : NActors::TActor<TAcceptorActor>(&TAcceptorActor::StateInit)
        , Owner(owner)
        , Poller(poller)
    {
    }

    static constexpr char ActorName[] = "HTTP_ACCEPTOR_ACTOR";

protected:
    STFUNC(StateListening) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvPollerRegisterResult, Handle);
            HFunc(NActors::TEvPollerReady, Handle);
            HFunc(TEvHttpProxy::TEvHttpConnectionClosed, Handle);
            HFunc(TEvHttpProxy::TEvReportSensors, Handle);
        }
    }

    STFUNC(StateInit) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvHttpProxy::TEvAddListeningPort, HandleInit);
        }
    }

    void HandleInit(TEvHttpProxy::TEvAddListeningPort::TPtr event, const NActors::TActorContext& ctx) {
        TString address = event->Get()->Address;
        ui16 port = event->Get()->Port;
        MaxRecycledRequestsCount = event->Get()->MaxRecycledRequestsCount;
        Socket = new TSocketDescriptor(SocketType::GuessAddressFamily(address));
        // for unit tests :(
        SetSockOpt(Socket->Socket, SOL_SOCKET, SO_REUSEADDR, (int)true);
#ifdef SO_REUSEPORT
        SetSockOpt(Socket->Socket, SOL_SOCKET, SO_REUSEPORT, (int)true);
#endif
        SocketAddressType bindAddress(Socket->Socket.MakeAddress(address, port));
        Endpoint = std::make_shared<TPrivateEndpointInfo>(event->Get()->CompressContentTypes);
        Endpoint->Owner = ctx.SelfID;
        Endpoint->Proxy = Owner;
        Endpoint->WorkerName = event->Get()->WorkerName;
        Endpoint->Secure = event->Get()->Secure;
        int err = 0;
        if (Endpoint->Secure) {
            if (!event->Get()->SslCertificatePem.empty()) {
                Endpoint->SecureContext = TSslHelpers::CreateServerContext(event->Get()->SslCertificatePem);
            } else {
                Endpoint->SecureContext = TSslHelpers::CreateServerContext(event->Get()->CertificateFile, event->Get()->PrivateKeyFile);
            }
            if (Endpoint->SecureContext == nullptr) {
                err = -1;
                LOG_WARN_S(ctx, HttpLog, "Failed to construct server security context");
            }
        }
        if (err == 0) {
            err = Socket->Socket.Bind(bindAddress.get());
            if (err != 0) {
                LOG_WARN_S(
                    ctx,
                    HttpLog,
                    "Failed to bind " << bindAddress->ToString()
                    << ", code: " << err);
            }
        }
        TStringBuf schema = Endpoint->Secure ? "https://" : "http://";
        if (err == 0) {
            err = Socket->Socket.Listen(LISTEN_QUEUE);
            if (err == 0) {
                LOG_INFO_S(ctx, HttpLog, "Listening on " << schema << bindAddress->ToString());
                SetNonBlock(Socket->Socket);
                ctx.Send(Poller, new NActors::TEvPollerRegister(Socket, SelfId(), SelfId()));
                TBase::Become(&TAcceptorActor::StateListening);
                ctx.Send(event->Sender, new TEvHttpProxy::TEvConfirmListen(bindAddress, Endpoint), 0, event->Cookie);
                return;
            } else {
                LOG_WARN_S(
                    ctx,
                    HttpLog,
                    "Failed to listen on " << schema << bindAddress->ToString()
                    << ", code: " << err);
            }
        }
        LOG_WARN_S(ctx, HttpLog, "Failed to init - retrying...");
        ctx.ExecutorThread.Schedule(TDuration::Seconds(1), event.Release());
    }

    void Die(const NActors::TActorContext& ctx) override {
        ctx.Send(Owner, new TEvHttpProxy::TEvHttpAcceptorClosed(ctx.SelfID));
        for (const NActors::TActorId& connection : Connections) {
            ctx.Send(connection, new NActors::TEvents::TEvPoisonPill());
        }
    }

    void Handle(NActors::TEvPollerRegisterResult::TPtr ev, const NActors::TActorContext& /*ctx*/) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, false); // request read polling
    }

    void Handle(NActors::TEvPollerReady::TPtr, const NActors::TActorContext& ctx) {
        for (;;) {
            SocketAddressType addr;
            std::optional<SocketType> s = Socket->Socket.Accept(addr);
            if (!s) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    Y_ABORT_UNLESS(PollerToken);
                    if (PollerToken->RequestReadNotificationAfterWouldBlock()) {
                        continue; // we can try it again
                    }
                }
                break;
            }
            TIntrusivePtr<TSocketDescriptor> socket = new TSocketDescriptor(std::move(s).value());
            NActors::IActor* connectionSocket = nullptr;
            if (RecycledRequests.empty()) {
                connectionSocket = CreateIncomingConnectionActor(Endpoint, socket, addr);
            } else {
                connectionSocket = CreateIncomingConnectionActor(Endpoint, socket, addr, std::move(RecycledRequests.front()));
                RecycledRequests.pop_front();
            }
            NActors::TActorId connectionId = ctx.Register(connectionSocket);
            ctx.Send(Poller, new NActors::TEvPollerRegister(socket, connectionId, connectionId));
            Connections.emplace(connectionId);
        }
    }

    void Handle(TEvHttpProxy::TEvHttpConnectionClosed::TPtr event, const NActors::TActorContext&) {
        Connections.erase(event->Get()->ConnectionID);
        for (auto& req : event->Get()->RecycledRequests) {
            if (RecycledRequests.size() >= MaxRecycledRequestsCount) {
                break;
            }
            req->Clear();
            RecycledRequests.push_back(std::move(req));
        }
    }

    void Handle(TEvHttpProxy::TEvReportSensors::TPtr event, const NActors::TActorContext& ctx) {
        ctx.Send(event->Forward(Owner));
    }
};

NActors::IActor* CreateHttpAcceptorActor(const TActorId& owner, const TActorId& poller) {
    return new TAcceptorActor(owner, poller);
}

}
