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
    TEndpointInfo Endpoint; 
 
    TAcceptorActor(const TActorId& owner, const TActorId& poller)
        : NActors::TActor<TAcceptorActor>(&TAcceptorActor::StateInit) 
        , Owner(owner) 
        , Poller(poller) 
        , Socket(new TSocketDescriptor()) 
    { 
        // for unit tests :( 
        CheckedSetSockOpt(Socket->Socket, SOL_SOCKET, SO_REUSEADDR, (int)true, "reuse address"); 
#ifdef SO_REUSEPORT 
        CheckedSetSockOpt(Socket->Socket, SOL_SOCKET, SO_REUSEPORT, (int)true, "reuse port"); 
#endif 
    } 
 
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
        SocketAddressType bindAddress("::", event->Get()->Port); 
        Endpoint.Owner = ctx.SelfID; 
        Endpoint.Proxy = Owner; 
        Endpoint.WorkerName = event->Get()->WorkerName; 
        Endpoint.Secure = event->Get()->Secure; 
        int err = 0; 
        if (Endpoint.Secure) { 
            if (!event->Get()->SslCertificatePem.empty()) {
                Endpoint.SecureContext = TSslHelpers::CreateServerContext(event->Get()->SslCertificatePem);
            } else {
                Endpoint.SecureContext = TSslHelpers::CreateServerContext(event->Get()->CertificateFile, event->Get()->PrivateKeyFile);
            }
            if (Endpoint.SecureContext == nullptr) { 
                err = -1; 
                LOG_WARN_S(ctx, HttpLog, "Failed to construct server security context"); 
            } 
        } 
        if (err == 0) { 
            err = Socket->Socket.Bind(&bindAddress); 
        } 
        if (err == 0) { 
            err = Socket->Socket.Listen(LISTEN_QUEUE); 
            if (err == 0) { 
                LOG_INFO_S(ctx, HttpLog, "Listening on " << bindAddress.ToString()); 
                SetNonBlock(Socket->Socket); 
                ctx.Send(Poller, new NActors::TEvPollerRegister(Socket, SelfId(), SelfId()));
                TBase::Become(&TAcceptorActor::StateListening); 
                ctx.Send(event->Sender, new TEvHttpProxy::TEvConfirmListen(bindAddress), 0, event->Cookie); 
                return; 
            } 
        } 
        LOG_WARN_S(ctx, HttpLog, "Failed to listen on " << bindAddress.ToString() << " - retrying..."); 
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
        TIntrusivePtr<TSocketDescriptor> socket = new TSocketDescriptor(); 
        SocketAddressType addr; 
        int err;
        while ((err = Socket->Socket.Accept(&socket->Socket, &addr)) == 0) {
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
            socket = new TSocketDescriptor(); 
        } 
        if (err == -EAGAIN || err == -EWOULDBLOCK) { // request poller for further connection polling
            Y_VERIFY(PollerToken);
            PollerToken->Request(true, false);
        }
    } 
 
    void Handle(TEvHttpProxy::TEvHttpConnectionClosed::TPtr event, const NActors::TActorContext&) { 
        Connections.erase(event->Get()->ConnectionID); 
        for (auto& req : event->Get()->RecycledRequests) { 
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
