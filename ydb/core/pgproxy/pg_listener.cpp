
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/interconnect/poller_actor.h>
#include <util/network/sock.h>
#include "pg_proxy.h"
#include "pg_listener.h"
#include "pg_proxy_impl.h"
#include "pg_proxy_config.h"
#include "pg_sock64.h"
#include "pg_connection.h"
#include "pg_log_impl.h"

namespace NPG {

using namespace NActors;

class TPGListener : public TActorBootstrapped<TPGListener>, TNetworkConfig {
public:
    using TBase = NActors::TActor<TPGListener>;
    using TThis = TPGListener;
    TActorId Poller;
    TActorId DatabaseProxy;
    TListenerSettings Settings;
    TIntrusivePtr<TSocketDescriptor> Socket;
    NActors::TPollerToken::TPtr PollerToken;
    THashSet<TActorId> Connections;

    TPGListener(const TActorId& poller, const TActorId& databaseProxy, const TListenerSettings& settings)
        : Poller(poller)
        , DatabaseProxy(databaseProxy)
        , Settings(settings)
    {}

    STATEFN(StateWorking) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerRegisterResult, Handle);
            hFunc(NActors::TEvPollerReady, Handle);
        }
    }

    void Bootstrap() {
        TSocketType socket;
        TSocketAddressType bindAddress(socket.MakeAddress("::", Settings.Port));
        int err = socket.Bind(bindAddress.get());
        if (err == 0) {
            std::shared_ptr<TEndpointInfo> endpoint = std::make_shared<TEndpointInfo>();
            if (Settings.SslCertificatePem) {
                endpoint->SecureContext = TSslHelpers::CreateServerContext(Settings.SslCertificatePem);
            } else if (Settings.CertificateFile && Settings.PrivateKeyFile) {
                endpoint->SecureContext = TSslHelpers::CreateServerContext(Settings.CertificateFile, Settings.PrivateKeyFile);
            }
            Socket = new TSocketDescriptor(std::move(socket), endpoint);

            err = Socket->Listen(LISTEN_QUEUE);
            if (err == 0) {
                BLOG_D("Listening on " << bindAddress->ToString() << (endpoint->SecureContext ? " (ssl)" : ""));
                Socket->SetNonBlock();
                Send(Poller, new NActors::TEvPollerRegister(Socket, SelfId(), SelfId()));
                Become(&TThis::StateWorking);
                return;
            }
        }
        BLOG_ERROR("Failed to listen on " << bindAddress->ToString());
        //abort();
        PassAway();
    }

    void PassAway() override {
        for (const NActors::TActorId& connection : Connections) {
            Send(connection, new NActors::TEvents::TEvPoisonPill());
        }
    }

    void Handle(NActors::TEvPollerRegisterResult::TPtr ev) {
        PollerToken = std::move(ev->Get()->PollerToken);
        PollerToken->Request(true, false); // request read polling
    }

    void Handle(NActors::TEvPollerReady::TPtr) {
        for (;;) {
            TSocketAddressType addr;
            TIntrusivePtr<TSocketDescriptor> socket = Socket->Accept(addr);
            if (!socket) {
                break;
            }
            NActors::IActor* connectionSocket = CreatePGConnection(socket, addr, DatabaseProxy);
            NActors::TActorId connectionId = Register(connectionSocket);
            Send(Poller, new TEvPollerRegister(socket, connectionId, connectionId));
            Connections.emplace(connectionId);
        }
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) { // request poller for further connection polling
            Y_VERIFY(PollerToken);
            PollerToken->Request(true, false);
        }
    }
};


NActors::IActor* CreatePGListener(const TActorId& poller, const TActorId& databaseProxy, const TListenerSettings& settings) {
    return new TPGListener(poller, databaseProxy, settings);
}

}
