
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/poller_actor.h>
#include <util/network/sock.h>
#include <ydb/library/services/services.pb.h>

#include "sock_listener.h"
#include "sock_config.h"
#include "sock64.h"

namespace NKikimr::NRawSocket {

using namespace NActors;

class TSocketListener: public TActorBootstrapped<TSocketListener>, TNetworkConfig {
public:
    using TBase = TActor<TSocketListener>;
    using TThis = TSocketListener;

    TActorId Poller;
    TListenerSettings Settings;
    TConnectionCreator ConnectionCreator;
    NKikimrServices::EServiceKikimr Service;

    TIntrusivePtr<TSocketDescriptor> Socket;
    TPollerToken::TPtr PollerToken;
    THashSet<TActorId> Connections;

    EErrorAction ErrorAction;

    TSocketListener(const TActorId& poller, const TListenerSettings& settings, const TConnectionCreator& connectionCreator,
                    NKikimrServices::EServiceKikimr service, EErrorAction errorAction)
        : Poller(poller)
        , Settings(settings)
        , ConnectionCreator(connectionCreator)
        , Service(service)
        , ErrorAction(errorAction) {
    }

    STATEFN(StateWorking) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvPollerRegisterResult, Handle);
            hFunc(NActors::TEvPollerReady, Handle);
            hFunc(TEvents::TEvUnsubscribe, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void Bootstrap() {
        TSocketType socket({.TcpNotDelay = Settings.TcpNotDelay});
        TSocketAddressType bindAddress(socket.MakeAddress(Settings.Address, Settings.Port));
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
                LOG_INFO_S(*NActors::TlsActivationContext, Service,
                           "Listening on " << bindAddress->ToString() << (endpoint->SecureContext ? " (ssl)" : ""));
                Socket->SetNonBlock();
                Send(Poller, new NActors::TEvPollerRegister(Socket, SelfId(), SelfId()));
                Become(&TThis::StateWorking);
                return;
            } else {
                LOG_ERROR_S(*NActors::TlsActivationContext, Service, "Failed to listen on " << bindAddress->ToString() << ". Error: " << strerror(-err));
            }
        } else {
            LOG_ERROR_S(*NActors::TlsActivationContext, Service, "Failed to bind " << bindAddress->ToString() << ". Error: " << strerror(-err));
        }

        switch(ErrorAction) {
            case EErrorAction::Abort:
                Cerr << "Failed to set up listener on port " << Settings.Port
                                << " errno# " << -err << " (" << strerror(-err) << ")" << Endl;
                abort();
                break;
            
            case EErrorAction::Ignore:
                PassAway();
                break;
        }
    }

    void PassAway() override {
        for (const NActors::TActorId& connection : Connections) {
            Send(connection, new NActors::TEvents::TEvPoison());
        }
    }

    void Handle(TEvents::TEvUnsubscribe::TPtr ev) {
        Connections.erase(ev->Sender);
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
            NActors::IActor* connectionSocket = ConnectionCreator(SelfId(), socket, addr);
            NActors::TActorId connectionId = Register(connectionSocket);
            Send(Poller, new TEvPollerRegister(socket, connectionId, connectionId));
            Connections.emplace(connectionId);
        }
        int err = errno;
        if (err == EAGAIN || err == EWOULDBLOCK) { // request poller for further connection polling
            Y_ABORT_UNLESS(PollerToken);
            PollerToken->Request(true, false);
        }
    }
};

NActors::IActor* CreateSocketListener(const NActors::TActorId& poller, const TListenerSettings& settings,
                                      TConnectionCreator connectionCreator, NKikimrServices::EServiceKikimr service,
                                      EErrorAction errorAction) {
    return new TSocketListener(poller, settings, connectionCreator, service, errorAction);
}

} // namespace NKikimr::NRawSocket
