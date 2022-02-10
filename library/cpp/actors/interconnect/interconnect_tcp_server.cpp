#include "interconnect_tcp_server.h"
#include "interconnect_handshake.h"

#include <library/cpp/actors/core/log.h>
#include <library/cpp/actors/protos/services_common.pb.h>

#include "interconnect_common.h"

namespace NActors {
    TInterconnectListenerTCP::TInterconnectListenerTCP(const TString& address, ui16 port, TInterconnectProxyCommon::TPtr common, const TMaybe<SOCKET>& socket)
        : TActor(&TThis::Initial)
        , TInterconnectLoggingBase(Sprintf("ICListener: %s", SelfId().ToString().data())) 
        , Address(address.c_str(), port)
        , Listener(
            socket
            ? new NInterconnect::TStreamSocket(*socket)
            : nullptr)
        , ExternalSocket(!!Listener)
        , ProxyCommonCtx(std::move(common))
    {
        if (ExternalSocket) {
            SetNonBlock(*Listener);
        }
    }

    TAutoPtr<IEventHandle> TInterconnectListenerTCP::AfterRegister(const TActorId& self, const TActorId& parentId) {
        return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
    }

    void TInterconnectListenerTCP::Die(const TActorContext& ctx) {
        LOG_DEBUG_IC("ICL08", "Dying");
        TActor::Die(ctx);
    }

    int TInterconnectListenerTCP::Bind() {
        NInterconnect::TAddress addr = Address;

        if (ProxyCommonCtx->Settings.BindOnAllAddresses) {
            switch (addr.GetFamily()) {
                case AF_INET: {
                    auto *sa = reinterpret_cast<sockaddr_in*>(addr.SockAddr());
                    sa->sin_addr = {INADDR_ANY};
                    break;
                }

                case AF_INET6: {
                    auto *sa = reinterpret_cast<sockaddr_in6*>(addr.SockAddr());
                    sa->sin6_addr = in6addr_any;
                    break;
                }

                default:
                    Y_FAIL("Unsupported address family");
            }
        }

        Listener = NInterconnect::TStreamSocket::Make(addr.GetFamily());
        if (*Listener == -1) {
            return errno;
        }
        SetNonBlock(*Listener);
        Listener->SetSendBufferSize(ProxyCommonCtx->Settings.GetSendBufferSize()); // TODO(alexvru): WTF?
        SetSockOpt(*Listener, SOL_SOCKET, SO_REUSEADDR, 1);
        if (const auto e = -Listener->Bind(addr)) {
            return e;
        } else if (const auto e = -Listener->Listen(SOMAXCONN)) {
            return e;
        } else {
            return 0;
        }
    }

    void TInterconnectListenerTCP::Bootstrap(const TActorContext& ctx) {
        if (!Listener) {
            if (const int err = Bind()) {
                LOG_ERROR_IC("ICL01", "Bind failed: %s (%s)", strerror(err), Address.ToString().data());
                Listener.Reset();
                Become(&TThis::Initial, TDuration::Seconds(1), new TEvents::TEvBootstrap);
                return;
            }
        }
        if (const auto& callback = ProxyCommonCtx->InitWhiteboard) {
            callback(Address.GetPort(), TlsActivationContext->ExecutorThread.ActorSystem);
        }
        const bool success = ctx.Send(MakePollerActorId(), new TEvPollerRegister(Listener, SelfId(), {}));
        Y_VERIFY(success);
        Become(&TThis::Listen);
    }

    void TInterconnectListenerTCP::Handle(TEvPollerRegisterResult::TPtr ev, const TActorContext& ctx) {
        PollerToken = std::move(ev->Get()->PollerToken);
        Process(ctx);
    }

    void TInterconnectListenerTCP::Process(const TActorContext& ctx) {
        for (;;) {
            NInterconnect::TAddress address;
            const int r = Listener->Accept(address);
            if (r >= 0) {
                LOG_DEBUG_IC("ICL04", "Accepted from: %s", address.ToString().data());
                auto socket = MakeIntrusive<NInterconnect::TStreamSocket>(static_cast<SOCKET>(r));
                ctx.Register(CreateIncomingHandshakeActor(ProxyCommonCtx, std::move(socket)));
                continue;
            } else if (-r != EAGAIN && -r != EWOULDBLOCK) {
                Y_VERIFY(-r != ENFILE && -r != EMFILE && !ExternalSocket);
                LOG_ERROR_IC("ICL06", "Listen failed: %s (%s)", strerror(-r), Address.ToString().data());
                Listener.Reset();
                PollerToken.Reset();
                Become(&TThis::Initial, TDuration::Seconds(1), new TEvents::TEvBootstrap);
            } else if (PollerToken) {
                PollerToken->Request(true, false);
            }
            break;
        }
    }

}
