#include "interconnect_tcp_server.h"
#include "interconnect_handshake.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include "interconnect_common.h"

namespace NActors {
    TInterconnectListenerTCP::TInterconnectListenerTCP(const TString& address, ui16 port, TInterconnectProxyCommon::TPtr common, const TMaybe<SOCKET>& socket)
        : TActor(&TThis::Initial)
        , TInterconnectLoggingBase(Sprintf("ICListener: %s", SelfId().ToString().data()))
        , Address(address)
        , Port(port)
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
        auto doTry = [&](NInterconnect::TAddress addr) {
            int error;
            Listener = NInterconnect::TStreamSocket::Make(addr.GetFamily(), &error);
            if (*Listener == -1) {
                return error;
            }
            SetNonBlock(*Listener);
            if (const auto& buffer = ProxyCommonCtx->Settings.TCPSocketBufferSize) {
                Listener->SetSendBufferSize(buffer);
            }
            SetSockOpt(*Listener, SOL_SOCKET, SO_REUSEADDR, 1);
            if (addr.GetFamily() == AF_INET6) {
                SetSockOpt(*Listener, IPPROTO_IPV6, IPV6_V6ONLY, 0);
            }
            const ui32 backlog = ProxyCommonCtx->Settings.SocketBacklogSize;
            if (const auto e = -Listener->Bind(addr)) {
                return e;
            } else if (const auto e = -Listener->Listen(backlog ? backlog : SOMAXCONN)) {
                return e;
            } else {
                return 0;
            }
        };

        if (Address) {
            NInterconnect::TAddress addr(Address, Port);
            if (ProxyCommonCtx->Settings.BindOnAllAddresses) {
                addr = addr.GetFamily() == AF_INET ? NInterconnect::TAddress::AnyIPv4(Port) :
                    addr.GetFamily() == AF_INET6 ? NInterconnect::TAddress::AnyIPv6(Port) : addr;
            }
            return doTry(addr);
        } else {
            int error = doTry(NInterconnect::TAddress::AnyIPv6(Port));
            if (error == EAFNOSUPPORT || error == EPROTONOSUPPORT) {
                error = doTry(NInterconnect::TAddress::AnyIPv4(Port));
            }
            return error;
        }
    }

    void TInterconnectListenerTCP::Bootstrap(const TActorContext& ctx) {
        if (!Listener) {
            if (const int err = Bind()) {
                LOG_ERROR_IC("ICL01", "Bind failed: %s (%s:%u)", strerror(err), Address.data(), Port);
                Listener.Reset();
                Become(&TThis::Initial, TDuration::Seconds(1), new TEvents::TEvBootstrap);
                return;
            }
        }
        if (const auto& callback = ProxyCommonCtx->InitWhiteboard) {
            callback(Port, TlsActivationContext->ExecutorThread.ActorSystem);
        }
        const bool success = ctx.Send(MakePollerActorId(), new TEvPollerRegister(Listener, SelfId(), {}));
        Y_ABORT_UNLESS(success);
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
                Y_ABORT_UNLESS(-r != ENFILE && -r != EMFILE && !ExternalSocket);
                LOG_ERROR_IC("ICL06", "Listen failed: %s (%s:%u)", strerror(-r), Address.data(), Port);
                Listener.Reset();
                PollerToken.Reset();
                Become(&TThis::Initial, TDuration::Seconds(1), new TEvents::TEvBootstrap);
            } else if (PollerToken && PollerToken->RequestReadNotificationAfterWouldBlock()) {
                continue;
            }
            break;
        }
    }

}
