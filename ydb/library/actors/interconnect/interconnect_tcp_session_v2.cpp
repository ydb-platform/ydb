#include "interconnect_tcp_session_v2.h"
#include "interconnect_tcp_proxy.h"

#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NActors {

    class TInterconnectSessionTCPv2::TDirectSessionV2 final : public IDirectSession {
        TIntrusivePtr<IUringEngine> Engine;
        ui64 Conn;
        std::atomic_bool Stopped{false};

    public:
        TDirectSessionV2(TIntrusivePtr<IUringEngine> engine, ui64 conn)
            : Engine(std::move(engine))
            , Conn(conn)
        {}

        bool Send(TAutoPtr<IEventHandle> ev, TIntrusivePtr<IReceiveCallback> replyCallback) override {
            if (Stopped) {
                return false;
            }
            Engine->Send(Conn, std::unique_ptr<IEventHandle>(ev.Release()), std::move(replyCallback));
            return true;
        }

        void RegisterReceiveCallback(const TActorId& localActorId, TIntrusivePtr<IReceiveCallback> callback) override {
            if (Stopped) {
                return;
            }
            Engine->RegisterReceiveCallback(Conn, localActorId, std::move(callback));
        }

        void UnregisterReceiveCallback(const TActorId& localActorId) override {
            if (Stopped) {
                return;
            }
            Engine->RegisterReceiveCallback(Conn, localActorId, nullptr);
        }

        void Shutdown() {
            Stopped.store(true);
        }
    };

    TInterconnectSessionTCPv2::TInterconnectSessionTCPv2(TInterconnectProxyTCP* const proxy)
        : TActor(&TInterconnectSessionTCPv2::StateFunc)
        , Proxy(proxy)
    {}

    void TInterconnectSessionTCPv2::Init(const TSessionParams& params) {
        Params = params;
        // v2 does not support encryption
        Y_ABORT_UNLESS(!Params.Encryption);
        Proxy->Metrics->SetPeerScopeId(Params.PeerScopeId);
        Proxy->Metrics->SetConnected(0);
        SetPrefix(Sprintf("SessionV2 %s [node %" PRIu32 "]", SelfId().ToString().data(), Proxy->PeerNodeId));
        LOG_INFO_IC_SESSION("ICS90", "v2 session created");
    }

    void TInterconnectSessionTCPv2::SetNewConnection(TEvHandshakeDone::TPtr& ev) {
        // v2 establishes exactly one connection for its lifetime (no continuation)
        Y_ABORT_UNLESS(!Socket, "TInterconnectSessionTCPv2 does not support connection continuation");

        LOG_INFO_IC_SESSION("ICS91", "handshake done sender: %s self: %s peer: %s socket: %" PRIi64,
            ev->Sender.ToString().data(), ev->Get()->Self.ToString().data(), ev->Get()->Peer.ToString().data(),
            i64(*ev->Get()->Socket));

        Socket = std::move(ev->Get()->Socket);
        XdcSocket = std::move(ev->Get()->XdcSocket); // unused by v2 (no external data channel)

        Proxy->Metrics->SetConnected(1);

        // Register the socket with the shared io_uring engine and start driving traffic.
        Y_ABORT_UNLESS(Proxy->Common->UringEngineV2);
        auto onDisconnectCallback = [selfId = SelfId(), as = TActivationContext::ActorSystem()](TDisconnectReason reason) {
            as->Send(selfId, new TEvPrivate::TEvTerminate(reason));
        };
        SetNonBlock(*Socket, false);
        EngineHandle = Proxy->Common->UringEngineV2->Register(Socket, SelfId(),
            Proxy->Common->Settings.ChecksumInterconnectSessionV2, Params.PeerScopeId, onDisconnectCallback,
            SelfId().NodeId() < Proxy->PeerNodeId, ClockSkew, PingRTT);
        if (!EngineHandle) {
            LOG_ERROR_IC_SESSION("ICS99", "v2 io_uring engine failed to register the connection");
            return Terminate(TDisconnectReason::LostConnection());
        }

        DirectSession = std::make_shared<TDirectSessionV2>(Proxy->Common->UringEngineV2, EngineHandle);
    }

    void TInterconnectSessionTCPv2::Terminate(TDisconnectReason reason) {
        LOG_INFO_IC_SESSION("ICS92", "v2 session terminated reason# %s", reason.ToString().data());

        IActor::InvokeOtherActor(*Proxy, &TInterconnectProxyTCP::UnregisterSession, this);

        // atomically disconnect the direct interface so racing user threads observe a clean shutdown
        if (DirectSession) {
            DirectSession->Shutdown();
        }

        // Shutting down the socket forces any in-flight writev/recv to complete promptly.
        if (Socket) {
            Socket->Shutdown(SHUT_RDWR);
        }
        if (XdcSocket) {
            XdcSocket->Shutdown(SHUT_RDWR);
        }

        for (const auto& [actorId, cookie] : Subscribers) {
            Send(actorId, new TEvInterconnect::TEvNodeDisconnected(Proxy->PeerNodeId), 0, cookie);
        }
        Subscribers.clear();

        Proxy->Metrics->SetConnected(0);

        Proxy->Common->UringEngineV2->Unregister(EngineHandle);

        TActor::PassAway();
    }

    THolder<TEvHandshakeAck> TInterconnectSessionTCPv2::ProcessHandshakeRequest(TEvHandshakeAsk::TPtr& ev) {
        Y_UNUSED(ev);
        // v2 does not support continuation; the proxy is expected to reject such requests via
        // SupportsContinuation(), so we should never get here.
        Y_ABORT("TInterconnectSessionTCPv2 does not support handshake continuation");
    }

    void TInterconnectSessionTCPv2::StartHandshake() {
        // no continuation -- lost connection means the session is gone
        LOG_INFO_IC_SESSION("ICS93", "StartHandshake on v2 session -> terminating (no continuation)");
        Terminate(TDisconnectReason::LostConnection());
    }

    void TInterconnectSessionTCPv2::ReestablishConnectionWithHandshake(TDisconnectReason reason) {
        // no continuation -- lost connection means the session is gone
        LOG_INFO_IC_SESSION("ICS94", "ReestablishConnectionWithHandshake on v2 session -> terminating (no continuation)");
        Terminate(std::move(reason));
    }

    void TInterconnectSessionTCPv2::CloseInputSession() {
        // v2 has no separate input session; a request to close the input means the connection is being
        // torn down
        Terminate(TDisconnectReason::UserRequest());
    }

    void TInterconnectSessionTCPv2::AddSubscriber(const TActorId& actorId, ui64 cookie) {
        Subscribers[actorId] = cookie;
    }

    IEventBase* TInterconnectSessionTCPv2::MakeNodeConnectedEvent() const {
        return new TEvInterconnect::TEvNodeConnected(Proxy->PeerNodeId, DirectSession);
    }

    void TInterconnectSessionTCPv2::EnqueueOutgoing(TAutoPtr<IEventHandle> ev) {
        if (Proxy->Common->Settings.EnablePreserializeInV2) {
            ev->Preserialize();
        }
        Proxy->Common->UringEngineV2->Send(EngineHandle, std::unique_ptr<IEventHandle>(ev.Release()));
    }

    void TInterconnectSessionTCPv2::Forward(STATEFN_SIG) {
        Proxy->ValidateEvent(ev, "Forward");
        if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
            AddSubscriber(ev->Sender, ev->Cookie);
            Send(ev->Sender, MakeNodeConnectedEvent(), 0, ev->Cookie);
        }
        EnqueueOutgoing(std::move(ev));
    }

    void TInterconnectSessionTCPv2::ForwardWithSubscribe(STATEFN_SIG) {
        Proxy->ValidateEvent(ev, "ForwardWithSubscribe");
        auto msg = ev->Release<TEvForwardSubscribeSession>();
        Y_ABORT_UNLESS(msg->Event);
        AddSubscriber(msg->Event->Sender, msg->Event->Cookie);
        Send(msg->Event->Sender, MakeNodeConnectedEvent(), 0, msg->Event->Cookie);
        EnqueueOutgoing(TAutoPtr<IEventHandle>(msg->Event.Release()));
    }

    void TInterconnectSessionTCPv2::HandleSubscribe(STATEFN_SIG) {
        LOG_DEBUG_IC_SESSION("ICS96", "subscribe for session state for %s", ev->Sender.ToString().data());
        AddSubscriber(ev->Sender, ev->Cookie);
        Send(ev->Sender, MakeNodeConnectedEvent(), 0, ev->Cookie);
    }

    void TInterconnectSessionTCPv2::HandleUnsubscribe(STATEFN_SIG) {
        LOG_DEBUG_IC_SESSION("ICS97", "unsubscribe for session state for %s", ev->Sender.ToString().data());
        Subscribers.erase(ev->Sender);
    }

    void TInterconnectSessionTCPv2::HandlePoison() {
        Terminate(TDisconnectReason::UserRequest());
    }

    void TInterconnectSessionTCPv2::GenerateHttpInfo(NMon::TEvHttpInfoRes::TPtr& ev) {
        TStringStream str;
        ev->Get()->Output(str);
        str << "<div class=\"panel panel-info\">"
               "<div class=\"panel-heading\">Session (v2)</div>"
               "<div class=\"panel-body\">";
        str << "<table class=\"table\">";
//        str << "<tr><td>Registered</td><td>" << (Registered ? "true" : "false") << "</td></tr>";
        str << "<tr><td>EngineHandle</td><td>" << EngineHandle << "</td></tr>";
//        str << "<tr><td>OutstandingWrites</td><td>" << PendingBatches.size() << "</td></tr>";
        str << "<tr><td>BytesSent</td><td>" << BytesSent << "</td></tr>";
        str << "<tr><td>BytesReceived</td><td>" << BytesReceived << "</td></tr>";
        str << "</table>";
        str << "</div></div>";
        TActivationContext::Send(new IEventHandle(ev->Recipient, ev->Sender, new NMon::TEvHttpInfoRes(str.Str())));
    }

}
