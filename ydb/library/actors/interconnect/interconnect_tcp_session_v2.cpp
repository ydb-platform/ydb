#include "interconnect_tcp_session_v2.h"
#include "interconnect_tcp_proxy.h"

#include <util/stream/str.h>

#define YDB_LOG_THIS_FILE_COMPONENT ::NActorsServices::INTERCONNECT_SESSION

namespace NActors {

    TInterconnectSessionTCPv2::TInterconnectSessionTCPv2(TInterconnectProxyTCP* const proxy)
        : TActor(&TInterconnectSessionTCPv2::StateFunc)
        , Proxy(proxy)
    { }

    void TInterconnectSessionTCPv2::Init(const TSessionParams& params) {
        Params = params;
        // v2 does not support encryption
        Y_ABORT_UNLESS(!Params.Encryption);
        Proxy->Metrics->SetPeerScopeId(Params.PeerScopeId);
        Proxy->Metrics->SetConnected(0);
        SetPrefix(Sprintf("SessionV2 %s [node %" PRIu32 "]", SelfId().ToString().data(), Proxy->PeerNodeId));

        YDB_LOG_INFO("V2 session created",
            {"marker", "ICS90"});
        DirectSession = std::make_shared<TDirectSessionV2>();
    }

    void TInterconnectSessionTCPv2::SetNewConnection(TEvHandshakeDone::TPtr& ev) {
        // v2 establishes exactly one connection for its lifetime (no continuation)
        Y_ABORT_UNLESS(!Socket, "TInterconnectSessionTCPv2 does not support connection continuation");

        YDB_LOG_INFO("Handshake done socket: %li",
            {"marker", "ICS91"},
            {"sender", ev->Sender},
            {"self", ev->Get()->Self},
            {"peer", ev->Get()->Peer},
            {"socket", i64(*ev->Get()->Socket)});

        Socket = std::move(ev->Get()->Socket);
        XdcSocket = std::move(ev->Get()->XdcSocket);

        Proxy->Metrics->SetConnected(1);

        // NOTE: data-plane setup (input session, poller registration, traffic generation) is stubbed.
        // Subscribers receive the direct interface through TEvNodeConnected upon subscription.
    }

    void TInterconnectSessionTCPv2::Terminate(TDisconnectReason reason) {
        YDB_LOG_INFO("V2 session terminated",
            {"marker", "ICS92"},
            {"reason", reason});

        IActor::InvokeOtherActor(*Proxy, &TInterconnectProxyTCP::UnregisterSession, this);

        // atomically disconnect the direct interface so racing user threads observe a clean shutdown
        if (DirectSession) {
            DirectSession->Shutdown();
        }

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
        YDB_LOG_INFO("StartHandshake on v2 session -> terminating (no continuation)",
            {"marker", "ICS93"});
        Terminate(TDisconnectReason::LostConnection());
    }

    void TInterconnectSessionTCPv2::ReestablishConnectionWithHandshake(TDisconnectReason reason) {
        // no continuation -- lost connection means the session is gone
        YDB_LOG_INFO("ReestablishConnectionWithHandshake on v2 session -> terminating (no continuation)",
            {"marker", "ICS94"});
        Terminate(std::move(reason));
    }

    void TInterconnectSessionTCPv2::CloseInputSession() {
        // no input session in the current stub
    }

    void TInterconnectSessionTCPv2::AddSubscriber(const TActorId& actorId, ui64 cookie) {
        Subscribers[actorId] = cookie;
    }

    IEventBase* TInterconnectSessionTCPv2::MakeNodeConnectedEvent() const {
        return new TEvInterconnect::TEvNodeConnected(Proxy->PeerNodeId, DirectSession);
    }

    void TInterconnectSessionTCPv2::Forward(STATEFN_SIG) {
        Proxy->ValidateEvent(ev, "Forward");
        if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
            AddSubscriber(ev->Sender, ev->Cookie);
            Send(ev->Sender, MakeNodeConnectedEvent(), 0, ev->Cookie);
        }
        // data-plane stub: the payload event is dropped for now
        LOG_DEBUG_IC_SESSION("ICS95", "v2 stub dropping forwarded event to %s", ev->Recipient.ToString().data());
    }

    void TInterconnectSessionTCPv2::ForwardWithSubscribe(STATEFN_SIG) {
        Proxy->ValidateEvent(ev, "ForwardWithSubscribe");
        auto msg = ev->Release<TEvForwardSubscribeSession>();
        Y_ABORT_UNLESS(msg->Event);
        AddSubscriber(msg->Event->Sender, msg->Event->Cookie);
        Send(msg->Event->Sender, MakeNodeConnectedEvent(), 0, msg->Event->Cookie);
        // data-plane stub: the wrapped payload event is dropped for now
    }

    void TInterconnectSessionTCPv2::HandleSubscribe(STATEFN_SIG) {
        YDB_LOG_DEBUG("Subscribe for session state",
            {"marker", "ICS96"},
            {"sender", ev->Sender});
        AddSubscriber(ev->Sender, ev->Cookie);
        Send(ev->Sender, MakeNodeConnectedEvent(), 0, ev->Cookie);
    }

    void TInterconnectSessionTCPv2::HandleUnsubscribe(STATEFN_SIG) {
        YDB_LOG_DEBUG("Unsubscribe for session state",
            {"marker", "ICS97"},
            {"sender", ev->Sender});
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
               "<div class=\"panel-body\">TInterconnectSessionTCPv2: data plane not yet implemented</div>"
               "</div>";
        TActivationContext::Send(new IEventHandle(ev->Recipient, ev->Sender, new NMon::TEvHttpInfoRes(str.Str())));
    }

}
