#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>

#include "interconnect_session_iface.h"
#include "interconnect_direct_session.h"
#include "logging/logging.h"

#include <memory>
#include <unordered_map>

namespace NActors {

    class TInterconnectProxyTCP;

    // A new interconnect session actor. Unlike TInterconnectSessionTCP it:
    //   * does not support session continuation over several consecutive TCP connections;
    //   * does not support encryption;
    //   * exposes a thread-safe direct send/receive interface (IDirectSession) to subscribers via
    //     TEvInterconnect::TEvNodeConnected.
    //
    // NOTE: the data-plane operations (packetization, socket IO, input session, pinging, metrics, ...)
    // are intentionally left as stubs in this first integration step.
    class TInterconnectSessionTCPv2
       : public TActor<TInterconnectSessionTCPv2>
       , public TInterconnectLoggingBase
       , public IInterconnectSession
    {
    public:
        static constexpr EActivityType ActorActivityType() {
            return EActivityType::INTERCONNECT_SESSION_TCP;
        }

        TInterconnectSessionTCPv2(TInterconnectProxyTCP* proxy, TSessionParams params);

        // IInterconnectSession
        IActor& SessionActor() noexcept override { return *this; }

        void Init() override;
        void SetNewConnection(TEvHandshakeDone::TPtr& ev) override;
        void Terminate(TDisconnectReason reason) override;
        THolder<TEvHandshakeAck> ProcessHandshakeRequest(TEvHandshakeAsk::TPtr& ev) override;
        void StartHandshake() override;
        void ReestablishConnectionWithHandshake(TDisconnectReason reason) override;
        void CloseInputSession() override;
        bool IsRdmaInUse() override { return false; }
        bool HasRdmaState() const override { return false; }
        bool SupportsContinuation() const override { return false; }

        const TSessionParams& GetParams() const override { return Params; }
        const TIntrusivePtr<NInterconnect::TStreamSocket>& GetSocket() const override { return Socket; }
        ui64 GetTotalOutputQueueSize() const override { return 0; }
        std::optional<ui8> GetXDCFlags() const override { return std::nullopt; }
        TDuration GetPingRTT() const override { return TDuration::Zero(); }
        i64 GetClockSkew() const override { return 0; }
        void GenerateHttpInfo(NMon::TEvHttpInfoRes::TPtr& ev) override;

    private:
        friend class TActor<TInterconnectSessionTCPv2>;

        STATEFN(StateFunc) {
            STRICT_STFUNC_BODY(
                fFunc(TEvInterconnect::EvForward, Forward)
                fFunc(TEvForwardSubscribeSession::EventType, ForwardWithSubscribe)
                fFunc(TEvInterconnect::TEvConnectNode::EventType, HandleSubscribe)
                fFunc(TEvents::TEvSubscribe::EventType, HandleSubscribe)
                fFunc(TEvents::TEvUnsubscribe::EventType, HandleUnsubscribe)
                cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison)
                cFunc(TEvInterconnect::EvForwardDelayed, IgnoreForwardDelayed)
            )
        }

        void Forward(STATEFN_SIG);
        void ForwardWithSubscribe(STATEFN_SIG);
        void HandleSubscribe(STATEFN_SIG);
        void HandleUnsubscribe(STATEFN_SIG);
        void HandlePoison();
        void IgnoreForwardDelayed() {}

        void AddSubscriber(const TActorId& actorId, ui64 cookie);
        IEventBase* MakeNodeConnectedEvent() const;

        TInterconnectProxyTCP* const Proxy;
        const TSessionParams Params;

        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
        TIntrusivePtr<NInterconnect::TStreamSocket> XdcSocket;

        // thread-safe direct send/receive handle shared with users via TEvNodeConnected
        std::shared_ptr<TDirectSession> DirectSession;

        // subscribers awaiting connection state notifications (actor id -> cookie)
        std::unordered_map<TActorId, ui64, TActorId::THash> Subscribers;
    };

}
