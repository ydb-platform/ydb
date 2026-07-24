#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/log.h>

#include "interconnect_session_iface.h"
#include "interconnect_direct_session.h"
#include "interconnect_uring_engine.h"
#include "v2_event_serializer.h"
#include "logging/logging.h"

#include <deque>
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
    // Its data plane serializes events with TEventSerializer/TEventDeserializer and moves bytes through
    // a single shared io_uring ring owned by TInterconnectUringEngineActor. Outgoing events (both the
    // normal actor-system Forward path and IDirectSession::Send) are serialized and handed to the engine
    // one buffer at a time (single send in flight); incoming bytes are fed to the deserializer and each
    // reconstructed event is offered to the direct-session receive table before falling back to normal
    // actor-system delivery.
    class TInterconnectSessionTCPv2
       : public TActor<TInterconnectSessionTCPv2>
       , public TInterconnectLoggingBase
       , public IInterconnectSession
    {
    public:
        static constexpr EActivityType ActorActivityType() {
            return EActivityType::INTERCONNECT_SESSION_TCP;
        }

        explicit TInterconnectSessionTCPv2(TInterconnectProxyTCP* proxy);

        // IInterconnectSession
        IActor& SessionActor() noexcept override { return *this; }

        void Init(const TSessionParams& params) override;
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
        TDuration GetPingRTT() const override { return TDuration::FromValue(PingRTT->load()); }
        i64 GetClockSkew() const override { return ClockSkew->load(); }
        void GenerateHttpInfo(NMon::TEvHttpInfoRes::TPtr& ev) override;

    private:
        friend class TActor<TInterconnectSessionTCPv2>;

        struct TEvPrivate {
            enum {
                EvTerminate = EventSpaceBegin(TEvents::ES_PRIVATE),
            };

            struct TEvTerminate : TEventLocal<TEvTerminate, EvTerminate> {
                TDisconnectReason Reason;

                TEvTerminate(TDisconnectReason reason) : Reason(reason) {}
            };
        };

        STATEFN(StateFunc) {
            STRICT_STFUNC_BODY(
                fFunc(TEvInterconnect::EvForward, Forward)
                fFunc(TEvForwardSubscribeSession::EventType, ForwardWithSubscribe)
                fFunc(TEvInterconnect::TEvConnectNode::EventType, HandleSubscribe)
                fFunc(TEvents::TEvSubscribe::EventType, HandleSubscribe)
                fFunc(TEvents::TEvUnsubscribe::EventType, HandleUnsubscribe)
                cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison)
                cFunc(TEvInterconnect::EvForwardDelayed, IgnoreForwardDelayed)
                hFunc(TEvPrivate::TEvTerminate, [&](auto& ev) { Terminate(ev->Get()->Reason); });
            )
        }

        void Forward(STATEFN_SIG);
        void ForwardWithSubscribe(STATEFN_SIG);
        void HandleSubscribe(STATEFN_SIG);
        void HandleUnsubscribe(STATEFN_SIG);
        void HandlePoison();
        void IgnoreForwardDelayed() {}

        void EnqueueOutgoing(TAutoPtr<IEventHandle> ev);

        void AddSubscriber(const TActorId& actorId, ui64 cookie);
        IEventBase* MakeNodeConnectedEvent() const;

    private:
        TInterconnectProxyTCP* const Proxy;
        TSessionParams Params;

        TIntrusivePtr<NInterconnect::TStreamSocket> Socket;
        TIntrusivePtr<NInterconnect::TStreamSocket> XdcSocket;

        // thread-safe direct send/receive handle shared with users via TEvNodeConnected
        class TDirectSessionV2;
        std::shared_ptr<TDirectSessionV2> DirectSession;

        // io_uring data plane
        ui64 EngineHandle = 0;

        ui64 BytesSent = 0;
        ui64 BytesReceived = 0;

        // subscribers awaiting connection state notifications (actor id -> cookie)
        THashMap<TActorId, ui64> Subscribers;

        std::shared_ptr<std::atomic<int64_t>> ClockSkew = std::make_shared<std::atomic<int64_t>>();
        std::shared_ptr<std::atomic<uint64_t>> PingRTT = std::make_shared<std::atomic<uint64_t>>();
    };

}
