#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/events.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include "interconnect_common.h"
#include "interconnect_counters.h"
#include "interconnect_tcp_session.h"
#include "profiler.h"

#define ICPROXY_PROFILED TFunction func(*this, __func__, __LINE__)

namespace NActors {


    /* WARNING: all proxy actors should be alive during actorsystem activity */
    class TInterconnectProxyTCP
        : public TActor<TInterconnectProxyTCP>
        , public TInterconnectLoggingBase
        , public TProfiled
    {
        enum {
            EvCleanupEventQueue = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvQueryStats,
            EvStats,
            EvPassAwayIfNeeded,
        };

        struct TEvCleanupEventQueue : TEventLocal<TEvCleanupEventQueue, EvCleanupEventQueue> {};

    public:
        struct TEvQueryStats : TEventLocal<TEvQueryStats, EvQueryStats> {};

        struct TProxyStats {
            TString Path;
            TString State;
            TScopeId PeerScopeId;
            TInstant LastSessionDieTime;
            ui64 TotalOutputQueueSize;
            bool Connected;
            bool ExternalDataChannel;
            TString Host;
            ui16 Port;
            TInstant LastErrorTimestamp;
            TString LastErrorKind;
            TString LastErrorExplanation;
            TDuration Ping;
            i64 ClockSkew;
            TString Encryption;
            enum XDCFlags {
                NONE = 0,
                MSG_ZERO_COPY_SEND = 1,
            };
            ui8 XDCFlags;
        };

        struct TEvStats : TEventLocal<TEvStats, EvStats> {
            ui32 PeerNodeId;
            TProxyStats ProxyStats;
        };

        static constexpr EActivityType ActorActivityType() {
            return EActivityType::INTERCONNECT_PROXY_TCP;
        }

        TInterconnectProxyTCP(const ui32 node, TInterconnectProxyCommon::TPtr common, IActor **dynamicPtr = nullptr);

        STFUNC(StateInit) {
            Bootstrap();
            if (ev->Type != TEvents::TSystem::Bootstrap) { // for dynamic nodes we do not receive Bootstrap event
                Receive(ev);
            }
        }

        void Bootstrap();
        void Registered(TActorSystem* sys, const TActorId& owner) override;

    private:
        friend class TInterconnectSessionTCP;
        friend class TInterconnectSessionTCPv0;
        friend class THandshake;
        friend class TInputSessionTCP;

        void UnregisterSession(TInterconnectSessionTCP* session);

#define SESSION_EVENTS(HANDLER)                                \
    fFunc(TEvInterconnect::EvForward, HANDLER)                 \
    fFunc(TEvInterconnect::TEvConnectNode::EventType, HANDLER) \
    fFunc(TEvents::TEvSubscribe::EventType, HANDLER)           \
    fFunc(TEvents::TEvUnsubscribe::EventType, HANDLER)

#define INCOMING_HANDSHAKE_EVENTS(HANDLER)         \
    fFunc(TEvHandshakeAsk::EventType, HANDLER)     \
    fFunc(TEvHandshakeRequest::EventType, HANDLER)

#define HANDSHAKE_STATUS_EVENTS(HANDLER) \
    hFunc(TEvHandshakeDone, HANDLER)     \
    hFunc(TEvHandshakeFail, HANDLER)

#define PROXY_STFUNC(STATE, SESSION_HANDLER, INCOMING_HANDSHAKE_HANDLER,                        \
                     HANDSHAKE_STATUS_HANDLER, DISCONNECT_HANDLER,                              \
                     WAKEUP_HANDLER, NODE_INFO_HANDLER)                                         \
    STATEFN(STATE) {                                                                            \
        const ui32 type = ev->GetTypeRewrite();                                                 \
        const bool profiled = type != TEvInterconnect::EvForward                                \
            && type != TEvInterconnect::EvConnectNode                                           \
            && type != TEvents::TSystem::Subscribe                                              \
            && type != TEvents::TSystem::Unsubscribe;                                           \
        if (profiled) {                                                                         \
            TProfiled::Start();                                                                 \
        }                                                                                       \
        {                                                                                       \
            TProfiled::TFunction func(*this, __func__, __LINE__);                               \
            switch (type) {                                                                     \
                SESSION_EVENTS(SESSION_HANDLER)                                                 \
                INCOMING_HANDSHAKE_EVENTS(INCOMING_HANDSHAKE_HANDLER)                           \
                HANDSHAKE_STATUS_EVENTS(HANDSHAKE_STATUS_HANDLER)                               \
                cFunc(TEvInterconnect::EvDisconnect, DISCONNECT_HANDLER)                        \
                hFunc(TEvents::TEvWakeup, WAKEUP_HANDLER)                                       \
                hFunc(TEvGetSecureSocket, Handle)                                               \
                hFunc(NMon::TEvHttpInfo, GenerateHttpInfo)                                      \
                cFunc(EvCleanupEventQueue, HandleCleanupEventQueue)                             \
                hFunc(TEvInterconnect::TEvNodeInfo, NODE_INFO_HANDLER)                          \
                cFunc(TEvInterconnect::EvClosePeerSocket, HandleClosePeerSocket)                \
                cFunc(TEvInterconnect::EvCloseInputSession, HandleCloseInputSession)            \
                cFunc(TEvInterconnect::EvPoisonSession, HandlePoisonSession)                    \
                hFunc(TEvSessionBufferSizeRequest, HandleSessionBufferSizeRequest)              \
                hFunc(TEvQueryStats, Handle)                                                    \
                cFunc(TEvInterconnect::EvTerminate, HandleTerminate)                            \
                cFunc(EvPassAwayIfNeeded, HandlePassAwayIfNeeded)                               \
                hFunc(TEvSubscribeForConnection, Handle);                                       \
                hFunc(TEvReportConnection, Handle);                                             \
                default:                                                                        \
                    Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);                        \
            }                                                                                   \
        }                                                                                       \
        if (profiled) {                                                                         \
            if (TProfiled::Duration() >= TDuration::MilliSeconds(16)) {                         \
                const TString report = TProfiled::Format();                                     \
                LOG_ERROR_IC("ICP35", "event processing took too much time %s", report.data()); \
            }                                                                                   \
            TProfiled::Finish();                                                                \
        }                                                                                       \
    }

        template <typename T>
        void Ignore(T& /*ev*/) {
            ICPROXY_PROFILED;
        }

        void Ignore() {
            ICPROXY_PROFILED;
        }

        void Ignore(TEvHandshakeDone::TPtr& ev) {
            ICPROXY_PROFILED;

            Y_ABORT_UNLESS(ev->Sender != IncomingHandshakeActor);
            Y_ABORT_UNLESS(ev->Sender != OutgoingHandshakeActor);
        }

        void Ignore(TEvHandshakeFail::TPtr& ev) {
            ICPROXY_PROFILED;

            Y_ABORT_UNLESS(ev->Sender != IncomingHandshakeActor);
            Y_ABORT_UNLESS(ev->Sender != OutgoingHandshakeActor);
            LogHandshakeFail(ev, true);
        }

        const char* State = nullptr;
        TInstant StateSwitchTime;

        template <typename... TArgs>
        void SwitchToState(int line, const char* name, TArgs&&... args) {
            ICPROXY_PROFILED;

            LOG_DEBUG_IC("ICP77", "@%d %s -> %s", line, State, name);
            State = name;
            StateSwitchTime = TActivationContext::Now();
            Become(std::forward<TArgs>(args)...);
            Y_ABORT_UNLESS(!Terminated || CurrentStateFunc() == &TThis::HoldByError); // ensure we never escape this state
            if (CurrentStateFunc() != &TThis::PendingActivation) {
                PassAwayTimestamp = TMonotonic::Max();
            } else if (DynamicPtr) {
                PassAwayTimestamp = TActivationContext::Monotonic() + TDuration::Seconds(15);
                if (!PassAwayScheduled) {
                    TActivationContext::Schedule(PassAwayTimestamp, new IEventHandle(EvPassAwayIfNeeded, 0, SelfId(),
                        {}, nullptr, 0));
                    PassAwayScheduled = true;
                }
            }
        }

        TMonotonic PassAwayTimestamp;
        bool PassAwayScheduled = false;

        void SwitchToInitialState() {
            ICPROXY_PROFILED;

            Y_ABORT_UNLESS(!PendingSessionEvents && !PendingIncomingHandshakeEvents, "%s PendingSessionEvents# %zu"
                " PendingIncomingHandshakeEvents# %zu State# %s", LogPrefix.data(), PendingSessionEvents.size(),
                PendingIncomingHandshakeEvents.size(), State);
            SwitchToState(__LINE__, "PendingActivation", &TThis::PendingActivation);
        }

        void HandlePassAwayIfNeeded() {
            Y_ABORT_UNLESS(PassAwayScheduled);
            const TMonotonic now = TActivationContext::Monotonic();
            if (now >= PassAwayTimestamp) {
                PassAway();
            } else if (PassAwayTimestamp != TMonotonic::Max()) {
                TActivationContext::Schedule(PassAwayTimestamp, new IEventHandle(EvPassAwayIfNeeded, 0, SelfId(),
                    {}, nullptr, 0));
            } else {
                PassAwayScheduled = false;
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PendingActivation
        //
        // In this state we are just waiting for some activities, which may include:
        // * an external Session event
        // * incoming handshake request
        //
        // Upon receiving such event, we put it to corresponding queue and initiate start up by calling IssueGetNodeRequest,
        // which, as the name says, issued TEvGetNode to the nameservice and arms timer to handle timeout (which should not
        // occur, but we want to be sure we don't hang on this), and then switches to PendingNodeInfo state.

        PROXY_STFUNC(PendingActivation,
                     RequestNodeInfo,                     // Session events
                     RequestNodeInfoForIncomingHandshake, // Incoming handshake requests
                     Ignore,                              // Handshake status
                     Ignore,                              // Disconnect request
                     Ignore,                              // Wakeup
                     Ignore                               // Node info
        )

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PendingNodeInfo
        //
        // This state is entered when we asked nameserver to provide description for peer node we are working with. All
        // external Session events and incoming handshake requests are enqueued into their respective queues, TEvNodeInfo
        // is main event that triggers processing. On success, we try to initiate outgoing handshake if needed, or process
        // incoming handshakes. On error, we enter HoldByError state.
        //
        // NOTE: handshake status events are also enqueued as the handshake actor may have generated failure event due to
        //       timeout or some other reason without waiting for acknowledge, and it must be processed correctly to prevent
        //       session hang

        PROXY_STFUNC(PendingNodeInfo,
                     EnqueueSessionEvent,           // Session events
                     EnqueueIncomingHandshakeEvent, // Incoming handshake requests
                     EnqueueIncomingHandshakeEvent, // Handshake status
                     Disconnect,                    // Disconnect request
                     ConfigureTimeout,              // Wakeup
                     Configure                      // Node info
        )

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PendingConnection
        //
        // Here we have issued outgoing handshake or have accepted (or may be both) incoming handshake and we are waiting for
        // the status of the handshake. When one if handshakes finishes, we use this status to establish connection (or to
        // go to error state). When one handshake terminates with error while other is running, we will still wait for the
        // second one to finish.

        PROXY_STFUNC(PendingConnection,
                     EnqueueSessionEvent,   // Session events
                     IncomingHandshake,     // Incoming handshake requests
                     HandleHandshakeStatus, // Handshake status
                     Disconnect,            // Disconnect request
                     Ignore,                // Wakeup
                     Ignore                 // Node info
        )

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // StateWork
        //
        // We have accepted session and process any incoming messages with the session. Incoming handshakes are accepted
        // concurrently and applied when finished.

        PROXY_STFUNC(StateWork,
                     ForwardSessionEventToSession, // Session events
                     IncomingHandshake,            // Incoming handshake requests
                     HandleHandshakeStatus,        // Handshake status
                     Disconnect,                   // Disconnect request
                     Ignore,                       // Wakeup
                     Ignore                        // Node info
        )

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // HoldByError
        //
        // When something bad happens with the connection, we sleep in this state. After wake up we go back to
        // PendingActivation.

        PROXY_STFUNC(HoldByError,
                     DropSessionEvent,                    // Session events
                     RequestNodeInfoForIncomingHandshake, // Incoming handshake requests
                     Ignore,                              // Handshake status
                     Ignore,                              // Disconnect request
                     WakeupFromErrorState,                // Wakeup
                     Ignore                               // Node info
        )

#undef SESSION_EVENTS
#undef INCOMING_HANDSHAKE_EVENTS
#undef HANDSHAKE_STATUS_EVENTS
#undef PROXY_STFUNC

        void ForwardSessionEventToSession(STATEFN_SIG);
        void EnqueueSessionEvent(STATEFN_SIG);

        // Incoming handshake handlers, including special wrapper when the IncomingHandshake is used as fFunc
        void IncomingHandshake(STATEFN_SIG) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvHandshakeAsk, IncomingHandshake);
                hFunc(TEvHandshakeRequest, IncomingHandshake);
                default:
                    Y_ABORT();
            }
        }
        void IncomingHandshake(TEvHandshakeAsk::TPtr& ev);
        void IncomingHandshake(TEvHandshakeRequest::TPtr& ev);

        void RequestNodeInfo(STATEFN_SIG);
        void RequestNodeInfoForIncomingHandshake(STATEFN_SIG);

        void StartInitialHandshake();
        void StartResumeHandshake(ui64 inputCounter);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Incoming handshake event queue processing
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void EnqueueIncomingHandshakeEvent(STATEFN_SIG);
        void EnqueueIncomingHandshakeEvent(TEvHandshakeDone::TPtr& ev);
        void EnqueueIncomingHandshakeEvent(TEvHandshakeFail::TPtr& ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PendingNodeInfo
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        IEventBase* ConfigureTimeoutCookie; // pointer to the scheduled event used to match sent and received events

        void StartConfiguring();
        void Configure(TEvInterconnect::TEvNodeInfo::TPtr& ev);
        void ConfigureTimeout(TEvents::TEvWakeup::TPtr& ev);
        void ProcessConfigured();

        void HandleHandshakeStatus(TEvHandshakeDone::TPtr& ev);
        void HandleHandshakeStatus(TEvHandshakeFail::TPtr& ev);

        void TransitToErrorState(TString Explanation, bool updateErrorLog = true);
        void WakeupFromErrorState(TEvents::TEvWakeup::TPtr& ev);
        void Disconnect();

        const ui32 PeerNodeId;
        IActor **DynamicPtr;

        void ValidateEvent(TAutoPtr<IEventHandle>& ev, const char* func) {
            if (SelfId().NodeId() == PeerNodeId) {
                TString msg = Sprintf("Event Type# 0x%08" PRIx32 " TypeRewrite# 0x%08" PRIx32
                    " from Sender# %s sent to the proxy for the node itself via Interconnect;"
                    " THIS IS NOT A BUG IN INTERCONNECT, check the event sender instead",
                    ev->Type, ev->GetTypeRewrite(), ev->Sender.ToString().data());
                LOG_ERROR_IC("ICP03", "%s", msg.data());
                Y_DEBUG_ABORT_UNLESS(false, "%s", msg.data());
            }

            Y_ABORT_UNLESS(ev->GetTypeRewrite() != TEvInterconnect::EvForward || ev->Recipient.NodeId() == PeerNodeId,
                "Recipient/Proxy NodeId mismatch Recipient# %s Type# 0x%08" PRIx32 " PeerNodeId# %" PRIu32 " Func# %s",
                ev->Recipient.ToString().data(), ev->Type, PeerNodeId, func);
        }

        // Common with helpers
        // All proxy actors share the same information in the object
        // read only
        TInterconnectProxyCommon::TPtr const Common;

        const TActorId& GetNameserviceId() const {
            return Common->NameserviceId;
        }

        TString TechnicalPeerHostName;

        std::shared_ptr<IInterconnectMetrics> Metrics;

        void HandleClosePeerSocket();
        void HandleCloseInputSession();
        void HandlePoisonSession();

        void HandleSessionBufferSizeRequest(TEvSessionBufferSizeRequest::TPtr& ev);

        bool CleanupEventQueueScheduled = false;
        void ScheduleCleanupEventQueue();
        void HandleCleanupEventQueue();
        void CleanupEventQueue();

        // hold all events before connection is established
        struct TPendingSessionEvent {
            TMonotonic Deadline;
            ui32 Size;
            THolder<IEventHandle> Event;

            TPendingSessionEvent(TMonotonic deadline, ui32 size, TAutoPtr<IEventHandle> event)
                : Deadline(deadline)
                , Size(size)
                , Event(event)
            {}
        };
        TDeque<TPendingSessionEvent> PendingSessionEvents;
        ui64 PendingSessionEventsSize = 0;
        void ProcessPendingSessionEvents();
        void DropSessionEvent(STATEFN_SIG);

        TInterconnectSessionTCP* Session = nullptr;
        TActorId SessionID;

        // virtual ids used during handshake to check if it is the connection
        // for the same session or to find out the latest shandshake
        // it's virtual because session actor apears after successfull handshake
        TActorId SessionVirtualId;
        TActorId RemoteSessionVirtualId;

        TActorId GenerateSessionVirtualId() {
            ICPROXY_PROFILED;

            const ui64 localId = TlsActivationContext->ExecutorThread.ActorSystem->AllocateIDSpace(1);
            return NActors::TActorId(SelfId().NodeId(), 0, localId, 0);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TActorId IncomingHandshakeActor;
        TInstant IncomingHandshakeActorFilledIn;
        TInstant IncomingHandshakeActorReset;
        TMaybe<ui64> LastSerialFromIncomingHandshake;
        THolder<IEventBase> HeldHandshakeReply;

        void DropIncomingHandshake(bool poison = true) {
            ICPROXY_PROFILED;

            if (const TActorId& actorId = std::exchange(IncomingHandshakeActor, TActorId())) {
                LOG_DEBUG_IC("ICP111", "dropped incoming handshake: %s poison: %s", actorId.ToString().data(),
                             poison ? "true" : "false");
                if (poison) {
                    Send(actorId, new TEvents::TEvPoisonPill);
                }
                LastSerialFromIncomingHandshake.Clear();
                HeldHandshakeReply.Reset();
                IncomingHandshakeActorReset = TActivationContext::Now();
            }
        }

        void DropOutgoingHandshake(bool poison = true) {
            ICPROXY_PROFILED;

            if (const TActorId& actorId = std::exchange(OutgoingHandshakeActor, TActorId())) {
                LOG_DEBUG_IC("ICP052", "dropped outgoing handshake: %s poison: %s", actorId.ToString().data(),
                             poison ? "true" : "false");
                if (poison) {
                    Send(actorId, new TEvents::TEvPoisonPill);
                }
                OutgoingHandshakeActorReset = TActivationContext::Now();
            }
        }

        void DropHandshakes() {
            ICPROXY_PROFILED;

            DropIncomingHandshake();
            DropOutgoingHandshake();
        }

        void PrepareNewSessionHandshake() {
            ICPROXY_PROFILED;

            // drop existing session if we have one
            if (Session) {
                LOG_INFO_IC("ICP04", "terminating current session as we are negotiating a new one");
                IActor::InvokeOtherActor(*Session, &TInterconnectSessionTCP::Terminate, TDisconnectReason::NewSession());
            }

            // ensure we have no current session
            Y_ABORT_UNLESS(!Session);

            // switch to pending connection state -- we wait for handshakes, we want more handshakes!
            SwitchToState(__LINE__, "PendingConnection", &TThis::PendingConnection);
        }

        void IssueIncomingHandshakeReply(const TActorId& handshakeId, ui64 peerLocalId,
                                         THolder<IEventBase> event);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::unordered_map<TString, TActorId> ConnectionSubscriptions;

        void Handle(TEvSubscribeForConnection::TPtr ev) {
            auto& msg = *ev->Get();
            if (msg.Subscribe) {
                if (const auto [it, inserted] = ConnectionSubscriptions.emplace(msg.HandshakeId, ev->Sender); !inserted) {
                    Y_DEBUG_ABORT_UNLESS(false);
                    ConnectionSubscriptions.erase(it); // collision happened somehow?
                }
            } else {
                ConnectionSubscriptions.erase(msg.HandshakeId);
            }
        }

        void Handle(TEvReportConnection::TPtr ev) {
            if (auto nh = ConnectionSubscriptions.extract(ev->Get()->HandshakeId)) {
                TActivationContext::Send(IEventHandle::Forward(ev, nh.mapped()));
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        TActorId OutgoingHandshakeActor;
        TInstant OutgoingHandshakeActorCreated;
        TInstant OutgoingHandshakeActorReset;

        TInstant LastSessionDieTime;

        void GenerateHttpInfo(NMon::TEvHttpInfo::TPtr& ev);

        void Handle(TEvQueryStats::TPtr& ev);

        TDuration HoldByErrorWakeupDuration = TDuration::Zero();
        TEvents::TEvWakeup* HoldByErrorWakeupCookie;

        THolder<TProgramInfo> RemoteProgramInfo;
        NInterconnect::TSecureSocketContext::TPtr SecureContext;

        void Handle(TEvGetSecureSocket::TPtr ev) {
            auto socket = MakeIntrusive<NInterconnect::TSecureSocket>(*ev->Get()->Socket, SecureContext);
            Send(ev->Sender, new TEvSecureSocket(std::move(socket)));
        }

        TDeque<THolder<IEventHandle>> PendingIncomingHandshakeEvents;

        TDeque<std::tuple<TInstant, TString, TString, ui32>> ErrorStateLog;

        void UpdateErrorStateLog(TInstant now, TString kind, TString explanation) {
            ICPROXY_PROFILED;

            if (ErrorStateLog) {
                auto& back = ErrorStateLog.back();
                TString lastKind, lastExpl;
                if (kind == std::get<1>(back) && explanation == std::get<2>(back)) {
                    std::get<0>(back) = now;
                    ++std::get<3>(back);
                    return;
                }
            }

            ErrorStateLog.emplace_back(now, std::move(kind), std::move(explanation), 1);
            if (ErrorStateLog.size() > 20) {
                ErrorStateLog.pop_front();
            }
        }

        void LogHandshakeFail(TEvHandshakeFail::TPtr& ev, bool inconclusive);

        bool Terminated = false;
        void HandleTerminate();

        void PassAway() override;
    };

}
