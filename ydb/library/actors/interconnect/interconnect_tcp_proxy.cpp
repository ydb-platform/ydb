#include "interconnect_tcp_proxy.h"
#include "interconnect_handshake.h"
#include "interconnect_tcp_session.h"
#include "interconnect_tcp_session_v2.h"
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <util/system/getpid.h>

namespace NActors {
    static constexpr TDuration GetNodeRequestTimeout = TDuration::Seconds(5);
    static constexpr TDuration BaseRdmaRetryDelay = TDuration::Seconds(5);
    static constexpr ui32 MaxSafeRdmaRetryBackoffLevel = 30;
    static constexpr TDuration RdmaRetryStateCheckDelay = TDuration::Seconds(15);

    static TString PeerNameForHuman(const TString& longName, ui16 port) {
        TStringBuf token;
        TStringBuf(longName).NextTok('.', token);
        return TStringBuilder()
            << (token.size() > 0 ? TString(token) : longName)
            << ':'
            << port;
    }

    TInterconnectProxyTCP::TInterconnectProxyTCP(const ui32 node, TInterconnectProxyCommon::TPtr common,
            IActor **dynamicPtr)
        : TActor(&TThis::StateInit)
        , PeerNodeId(node)
        , DynamicPtr(dynamicPtr)
        , Common(std::move(common))
        , SecureContext(new NInterconnect::TSecureSocketContext(Common))
    {
        Y_ABORT_UNLESS(Common);
        Y_ABORT_UNLESS(Common->NameserviceId);
        if (DynamicPtr) {
            Y_ABORT_UNLESS(!*DynamicPtr);
            *DynamicPtr = this;
        }
        NumDisconnects.fill(0);
    }

    void TInterconnectProxyTCP::Bootstrap() {
        SetPrefix(Sprintf("Proxy %s [node %" PRIu32 "]", SelfId().ToString().data(), PeerNodeId));

        SwitchToInitialState();

        LOG_INFO_IC("ICP01", "ready to work");
    }

    void TInterconnectProxyTCP::Registered(TActorSystem* sys, const TActorId& owner) {
        if (!DynamicPtr) {
            // perform usual bootstrap for static nodes
            sys->Send(new IEventHandle(TEvents::TSystem::Bootstrap, 0, SelfId(), owner, nullptr, 0));
        }
        if (const auto& mon = Common->RegisterMonPage) {
            TString path = Sprintf("peer%04" PRIu32, PeerNodeId);
            TString title = Sprintf("Peer #%04" PRIu32, PeerNodeId);
            mon(path, title, sys, SelfId());
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // PendingActivation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TInterconnectProxyTCP::RequestNodeInfo(STATEFN_SIG) {
        ICPROXY_PROFILED;

        if (ev->GetTypeRewrite() == TEvents::TSystem::Unsubscribe) {
            // do not initiate new session upon receiving this event
            return;
        }

        Y_ABORT_UNLESS(!IncomingHandshakeActor && !OutgoingHandshakeActor && !PendingIncomingHandshakeEvents && !PendingSessionEvents);
        EnqueueSessionEvent(ev);
        StartConfiguring();
    }

    void TInterconnectProxyTCP::RequestNodeInfoForIncomingHandshake(STATEFN_SIG) {
        ICPROXY_PROFILED;

        if (!Terminated) {
            Y_ABORT_UNLESS(!IncomingHandshakeActor && !OutgoingHandshakeActor && !PendingIncomingHandshakeEvents && !PendingSessionEvents);
            EnqueueIncomingHandshakeEvent(ev);
            StartConfiguring();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // PendingNodeInfo
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void TInterconnectProxyTCP::StartConfiguring() {
        ICPROXY_PROFILED;

        Y_ABORT_UNLESS(!IncomingHandshakeActor && !OutgoingHandshakeActor);

        // issue node info request
        Send(Common->NameserviceId, new TEvInterconnect::TEvGetNode(PeerNodeId));

        // arm configure timer; store pointer to event to ensure that we will handle correct one if there were any other
        // wakeup events in flight
        SwitchToState(__LINE__, "PendingNodeInfo", &TThis::PendingNodeInfo, GetNodeRequestTimeout,
                      ConfigureTimeoutCookie = new TEvents::TEvWakeup);
    }

    void TInterconnectProxyTCP::Configure(TEvInterconnect::TEvNodeInfo::TPtr& ev) {
        ICPROXY_PROFILED;

        Y_ABORT_UNLESS(!IncomingHandshakeActor && !OutgoingHandshakeActor && !Session);

        if (!ev->Get()->Node) {
            TransitToErrorState("cannot get node info");
        } else {
            auto& info = *ev->Get()->Node;
            TString name = PeerNameForHuman(info.Host, info.Port);
            TechnicalPeerHostName = info.Host;
            TechnicalPeerPort = info.Port;
            if (!Metrics) {
                Metrics = Common->Metrics ? CreateInterconnectMetrics(Common) : CreateInterconnectCounters(Common);
            }
            const TString peerLabel = Common->Settings.MergePerHostCounters ? info.Host
                : Common->Settings.MergePerScopeClassCounters ? TString("unknown")
                : name;
            Metrics->SetPeerInfo(name, info.Location.GetDataCenterId(), peerLabel);
            PeerBridgePileName = info.Location.GetBridgePileName();

            LOG_DEBUG_IC("ICP02", "configured for host %s", name.data());

            ProcessConfigured();
        }
    }

    void TInterconnectProxyTCP::ConfigureTimeout(TEvents::TEvWakeup::TPtr& ev) {
        ICPROXY_PROFILED;

        if (ev->Get() == ConfigureTimeoutCookie) {
            TransitToErrorState("timed out while waiting for node info");
        }
    }

    void TInterconnectProxyTCP::ProcessConfigured() {
        ICPROXY_PROFILED;

        // if the request was initiated by some activity involving Interconnect, then we are expected to start handshake
        if (PendingSessionEvents) {
            StartInitialHandshake();
        }

        // process incoming handshake requests; all failures were ejected from the queue along with the matching initiation requests
        for (THolder<IEventHandle>& ev : PendingIncomingHandshakeEvents) {
            TAutoPtr<IEventHandle> x(ev.Release());
            IncomingHandshake(x);
        }
        PendingIncomingHandshakeEvents.clear();

        // possible situation -- incoming handshake arrives, but actually it is not satisfied and rejected; in this case
        // we are going to return to initial state as we have nothing to do
        if (!IncomingHandshakeActor && !OutgoingHandshakeActor) {
            SwitchToInitialState();
        }
    }

    void TInterconnectProxyTCP::StartInitialHandshake() {
        ICPROXY_PROFILED;

        // since we are starting initial handshake for some reason, we'll drop any existing handshakes, if any
        DropHandshakes();

        // create and register handshake actor
        OutgoingHandshakeActor = Register(CreateOutgoingHandshakeActor(Common, GenerateSessionVirtualId(),
            TActorId(), PeerNodeId, 0, TechnicalPeerHostName, TSessionParams()), TMailboxType::ReadAsFilled);
        OutgoingHandshakeActorCreated = TActivationContext::Now();

        // prepare for new handshake
        PrepareNewSessionHandshake();
    }

    void TInterconnectProxyTCP::StartResumeHandshake(ui64 inputCounter) {
        ICPROXY_PROFILED;

        // drop outgoing handshake if we have one; keep incoming handshakes as they may be useful
        DropOutgoingHandshake();

        // ensure that we have session
        Y_ABORT_UNLESS(Session);

        // ensure that we have both virtual ids
        Y_ABORT_UNLESS(SessionVirtualId);
        Y_ABORT_UNLESS(RemoteSessionVirtualId);

        // create and register handshake actor
        OutgoingHandshakeActor = Register(CreateOutgoingHandshakeActor(Common, SessionVirtualId,
            RemoteSessionVirtualId, PeerNodeId, inputCounter, TechnicalPeerHostName, Session->GetParams()),
            TMailboxType::ReadAsFilled);
        OutgoingHandshakeActorCreated = TActivationContext::Now();
    }

    void TInterconnectProxyTCP::IssueIncomingHandshakeReply(const TActorId& handshakeId, ui64 peerLocalId,
            THolder<IEventBase> event) {
        ICPROXY_PROFILED;

        Y_ABORT_UNLESS(!IncomingHandshakeActor);
        IncomingHandshakeActor = handshakeId;
        IncomingHandshakeActorFilledIn = TActivationContext::Now();
        Y_ABORT_UNLESS(!LastSerialFromIncomingHandshake || *LastSerialFromIncomingHandshake <= peerLocalId);
        LastSerialFromIncomingHandshake = peerLocalId;

        if (OutgoingHandshakeActor && SelfId().NodeId() < PeerNodeId) {
            // Both outgoing and incoming handshake are in progress. To prevent race condition during semultanous handshake
            // incoming handshake must be held till outgoing handshake is complete or failed
            LOG_DEBUG_IC("ICP06", "reply for incoming handshake (actor %s) is held", IncomingHandshakeActor.ToString().data());
            HeldHandshakeReply = std::move(event);

            // Check that we are in one of acceptable states that would properly handle handshake statuses.
            const auto state = CurrentStateFunc();
            Y_ABORT_UNLESS(state == &TThis::PendingConnection || state == &TThis::StateWork, "invalid handshake request in state# %s", State);
        } else {
            LOG_DEBUG_IC("ICP07", "issued incoming handshake reply");

            // No race, so we can send reply immediately.
            Y_ABORT_UNLESS(!HeldHandshakeReply);
            Send(IncomingHandshakeActor, event.Release());

            // Start waiting for handshake reply, if not yet started; also, if session is already created, then we don't
            // switch from working state.
            if (!Session) {
                LOG_INFO_IC("ICP08", "No active sessions, becoming PendingConnection");
                SwitchToState(__LINE__, "PendingConnection", &TThis::PendingConnection);
            } else {
                Y_ABORT_UNLESS(CurrentStateFunc() == &TThis::StateWork);
            }
        }
    }

    void TInterconnectProxyTCP::IncomingHandshake(TEvHandshakeAsk::TPtr& ev) {
        ICPROXY_PROFILED;

        TEvHandshakeAsk *msg = ev->Get();

        // TEvHandshakeAsk is only applicable for continuation requests
        LOG_DEBUG_IC("ICP09", "(actor %s) from: %s for: %s", ev->Sender.ToString().data(),
                     ev->Get()->Self.ToString().data(), ev->Get()->Peer.ToString().data());

        if (!Session) {
            // if there is no open session, report error -- continuation request works only with open sessions
            LOG_NOTICE_IC("ICP12", "(actor %s) peer tries to resume nonexistent session Self# %s Peer# %s",
                ev->Sender.ToString().data(), msg->Self.ToString().data(), msg->Peer.ToString().data());
        } else if (SessionVirtualId != ev->Get()->Peer || RemoteSessionVirtualId != ev->Get()->Self) {
            // check session virtual ids for continuation
            LOG_NOTICE_IC("ICP13", "(actor %s) virtual id mismatch with existing session (Peer: %s Self: %s"
                   " SessionVirtualId: %s RemoteSessionVirtualId: %s)", ev->Sender.ToString().data(),
                  ev->Get()->Peer.ToString().data(), ev->Get()->Self.ToString().data(), SessionVirtualId.ToString().data(),
                  RemoteSessionVirtualId.ToString().data());
        } else if (!Session->SupportsContinuation()) {
            // v2 sessions do not support continuation; reject the resume request so the peer establishes
            // a brand new session instead
            LOG_NOTICE_IC("ICP14", "(actor %s) rejecting resume for session that does not support continuation"
                " Self# %s Peer# %s", ev->Sender.ToString().data(), msg->Self.ToString().data(),
                msg->Peer.ToString().data());
        } else if (Session->HasRdmaState()) {
            LOG_NOTICE_IC("ICRDMA", "(actor %s) rejecting graceful reconnect for RDMA session Self# %s Peer# %s",
                ev->Sender.ToString().data(), msg->Self.ToString().data(), msg->Peer.ToString().data());
            InvokeSession(&IInterconnectSession::Terminate, TDisconnectReason::NewSession());
        } else {
            // if we already have incoming handshake, then terminate existing one
            DropIncomingHandshake();

            // issue reply to the sender, possibly holding it while outgoing handshake is at race
            THolder<IEventBase> reply = InvokeSession(&IInterconnectSession::ProcessHandshakeRequest, ev);
            return IssueIncomingHandshakeReply(ev->Sender, RemoteSessionVirtualId.LocalId(), std::move(reply));
        }

        // error case -- report error to the handshake actor
        Send(ev->Sender, new TEvHandshakeNak);
    }

    void TInterconnectProxyTCP::IncomingHandshake(TEvHandshakeRequest::TPtr& ev) {
        ICPROXY_PROFILED;

        LOG_DEBUG_IC("ICP17", "incoming handshake (actor %s)", ev->Sender.ToString().data());

        const auto& record = ev->Get()->Record;
        ui64 remotePID = record.GetProgramPID();
        ui64 remoteStartTime = record.GetProgramStartTime();
        ui64 remoteSerial = record.GetSerial();

        if (RemoteProgramInfo && remotePID == RemoteProgramInfo->PID && remoteStartTime == RemoteProgramInfo->StartTime) {
            if (remoteSerial < RemoteProgramInfo->Serial) {
                LOG_INFO_IC("ICP18", "handshake (actor %s) is too old", ev->Sender.ToString().data());
                Send(ev->Sender, new TEvents::TEvPoisonPill);
                return;
            } else {
                RemoteProgramInfo->Serial = remoteSerial;
            }
        } else {
            const auto ptr = new TProgramInfo;
            ptr->PID = remotePID;
            ptr->StartTime = remoteStartTime;
            ptr->Serial = remoteSerial;
            RemoteProgramInfo.Reset(ptr);
        }

        /* Let's check peer technical hostname */
        if (record.HasSenderHostName() && TechnicalPeerHostName != record.GetSenderHostName()) {
            Send(ev->Sender, new TEvHandshakeReplyError("host name mismatch"));
            return;
        }

        // check sender actor id and check if it is not very old
        if (LastSerialFromIncomingHandshake) {
            const ui64 serial = record.GetSerial();
            if (serial < *LastSerialFromIncomingHandshake) {
                LOG_NOTICE_IC("ICP15", "Handshake# %s has duplicate serial# %" PRIu64
                    " LastSerialFromIncomingHandshake# %" PRIu64, ev->Sender.ToString().data(),
                    serial, *LastSerialFromIncomingHandshake);
                Send(ev->Sender, new TEvHandshakeReplyError("duplicate serial"));
                return;
            } else if (serial == *LastSerialFromIncomingHandshake) {
                LOG_NOTICE_IC("ICP00", "Handshake# %s is obsolete, serial# %" PRIu64
                    " LastSerialFromIncomingHandshake# %" PRIu64, ev->Sender.ToString().data(),
                    serial, *LastSerialFromIncomingHandshake);
                Send(ev->Sender, new TEvents::TEvPoisonPill);
                return;
            }
        }

        // drop incoming handshake as this is definitely more recent
        DropIncomingHandshake();

        // prepare for new session
        PrepareNewSessionHandshake();

        auto event = MakeHolder<TEvHandshakeReplyOK>();
        auto* pb = event->Record.MutableSuccess();
        const TActorId virtualId = GenerateSessionVirtualId();
        pb->SetProtocol(INTERCONNECT_PROTOCOL_VERSION);
        pb->SetSenderActorId(virtualId.ToString());
        pb->SetProgramPID(GetPID());
        pb->SetProgramStartTime(Common->StartTime);
        pb->SetSerial(virtualId.LocalId());

        IssueIncomingHandshakeReply(ev->Sender, 0, std::move(event));
    }

    void TInterconnectProxyTCP::HandleHandshakeStatus(TEvHandshakeDone::TPtr& ev) {
        ICPROXY_PROFILED;

        TEvHandshakeDone *msg = ev->Get();

        bool runDelayedRdmaHandshakeTimer = false;
        const bool rdmaHandshakeSucceeded = msg->RdmaHanshakeResult.IsOk();

        const auto handshakeSuccessLogPriority = HoldByErrorWakeupDuration != TDuration::Zero()
            ? NLog::PRI_NOTICE
            : NLog::PRI_INFO;

        // Terminate handshake actor working in opposite direction, if set up.
        if (ev->Sender == IncomingHandshakeActor) {
            LOG_LOG_IC(NActorsServices::INTERCONNECT, "ICP19", handshakeSuccessLogPriority, "incoming handshake succeeded");
            DropIncomingHandshake(false);
            DropOutgoingHandshake();
        } else if (ev->Sender == OutgoingHandshakeActor) {
            LOG_LOG_IC(NActorsServices::INTERCONNECT, "ICP20", handshakeSuccessLogPriority, "outgoing handshake succeeded");
            if (auto rdmaDisabled = ev->Get()->RdmaHanshakeResult.GetDisabled()) {
                runDelayedRdmaHandshakeTimer = rdmaDisabled->RunDelayedHandshake;
            }
            DropIncomingHandshake();
            DropOutgoingHandshake(false);
        } else {
            /* It seems to be an old handshake. */
            return;
        }

        // drop any pending XDC subscriptions
        ConnectionSubscriptions.clear();

        Y_ABORT_UNLESS(!IncomingHandshakeActor && !OutgoingHandshakeActor);
        SwitchToState(__LINE__, "StateWork", &TThis::StateWork);

        if (Session) {
            // this is continuation request, check that virtual ids match
            Y_ABORT_UNLESS(SessionVirtualId == msg->Self && RemoteSessionVirtualId == msg->Peer);
        } else {
            // this is initial request, check that we have virtual ids not filled in
            Y_ABORT_UNLESS(!SessionVirtualId && !RemoteSessionVirtualId);
        }

        auto error = [&](const char* description) {
            TransitToErrorState(description);
        };

        if (Session && msg->RdmaHanshakeResult.HasPreinitedSession()) {
            return error("Unexpected prepared session while we already has one");
        }

        // If session is not created, then create new one.
        if (!Session) {
            RemoteProgramInfo = std::move(msg->ProgramInfo);
            if (!RemoteProgramInfo) {
                // we have received resume handshake, but session was closed concurrently while handshaking
                return error("Session continuation race");
            }

            // Create new session actor.
            ++RdmaRetryWatchdogCookie;
            if (msg->Params.UseSessionV2) {
                Session = new TInterconnectSessionTCPv2(this);
            } else {
                Session = new TInterconnectSessionTCP(this);
            }
            SessionID = RegisterWithSameMailbox(&Session->SessionActor());
            InvokeSession(&IInterconnectSession::Init, msg->Params);
            SessionVirtualId = msg->Self;
            RemoteSessionVirtualId = msg->Peer;
            LOG_INFO_IC("ICP22", "created new session: %s", SessionID.ToString().data());
        }

        // ensure that we have session local/peer virtual ids
        Y_ABORT_UNLESS(Session && SessionVirtualId && RemoteSessionVirtualId);

        // Set up new connection for the session.
        InvokeSession(&IInterconnectSession::SetNewConnection, ev);

        // Reset retry timer
        HoldByErrorWakeupDuration = TDuration::Zero();

        /* Forward all held events */
        ProcessPendingSessionEvents();

        if (rdmaHandshakeSucceeded) {
            RegisterRdmaSuccess();
        }

        if (runDelayedRdmaHandshakeTimer) {
            RegisterRdmaFailure();
        }
    }

    void TInterconnectProxyTCP::HandleHandshakeStatus(TEvHandshakeFail::TPtr& ev) {
        ICPROXY_PROFILED;

        // update error state log; this fail is inconclusive unless this is the last pending handshake
        const bool inconclusive = (ev->Sender != IncomingHandshakeActor && ev->Sender != OutgoingHandshakeActor) ||
            (IncomingHandshakeActor && OutgoingHandshakeActor);
        LogHandshakeFail(ev, inconclusive);

        const auto handshakeFailLogPriority = HoldByErrorWakeupDuration != TDuration::Zero()
            ? NLog::PRI_DEBUG
            : NLog::PRI_NOTICE;

        if (ev->Sender == IncomingHandshakeActor) {
            if (handshakeFailLogPriority == NLog::PRI_NOTICE) {
                LogHandshakeStatusNotice("ICP24", EHandshakeStatusDirection::Incoming, *ev->Get());
            } else {
                LOG_LOG_IC(NActorsServices::INTERCONNECT, "ICP24", handshakeFailLogPriority,
                           "incoming handshake failed, temporary: %" PRIu32 " explanation: %s outgoing: %s",
                           ui32(ev->Get()->Temporary), ev->Get()->Explanation.data(), OutgoingHandshakeActor.ToString().data());
            }
            DropIncomingHandshake(false);
        } else if (ev->Sender == OutgoingHandshakeActor) {
            if (handshakeFailLogPriority == NLog::PRI_NOTICE) {
                LogHandshakeStatusNotice("ICP25", EHandshakeStatusDirection::Outgoing, *ev->Get());
            } else {
                LOG_LOG_IC(NActorsServices::INTERCONNECT, "ICP25", handshakeFailLogPriority,
                           "outgoing handshake failed, temporary: %" PRIu32 " explanation: %s incoming: %s held: %s",
                           ui32(ev->Get()->Temporary), ev->Get()->Explanation.data(), IncomingHandshakeActor.ToString().data(),
                           HeldHandshakeReply ? "yes" : "no");
            }
            DropOutgoingHandshake(false);

            if (IEventBase* reply = HeldHandshakeReply.Release()) {
                Y_ABORT_UNLESS(IncomingHandshakeActor);
                LOG_DEBUG_IC("ICP26", "sent held handshake reply to %s", IncomingHandshakeActor.ToString().data());
                Send(IncomingHandshakeActor, reply);
            }

            // if we have no current session, then we have to drop all pending events as the outgoing handshake has failed
            ProcessPendingSessionEvents();
        } else {
            /* It seems to be an old fail, just ignore it */
            LOG_DEBUG_IC("ICP27", "obsolete handshake fail ignored");
            return;
        }

        if (Metrics) {
            Metrics->IncHandshakeFails();
        }
        if (Common->Settings.MergePerHostCounters) {
            LOG_NOTICE_IC("ICP36", "peer-level handshake fail PeerNodeId# %" PRIu32 " Peer# %s Host# %s Temporary# %u"
                " Explanation# %s", PeerNodeId, Metrics ? Metrics->GetHumanFriendlyPeerHostName().data() : "",
                TechnicalPeerHostName.data(), ui32(ev->Get()->Temporary), ev->Get()->Explanation.data());
        }

        if (IncomingHandshakeActor || OutgoingHandshakeActor) {
            // one of handshakes is still going on
            LOG_DEBUG_IC("ICP28", "other handshake is still going on");
            return;
        }

        switch (ev->Get()->Temporary) {
            case TEvHandshakeFail::HANDSHAKE_FAIL_TRANSIENT:
                if (!Session) {
                    if (PendingSessionEvents) {
                        // try to start outgoing handshake as we have some events enqueued
                        StartInitialHandshake();
                    } else {
                        // return back to initial state as we have no session and no pending handshakes
                        SwitchToInitialState();
                    }
                } else if (Session->GetSocket()) {
                    // try to reestablish connection -- meaning restart handshake from the last known position
                    InvokeSession(&IInterconnectSession::ReestablishConnectionWithHandshake,
                        TDisconnectReason::HandshakeFailTransient());
                } else {
                    // we have no active connection in that session, so just restart handshake from last known position
                    InvokeSession(&IInterconnectSession::StartHandshake);
                }
                break;

            case TEvHandshakeFail::HANDSHAKE_FAIL_SESSION_MISMATCH:
                StartInitialHandshake();
                break;

            case TEvHandshakeFail::HANDSHAKE_FAIL_PERMANENT:
                TString timeExplanation = LastSessionDieTime != TInstant::Zero()
                                          ? " LastSessionDieTime# " + LastSessionDieTime.ToString()
                                          : TString();
                if (Session) {
                    InvokeSession(&IInterconnectSession::Terminate,
                        TDisconnectReason::HandshakeFailPermanent());
                }
                TransitToErrorState(ev->Get()->Explanation + timeExplanation, false, ev->Get());
                break;
        }
    }

    void TInterconnectProxyTCP::LogHandshakeFail(TEvHandshakeFail::TPtr& ev, bool inconclusive) {
        ICPROXY_PROFILED;

        TString kind = "unknown";
        switch (ev->Get()->Temporary) {
            case TEvHandshakeFail::HANDSHAKE_FAIL_TRANSIENT:
                kind = Session ? "transient w/session" : "transient w/o session";
                break;

            case TEvHandshakeFail::HANDSHAKE_FAIL_SESSION_MISMATCH:
                kind = "session_mismatch";
                break;

            case TEvHandshakeFail::HANDSHAKE_FAIL_PERMANENT:
                kind = "permanent";
                break;
        }
        if (inconclusive) {
            kind += " inconclusive";
        }
        UpdateErrorStateLog(TActivationContext::Now(), kind, ev->Get()->Explanation);
    }

    void TInterconnectProxyTCP::Handle(TEvProxyCall::TPtr& ev) {
        ICPROXY_PROFILED;
        if (Session) {
            ev->Get()->ReportError("Proxy call over owned session is not supported yet");
        } else {
            ev->Get()->Call(this);
        }
        TlsActivationContext->Send(ev->Forward(ev->Sender));
    }

    void TInterconnectProxyTCP::ProcessPendingSessionEvents() {
        ICPROXY_PROFILED;

        while (PendingSessionEvents) {
            TPendingSessionEvent ev = std::move(PendingSessionEvents.front());
            PendingSessionEventsSize -= ev.Size;
            TAutoPtr<IEventHandle> event(ev.Event.Release());
            PendingSessionEvents.pop_front();

            if (Session) {
                ForwardSessionEventToSession(event);
            } else {
                DropSessionEvent(event);
            }
        }
    }

    void TInterconnectProxyTCP::DropSessionEvent(STATEFN_SIG) {
        ICPROXY_PROFILED;

        ValidateEvent(ev, "DropSessionEvent");
        switch (ev->GetTypeRewrite()) {
            case TEvInterconnect::EvForward:
                if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
                    Send(ev->Sender, new TEvInterconnect::TEvNodeDisconnected(PeerNodeId), 0, ev->Cookie);
                }
                TActivationContext::Send(IEventHandle::ForwardOnNondelivery(ev, TEvents::TEvUndelivered::Disconnected));
                break;

            case TEvForwardSubscribeSession::EventType: {
                auto msg = ev->Release<TEvForwardSubscribeSession>();
                if (msg->Event) {
                    Send(msg->Event->Sender, new TEvInterconnect::TEvNodeDisconnected(PeerNodeId), 0, msg->Event->Cookie);
                    TActivationContext::Send(IEventHandle::ForwardOnNondelivery(std::unique_ptr<IEventHandle>(msg->Event.Release()),
                        TEvents::TEvUndelivered::Disconnected));
                }
                break;
            }

            case TEvInterconnect::TEvConnectNode::EventType:
            case TEvents::TEvSubscribe::EventType:
                Send(ev->Sender, new TEvInterconnect::TEvNodeDisconnected(PeerNodeId), 0, ev->Cookie);
                break;

            case TEvents::TEvUnsubscribe::EventType:
                /* Do nothing */
                break;

            default:
                Y_ABORT("Unexpected type of event in held event queue");
        }
    }

    void TInterconnectProxyTCP::UnregisterSession(IInterconnectSession* session) {
        ICPROXY_PROFILED;

        Y_ABORT_UNLESS(Session && Session == session && SessionID);

        LOG_INFO_IC("ICP30", "unregister session Session# %s VirtualId# %s", SessionID.ToString().data(),
            SessionVirtualId.ToString().data());

        Session = nullptr;
        SessionID = TActorId();
        DelayedRdmaHandshakeTimeout = TDuration::Zero();
        SetRdmaRetryWatchdogPending(false);

        // drop all pending events as we are closed
        ProcessPendingSessionEvents();

        // reset virtual ids as this session is terminated
        SessionVirtualId = TActorId();
        RemoteSessionVirtualId = TActorId();

        if (Metrics) {
            Metrics->IncSessionDeaths();
        }
        LastSessionDieTime = TActivationContext::Now();

        if (IncomingHandshakeActor || OutgoingHandshakeActor) {
            PrepareNewSessionHandshake();
        } else {
            SwitchToInitialState();
        }
    }

    void TInterconnectProxyTCP::EnqueueSessionEvent(STATEFN_SIG) {
        ICPROXY_PROFILED;

        if (InErrorState) {
            return DropSessionEvent(ev);
        }

        ValidateEvent(ev, "EnqueueSessionEvent");
        const ui32 size = ev->GetSize();
        PendingSessionEventsSize += size;
        PendingSessionEvents.emplace_back(TActivationContext::Monotonic() + Common->Settings.MessagePendingTimeout, size, ev);
        ScheduleCleanupEventQueue();
        CleanupEventQueue();
    }

    void TInterconnectProxyTCP::EnqueueIncomingHandshakeEvent(STATEFN_SIG) {
        ICPROXY_PROFILED;

        // enqueue handshake request
        Y_UNUSED();
        PendingIncomingHandshakeEvents.emplace_back(ev);
    }

    void TInterconnectProxyTCP::EnqueueIncomingHandshakeEvent(TEvHandshakeDone::TPtr& /*ev*/) {
        ICPROXY_PROFILED;

        // TEvHandshakeDone can't get into the queue, because we have to process handshake request first; this may be the
        // race with the previous handshakes, so simply ignore it
    }

    void TInterconnectProxyTCP::EnqueueIncomingHandshakeEvent(TEvHandshakeFail::TPtr& ev) {
        ICPROXY_PROFILED;

        for (auto it = PendingIncomingHandshakeEvents.begin(); it != PendingIncomingHandshakeEvents.end(); ++it) {
            THolder<IEventHandle>& pendingEvent = *it;
            if (pendingEvent->Sender == ev->Sender) {
                // we have found cancellation request for the pending handshake request; so simply remove it from the
                // deque, as we are not interested in failure reason; must likely it happens because of handshake timeout
                if (pendingEvent->GetTypeRewrite() == TEvHandshakeFail::EventType) {
                    TEvHandshakeFail::TPtr tmp(static_cast<TEventHandle<TEvHandshakeFail>*>(pendingEvent.Release()));
                    LogHandshakeFail(tmp, true);
                }
                PendingIncomingHandshakeEvents.erase(it);
                break;
            }
        }
    }

    void TInterconnectProxyTCP::ForwardSessionEventToSession(STATEFN_SIG) {
        ICPROXY_PROFILED;

        Y_ABORT_UNLESS(Session && SessionID);
        ValidateEvent(ev, "ForwardSessionEventToSession");
        IActor::InvokeOtherActor(Session->SessionActor(), &IActor::Receive, ev);
    }

    void TInterconnectProxyTCP::SetRdmaRetryWatchdogPending(bool pending) {
        if (RdmaRetryWatchdogPending == pending) {
            return;
        }

        RdmaRetryWatchdogPending = pending;
        if (Metrics) {
            Metrics->SetRdmaRetryWatchdogPending(pending ? 1 : 0);
        }
    }

    TDuration TInterconnectProxyTCP::GetNextRdmaRetryDelay() const {
        const ui32 failureIndex = ConsecutiveRdmaFailures ? ConsecutiveRdmaFailures - 1 : 0;
        const ui32 maxBackoffLevel = Min(Common->Settings.MaxRdmaRetryBackoffLevel, MaxSafeRdmaRetryBackoffLevel);
        const ui32 backoffLevel = Min(failureIndex, maxBackoffLevel);
        return BaseRdmaRetryDelay * (1u << backoffLevel);
    }

    TDuration TInterconnectProxyTCP::GetMaxRdmaRetryDelay() const {
        const ui32 maxBackoffLevel = Min(Common->Settings.MaxRdmaRetryBackoffLevel, MaxSafeRdmaRetryBackoffLevel);
        return BaseRdmaRetryDelay * (1u << maxBackoffLevel);
    }

    void TInterconnectProxyTCP::RegisterRdmaSuccess() {
        LastRdmaSuccessAt = TActivationContext::Now();
    }

    void TInterconnectProxyTCP::RegisterRdmaFailure() {
        const TInstant now = TActivationContext::Now();
        if (LastRdmaSuccessAt != TInstant::Zero() && LastRdmaSuccessAt > LastRdmaFailureAt) {
            const TDuration stableRdmaPeriod = now - LastRdmaSuccessAt;
            const TDuration resetDelay = GetMaxRdmaRetryDelay();
            if (stableRdmaPeriod >= resetDelay) {
                LOG_NOTICE_IC("ICP40", "reset rdma retry failures after stable rdma period for session: %s"
                    " failures: %" PRIu32 " stable: %s threshold: %s", SessionID.ToString().data(),
                    ConsecutiveRdmaFailures, stableRdmaPeriod.ToString().data(), resetDelay.ToString().data());
                ConsecutiveRdmaFailures = 0;
            }
        }
        LastRdmaFailureAt = now;

        if (ConsecutiveRdmaFailures != Max<ui32>()) {
            ++ConsecutiveRdmaFailures;
        }
        ScheduleDelayedRdmaHandshake();
    }

    void TInterconnectProxyTCP::ScheduleDelayedRdmaHandshake() {
        if (DelayedRdmaHandshakeTimeout) {
            LOG_DEBUG_IC("ICP37", "rdma delayed handshake already scheduled for session: %s failures: %" PRIu32
                " timeout: %s", SessionID.ToString().data(), ConsecutiveRdmaFailures,
                DelayedRdmaHandshakeTimeout.ToString().data());
            return;
        }

        DelayedRdmaHandshakeTimeout = GetNextRdmaRetryDelay();
        SetRdmaRetryWatchdogPending(true);
        LOG_NOTICE_IC("ICP38", "schedule delayed rdma handshake for session: %s failures: %" PRIu32 " timeout: %s",
            SessionID.ToString().data(), ConsecutiveRdmaFailures, DelayedRdmaHandshakeTimeout.ToString().data());
        TActivationContext::Schedule(DelayedRdmaHandshakeTimeout, new IEventHandle(EvRdmaPendingHandshake, 0, SelfId(),
            {}, nullptr, RdmaRetryWatchdogCookie));
    }

    void TInterconnectProxyTCP::HandleRdmaDelayedHandshake(STATEFN_SIG) {
        if (ev->Cookie != RdmaRetryWatchdogCookie) {
            LOG_DEBUG_IC("ICP41", "ignore stale rdma retry watchdog event for session: %s"
                " event_cookie: %" PRIu64 " current_cookie: %" PRIu64, SessionID.ToString().data(),
                ev->Cookie, RdmaRetryWatchdogCookie);
            return;
        }

        if (!DelayedRdmaHandshakeTimeout) {
            return;
        }

        DelayedRdmaHandshakeTimeout = TDuration::Zero();

        if (Terminated || !Session) {
            SetRdmaRetryWatchdogPending(false);
            return;
        }

        if (CurrentStateFunc() == &TThis::StateWork) {
            SetRdmaRetryWatchdogPending(false);
            // There is a chance that session was promouted to use RDMA without us.
            if (!InvokeSession(&IInterconnectSession::IsRdmaInUse)) {
                InvokeSession(&IInterconnectSession::Terminate, TDisconnectReason::NewSession());
            }
        } else {
            LOG_WARN_IC("ICP39", "restart delayed rdma handshake for session: %s", SessionID.ToString().data());
            DelayedRdmaHandshakeTimeout = RdmaRetryStateCheckDelay;
            SetRdmaRetryWatchdogPending(true);
            TActivationContext::Schedule(DelayedRdmaHandshakeTimeout, new IEventHandle(EvRdmaPendingHandshake, 0, SelfId(),
                {}, nullptr, RdmaRetryWatchdogCookie));
        }
    }

    void TInterconnectProxyTCP::GenerateHttpInfo(NMon::TEvHttpInfo::TPtr& ev) {
        ICPROXY_PROFILED;

        LOG_INFO_IC("ICP31", "proxy http called");

        TStringStream str;

        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Proxy";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {
                                    str << "Sensor";
                                }
                                TABLEH() {
                                    str << "Value";
                                }
                            }
                        }
#define MON_VAR(NAME)     \
    TABLER() {            \
        TABLED() {        \
            str << #NAME; \
        }                 \
        TABLED() {        \
            str << NAME;  \
        }                 \
    }

                        TABLEBODY() {
                            MON_VAR(TActivationContext::Now())
                            MON_VAR(SessionID)
                            MON_VAR(LastSessionDieTime)
                            MON_VAR(IncomingHandshakeActor)
                            MON_VAR(IncomingHandshakeActorFilledIn)
                            MON_VAR(IncomingHandshakeActorReset)
                            MON_VAR(OutgoingHandshakeActor)
                            MON_VAR(OutgoingHandshakeActorCreated)
                            MON_VAR(OutgoingHandshakeActorReset)
                            MON_VAR(State)
                            MON_VAR(StateSwitchTime)
                        }
                    }
                }
            }

            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Error Log";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() {
                                    str << "Timestamp";
                                }
                                TABLEH() {
                                    str << "Elapsed";
                                }
                                TABLEH() {
                                    str << "Kind";
                                }
                                TABLEH() {
                                    str << "Explanation";
                                }
                            }
                        }
                        TABLEBODY() {
                            const TInstant now = TActivationContext::Now();
                            const TInstant barrier = now - TDuration::Minutes(1);
                            for (auto it = ErrorStateLog.rbegin(); it != ErrorStateLog.rend(); ++it) {
                                auto wrapper = [&](const auto& lambda) {
                                    if (std::get<0>(*it) > barrier) {
                                        str << "<strong>";
                                        lambda();
                                        str << "</strong>";
                                    } else {
                                        lambda();
                                    }
                                };
                                TABLER() {
                                    TABLED() {
                                        wrapper([&] {
                                            str << std::get<0>(*it);
                                        });
                                    }
                                    TABLED() {
                                        wrapper([&] {
                                            str << now - std::get<0>(*it);
                                        });
                                    }
                                    TABLED() {
                                        wrapper([&] {
                                            str << std::get<1>(*it);
                                        });
                                    }
                                    TABLED() {
                                        wrapper([&] {
                                            str << std::get<2>(*it);
                                        });

                                        ui32 rep = std::get<3>(*it);
                                        if (rep != 1) {
                                            str << " <strong>x" << rep << "</strong>";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        TAutoPtr<IEventHandle> h(new IEventHandle(ev->Sender, ev->Recipient, new NMon::TEvHttpInfoRes(str.Str())));
        if (Session) {
            switch (auto& ev = h; ev->GetTypeRewrite()) {
                hFunc(NMon::TEvHttpInfoRes, Session->GenerateHttpInfo);
                default:
                    Y_ABORT();
            }
        } else {
            TActivationContext::Send(h.Release());
        }
    }

    TString TInterconnectProxyTCP::FormatRemoteNodeForLog(const TEvHandshakeFail& handshakeFail) const {
        TString peerName;
        if (TechnicalPeerHostName && TechnicalPeerPort) {
            peerName = TStringBuilder() << TechnicalPeerHostName << ':' << TechnicalPeerPort;
        } else {
            peerName = handshakeFail.PeerHostName;
        }

        TStringBuilder remoteNode;
        remoteNode << "remote node " << PeerNodeId;
        if (peerName) {
            remoteNode << " (" << peerName << ")";
        }
        return remoteNode;
    }

    TString TInterconnectProxyTCP::FormatHandshakeFailNotice(TStringBuf explanation, const TEvHandshakeFail& handshakeFail) const {
        const ui32 selfNodeId = SelfId().NodeId();
        const TString remoteNode = FormatRemoteNodeForLog(handshakeFail);

        TStringBuilder notice;
        switch (handshakeFail.Reason) {
            case TEvHandshakeFail::EReason::RemoteNodeDoesNotKnowLocalNode:
                notice << "local node " << selfNodeId << " cannot join cluster: " << remoteNode
                       << " has no node " << selfNodeId << " in its configuration; update config on running cluster nodes";
                break;

            case TEvHandshakeFail::EReason::Unspecified:
            default:
                notice << "local node " << selfNodeId << " cannot connect to " << remoteNode << "; reason: ";
                if (handshakeFail.PeerError) {
                    notice << handshakeFail.PeerError;
                } else {
                    notice << explanation;
                }
                break;
        }
        return notice;
    }

    void TInterconnectProxyTCP::LogHandshakeStatusNotice(TStringBuf marker, EHandshakeStatusDirection direction, const TEvHandshakeFail& handshakeFail) const {
        auto& ctx = TActivationContext::AsActorContext();
        const auto makeNotice = [&] {
            const TStringBuf directionName = direction == EHandshakeStatusDirection::Incoming
                                                        ? TStringBuf("incoming")
                                                        : TStringBuf("outgoing");

            TStringBuilder notice;
            notice << FormatHandshakeFailNotice(handshakeFail.Explanation, handshakeFail);

            TStringBuilder debugInfo;
            debugInfo << directionName << " handshake failed, temporary=" << ui32(handshakeFail.Temporary);
            switch (direction) {
                case EHandshakeStatusDirection::Incoming:
                    debugInfo << " outgoing=" << OutgoingHandshakeActor;
                    break;

                case EHandshakeStatusDirection::Outgoing:
                    debugInfo << " incoming=" << IncomingHandshakeActor
                        << " held=" << (HeldHandshakeReply ? "yes" : "no");
                    break;
            }

            AppendHandshakeFailDebugInfo(notice, marker, handshakeFail.Explanation, debugInfo);
            return notice;
        };

        LOG_NOTICE_SOURCELESS(ctx, NActorsServices::INTERCONNECT, "[YDBE-02001] %s", makeNotice().data());
    }

    void TInterconnectProxyTCP::AppendSuppressedErrorStateLogs(TStringBuilder& stream, ui64 globalSuppressed, ui64 perPeerSuppressed) const {
        if (globalSuppressed || perPeerSuppressed) {
            stream << " (skipped " << globalSuppressed << " repeated messages total, " << perPeerSuppressed << " for remote node " << PeerNodeId << ")";
        }
    }

    void TInterconnectProxyTCP::AppendHandshakeFailDebugInfo(TStringBuilder& stream, TStringBuf marker, TStringBuf rawReason, TStringBuf extraDebugInfo) const {
        stream << "; debug: " << LogPrefix << " " << marker << " raw_reason=" << rawReason;
        if (extraDebugInfo) {
            stream << " " << extraDebugInfo;
        }
    }

    void TInterconnectProxyTCP::TransitToErrorState(TString explanation, bool updateErrorLog, const TEvHandshakeFail* handshakeFail) {
        ICPROXY_PROFILED;

        static constexpr TDuration kErrorStateLogInterval = TDuration::Seconds(30);

        const TInstant now = TActivationContext::Now();
        auto& ctx = TActivationContext::AsActorContext();
        const bool noticeEnabled = IS_CTX_LOG_PRIORITY_ENABLED(ctx, NLog::PRI_NOTICE, NActorsServices::INTERCONNECT, 0ull);
        const bool logNotice = noticeEnabled && [&] {
            if (LastErrorStateLogAt != TInstant::Zero() && now - LastErrorStateLogAt < kErrorStateLogInterval) {
                return false;
            }

            const uint64_t nowMicroSeconds = now.MicroSeconds();
            for (uint64_t lastMicroSeconds = Common->ErrorStateLogLastMicroSeconds.load(std::memory_order_relaxed);;) {
                if (lastMicroSeconds && nowMicroSeconds - lastMicroSeconds < kErrorStateLogInterval.MicroSeconds()) {
                    return false;
                }
                if (Common->ErrorStateLogLastMicroSeconds.compare_exchange_weak(lastMicroSeconds, nowMicroSeconds, std::memory_order_acq_rel)) {
                    return true;
                }
            }
        }();

        if (logNotice) {
            const ui64 perPeerSuppressed = ErrorStateLogSuppressed;
            const ui64 globalSuppressed = Common->ErrorStateLogSuppressed.exchange(0, std::memory_order_acq_rel);

            if (handshakeFail) {
                const auto makeNotice = [&] {
                    TStringBuilder notice;
                    notice << FormatHandshakeFailNotice(explanation, *handshakeFail);
                    AppendSuppressedErrorStateLogs(notice, globalSuppressed, perPeerSuppressed);
                    AppendHandshakeFailDebugInfo(notice, "ICP32", explanation);
                    return notice;
                };
                LOG_NOTICE_SOURCELESS(ctx, NActorsServices::INTERCONNECT, "[YDBE-02001] %s", makeNotice().data());

                LOG_DEBUG_IC("ICP32", "transit to hold-by-error state Explanation# %s", explanation.data());
            } else {
                const auto makeNotice = [&] {
                    TStringBuilder notice;
                    notice << "transit to hold-by-error state Explanation# " << explanation;
                    AppendSuppressedErrorStateLogs(notice, globalSuppressed, perPeerSuppressed);
                    return notice;
                };
                LOG_NOTICE_IC("ICP32", "%s", makeNotice().data());
            }
            LastErrorStateLogAt = now;
            ErrorStateLogSuppressed = 0;
        } else {
            if (noticeEnabled) {
                ++ErrorStateLogSuppressed;
                Common->ErrorStateLogSuppressed.fetch_add(1, std::memory_order_relaxed);
            }
            LOG_DEBUG_IC("ICP32", "transit to hold-by-error state Explanation# %s", explanation.data());
        }
        LOG_INFO(*TlsActivationContext, NActorsServices::INTERCONNECT_STATUS, "[%u] error state: %s", PeerNodeId, explanation.data());

        if (updateErrorLog) {
            UpdateErrorStateLog(TActivationContext::Now(), "permanent conclusive", explanation);
        }

        Y_ABORT_UNLESS(Session == nullptr);
        Y_ABORT_UNLESS(!SessionID);

        // recalculate wakeup timeout -- if this is the first failure, then we sleep for default timeout; otherwise we
        // sleep N times longer than the previous try, but not longer than desired number of seconds
        auto& s = Common->Settings;
        HoldByErrorWakeupDuration = HoldByErrorWakeupDuration != TDuration::Zero()
            ? Min(HoldByErrorWakeupDuration * s.ErrorSleepRetryMultiplier, s.MaxErrorSleep)
            : Common->Settings.FirstErrorSleep;

        // transit to required state and arm wakeup timer
        if (Terminated) {
            // switch to this state permanently
            SwitchToState(__LINE__, "HoldByError", &TThis::HoldByError);
            HoldByErrorWakeupCookie = nullptr;
        } else {
            SwitchToState(__LINE__, "HoldByError", &TThis::HoldByError, HoldByErrorWakeupDuration,
                          HoldByErrorWakeupCookie = new TEvents::TEvWakeup);
        }

        /* Process all pending events. */
        ProcessPendingSessionEvents();

        /* Terminate handshakes */
        DropHandshakes();

        /* Terminate pending incoming handshake requests. */
        for (auto& ev : PendingIncomingHandshakeEvents) {
            Send(ev->Sender, new TEvents::TEvPoisonPill);
            if (ev->GetTypeRewrite() == TEvHandshakeFail::EventType) {
                TEvHandshakeFail::TPtr tmp(static_cast<TEventHandle<TEvHandshakeFail>*>(ev.Release()));
                LogHandshakeFail(tmp, true);
            }
        }
        PendingIncomingHandshakeEvents.clear();
    }

    void TInterconnectProxyTCP::WakeupFromErrorState(TEvents::TEvWakeup::TPtr& ev) {
        ICPROXY_PROFILED;

        LOG_INFO_IC("ICP33", "wake up from error state");

        if (ev->Get() == HoldByErrorWakeupCookie) {
            SwitchToInitialState();
        }
    }

    void TInterconnectProxyTCP::Disconnect() {
        ICPROXY_PROFILED;

        // terminate handshakes (if any)
        DropHandshakes();

        if (Session) {
            InvokeSession(&IInterconnectSession::Terminate, TDisconnectReason::UserRequest());
        } else {
            TransitToErrorState("forced disconnect");
        }
    }

    void TInterconnectProxyTCP::ScheduleCleanupEventQueue() {
        ICPROXY_PROFILED;

        if (!CleanupEventQueueScheduled && PendingSessionEvents) {
            // apply batching at 50 ms granularity
            Schedule(Max(TDuration::MilliSeconds(50), PendingSessionEvents.front().Deadline - TActivationContext::Monotonic()), new TEvCleanupEventQueue);
            CleanupEventQueueScheduled = true;
        }
    }

    void TInterconnectProxyTCP::HandleCleanupEventQueue() {
        ICPROXY_PROFILED;

        Y_ABORT_UNLESS(CleanupEventQueueScheduled);
        CleanupEventQueueScheduled = false;
        CleanupEventQueue();
        ScheduleCleanupEventQueue();
    }

    void TInterconnectProxyTCP::CleanupEventQueue() {
        ICPROXY_PROFILED;

        const TMonotonic now = TActivationContext::Monotonic();
        while (PendingSessionEvents) {
            TPendingSessionEvent& ev = PendingSessionEvents.front();
            if (now >= ev.Deadline || PendingSessionEventsSize > Common->Settings.MessagePendingSize) {
                TAutoPtr<IEventHandle> event(ev.Event.Release());
                PendingSessionEventsSize -= ev.Size;
                DropSessionEvent(event);
                PendingSessionEvents.pop_front();
            } else {
                break;
            }
        }
    }

    void TInterconnectProxyTCP::HandleClosePeerSocket() {
        HandleClosePeerSocket("closed connection by debug command");
    }

    void TInterconnectProxyTCP::HandleClosePeerSocket(std::span<const char> logEntry) {
        ICPROXY_PROFILED;

        if (Session && Session->GetSocket()) {
            LOG_INFO_IC("ICP34", logEntry.data());
            Session->GetSocket()->Shutdown(SHUT_RDWR);
        }
    }

    void TInterconnectProxyTCP::HandleCloseInputSession() {
        ICPROXY_PROFILED;

        if (Session) {
            InvokeSession(&IInterconnectSession::CloseInputSession);
        }
    }

    void TInterconnectProxyTCP::HandlePoisonSession() {
        ICPROXY_PROFILED;

        if (Session) {
            InvokeSession(&IInterconnectSession::Terminate, TDisconnectReason::Debug());
        }
    }

    void TInterconnectProxyTCP::HandleSessionBufferSizeRequest(TEvSessionBufferSizeRequest::TPtr& ev) {
        ICPROXY_PROFILED;

        ui64 bufSize = 0;
        if (Session) {
            bufSize = Session->GetTotalOutputQueueSize();
        }

        Send(ev->Sender, new TEvSessionBufferSizeResponse(SessionID, bufSize));
    }

    void TInterconnectProxyTCP::Handle(TEvQueryStats::TPtr& ev) {
        ICPROXY_PROFILED;

        TProxyStats stats;
        stats.Path = Sprintf("peer%04" PRIu32, PeerNodeId);
        stats.State = State;
        stats.PeerScopeId = Session ? Session->GetParams().PeerScopeId : TScopeId();
        stats.LastSessionDieTime = LastSessionDieTime;
        stats.TotalOutputQueueSize = Session ? Session->GetTotalOutputQueueSize() : 0;
        stats.Connected = Session ? (bool)Session->GetSocket() : false;
        if (Session) {
            if (const auto xdcFlags = Session->GetXDCFlags()) {
                stats.ExternalDataChannel = true;
                stats.XDCFlags = *xdcFlags;
            } else {
                stats.ExternalDataChannel = false;
                stats.XDCFlags = 0;
            }
        }
        stats.Host = TechnicalPeerHostName;
        stats.Port = 0;
        ui32 rep = 0;
        std::tie(stats.LastErrorTimestamp, stats.LastErrorKind, stats.LastErrorExplanation, rep) = ErrorStateLog
            ? ErrorStateLog.back()
            : std::make_tuple(TInstant(), TString(), TString(), 1U);
        if (rep != 1) {
            stats.LastErrorExplanation += Sprintf(" x%" PRIu32, rep);
        }
        stats.Ping = Session ? Session->GetPingRTT() : TDuration::Zero();
        stats.ClockSkew = Session ? Session->GetClockSkew() : 0;
        if (Session) {
            if (auto *x = dynamic_cast<NInterconnect::TSecureSocket*>(Session->GetSocket().Get())) {
                stats.Encryption = Sprintf("%s/%u", x->GetCipherName().data(), x->GetCipherBits());
            } else {
                stats.Encryption = "none";
            }
        }

        auto response = MakeHolder<TEvStats>();
        response->PeerNodeId = PeerNodeId;
        response->ProxyStats = std::move(stats);
        Send(ev->Sender, response.Release());
    }

    void TInterconnectProxyTCP::HandleTerminate() {
        ICPROXY_PROFILED;

        DelayedRdmaHandshakeTimeout = TDuration::Zero();
        SetRdmaRetryWatchdogPending(false);

        if (Session) {
            InvokeSession(&IInterconnectSession::Terminate, TDisconnectReason());
        }
        Terminated = true;
        TransitToErrorState("terminated");
    }

    void TInterconnectProxyTCP::PassAway() {
        DelayedRdmaHandshakeTimeout = TDuration::Zero();
        SetRdmaRetryWatchdogPending(false);

        if (Session) {
            InvokeSession(&IInterconnectSession::Terminate, TDisconnectReason());
        }
        if (DynamicPtr) {
            Y_ABORT_UNLESS(*DynamicPtr == this);
            *DynamicPtr = nullptr;
        }
        // TODO: unregister actor mon page
        TActor::PassAway();
    }

    void TInterconnectProxyTCP::RegisterDisconnect() {
        const TMonotonic now = TActivationContext::Monotonic();
        ShiftDisconnectWindow(now);
        ++NumDisconnectsInLastHour;
        ++NumDisconnects[NumDisconnectsIndex];
    }

    ui32 TInterconnectProxyTCP::GetDisconnectCountInLastHour() {
        ShiftDisconnectWindow(TMonotonic::Now());
        return NumDisconnectsInLastHour;
    }

    void TInterconnectProxyTCP::ShiftDisconnectWindow(TMonotonic now) {
        const ui64 currentMinutes = now.Minutes();
        if (FirstDisconnectWindowMinutes) {
            const ui32 steps = currentMinutes - FirstDisconnectWindowMinutes;
            if (steps < NumDisconnectsSize) { // advance window by "steps" items, clearing them
                for (ui32 i = 0; i < steps; ++i) {
                    NumDisconnectsInLastHour -= std::exchange(NumDisconnects[++NumDisconnectsIndex %= NumDisconnectsSize], 0);
                }
            } else { // window has been fully flushed
                NumDisconnects.fill(0);
                NumDisconnectsInLastHour = 0;
            }
        }
        FirstDisconnectWindowMinutes = currentMinutes;
    }

    TActorId TInterconnectProxyTCP::GenerateSessionVirtualId() {
        ICPROXY_PROFILED;

        const ui64 localId = TActivationContext::ActorSystem()->AllocateIDSpace(1);
        return NActors::TActorId(SelfId().NodeId(), 0, localId, 0);
    }

}
