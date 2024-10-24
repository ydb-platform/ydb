#include "single_thread_ic_mock.h"
#include "testactorsys.h"
#include "stlog.h"
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/base/domain.h>

using namespace NActors;
using namespace NKikimr;

//#define LOG_MOCK(MSG) do { Cerr << "IC: " << (void*)this << '@' << SelfId() << ' ' << Prefix << MSG << Endl; } while (false)
#define LOG_MOCK(MSG) do {} while (false)

using TMock = TSingleThreadInterconnectMock;

struct TMock::TNode {
    const ui32 NodeId;
    const ui64 BurstCapacityBytes;
    const ui64 BytesPerSecond;
    TInstant NextActionTimestamp;

    TNode(ui32 nodeId, ui64 burstCapacityBytes, ui64 bytesPerSecond)
        : NodeId(nodeId)
        , BurstCapacityBytes(burstCapacityBytes)
        , BytesPerSecond(bytesPerSecond)
    {}

    TInstant Enqueue(ui64 size, TInstant clock) {
        if (BytesPerSecond == Max<ui64>()) {
            return clock;
        }
        NextActionTimestamp = Max(clock - TDuration::MicroSeconds(BurstCapacityBytes * 1'000'000 / BytesPerSecond),
            NextActionTimestamp) + TDuration::MicroSeconds((size * 1'000'000 + BytesPerSecond - 1) / BytesPerSecond);
        return Max(NextActionTimestamp, clock);
    }
};

class TMock::TProxyActor : public TActor<TProxyActor> {
    friend class TSessionActor;

    const TString Prefix;
    TMock* const Mock;
    std::shared_ptr<TNode> Node;
    TProxyActor *Peer;
    const ui32 PeerNodeId;
    TIntrusivePtr<TInterconnectProxyCommon> Common;
    TSessionActor *SessionActor = nullptr;
    bool Working = false;
    std::deque<std::unique_ptr<IEventHandle>> PendingEvents;
    ui64 DropPendingEventsCookie = 0;

    enum {
        EvDropPendingEvents = EventSpaceBegin(TEvents::ES_PRIVATE),
    };

public:
    TProxyActor(TMock *mock, std::shared_ptr<TNode> node, TProxyActor *peer, ui32 peerNodeId,
            TIntrusivePtr<TInterconnectProxyCommon> common)
        : TActor(&TThis::StateFunc)
        , Prefix(TStringBuilder() << "Proxy[" << node->NodeId << ":" << peerNodeId << "] ")
        , Mock(mock)
        , Node(std::move(node))
        , Peer(peer)
        , PeerNodeId(peerNodeId)
        , Common(std::move(common))
    {
        LOG_MOCK("Created Peer# " << (void*)Peer);
        if (Peer) {
            Peer->Peer = this;
        }
    }

    ~TProxyActor();

    void Registered(TActorSystem *as, const TActorId& parent) override {
        TActor::Registered(as, parent);
        LOG_MOCK("Registered");
        Working = true;
        if (Peer && Peer->Working) {
            ProcessPendingEvents();
            Peer->ProcessPendingEvents();
        }
    }

    TActorId CreateSession();
    void ForwardToSession(TAutoPtr<IEventHandle> ev);

    void DropSessionEvent(std::unique_ptr<IEventHandle> ev);
    void HandleDropPendingEvents(TAutoPtr<IEventHandle> ev);
    void ProcessPendingEvents();

    void ShutdownConnection();
    void DetachSessionActor();

    STRICT_STFUNC(StateFunc,
        fFunc(EvDropPendingEvents, HandleDropPendingEvents);
        fFunc(TEvInterconnect::EvForward, ForwardToSession);
        fFunc(TEvInterconnect::EvConnectNode, ForwardToSession);
        fFunc(TEvents::TSystem::Subscribe, ForwardToSession);
        fFunc(TEvents::TSystem::Unsubscribe, ForwardToSession);
        cFunc(TEvInterconnect::EvDisconnect, ShutdownConnection);
    )
};

class TMock::TSessionActor : public TActor<TSessionActor> {
    const TString Prefix;
    TProxyActor *Proxy;
    std::unordered_map<TActorId, ui64, THash<TActorId>> Subscribers;

    std::unordered_map<ui16, std::deque<std::unique_ptr<IEventHandle>>> Outbox;
    bool SendPending = false;
    TInstant NextSendTimestamp;

    std::deque<std::unique_ptr<IEventHandle>> Inbox;
    bool ReceivePending = false;
    TInstant NextReceiveTimestamp;

    enum {
        EvSend = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvReceive,
    };

    friend class TProxyActor;

public:
    // The SessionActor exists when and only when there are two working peers connected together and both of them have
    // their respective sessions. That is, when one of these conditions break, we have to terminate both sessions of a
    // connection. There are also some other things to consider: session can be destroyed via dtor after proxy destruction,
    // so it can't access proxy's fields from its destruction path. It is peer proxy's responsibility to destroy both
    // sessions upon destruction.

    TSessionActor(TProxyActor *proxy)
        : TActor(&TThis::StateFunc)
        , Prefix(TStringBuilder() << "Session[" << proxy->Node->NodeId << ":" << proxy->PeerNodeId << "] ")
        , Proxy(proxy)
    {
        LOG_MOCK("Created Proxy# " << (void*)Proxy);
    }

    ~TSessionActor() {
        LOG_MOCK("Destroyed Proxy# " << (void*)Proxy);
        if (Proxy) {
            Proxy->SessionActor = nullptr;
        }
    }

    // Shut down local part of this session.
    void ShutdownSession() {
        LOG_MOCK("ShutdownSession Proxy# " << (void*)Proxy);

        if (!Proxy) {
            return;
        }

        STLOG(PRI_DEBUG, INTERCONNECT_SESSION, STIM01, Prefix << "ShutdownSession", (SelfId, SelfId()));

        // notify all subscribers
        for (const auto& [actorId, cookie] : Subscribers) {
            LOG_MOCK("Sending unsubscribe actorId# " << actorId << " cookie# " << cookie);
            auto ev = std::make_unique<TEvInterconnect::TEvNodeDisconnected>(Proxy->PeerNodeId);
            Proxy->Mock->TestActorSystem->Send(new IEventHandle(actorId, SelfId(), ev.release(), 0, cookie),
                Proxy->Node->NodeId);
        }

        // drop unsent messages
        for (auto& [_, queue] : Outbox) {
            for (auto& ev : queue) {
                Proxy->Mock->TestActorSystem->Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::Disconnected).release(),
                    Proxy->Node->NodeId);
            }
        }

        // transit to self-destruction state
        Proxy->Mock->TestActorSystem->Send(new IEventHandle(TEvents::TSystem::Poison, 0, SelfId(), {}, nullptr, 0),
            Proxy->Node->NodeId);
        Become(&TThis::StateUndelivered); // do not handle further events

        // no more proxy connected to this actor
        Proxy = nullptr;
    }

    void StateUndelivered(STFUNC_SIG) {
        if (ev->GetTypeRewrite() == TEvents::TSystem::Poison) {
            TActor::PassAway();
        } else {
            LOG_MOCK("Undelivered event Sender# " << ev->Sender << " Type# " << ev->GetTypeRewrite());
            TActivationContext::Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown));
        }
    }

    void PassAway() override {
        Proxy->ShutdownConnection();
        TActor::PassAway();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandleForward(TAutoPtr<IEventHandle> ev) {
        STLOG(PRI_DEBUG, INTERCONNECT_SESSION, STIM02, Prefix << "HandleForward", (SelfId, SelfId()),
            (Type, ev->Type), (TypeName, Proxy->Mock->TestActorSystem->GetEventName(ev->Type)),
            (Sender, ev->Sender), (Recipient, ev->Recipient), (Flags, ev->Flags), (Cookie, ev->Cookie));

        if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
            Subscribe(ev->Sender, ev->Cookie);
        }
        if (SendPending) {
            const ui16 ch = ev->GetChannel();
            Outbox[ch].emplace_back(ev.Release());
        } else {
            ScheduleSendEvent(ev);
            HandleSend(ev);
        }
    }

    void HandleSend(TAutoPtr<IEventHandle> ev) {
        while (ev) {
            STLOG(PRI_TRACE, INTERCONNECT_SESSION, STIM03, Prefix << "HandleSend", (SelfId, SelfId()),
                (Type, ev->Type), (Sender, ev->Sender), (Recipient, ev->Recipient), (Flags, ev->Flags),
                (Cookie, ev->Cookie));

            const TInstant now = TActivationContext::Now();
            Y_ABORT_UNLESS(now == NextSendTimestamp);
            Y_ABORT_UNLESS(Proxy->Peer && Proxy->Peer->SessionActor);
            const_cast<TScopeId&>(ev->OriginScopeId) = Proxy->Common->LocalScopeId;
            Proxy->Peer->SessionActor->PutToInbox(ev);

            if (Outbox.empty()) {
                SendPending = false;
                break;
            } else {
                auto it = Outbox.begin();
                std::advance(it, RandomNumber(Outbox.size()));
                auto& q = it->second;
                ev = q.front().release();
                q.pop_front();
                if (q.empty()) {
                    Outbox.erase(it);
                }

                ScheduleSendEvent(ev);
            }
        }
    }

    void ScheduleSendEvent(TAutoPtr<IEventHandle>& ev) {
        const TInstant now = TActivationContext::Now();
        NextSendTimestamp = Proxy->Node->Enqueue(ev->GetSize(), now);
        if (now < NextSendTimestamp) {
            ev->Rewrite(EvSend, SelfId());
            TActivationContext::Schedule(NextSendTimestamp, ev.Release());
            SendPending = true;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void PutToInbox(TAutoPtr<IEventHandle> ev) {
        if (ReceivePending) {
            Inbox.emplace_back(ev.Release());
        } else {
            ScheduleReceiveEvent(ev);
            HandleReceive(ev);
        }
    }

    void HandleReceive(TAutoPtr<IEventHandle> ev) {
        while (ev) {
            const TInstant now = TActivationContext::Now();
            Y_ABORT_UNLESS(now == NextReceiveTimestamp);

            auto fw = std::make_unique<IEventHandle>(
                SelfId(),
                ev->Type,
                ev->Flags & ~IEventHandle::FlagForwardOnNondelivery,
                ev->Recipient,
                ev->Sender,
                ev->ReleaseChainBuffer(),
                ev->Cookie,
                ev->OriginScopeId,
                std::move(ev->TraceId)
            );

            STLOG(PRI_TRACE, INTERCONNECT_SESSION, STIM04, Prefix << "HandleReceive", (SelfId, SelfId()),
                (Type, fw->Type), (Sender, fw->Sender), (Recipient, fw->Recipient), (Flags, fw->Flags),
                (Cookie, ev->Cookie));

            auto& common = Proxy->Common;
            if (!common->EventFilter || common->EventFilter->CheckIncomingEvent(*fw, common->LocalScopeId)) {
                Proxy->Mock->TestActorSystem->Send(fw.release(), Proxy->Node->NodeId);
            }

            if (Inbox.empty()) {
                ReceivePending = false;
                break;
            } else {
                ev = Inbox.front().release();
                Inbox.pop_front();
                ScheduleReceiveEvent(ev);
            }
        }
    }

    void ScheduleReceiveEvent(TAutoPtr<IEventHandle>& ev) {
        const TInstant now = TActivationContext::Now();
        NextReceiveTimestamp = Proxy->Node->Enqueue(ev->GetSize(), now);
        if (now < NextReceiveTimestamp) {
            ev->Rewrite(EvReceive, SelfId());
            Proxy->Mock->TestActorSystem->Schedule(NextReceiveTimestamp, ev.Release(), nullptr, Proxy->Node->NodeId);
            ReceivePending = true;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void Handle(TEvInterconnect::TEvConnectNode::TPtr ev) {
        Subscribe(ev->Sender, ev->Cookie);
    }

    void Handle(TEvents::TEvSubscribe::TPtr ev) {
        Subscribe(ev->Sender, ev->Cookie);
    }

    void Subscribe(const TActorId& actorId, ui64 cookie) {
        LOG_MOCK("Subscribe actorId# " << actorId << " cookie# " << cookie);
        Subscribers[actorId] = cookie;
        Send(actorId, new TEvInterconnect::TEvNodeConnected(Proxy->PeerNodeId), 0, cookie);
    }

    void Handle(TEvents::TEvUnsubscribe::TPtr ev) {
        LOG_MOCK("Unsubscribe actorId# " << ev->Sender);
        Subscribers.erase(ev->Sender);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    STRICT_STFUNC(StateFunc,
        fFunc(TEvInterconnect::EvForward, HandleForward);
        fFunc(EvSend, HandleSend);
        fFunc(EvReceive, HandleReceive);
        hFunc(TEvInterconnect::TEvConnectNode, Handle);
        hFunc(TEvents::TEvSubscribe, Handle);
        hFunc(TEvents::TEvUnsubscribe, Handle);
        cFunc(TEvents::TSystem::Poison, PassAway);
    )
};

TMock::TProxyActor::~TProxyActor() {
    LOG_MOCK("Destroyed Peer# " << (void*)Peer << " Session# " << (void*)(Peer ? Peer->SessionActor : nullptr));

    const auto it = Mock->Proxies.find({Node->NodeId, PeerNodeId});
    Y_ABORT_UNLESS(it != Mock->Proxies.end());
    Y_ABORT_UNLESS(it->second == this);
    Mock->Proxies.erase(it);

    if (Peer) {
        Peer->DetachSessionActor();
        Peer->Peer = nullptr;
    }
    DetachSessionActor();
}

TActorId TMock::TProxyActor::CreateSession() {
    Y_ABORT_UNLESS(SelfId());
    Y_ABORT_UNLESS(Working);
    Y_ABORT_UNLESS(!SessionActor);
    LOG_MOCK("CreateSession");
    SessionActor = new TSessionActor(this);
    const TActorId self = SelfId();
    return Mock->TestActorSystem->Register(SessionActor, self, self.PoolID(), self.Hint(), Node->NodeId);
}

void TMock::TProxyActor::ForwardToSession(TAutoPtr<IEventHandle> ev) {
    if (SessionActor) {
        InvokeOtherActor(*SessionActor, &TSessionActor::Receive, ev);
    } else {
        const bool first = PendingEvents.empty();
        PendingEvents.emplace_back(ev.Release());
        if (first) {
            TActivationContext::Schedule(TDuration::Seconds(5), new IEventHandle(EvDropPendingEvents, 0, SelfId(), {},
                nullptr, ++DropPendingEventsCookie));
        }
    }
}

void TMock::TProxyActor::DropSessionEvent(std::unique_ptr<IEventHandle> ev) {
    switch (ev->GetTypeRewrite()) {
        case TEvInterconnect::EvForward:
            if (ev->Flags & IEventHandle::FlagSubscribeOnSession) {
                Send(ev->Sender, new TEvInterconnect::TEvNodeDisconnected(PeerNodeId), 0, ev->Cookie);
            }
            TActivationContext::Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::Disconnected));
            break;

        case TEvInterconnect::EvConnectNode:
        case TEvents::TSystem::Subscribe:
            Send(ev->Sender, new TEvInterconnect::TEvNodeDisconnected(PeerNodeId), 0, ev->Cookie);
            break;

        case TEvents::TSystem::Unsubscribe:
            break;

        default:
            Y_ABORT();
    }
}

void TMock::TProxyActor::HandleDropPendingEvents(TAutoPtr<IEventHandle> ev) {
    if (ev->Cookie == DropPendingEventsCookie) {
        for (auto& ev : std::exchange(PendingEvents, {})) {
            DropSessionEvent(std::move(ev));
        }
    }
}

void TMock::TProxyActor::ProcessPendingEvents() {
    Y_ABORT_UNLESS(!SessionActor);
    const TActorId sessionActorId = CreateSession();
    for (auto& ev : std::exchange(PendingEvents, {})) {
        ev->Rewrite(ev->GetTypeRewrite(), sessionActorId);
        Mock->TestActorSystem->Send(ev.release(), Node->NodeId);
    }
}

void TMock::TProxyActor::ShutdownConnection() {
    Y_ABORT_UNLESS(Peer && ((SessionActor && Peer->SessionActor) || (!SessionActor && !Peer->SessionActor)));
    DetachSessionActor();
    Peer->DetachSessionActor();
}

void TMock::TProxyActor::DetachSessionActor() {
    if (auto *p = std::exchange(SessionActor, nullptr)) {
        p->ShutdownSession();
    }
}

TMock::TSingleThreadInterconnectMock(ui64 burstCapacityBytes, ui64 bytesPerSecond,
        TTestActorSystem *tas)
    : BurstCapacityBytes(burstCapacityBytes)
    , BytesPerSecond(bytesPerSecond)
    , TestActorSystem(tas)
{}

TMock::~TSingleThreadInterconnectMock()
{}

std::unique_ptr<IActor> TMock::CreateProxyActor(ui32 nodeId, ui32 peerNodeId,
        TIntrusivePtr<TInterconnectProxyCommon> common) {
    Y_ABORT_UNLESS(nodeId != peerNodeId);

    auto& ptr = Proxies[{nodeId, peerNodeId}];
    Y_ABORT_UNLESS(!ptr); // no multiple proxies for the same direction are allowed

    auto& node = Nodes[nodeId];
    if (!node) {
        node = std::make_shared<TNode>(nodeId, BurstCapacityBytes, BytesPerSecond);
    }

    const auto it = Proxies.find({peerNodeId, nodeId});
    TProxyActor *peer = it != Proxies.end() ? it->second : nullptr;

    auto proxy = std::make_unique<TProxyActor>(this, node, peer, peerNodeId, std::move(common));
    ptr = proxy.get();
    return proxy;
}
