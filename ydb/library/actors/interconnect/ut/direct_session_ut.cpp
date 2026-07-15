#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/ut/lib/test_events.h>
#include <ydb/library/actors/interconnect/interconnect_direct_session.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/algorithm.h>
#include <util/system/mutex.h>

#include <atomic>

using namespace NActors;

namespace {

// Echo actor living on the peer node: replies to every TEvTest with a TEvTestResponse addressed back
// to the sender, echoing the sequence number.
class TEchoActor: public TActor<TEchoActor> {
public:
    TEchoActor()
        : TActor(&TThis::StateFunc)
    {}

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvTest, Handle)
    )

    void Handle(TEvTest::TPtr& ev) {
        Send(ev->Sender, new TEvTestResponse(ev->Get()->Record.GetSequenceNumber()), 0, ev->Cookie);
    }
};

// Ordinary actor on the local node collecting TEvTestResponse events delivered through the actor
// system, i.e. replies that were NOT intercepted by a direct-session receive callback.
class TResponseCollectorActor: public TActor<TResponseCollectorActor> {
public:
    TResponseCollectorActor()
        : TActor(&TThis::StateFunc)
    {}

    size_t GetCount() const {
        return Count.load(std::memory_order_acquire);
    }

    TVector<ui64> GetReceived() const {
        TGuard<TMutex> guard(Lock);
        return Received;
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvTestResponse, Handle)
    )

    void Handle(TEvTestResponse::TPtr& ev) {
        {
            TGuard<TMutex> guard(Lock);
            Received.push_back(ev->Get()->Record.GetConfirmedSequenceNumber());
        }
        Count.fetch_add(1, std::memory_order_release);
    }

    mutable TMutex Lock;
    TVector<ui64> Received;
    std::atomic<size_t> Count = 0;
};

// Direct-session receive callback recording the sequence numbers of the replies it receives.
class TCollectingReceiveCallback: public IReceiveCallback {
public:
    void Receive(TAutoPtr<IEventHandle> ev) override {
        ui64 seq = Max<ui64>();
        if (ev->GetTypeRewrite() == TEvTestResponse::EventType) {
            seq = ev->Get<TEvTestResponse>()->Record.GetConfirmedSequenceNumber();
        }
        {
            TGuard<TMutex> guard(Lock);
            Received.push_back(seq);
        }
        Count.fetch_add(1, std::memory_order_release);
    }

    size_t GetCount() const {
        return Count.load(std::memory_order_acquire);
    }

    TVector<ui64> GetReceived() const {
        TGuard<TMutex> guard(Lock);
        return Received;
    }

private:
    mutable TMutex Lock;
    TVector<ui64> Received;
    std::atomic<size_t> Count = 0;
};

// Subscribes to the interconnect session with peerNodeId and fulfils the promise with the direct
// session handle carried by TEvNodeConnected.
class TDirectSessionGrabber: public TActorBootstrapped<TDirectSessionGrabber> {
public:
    TDirectSessionGrabber(ui32 peerNodeId, NThreading::TPromise<std::shared_ptr<IDirectSession>> promise)
        : PeerNodeId(peerNodeId)
        , Promise(std::move(promise))
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
        Send(TActivationContext::ActorSystem()->InterconnectProxy(PeerNodeId), new TEvInterconnect::TEvConnectNode);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvInterconnect::TEvNodeConnected, Handle)
        cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, HandleDisconnected)
    )

    void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        if (!Resolved) {
            Resolved = true;
            Promise.SetValue(ev->Get()->DirectSession);
        }
    }

    void HandleDisconnected() {}

    const ui32 PeerNodeId;
    NThreading::TPromise<std::shared_ptr<IDirectSession>> Promise;
    bool Resolved = false;
};

std::shared_ptr<IDirectSession> GrabDirectSession(TTestICCluster& cluster, ui32 fromNode, ui32 peerNode) {
    auto promise = NThreading::NewPromise<std::shared_ptr<IDirectSession>>();
    auto future = promise.GetFuture();
    cluster.RegisterActor(new TDirectSessionGrabber(peerNode, promise), fromNode);
    UNIT_ASSERT_C(future.Wait(TDuration::Seconds(10)), "timed out waiting for TEvNodeConnected");
    auto directSession = future.GetValueSync();
    UNIT_ASSERT_C(directSession, "v1 session did not provide a direct session handle");
    return directSession;
}

template <typename TCallback>
void WaitFor(TDuration timeout, TCallback&& cb, TStringBuf what) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        if (cb()) {
            return;
        }
        Sleep(TDuration::MilliSeconds(10));
    }
    UNIT_FAIL(TStringBuilder() << "condition not met: " << what);
}

TAutoPtr<IEventHandle> MakeRequest(const TActorId& recipient, const TActorId& sender, ui64 seq) {
    return new IEventHandle(recipient, sender, new TEvTest(seq));
}

} // namespace

Y_UNIT_TEST_SUITE(InterconnectDirectSession) {

    // Replies addressed to a registered local id must be delivered to the receive callback.
    Y_UNIT_TEST(ReceivesExpectedReplies) {
        TTestICCluster cluster(2);
        const TActorId echoId = cluster.RegisterActor(new TEchoActor, 2);
        auto directSession = GrabDirectSession(cluster, 1, 2);

        // Fabricated local actor id -- it only needs to be routable to node 1; receive interception
        // does not require it to be a live actor.
        const TActorId localId(1, 0, 0xD1EC7A, 0);

        auto callback = MakeIntrusive<TCollectingReceiveCallback>();
        directSession->RegisterReceiveCallback(localId, callback);

        constexpr ui64 N = 16;
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT(directSession->Send(MakeRequest(echoId, localId, i)));
        }

        WaitFor(TDuration::Seconds(15), [&] { return callback->GetCount() >= N; },
            "direct session received all expected replies");

        auto received = callback->GetReceived();
        Sort(received);
        UNIT_ASSERT_VALUES_EQUAL(received.size(), N);
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(received[i], i);
        }
    }

    // A callback registered for one id must not receive replies addressed to another id; those must
    // flow through the normal actor-system path instead.
    Y_UNIT_TEST(DoesNotReceiveUnexpectedReplies) {
        TTestICCluster cluster(2);
        const TActorId echoId = cluster.RegisterActor(new TEchoActor, 2);
        auto directSession = GrabDirectSession(cluster, 1, 2);

        auto* collector = new TResponseCollectorActor;
        const TActorId collectorId = cluster.RegisterActor(collector, 1);

        const TActorId registeredId(1, 0, 0xC0FFEE, 0);
        auto callback = MakeIntrusive<TCollectingReceiveCallback>();
        directSession->RegisterReceiveCallback(registeredId, callback);

        // Replies addressed to the collector (no callback) must go through the actor system.
        constexpr ui64 M = 8;
        for (ui64 i = 0; i < M; ++i) {
            UNIT_ASSERT(directSession->Send(MakeRequest(echoId, collectorId, 1000 + i)));
        }
        WaitFor(TDuration::Seconds(15), [&] { return collector->GetCount() >= M; },
            "collector received actor-system replies");

        // Replies addressed to the registered id must go to the callback.
        constexpr ui64 N = 8;
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT(directSession->Send(MakeRequest(echoId, registeredId, i)));
        }
        WaitFor(TDuration::Seconds(15), [&] { return callback->GetCount() >= N; },
            "callback received its own replies");

        // Give any (erroneous) cross-delivery a chance to arrive before asserting exact counts.
        Sleep(TDuration::MilliSeconds(500));

        UNIT_ASSERT_VALUES_EQUAL(callback->GetCount(), N);
        UNIT_ASSERT_VALUES_EQUAL(collector->GetCount(), M);
    }

    // A registered callback shadows the actor with the same id; after unregister the actor receives
    // its events again.
    Y_UNIT_TEST(UnregisterRestoresActorSystemDelivery) {
        TTestICCluster cluster(2);
        const TActorId echoId = cluster.RegisterActor(new TEchoActor, 2);
        auto directSession = GrabDirectSession(cluster, 1, 2);

        auto* collector = new TResponseCollectorActor;
        const TActorId collectorId = cluster.RegisterActor(collector, 1);

        auto callback = MakeIntrusive<TCollectingReceiveCallback>();
        directSession->RegisterReceiveCallback(collectorId, callback);

        // While registered, replies to collectorId are intercepted by the callback.
        constexpr ui64 N = 8;
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT(directSession->Send(MakeRequest(echoId, collectorId, i)));
        }
        WaitFor(TDuration::Seconds(15), [&] { return callback->GetCount() >= N; },
            "callback intercepted replies while registered");
        Sleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(collector->GetCount(), 0);

        // After unregister, replies to collectorId flow through the actor system again.
        directSession->UnregisterReceiveCallback(collectorId);

        constexpr ui64 M = 8;
        for (ui64 i = 0; i < M; ++i) {
            UNIT_ASSERT(directSession->Send(MakeRequest(echoId, collectorId, 100 + i)));
        }
        WaitFor(TDuration::Seconds(15), [&] { return collector->GetCount() >= M; },
            "collector received replies after unregister");
        Sleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(callback->GetCount(), N);
    }

    // The reply callback passed to Send() must be registered for ev->Sender before the event goes out.
    Y_UNIT_TEST(SendWithReplyCallbackRegistersReceiver) {
        TTestICCluster cluster(2);
        const TActorId echoId = cluster.RegisterActor(new TEchoActor, 2);
        auto directSession = GrabDirectSession(cluster, 1, 2);

        const TActorId localId(1, 0, 0xBEEF, 0);
        auto callback = MakeIntrusive<TCollectingReceiveCallback>();

        UNIT_ASSERT(directSession->Send(MakeRequest(echoId, localId, 777), callback));

        WaitFor(TDuration::Seconds(15), [&] { return callback->GetCount() >= 1; },
            "reply callback registered via Send received the reply");

        auto received = callback->GetReceived();
        UNIT_ASSERT_VALUES_EQUAL(received.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(received[0], 777u);
    }
}
