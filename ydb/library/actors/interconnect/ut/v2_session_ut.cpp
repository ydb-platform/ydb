#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/ut/lib/test_events.h>
#include <ydb/library/actors/interconnect/interconnect_direct_session.h>
#include <ydb/library/actors/interconnect/uring_context.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/algorithm.h>
#include <util/generic/scope.h>
#include <util/system/env.h>
#include <util/system/mutex.h>

#include <atomic>

using namespace NActors;

namespace {

    // Echo actor on the peer node: replies to every TEvTest with a TEvTestResponse addressed back to
    // the sender, echoing the sequence number.
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

    // Collects TEvTestResponse delivered through the normal actor system.
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

    // Collects incoming TEvTest payloads (used to verify large/multi-chunk transfers end to end).
    class TPayloadCollectorActor: public TActor<TPayloadCollectorActor> {
    public:
        TPayloadCollectorActor()
            : TActor(&TThis::StateFunc)
        {}

        size_t GetCount() const {
            return Count.load(std::memory_order_acquire);
        }

        TString GetLastPayload() const {
            TGuard<TMutex> guard(Lock);
            return LastPayload;
        }

    private:
        STRICT_STFUNC(StateFunc,
            hFunc(TEvTest, Handle)
        )

        void Handle(TEvTest::TPtr& ev) {
            {
                TGuard<TMutex> guard(Lock);
                LastPayload = ev->Get()->Record.GetPayload();
            }
            Count.fetch_add(1, std::memory_order_release);
        }

        mutable TMutex Lock;
        TString LastPayload;
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

    // Echoes each TEvTest back to its sender as a TEvTestResponse, preserving channel and cookie.
    class TLoadEchoActor: public TActor<TLoadEchoActor> {
    public:
        TLoadEchoActor()
            : TActor(&TThis::StateFunc)
        {}

    private:
        STRICT_STFUNC(StateFunc,
            hFunc(TEvTest, Handle)
        )

        void Handle(TEvTest::TPtr& ev) {
            Send(ev->Sender, new TEvTestResponse(ev->Get()->Record.GetSequenceNumber()),
                IEventHandle::MakeFlags(ev->GetChannel(), 0), ev->Cookie);
        }
    };

    // Mimics the interconnect load test: keeps up to InFlyMax messages (each carrying a rope payload on
    // a given channel) in flight and only generates more as responses return; fulfils the promise once
    // Total responses have been received.
    class TLoadDriverActor: public TActorBootstrapped<TLoadDriverActor> {
    public:
        TLoadDriverActor(const TActorId& responder, ui32 total, ui32 inFlyMax, ui32 payloadSize, ui16 channel,
                NThreading::TPromise<ui32> done)
            : Responder(responder)
            , Total(total)
            , InFlyMax(inFlyMax)
            , PayloadSize(payloadSize)
            , Channel(channel)
            , Done(std::move(done))
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            Generate();
        }

    private:
        STRICT_STFUNC(StateFunc,
            hFunc(TEvTestResponse, Handle)
            hFunc(TEvents::TEvUndelivered, HandleUndelivered)
        )

        void SendOne(ui64 seq) {
            auto ev = MakeHolder<TEvTest>(seq);
            if (PayloadSize) {
                TString payload = TString::Uninitialized(PayloadSize);
                memset(payload.Detach(), '*', payload.size());
                ev->AddPayload(TRope(std::move(payload)));
            }
            Send(Responder, ev.Release(), IEventHandle::MakeFlags(Channel, 0) | IEventHandle::FlagTrackDelivery, seq);
            ++InFly;
        }

        // A tracked event may bounce back as undelivered while the interconnect connection is still being
        // (re)established (e.g. handshake races at startup); just retry it -- this is how a real client uses
        // FlagTrackDelivery.
        void HandleUndelivered(TEvents::TEvUndelivered::TPtr& ev) {
            --InFly;
            SendOne(ev->Cookie);
        }

        void Generate() {
            while (InFly < InFlyMax && Sent < Total) {
                SendOne(Sent);
                ++Sent;
            }
        }

        void Handle(TEvTestResponse::TPtr& ev) {
            // Get() parses the response -> detects receive-side corruption on the driver's connection.
            const ui64 seq = ev->Get()->Record.GetConfirmedSequenceNumber();
            Y_ABORT_UNLESS(seq < Total, "corrupted response: seq# %" PRIu64 " total# %" PRIu32, seq, Total);
            --InFly;
            ++Received;
            if (Received >= Total) {
                if (!Finished) {
                    Finished = true;
                    Done.SetValue(Received);
                }
            } else {
                Generate();
            }
        }

        const TActorId Responder;
        const ui32 Total;
        const ui32 InFlyMax;
        const ui32 PayloadSize;
        const ui16 Channel;
        NThreading::TPromise<ui32> Done;
        ui32 Sent = 0;
        ui32 Received = 0;
        ui32 InFly = 0;
        bool Finished = false;
    };

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

    // Subscribes to a peer's connection state and counts connect/disconnect notifications, re-subscribing
    // after every disconnect so a whole connect/disconnect/reconnect churn is observable.
    class TConnectionMonitorActor: public TActorBootstrapped<TConnectionMonitorActor> {
    public:
        explicit TConnectionMonitorActor(ui32 peerNodeId)
            : PeerNodeId(peerNodeId)
        {}

        void Bootstrap() {
            Become(&TThis::StateFunc);
            Subscribe();
        }

        size_t Connects() const { return ConnectCount.load(std::memory_order_acquire); }
        size_t Disconnects() const { return DisconnectCount.load(std::memory_order_acquire); }

    private:
        STRICT_STFUNC(StateFunc,
            hFunc(TEvInterconnect::TEvNodeConnected, Handle)
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, HandleDisconnected)
        )

        void Subscribe() {
            Send(TActivationContext::ActorSystem()->InterconnectProxy(PeerNodeId), new TEvInterconnect::TEvConnectNode);
        }

        void Handle(TEvInterconnect::TEvNodeConnected::TPtr&) {
            ConnectCount.fetch_add(1, std::memory_order_release);
        }

        void HandleDisconnected() {
            DisconnectCount.fetch_add(1, std::memory_order_release);
            Subscribe(); // re-arm so the next connection is observed too
        }

        const ui32 PeerNodeId;
        std::atomic<size_t> ConnectCount{0};
        std::atomic<size_t> DisconnectCount{0};
    };

    std::unique_ptr<TTestICCluster> MakeV2Cluster(ui32 tcpSocketBufferSize = 0) {
        auto customizer = [tcpSocketBufferSize](ui32, TInterconnectSettings& settings) {
            settings.V2.Enable = true;
            settings.V2.ChecksumEvents = true;
            if (tcpSocketBufferSize) {
                settings.TCPSocketBufferSize = tcpSocketBufferSize;
            }
        };
        return std::make_unique<TTestICCluster>(
            /*numNodes=*/2, TChannelsConfig(), /*tiSettings=*/nullptr, /*loggerSettings=*/nullptr,
            TTestICCluster::EMPTY, /*checkerFactory=*/TTestICCluster::TCheckerFactory{},
            /*deadPeerTimeout=*/TDuration::Seconds(2), /*inflight=*/TNode::DefaultInflight(), customizer);
    }

    std::shared_ptr<IDirectSession> GrabDirectSession(TTestICCluster& cluster, ui32 fromNode, ui32 peerNode) {
        auto promise = NThreading::NewPromise<std::shared_ptr<IDirectSession>>();
        auto future = promise.GetFuture();
        cluster.RegisterActor(new TDirectSessionGrabber(peerNode, promise), fromNode);
        UNIT_ASSERT_C(future.Wait(TDuration::Seconds(10)), "timed out waiting for TEvNodeConnected");
        return future.GetValueSync();
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

    void AssertV2InUse(TTestICCluster& cluster, ui32 me, ui32 peer) {
        WaitFor(TDuration::Seconds(10), [&] {
            auto httpResp = cluster.GetSessionDbg(me, peer);
            if (!httpResp.Wait(TDuration::Seconds(2))) {
                return false;
            }
            return httpResp.GetValueSync().Contains("Session (v2)");
        }, "v2 session is in use");
    }

} // namespace

Y_UNIT_TEST_SUITE(InterconnectSessionV2) {

    // Normal actor-system traffic must round-trip over a v2 session.
    Y_UNIT_TEST(ActorSystemRoundTrip) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId echoId = cluster->RegisterActor(new TEchoActor, 2);

        auto* collector = new TResponseCollectorActor;
        const TActorId collectorId = cluster->RegisterActor(collector, 1);

        constexpr ui64 N = 32;
        for (ui64 i = 0; i < N; ++i) {
            cluster->GetNode(1)->GetActorSystem()->Send(new IEventHandle(echoId, collectorId, new TEvTest(i)));
        }

        WaitFor(TDuration::Seconds(20), [&] { return collector->GetCount() >= N; },
            "all actor-system replies received over v2");

        auto received = collector->GetReceived();
        Sort(received);
        UNIT_ASSERT_VALUES_EQUAL(received.size(), N);
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(received[i], i);
        }

        AssertV2InUse(*cluster, 1, 2);
        AssertV2InUse(*cluster, 2, 1);
    }

    // IDirectSession Send + registered receive callback must round-trip over a v2 session.
    Y_UNIT_TEST(DirectSessionRoundTrip) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId echoId = cluster->RegisterActor(new TEchoActor, 2);
        auto directSession = GrabDirectSession(*cluster, 1, 2);
        UNIT_ASSERT_C(directSession, "v2 session did not provide a direct session handle");

        const TActorId localId(1, 0, 0xD1EC7A, 0);
        auto callback = MakeIntrusive<TCollectingReceiveCallback>();
        directSession->RegisterReceiveCallback(localId, callback);

        constexpr ui64 N = 32;
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT(directSession->Send(new IEventHandle(echoId, localId, new TEvTest(i))));
        }

        WaitFor(TDuration::Seconds(20), [&] { return callback->GetCount() >= N; },
            "all direct-session replies received over v2");

        auto received = callback->GetReceived();
        Sort(received);
        UNIT_ASSERT_VALUES_EQUAL(received.size(), N);
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(received[i], i);
        }
    }

    // A large payload must survive chunk splitting/reassembly across the v2 wire format.
    Y_UNIT_TEST(LargePayloadRoundTrip) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        auto* collector = new TPayloadCollectorActor;
        const TActorId collectorId = cluster->RegisterActor(collector, 2);

        // well over the 16383-byte per-chunk limit to force multi-chunk splitting
        TString payload = TString::Uninitialized(200000);
        for (size_t i = 0; i < payload.size(); ++i) {
            payload[i] = static_cast<char>('a' + (i % 26));
        }

        const TActorId sender(1, 0, 0xBEEF, 0);
        cluster->GetNode(1)->GetActorSystem()->Send(new IEventHandle(collectorId, sender, new TEvTest(1, payload)));

        WaitFor(TDuration::Seconds(20), [&] { return collector->GetCount() >= 1; },
            "large payload event received over v2");

        UNIT_ASSERT_VALUES_EQUAL(collector->GetLastPayload().size(), payload.size());
        UNIT_ASSERT_EQUAL(collector->GetLastPayload(), payload);
    }

    // Reproduces the interconnect load test pattern: bounded in-flight window, rope payloads on a
    // non-default channel, sustained bidirectional traffic. A stall here manifests as a timeout.
    Y_UNIT_TEST(LoadLikeRoundTripWithPayload) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);

        auto promise = NThreading::NewPromise<ui32>();
        auto future = promise.GetFuture();
        constexpr ui32 kTotal = 20000;
        cluster->RegisterActor(new TLoadDriverActor(responder, kTotal, /*inFlyMax=*/16, /*payloadSize=*/4096,
            /*channel=*/1, promise), 1);

        UNIT_ASSERT_C(future.Wait(TDuration::Seconds(60)), "load-like round trip stalled");
        UNIT_ASSERT_VALUES_EQUAL(future.GetValueSync(), kTotal);
        AssertV2InUse(*cluster, 1, 2);
    }

    // Several channels active at once (a small-inline "system" channel alongside large rope-payload
    // channels), mimicking real traffic where a distconf event shares the v2 session with the load. The
    // echo actor parses every event via Get(), so any framing/interleaving corruption throws.
    Y_UNIT_TEST(LoadLikeRoundTripMultiChannel) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);

        constexpr ui32 kChannels = 6;
        constexpr ui32 kPerChannel = 5000;
        std::vector<NThreading::TFuture<ui32>> futures;
        for (ui16 ch = 0; ch < kChannels; ++ch) {
            auto promise = NThreading::NewPromise<ui32>();
            futures.push_back(promise.GetFuture());
            const ui32 payload = (ch % 3 == 0) ? 40 : (ch % 3 == 1 ? 4096 : 20000);
            cluster->RegisterActor(new TLoadDriverActor(responder, kPerChannel, /*inFlyMax=*/8, payload,
                /*channel=*/ch, promise), 1);
        }
        for (ui16 ch = 0; ch < kChannels; ++ch) {
            UNIT_ASSERT_C(futures[ch].Wait(TDuration::Seconds(90)), "multi-channel load stalled");
            UNIT_ASSERT_VALUES_EQUAL(futures[ch].GetValueSync(), kPerChannel);
        }
    }

    // Many connections forced into a single engine shard (one shared buffer ring / reaper), which is how
    // a real multi-node cluster looks but a 2-node test never does. Each node drives payload load to every
    // other node; corruption on any connection surfaces as a parse failure in Get().
    Y_UNIT_TEST(LoadLikeRoundTripSharedShardManyPeers) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        SetEnv("YDB_IC_V2_SHARDS", "1"); // force all connections onto one shard/buffer-ring/reaper
        Y_DEFER {
            UnsetEnv("YDB_IC_V2_SHARDS");
        };

        constexpr ui32 kNodes = 5;
        auto customizer = [](ui32, TInterconnectSettings& settings) {
            settings.V2.Enable = true;
        };
        auto cluster = std::make_unique<TTestICCluster>(
            kNodes, TChannelsConfig(), nullptr, nullptr, TTestICCluster::EMPTY,
            TTestICCluster::TCheckerFactory{}, TDuration::Seconds(2), TNode::DefaultInflight(), customizer);

        // an echo responder on every node
        std::vector<TActorId> responders(kNodes + 1);
        for (ui32 n = 1; n <= kNodes; ++n) {
            responders[n] = cluster->RegisterActor(new TLoadEchoActor, n);
        }

        // every node drives payload load to every other node concurrently
        constexpr ui32 kPerPair = 500;
        std::vector<NThreading::TFuture<ui32>> futures;
        for (ui32 from = 1; from <= kNodes; ++from) {
            for (ui32 to = 1; to <= kNodes; ++to) {
                if (from == to) {
                    continue;
                }
                auto promise = NThreading::NewPromise<ui32>();
                futures.push_back(promise.GetFuture());
                const ui32 payload = ((from + to) % 2) ? 8192 : 100;
                cluster->RegisterActor(new TLoadDriverActor(responders[to], kPerPair, /*inFlyMax=*/8, payload,
                    /*channel=*/static_cast<ui16>(from % 4), promise), from);
            }
        }
        for (auto& f : futures) {
            UNIT_ASSERT_C(f.Wait(TDuration::Seconds(30)), "shared-shard multi-peer load stalled");
            UNIT_ASSERT_VALUES_EQUAL(f.GetValueSync(), kPerPair);
        }
    }

    // Same as above but over the ordinary-buffer fallback recv path (no buffer ring) -- exactly what a
    // kernel-5.15 dev cluster uses. Reproduces multi-connection + fallback + heavy load together.
    Y_UNIT_TEST(LoadLikeRoundTripSharedShardFallbackManyPeers) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        SetEnv("YDB_IC_V2_SHARDS", "1");
        SetEnv("YDB_IC_V2_DISABLE_BUF_RING", "1");
        Y_DEFER {
            UnsetEnv("YDB_IC_V2_SHARDS");
            UnsetEnv("YDB_IC_V2_DISABLE_BUF_RING");
        };

        constexpr ui32 kNodes = 5;
        auto customizer = [](ui32, TInterconnectSettings& settings) {
            settings.V2.Enable = true;
        };
        auto cluster = std::make_unique<TTestICCluster>(
            kNodes, TChannelsConfig(), nullptr, nullptr, TTestICCluster::EMPTY,
            TTestICCluster::TCheckerFactory{}, TDuration::Seconds(2), TNode::DefaultInflight(), customizer);

        std::vector<TActorId> responders(kNodes + 1);
        for (ui32 n = 1; n <= kNodes; ++n) {
            responders[n] = cluster->RegisterActor(new TLoadEchoActor, n);
        }

        constexpr ui32 kPerPair = 500;
        std::vector<NThreading::TFuture<ui32>> futures;
        for (ui32 from = 1; from <= kNodes; ++from) {
            for (ui32 to = 1; to <= kNodes; ++to) {
                if (from == to) {
                    continue;
                }
                auto promise = NThreading::NewPromise<ui32>();
                futures.push_back(promise.GetFuture());
                const ui32 payload = ((from + to) % 2) ? 8192 : 100;
                cluster->RegisterActor(new TLoadDriverActor(responders[to], kPerPair, /*inFlyMax=*/8, payload,
                    /*channel=*/static_cast<ui16>(from % 4), promise), from);
            }
        }
        for (auto& f : futures) {
            UNIT_ASSERT_C(f.Wait(TDuration::Seconds(60)), "shared-shard fallback multi-peer load stalled");
            UNIT_ASSERT_VALUES_EQUAL(f.GetValueSync(), kPerPair);
        }
    }

    // Load-like pattern over the ordinary-buffer fallback path (kernels without buffer rings, e.g. 5.15),
    // which is what a real dev cluster is likely using.
    Y_UNIT_TEST(LoadLikeRoundTripFallback) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        SetEnv("YDB_IC_V2_DISABLE_BUF_RING", "1");
        Y_DEFER {
            UnsetEnv("YDB_IC_V2_DISABLE_BUF_RING");
        };
        auto cluster = MakeV2Cluster();
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);

        auto promise = NThreading::NewPromise<ui32>();
        auto future = promise.GetFuture();
        constexpr ui32 kTotal = 20000;
        cluster->RegisterActor(new TLoadDriverActor(responder, kTotal, /*inFlyMax=*/16, /*payloadSize=*/4096,
            /*channel=*/1, promise), 1);

        UNIT_ASSERT_C(future.Wait(TDuration::Seconds(60)), "load-like round trip stalled on fallback path");
        UNIT_ASSERT_VALUES_EQUAL(future.GetValueSync(), kTotal);
    }

    // Same load-like pattern but with a tiny TCP socket buffer to force backpressure -- frequent partial
    // writes and short reads, which loopback with default buffers never triggers.
    Y_UNIT_TEST(LoadLikeRoundTripWithBackpressure) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster(/*tcpSocketBufferSize=*/8192);
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);

        auto promise = NThreading::NewPromise<ui32>();
        auto future = promise.GetFuture();
        constexpr ui32 kTotal = 20000;
        cluster->RegisterActor(new TLoadDriverActor(responder, kTotal, /*inFlyMax=*/16, /*payloadSize=*/16384,
            /*channel=*/1, promise), 1);

        UNIT_ASSERT_C(future.Wait(TDuration::Seconds(60)), "load-like round trip stalled under backpressure");
        UNIT_ASSERT_VALUES_EQUAL(future.GetValueSync(), kTotal);
    }

    // Force the ordinary-buffer fallback (kernels without the provided-buffer-ring API) and make sure
    // both the actor-system path and a large multi-recv payload still round-trip over v2.
    Y_UNIT_TEST(FallbackWithoutBufRing) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        SetEnv("YDB_IC_V2_DISABLE_BUF_RING", "1");
        Y_DEFER {
            UnsetEnv("YDB_IC_V2_DISABLE_BUF_RING");
        };

        auto cluster = MakeV2Cluster();
        const TActorId echoId = cluster->RegisterActor(new TEchoActor, 2);

        auto* collector = new TResponseCollectorActor;
        const TActorId collectorId = cluster->RegisterActor(collector, 1);

        constexpr ui64 N = 16;
        for (ui64 i = 0; i < N; ++i) {
            cluster->GetNode(1)->GetActorSystem()->Send(new IEventHandle(echoId, collectorId, new TEvTest(i)));
        }
        WaitFor(TDuration::Seconds(20), [&] { return collector->GetCount() >= N; },
            "actor-system replies received over v2 fallback path");

        auto received = collector->GetReceived();
        Sort(received);
        UNIT_ASSERT_VALUES_EQUAL(received.size(), N);
        for (ui64 i = 0; i < N; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(received[i], i);
        }

        AssertV2InUse(*cluster, 1, 2);

        // payload well above the 64 KB fallback recv buffer, so reassembly must span several recvs
        auto* payloadCollector = new TPayloadCollectorActor;
        const TActorId pcId = cluster->RegisterActor(payloadCollector, 2);
        TString payload = TString::Uninitialized(300000);
        for (size_t i = 0; i < payload.size(); ++i) {
            payload[i] = static_cast<char>('a' + (i % 26));
        }
        const TActorId sender(1, 0, 0xBEEF, 0);
        cluster->GetNode(1)->GetActorSystem()->Send(new IEventHandle(pcId, sender, new TEvTest(1, payload)));

        WaitFor(TDuration::Seconds(20), [&] { return payloadCollector->GetCount() >= 1; },
            "large payload received over v2 fallback path");
        UNIT_ASSERT_VALUES_EQUAL(payloadCollector->GetLastPayload().size(), payload.size());
        UNIT_ASSERT_EQUAL(payloadCollector->GetLastPayload(), payload);
    }

    //
    // Teardown / disconnection coverage.
    //
    // These exercise the paths that run when a v2 session goes away: the engine must drain its in-flight
    // io_uring read/write before freeing the per-connection state (otherwise a completion dereferences
    // freed memory on the shard worker thread), the session actor must tear down cleanly, and a fresh
    // session must be establishable afterwards.
    //

    // A destroyed cluster (actor-system teardown) with idle-but-established v2 sessions in both directions
    // must shut down cleanly: engine reaper threads joined, sessions freed, no crash or hang.
    Y_UNIT_TEST(ActorSystemTeardownIdleSessions) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId echo1 = cluster->RegisterActor(new TEchoActor, 1);
        const TActorId echo2 = cluster->RegisterActor(new TEchoActor, 2);

        // establish a v2 session in each direction
        auto* c1 = new TResponseCollectorActor;
        auto* c2 = new TResponseCollectorActor;
        const TActorId c1Id = cluster->RegisterActor(c1, 1);
        const TActorId c2Id = cluster->RegisterActor(c2, 2);
        cluster->GetNode(1)->GetActorSystem()->Send(new IEventHandle(echo2, c1Id, new TEvTest(0)));
        cluster->GetNode(2)->GetActorSystem()->Send(new IEventHandle(echo1, c2Id, new TEvTest(0)));
        WaitFor(TDuration::Seconds(20), [&] { return c1->GetCount() >= 1 && c2->GetCount() >= 1; },
            "v2 sessions established both ways");
        AssertV2InUse(*cluster, 1, 2);
        AssertV2InUse(*cluster, 2, 1);

        // teardown: the engine must be stopped (reaper threads joined) with the still-armed recv in flight
        cluster.reset();
    }

    // Destroying the cluster while a load is actively flowing means the engine is torn down with reads and
    // writes in flight and connections still registered. Must not crash or hang.
    Y_UNIT_TEST(ActorSystemTeardownUnderLoad) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);

        // a large, essentially unbounded load so it is guaranteed still running at teardown
        auto promise = NThreading::NewPromise<ui32>();
        cluster->RegisterActor(new TLoadDriverActor(responder, /*total=*/1'000'000, /*inFlyMax=*/32,
            /*payloadSize=*/4096, /*channel=*/1, promise), 1);

        // let traffic actually start flowing (session up, io_uring ops in flight)
        Sleep(TDuration::MilliSeconds(300));

        // teardown mid-flight
        cluster.reset();
    }

    // Stopping the peer node must make the local side observe a disconnect (EOF on the socket propagates
    // through the engine to the session, which notifies subscribers).
    Y_UNIT_TEST(PeerStopTriggersDisconnect) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId echoId = cluster->RegisterActor(new TEchoActor, 2);

        auto* monitor = new TConnectionMonitorActor(2);
        cluster->RegisterActor(monitor, 1);

        // trigger the connection and wait until it is up
        auto* collector = new TResponseCollectorActor;
        const TActorId collectorId = cluster->RegisterActor(collector, 1);
        cluster->GetNode(1)->GetActorSystem()->Send(new IEventHandle(echoId, collectorId, new TEvTest(0)));
        WaitFor(TDuration::Seconds(20), [&] { return collector->GetCount() >= 1 && monitor->Connects() >= 1; },
            "v2 session established and observed by monitor");

        cluster->StopNode(2);

        WaitFor(TDuration::Seconds(20), [&] { return monitor->Disconnects() >= 1; },
            "local side observes disconnect after peer stop");
    }

    // Forcibly terminating the session (TEvPoisonSession -> IInterconnectSession::Terminate, i.e. the
    // synchronous Unregister-while-recv-armed path) must not crash, and a brand new session must round-trip
    // afterwards.
    Y_UNIT_TEST(ForcedSessionTeardownAndReconnect) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);

        auto runLoad = [&](ui32 total, TStringBuf what) {
            auto promise = NThreading::NewPromise<ui32>();
            auto future = promise.GetFuture();
            cluster->RegisterActor(new TLoadDriverActor(responder, total, /*inFlyMax=*/16, /*payloadSize=*/4096,
                /*channel=*/1, promise), 1);
            UNIT_ASSERT_C(future.Wait(TDuration::Seconds(60)), what);
            UNIT_ASSERT_VALUES_EQUAL(future.GetValueSync(), total);
        };

        // phase 1: a session is created and used
        runLoad(2000, "initial load stalled");
        AssertV2InUse(*cluster, 1, 2);

        // forcibly tear the session down
        cluster->GetNode(1)->Send(cluster->InterconnectProxy(2, 1), new TEvInterconnect::TEvPoisonSession);

        // phase 2: a fresh session must be established transparently and carry the load
        runLoad(2000, "load after forced teardown stalled -- reconnect broken");
        AssertV2InUse(*cluster, 1, 2);
    }

    // Shutting the peer socket (TEvClosePeerSocket -> the engine sees EOF -> session self-terminates) must
    // also recover with a new session.
    Y_UNIT_TEST(ClosePeerSocketAndReconnect) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);

        auto* monitor = new TConnectionMonitorActor(2);
        cluster->RegisterActor(monitor, 1);

        auto runLoad = [&](ui32 total, TStringBuf what) {
            auto promise = NThreading::NewPromise<ui32>();
            auto future = promise.GetFuture();
            cluster->RegisterActor(new TLoadDriverActor(responder, total, /*inFlyMax=*/16, /*payloadSize=*/4096,
                /*channel=*/1, promise), 1);
            UNIT_ASSERT_C(future.Wait(TDuration::Seconds(60)), what);
            UNIT_ASSERT_VALUES_EQUAL(future.GetValueSync(), total);
        };

        runLoad(2000, "initial load stalled");
        WaitFor(TDuration::Seconds(20), [&] { return monitor->Connects() >= 1; }, "connection observed");

        cluster->GetNode(1)->Send(cluster->InterconnectProxy(2, 1), new TEvInterconnect::TEvClosePeerSocket);
        WaitFor(TDuration::Seconds(20), [&] { return monitor->Disconnects() >= 1; },
            "disconnect observed after ClosePeerSocket");

        runLoad(2000, "load after ClosePeerSocket stalled -- reconnect broken");
    }

    // Repeatedly forcing the session down while traffic is actively flowing is the strongest reproducer of
    // the "free the connection while a read/write is in flight" teardown bug. After the churn the connection
    // must still be fully usable.
    Y_UNIT_TEST(RepeatedDisconnectChurnUnderLoad) {
        if (!TUringContext::IsAvailable()) {
            Cerr << "io_uring not available; skipping" << Endl;
            return;
        }
        auto cluster = MakeV2Cluster();
        const TActorId echoId = cluster->RegisterActor(new TEchoActor, 2);
        auto* collector = new TResponseCollectorActor;
        const TActorId collectorId = cluster->RegisterActor(collector, 1);

        // churn: keep some best-effort traffic in flight and repeatedly tear the session down mid-transfer
        for (int round = 0; round < 15; ++round) {
            for (ui64 i = 0; i < 200; ++i) {
                cluster->GetNode(1)->GetActorSystem()->Send(
                    new IEventHandle(echoId, collectorId, new TEvTest(i)));
            }
            Sleep(TDuration::MilliSeconds(15));
            cluster->GetNode(1)->Send(cluster->InterconnectProxy(2, 1), new TEvInterconnect::TEvPoisonSession);
        }

        // recovery: after all the churn a clean load must complete fully over a freshly established session
        const TActorId responder = cluster->RegisterActor(new TLoadEchoActor, 2);
        auto promise = NThreading::NewPromise<ui32>();
        auto future = promise.GetFuture();
        constexpr ui32 kTotal = 3000;
        cluster->RegisterActor(new TLoadDriverActor(responder, kTotal, /*inFlyMax=*/16, /*payloadSize=*/4096,
            /*channel=*/1, promise), 1);
        UNIT_ASSERT_C(future.Wait(TDuration::Seconds(60)), "connection did not recover after disconnect churn");
        UNIT_ASSERT_VALUES_EQUAL(future.GetValueSync(), kTotal);
    }
}
