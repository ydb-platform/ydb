#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <ydb/library/actors/interconnect/uring_context.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>

using namespace NActors;

Y_UNIT_TEST_SUITE(InterconnectUring) {

    // Every uring data-path test runs in three modes so the SQPOLL path stays covered:
    //   *_NoUring     - epoll backend (UseUring off): a control that the test itself is sound
    //   *_Uring       - io_uring, explicit submit (UseUring on, SQPOLL off; the default)
    //   *_UringSqpoll - io_uring with the shared SQPOLL kernel poll thread (EnableUringSQPOLL on)
    // The bodies are extracted into Do<Name>(mode) and emitted via URING_TEST_3MODES.

    // Skip only the uring variants when the kernel lacks io_uring; the no-uring variant always
    // runs. (If SQPOLL itself is unavailable the session falls back to epoll, so the SQPOLL
    // variant still passes functionally.)
    static bool UringUnavailable(TTestICCluster::Flags mode) {
        const bool wantUring = mode & (TTestICCluster::USE_URING | TTestICCluster::USE_URING_SQPOLL);
        if (wantUring && !TUringContext::IsSupported()) {
            Cerr << "io_uring not supported on this kernel, skipping" << Endl;
            return true;
        }
        return false;
    }

    static TTestICCluster::Flags WithMode(TTestICCluster::Flags base, TTestICCluster::Flags mode) {
        return static_cast<TTestICCluster::Flags>(base | mode);
    }

#define URING_TEST_3MODES(Name) \
    Y_UNIT_TEST(Name##_NoUring)     { Do##Name(TTestICCluster::EMPTY); } \
    Y_UNIT_TEST(Name##_Uring)       { Do##Name(TTestICCluster::USE_URING); } \
    Y_UNIT_TEST(Name##_UringSqpoll) { Do##Name(static_cast<TTestICCluster::Flags>(TTestICCluster::USE_URING | TTestICCluster::USE_URING_SQPOLL)); }

    Y_UNIT_TEST(UringSupported) {
        // Just verify the probe runs without crashing
        [[maybe_unused]] bool supported = TUringContext::IsSupported();
    }

    void DoBasicSendRecv(TTestICCluster::Flags mode) {
        if (UringUnavailable(mode)) {
            return;
        }

        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, mode);

        // Simple ping-pong: send an event from node 1 to node 2 and get a response
        class TPingActor : public TActorBootstrapped<TPingActor> {
        public:
            TPingActor(TActorId target, std::atomic<int>& counter)
                : Target(target)
                , Counter(counter)
            {}

            void Bootstrap() {
                Send(Target, new TEvents::TEvPing);
                Become(&TPingActor::StateFunc);
            }

            STRICT_STFUNC(StateFunc,
                cFunc(TEvents::TEvPong::EventType, HandlePong)
            )

            void HandlePong() {
                Counter.fetch_add(1);
                PassAway();
            }

        private:
            TActorId Target;
            std::atomic<int>& Counter;
        };

        class TPongActor : public TActorBootstrapped<TPongActor> {
        public:
            void Bootstrap() {
                Become(&TPongActor::StateFunc);
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvPing, Handle)
            )

            void Handle(TEvents::TEvPing::TPtr& ev) {
                Send(ev->Sender, new TEvents::TEvPong);
            }
        };

        std::atomic<int> counter{0};

        TActorId pongId = cluster.RegisterActor(new TPongActor, 2);
        cluster.RegisterActor(new TPingActor(pongId, counter), 1);

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        while (counter.load() == 0 && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT_VALUES_EQUAL(counter.load(), 1);
    }
    URING_TEST_3MODES(BasicSendRecv)

    void DoLargeMessage(TTestICCluster::Flags mode) {
        if (UringUnavailable(mode)) {
            return;
        }

        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, mode);

        class TReceiver : public TActorBootstrapped<TReceiver> {
        public:
            TReceiver(std::atomic<size_t>& bytesReceived)
                : BytesReceived(bytesReceived)
            {}

            void Bootstrap() {
                Become(&TReceiver::StateFunc);
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvBlob, Handle)
            )

            void Handle(TEvents::TEvBlob::TPtr& ev) {
                BytesReceived.fetch_add(ev->Get()->Blob.size());
            }

        private:
            std::atomic<size_t>& BytesReceived;
        };

        std::atomic<size_t> bytesReceived{0};
        const size_t messageSize = 1024 * 1024; // 1 MB

        TActorId receiverId = cluster.RegisterActor(new TReceiver(bytesReceived), 2);

        // Send a large blob from node 1 to node 2
        TString data(messageSize, 'X');
        cluster.GetNode(1)->Send(receiverId, new TEvents::TEvBlob(data));

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (bytesReceived.load() < messageSize && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(bytesReceived.load(), messageSize);
    }
    URING_TEST_3MODES(LargeMessage)

    void DoMultipleMessages(TTestICCluster::Flags mode) {
        if (UringUnavailable(mode)) {
            return;
        }

        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, mode);

        class TCountingReceiver : public TActorBootstrapped<TCountingReceiver> {
        public:
            TCountingReceiver(std::atomic<int>& counter)
                : Counter(counter)
            {}

            void Bootstrap() {
                Become(&TCountingReceiver::StateFunc);
            }

            STRICT_STFUNC(StateFunc,
                cFunc(TEvents::TEvPing::EventType, HandlePing)
            )

            void HandlePing() {
                Counter.fetch_add(1);
            }

        private:
            std::atomic<int>& Counter;
        };

        std::atomic<int> counter{0};
        const int numMessages = 1000;

        TActorId receiverId = cluster.RegisterActor(new TCountingReceiver(counter), 2);

        for (int i = 0; i < numMessages; ++i) {
            cluster.GetNode(1)->Send(receiverId, new TEvents::TEvPing);
        }

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(30);
        while (counter.load() < numMessages && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(counter.load(), numMessages);
    }
    URING_TEST_3MODES(MultipleMessages)

    void DoSustainedStream(TTestICCluster::Flags mode) {
        // Stream a large amount of data through many messages to force the provided-buffer
        // recv ring to recycle buffers repeatedly (and exercise the ENOBUFS re-arm path).
        if (UringUnavailable(mode)) {
            return;
        }

        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, mode);

        class TReceiver : public TActorBootstrapped<TReceiver> {
        public:
            TReceiver(std::atomic<size_t>& bytesReceived)
                : BytesReceived(bytesReceived)
            {}
            void Bootstrap() { Become(&TReceiver::StateFunc); }
            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvBlob, Handle)
            )
            void Handle(TEvents::TEvBlob::TPtr& ev) {
                BytesReceived.fetch_add(ev->Get()->Blob.size());
            }
        private:
            std::atomic<size_t>& BytesReceived;
        };

        std::atomic<size_t> bytesReceived{0};
        const size_t messageSize = 64 * 1024;
        const int numMessages = 1000;
        const size_t totalBytes = messageSize * numMessages;

        TActorId receiverId = cluster.RegisterActor(new TReceiver(bytesReceived), 2);

        TString data(messageSize, 'Z');
        for (int i = 0; i < numMessages; ++i) {
            cluster.GetNode(1)->Send(receiverId, new TEvents::TEvBlob(data));
        }

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
        while (bytesReceived.load() < totalBytes && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(bytesReceived.load(), totalBytes);
    }
    URING_TEST_3MODES(SustainedStream)

    void DoZeroCopyLargeMessages(TTestICCluster::Flags mode) {
        // Exercise the io_uring IORING_OP_SEND_ZC path (XDC) together with the NOTIF-gated
        // buffer lifetime. Content is verified to catch any premature buffer reuse.
        if (UringUnavailable(mode)) {
            return;
        }

        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr,
            WithMode(TTestICCluster::USE_ZC, mode));

        class TVerifier : public TActorBootstrapped<TVerifier> {
        public:
            TVerifier(std::atomic<int>& ok, std::atomic<int>& bad, char fill)
                : Ok(ok)
                , Bad(bad)
                , Fill(fill)
            {}
            void Bootstrap() { Become(&TVerifier::StateFunc); }
            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvBlob, Handle)
            )
            void Handle(TEvents::TEvBlob::TPtr& ev) {
                const TString& blob = ev->Get()->Blob;
                bool good = true;
                for (char c : blob) {
                    if (c != Fill) {
                        good = false;
                        break;
                    }
                }
                (good ? Ok : Bad).fetch_add(1);
            }
        private:
            std::atomic<int>& Ok;
            std::atomic<int>& Bad;
            const char Fill;
        };

        std::atomic<int> ok{0};
        std::atomic<int> bad{0};
        const char fill = 'Q';
        const size_t messageSize = 512 * 1024;
        const int numMessages = 50;

        TActorId receiverId = cluster.RegisterActor(new TVerifier(ok, bad, fill), 2);

        TString data(messageSize, fill);
        for (int i = 0; i < numMessages; ++i) {
            cluster.GetNode(1)->Send(receiverId, new TEvents::TEvBlob(data));
        }

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(60);
        while (ok.load() + bad.load() < numMessages && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(100));
        }

        UNIT_ASSERT_VALUES_EQUAL(bad.load(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ok.load(), numMessages);
    }
    URING_TEST_3MODES(ZeroCopyLargeMessages)

    void DoSessionChurnReconnect(TTestICCluster::Flags mode) {
        // Repeatedly tear the session down (TEvPoisonSession) and verify the connection
        // re-establishes and delivers every round. Before the reaper learned to release
        // session rings, each churn leaked an io_uring fd + eventfd + buffer ring and held
        // the socket open, so after a handful of cycles the node could no longer connect
        // (fd exhaustion / anchor-SQ abort) — exactly the cluster meltdown this guards.
        if (UringUnavailable(mode)) {
            return;
        }

        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, mode);

        class TPongActor : public TActorBootstrapped<TPongActor> {
        public:
            void Bootstrap() { Become(&TPongActor::StateFunc); }
            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvPing, Handle)
            )
            void Handle(TEvents::TEvPing::TPtr& ev) {
                Send(ev->Sender, new TEvents::TEvPong);
            }
        };

        class TPingActor : public TActorBootstrapped<TPingActor> {
        public:
            TPingActor(TActorId target, std::atomic<int>& counter)
                : Target(target)
                , Counter(counter)
            {}
            void Bootstrap() {
                Send(Target, new TEvents::TEvPing, IEventHandle::FlagTrackDelivery);
                Become(&TPingActor::StateFunc);
            }
            STRICT_STFUNC(StateFunc,
                cFunc(TEvents::TEvPong::EventType, HandlePong)
            )
            void HandlePong() {
                Counter.fetch_add(1);
                PassAway();
            }
        private:
            TActorId Target;
            std::atomic<int>& Counter;
        };

        TActorId pongId = cluster.RegisterActor(new TPongActor, 2);

        const int rounds = 40;
        for (int round = 0; round < rounds; ++round) {
            std::atomic<int> counter{0};
            cluster.RegisterActor(new TPingActor(pongId, counter), 1);

            const TInstant deadline = TInstant::Now() + TDuration::Seconds(20);
            while (counter.load() == 0 && TInstant::Now() < deadline) {
                Sleep(TDuration::MilliSeconds(20));
            }
            UNIT_ASSERT_C(counter.load() == 1,
                "round " << round << ": ping/pong did not complete (reconnect failed?)");

            // Force a session teardown so the next round must reconnect over a fresh ring.
            cluster.GetNode(1)->Send(cluster.InterconnectProxy(2, 1), new TEvInterconnect::TEvPoisonSession);
            Sleep(TDuration::MilliSeconds(50));
        }
    }
    URING_TEST_3MODES(SessionChurnReconnect)

    void DoIdleKeepAlive(TTestICCluster::Flags mode) {
        // Regression test for the io_uring idle-keepalive (DeadPeer) cluster meltdown.
        //
        // An idle connection exchanges nothing but periodic keepalive pings (every PingPeriod,
        // default 3s). The original cluster meltdown was traced to reaper bugs (eventfd never
        // registered, the anchor poll going silent after one fire) that starved completions on
        // idle sessions until the peer declared DeadPeer. This guards that an idle session is
        // kept alive across several ping cycles in every backend mode (epoll, uring, uring+SQPOLL).
        //
        // The settings below make this deterministic: PingPeriod (3s) < DeadPeer (6s) so a
        // healthy connection (one whose pings keep flowing) is never torn down. We subscribe to
        // the peer (establishing a session but sending no payload) and sit idle across several
        // ping/DeadPeer cycles, asserting the session is never declared dead.
        if (UringUnavailable(mode)) {
            return;
        }

        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr,
            mode, /*checkerFactory=*/{}, /*deadPeerTimeout=*/TDuration::Seconds(6));

        class TConnSubscriber : public TActorBootstrapped<TConnSubscriber> {
        public:
            TConnSubscriber(ui32 peerNodeId, std::atomic<int>& connects, std::atomic<int>& disconnects)
                : PeerNodeId(peerNodeId)
                , Connects(connects)
                , Disconnects(disconnects)
            {}
            void Bootstrap() {
                Become(&TConnSubscriber::StateFunc);
                Send(TActivationContext::InterconnectProxy(PeerNodeId), new TEvents::TEvSubscribe);
            }
            STRICT_STFUNC(StateFunc,
                cFunc(TEvInterconnect::TEvNodeConnected::EventType, HandleConnected)
                cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, HandleDisconnected)
            )
            void HandleConnected() { Connects.fetch_add(1); }
            void HandleDisconnected() { Disconnects.fetch_add(1); }
        private:
            const ui32 PeerNodeId;
            std::atomic<int>& Connects;
            std::atomic<int>& Disconnects;
        };

        std::atomic<int> connects{0};
        std::atomic<int> disconnects{0};

        cluster.RegisterActor(new TConnSubscriber(2, connects, disconnects), 1);

        // Wait for the session to come up.
        const TInstant connectDeadline = TInstant::Now() + TDuration::Seconds(10);
        while (connects.load() == 0 && TInstant::Now() < connectDeadline) {
            Sleep(TDuration::MilliSeconds(20));
        }
        UNIT_ASSERT_C(connects.load() >= 1, "session never connected");

        // Baseline right before going idle, then stay idle across several ping (3s) and
        // DeadPeer (6s) cycles. A healthy keepalive keeps the session up the whole time.
        const int connectsBaseline = connects.load();
        const int disconnectsBaseline = disconnects.load();

        Sleep(TDuration::Seconds(16));

        UNIT_ASSERT_C(disconnects.load() == disconnectsBaseline,
            "session was torn down while idle (keepalive stall): disconnects "
                << disconnectsBaseline << " -> " << disconnects.load());
        UNIT_ASSERT_C(connects.load() == connectsBaseline,
            "session reconnected while idle (keepalive stall): connects "
                << connectsBaseline << " -> " << connects.load());
    }
    URING_TEST_3MODES(IdleKeepAlive)

    Y_UNIT_TEST(FallbackToEpollWithEncryption) {
        // When TLS is enabled, even with UseUring=true, the session should
        // fall back to the epoll path. This test verifies no crash occurs.
        if (!TUringContext::IsSupported()) {
            Cerr << "io_uring not supported on this kernel, skipping" << Endl;
            return;
        }

        auto flags = static_cast<TTestICCluster::Flags>(
            TTestICCluster::USE_URING | TTestICCluster::USE_TLS);
        TTestICCluster cluster(2, NActors::TChannelsConfig(), nullptr, nullptr, flags);

        std::atomic<int> counter{0};

        class TPongActor : public TActorBootstrapped<TPongActor> {
        public:
            void Bootstrap() { Become(&TPongActor::StateFunc); }
            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvPing, Handle)
            )
            void Handle(TEvents::TEvPing::TPtr& ev) {
                Send(ev->Sender, new TEvents::TEvPong);
            }
        };

        class TPingActor : public TActorBootstrapped<TPingActor> {
        public:
            TPingActor(TActorId target, std::atomic<int>& c)
                : Target(target), Counter(c) {}
            void Bootstrap() {
                Send(Target, new TEvents::TEvPing);
                Become(&TPingActor::StateFunc);
            }
            STRICT_STFUNC(StateFunc,
                cFunc(TEvents::TEvPong::EventType, HandlePong)
            )
            void HandlePong() {
                Counter.fetch_add(1);
                PassAway();
            }
        private:
            TActorId Target;
            std::atomic<int>& Counter;
        };

        TActorId pongId = cluster.RegisterActor(new TPongActor, 2);
        cluster.RegisterActor(new TPingActor(pongId, counter), 1);

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        while (counter.load() == 0 && TInstant::Now() < deadline) {
            Sleep(TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT_VALUES_EQUAL(counter.load(), 1);
    }
}
