#include "dq_channel_test_harness.h"

// The life of a node session: created by the 1st channel, freed when idle - at once with a zero idle
// period - or when its peer is gone, torn down with the runtime, with its descriptors bound and unbound.

// A channel to completion, the sessions of both nodes freed while idle, then another channel on fresh ones
struct TIdleDestroyTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdleDestroyPeriod = TDuration::MilliSeconds(IdleDestroyMs);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;

        StartChannel(1, true);
        WaitChannel("1st channel");
        NActors::TActorId senderActorId;
        NActors::TActorId receiverActorId;
        if (IdleDestroyMs) {
            // still there right after the channel, gone once idle
            auto sender = FindNodeState(Service0, receiverNodeId);
            auto receiver = FindNodeState(Service1, senderNodeId);
            UNIT_ASSERT_C(sender && receiver, "sessions not found after the 1st channel");
            senderActorId = sender->NodeActorId;
            receiverActorId = receiver->NodeActorId;
        }
        // with a zero period the sessions go with the last descriptor, before this can look
        UNIT_ASSERT_C(WaitFor([&]() {
            return !FindNodeState(Service0, receiverNodeId) && !FindNodeState(Service1, senderNodeId);
        }, TDuration::Seconds(5)), "the idle sessions were not freed");
        CheckSensors();

        StartChannel(2, true);
        WaitChannel("2nd channel");
        if (IdleDestroyMs) {
            auto sender = FindNodeState(Service0, receiverNodeId);
            auto receiver = FindNodeState(Service1, senderNodeId);
            UNIT_ASSERT_C(sender && receiver, "sessions not found after the 2nd channel");
            UNIT_ASSERT_C(sender->NodeActorId != senderActorId && receiver->NodeActorId != receiverActorId, "the sessions were not recreated");
            UNIT_ASSERT_VALUES_EQUAL(GetGenMajor(sender), 1);
            UNIT_ASSERT_VALUES_EQUAL(GetGenMajor(receiver), 1);
        }
        UNIT_ASSERT_C(WaitFor([&]() {
            return !FindNodeState(Service0, receiverNodeId) && !FindNodeState(Service1, senderNodeId);
        }, TDuration::Seconds(5)), "the idle sessions were not freed again");

        CheckSensors();
        Destroy();
        CheckQuota();
    }

    ui64 IdleDestroyMs = 300;
};

// The receiver frees its session while the sender keeps its own: the next channel is sent to a session
// actor which is gone, the bounce starts a major reconciliation and the new receiver session takes the
// resent queue from 1
struct TPeerFreedTest : public TSessionTest {

    void Prepare() override {
        ExpectReconciliation = true;
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;

        StartChannel(1, true);
        WaitChannel("1st channel");
        auto sender = FindNodeState(Service0, receiverNodeId);
        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(sender && receiver, "sessions not found");
        WaitSettled(sender);
        auto genMajor = GetGenMajor(sender);

        receiver->Terminating.store(true);
        Service1->FreeNodeSession(senderNodeId, receiver->NodeActorId);
        UNIT_ASSERT_C(!FindNodeState(Service1, senderNodeId), "the receiver session is still there");

        StartChannel(2, true);
        WaitChannel([&]() { return TStringBuilder() << "G=" << GetGenMajor(sender) << ", log=" << GetReconciliationLog(sender); });
        UNIT_ASSERT_VALUES_EQUAL_C(GetGenMajor(sender), genMajor + 1, GetReconciliationLog(sender));
        UNIT_ASSERT_C(GetReconciliationLog(sender).Contains("U"), GetReconciliationLog(sender));
        UNIT_ASSERT_VALUES_EQUAL(GetCounter(Service0, "Session/Reconciliations", true), 1);

        auto replaced = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(replaced && replaced->NodeActorId != receiver->NodeActorId, "no new receiver session");
        UNIT_ASSERT_VALUES_EQUAL(replaced->OutputNodeGenMajor.load(), genMajor + 1);

        replaced.reset();
        receiver.reset();
        sender.reset();
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The runtime goes down with every channel open and data queued everywhere; nothing may crash and the
// quota of both sides must be released exactly once
struct TTeardownTest : public TLoadTest {

    void Run() override {
        Prepare();
        Init();
        Count = 20;
        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 10000, .MaxMessageSize = 10000 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 10000, .MaxMessageSize = 10000,
            .PauseMessageIndex = 5, .PauseDelayMs = 30000 };
        Start();

        // every producer is blocked at its window with the consumers stalled, both quotas are in use
        UNIT_ASSERT_C(WaitFor([&]() { return OutputQuotaManager->Quota.load() > 0 || InputQuotaManager->Quota.load() > 0; }, TDuration::Seconds(10)),
            "nothing in flight");
        Sleep(TDuration::MilliSeconds(500));
        UNIT_ASSERT_C(OutputQuotaManager->Quota.load() + InputQuotaManager->Quota.load() > 0, "nothing in flight");

        Destroy();
        CheckQuota();
    }
};

// An auto-created descriptor nobody binds to is erased once UnboundWaitPeriod is over: the input one a
// leading message creates, the output one an early finish of the peer creates
struct TUnboundCleanupTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.UnboundWaitPeriod = TDuration::MilliSeconds(200);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        // the consumer of channel 1 never starts, the producer waits for a finish confirmation for good
        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, false);

        std::shared_ptr<TNodeState> receiver;
        UNIT_ASSERT_C(WaitFor([&]() {
            return (receiver = FindNodeState(Service1, senderNodeId)) && GetInputCount(receiver) == 1;
        }, TDuration::Seconds(10)), "the leading message did not create an input descriptor");
        UNIT_ASSERT_VALUES_EQUAL(GetCounter(Service1, "InputBuffer/Count"), 1);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(5)),
            "the unbound input descriptor was not erased");
        UNIT_ASSERT_VALUES_EQUAL(GetCounter(Service1, "InputBuffer/Count"), 0);

        // the producer of channel 2 never starts, the consumer early-finishes at once
        ProducerSettings = TWorkerSettings{ .MessageCount = 0 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true };
        auto producer = Runtime->Register(new TProducerActor(Service0, 2, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service1, 2, ConsumerSettings, InputQuotaManager), NodeIndex1);
        Actors.insert(producer);
        Actors.insert(consumer);
        Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);

        auto sender = FindNodeState(Service0, receiverNodeId);
        UNIT_ASSERT_C(sender, "no sender session");
        // the descriptor of channel 1 is bound and stays, the one of channel 2 is unbound and goes
        UNIT_ASSERT_C(WaitFor([&]() { return GetOutputCount(sender) == 2; }, TDuration::Seconds(10)),
            "the early finish did not create an output descriptor");
        UNIT_ASSERT_VALUES_EQUAL(GetCounter(Service0, "OutputBuffer/Count"), 2);
        // the consumer completes through the finish the update pushed, without a producer at all
        auto finished = WaitFinished(Control1, NodeIndex1, "the consumer");
        UNIT_ASSERT_C(!finished.Error, finished.Reason);
        UNIT_ASSERT_C(WaitFor([&]() { return GetOutputCount(sender) == 1; }, TDuration::Seconds(5)),
            "the unbound output descriptor was not erased");
        UNIT_ASSERT_VALUES_EQUAL(GetCounter(Service0, "OutputBuffer/Count"), 1);

        sender.reset();
        receiver.reset();
        Destroy();
        CheckQuota();
    }
};

// TEvFreeNodeSession names the session actor which asked, and only a terminating session is freed
struct TStaleFreeTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        auto receiverNodeId = Runtime->GetNodeId(1);

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        WaitChannel("warm up");

        auto sender = FindNodeState(Service0, receiverNodeId);
        UNIT_ASSERT_C(sender, "no sender session");

        // a stale sender: not freed
        Runtime->Send(MakeChannelServiceActorID(Runtime->GetNodeId(0)), Control0, new TEvPrivate::TEvFreeNodeSession(receiverNodeId), NodeIndex0, true);
        Sleep(TDuration::MilliSeconds(200));
        UNIT_ASSERT_C(FindNodeState(Service0, receiverNodeId) == sender, "freed on a stale request");

        // the right sender, not terminating: not freed
        Service0->FreeNodeSession(receiverNodeId, sender->NodeActorId);
        UNIT_ASSERT_C(FindNodeState(Service0, receiverNodeId) == sender, "freed while not terminating");

        sender->Terminating.store(true);
        Service0->FreeNodeSession(receiverNodeId, sender->NodeActorId);
        UNIT_ASSERT_C(!FindNodeState(Service0, receiverNodeId), "not freed");

        sender.reset();
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The same channel id between new actors on the same session is a different channel
struct TSameChannelIdTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 1000 };
        ConsumerSettings = ProducerSettings;

        for (int i = 0; i < 3; i++) {
            StartChannel(1, true);
            WaitChannel(TStringBuilder() << "round " << i);
        }
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

Y_UNIT_TEST_SUITE(Channels20Session) {

    Y_UNIT_TEST(IdleSessionDestroyed2n) {
        TIdleDestroyTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(ZeroIdleDestroyFreesOnLastChannel2n) {
        TIdleDestroyTest test;
        test.Local = false;
        test.IdleDestroyMs = 0;
        test.Run();
    }

    Y_UNIT_TEST(ChannelAfterPeerSessionFreed2n) {
        TPeerFreedTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(TeardownWithOpenChannels2n) {
        TTeardownTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(TeardownWithOpenChannels1n) {
        TTeardownTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(UnboundDescriptorsCleanedUp2n) {
        TUnboundCleanupTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(StaleFreeNodeSessionIgnored2n) {
        TStaleFreeTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(SameChannelIdNewActors2n) {
        TSameChannelIdTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(SameChannelIdNewActors1n) {
        TSameChannelIdTest test;
        test.Local = true;
        test.Run();
    }

    // one session per direction serves both halves of the traffic, with losses and jitter on every worker
    Y_UNIT_TEST(ManyChannelsBothDirections2n) {
        TLoadTest test;
        test.Count = 200;
        test.Local = false;
        test.Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        test.Limits.IdlePingPeriod = TDuration::MilliSeconds(200);
        test.ProducerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 0, .MaxMessageSize = 20000, .RandomPauseMaxMs = 200 };
        test.ConsumerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 0, .MaxMessageSize = 20000, .RandomPauseMaxMs = 200 };
        test.Failures = TFailureSettings{ .Data = 2, .DataCount = 5000 };
        test.ExpectReconciliation = true;
        test.WaitTimeout = TDuration::Seconds(30);
        test.Run();
    }
}
