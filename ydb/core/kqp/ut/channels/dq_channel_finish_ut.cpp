#include "dq_channel_test_harness.h"

// How a channel ends: the finish chunk and its confirmation, the early finish of the consumer in every
// order relative to the producer, and the control chunks - checkpoints and watermarks - in band and
// after the finish.

// The producer is parked at HardLimit when the consumer lets go: the early finish must get through
struct TEarlyFinishAtHardLimitTest : public TSessionTest {

    void Prepare() override {
        Limits.RemoteChannelInflightBytes = 8_MB;
        Limits.RemoteChannelColdInflightBytes = 512_KB;
        Limits.LocalChannelInflightBytes = 8_MB;
        TSessionTest::Prepare();
    }

    EDqFillLevel FillLevel() {
        if (Local) {
            auto& registry = Service0->LocalBufferRegistry;
            std::lock_guard lock(registry->Mutex);
            for (auto& [info, weak] : registry->LocalBuffers) {
                if (auto buffer = weak.lock()) {
                    return buffer->GetFillLevel();
                }
            }
            return EDqFillLevel::NoLimit;
        }
        auto session = FindNodeState(Service0, Runtime->GetNodeId(1));
        if (!session) {
            return EDqFillLevel::NoLimit;
        }
        for (const auto& descriptor : GetOutputDescriptors(session)) {
            std::lock_guard lock(descriptor->FlowControlMutex);
            return descriptor->FillLevel;
        }
        return EDqFillLevel::NoLimit;
    }

    void Run() override {
        Prepare();
        Init();

        // 4 MB messages: the 3rd one fills the 8 MB window while the consumer sleeps after the 2nd
        ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 4000000, .MaxMessageSize = 4000000, .ExpectEarlyFinished = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 4000000, .MaxMessageSize = 4000000,
            .EarlyFinish = true, .PauseMessageIndex = 1, .PauseDelayMs = 1500 };

        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return FillLevel() == EDqFillLevel::HardLimit; }, TDuration::Seconds(10)),
            "the producer did not reach HardLimit");

        WaitChannel("early finish at HardLimit");
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The consumer early-finishes before the producer binds at all: the descriptor / local buffer carries
// the early finish to the producer when it comes
struct TEarlyFinishFirstTest : public TSessionTest {

    bool EarlyFinished() {
        if (Local) {
            auto& registry = Service0->LocalBufferRegistry;
            std::lock_guard lock(registry->Mutex);
            for (auto& [info, weak] : registry->LocalBuffers) {
                if (auto buffer = weak.lock()) {
                    return buffer->EarlyFinished.load();
                }
            }
            return false;
        }
        auto session = FindNodeState(Service0, Runtime->GetNodeId(1));
        if (!session) {
            return false;
        }
        for (const auto& descriptor : GetOutputDescriptors(session)) {
            return descriptor->EarlyFinished.load();
        }
        return false;
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 100, .ExpectEarlyFinished = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true };

        auto producer = Runtime->Register(new TProducerActor(Service0, 1, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service1, 1, ConsumerSettings, InputQuotaManager), NodeIndex1);
        Actors.insert(producer);
        Actors.insert(consumer);
        Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return EarlyFinished(); }, TDuration::Seconds(10)),
            "the early finish did not reach the output side");

        Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
        WaitChannel("early finish first");
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The producer has finished, its data and finish chunk queued at the receiver, when the consumer binds
// and either drains everything or early-finishes at once
struct TConsumerLastTest : public TSessionTest {

    bool FinishQueued() {
        if (Local) {
            auto& registry = Service0->LocalBufferRegistry;
            std::lock_guard lock(registry->Mutex);
            for (auto& [info, weak] : registry->LocalBuffers) {
                if (auto buffer = weak.lock()) {
                    std::lock_guard bufferLock(buffer->Mutex);
                    return buffer->FinishPushed;
                }
            }
            return false;
        }
        auto receiver = FindNodeState(Service1, Runtime->GetNodeId(0));
        if (!receiver) {
            return false;
        }
        for (const auto& descriptor : GetInputDescriptors(receiver)) {
            return descriptor->FinishPushed.load();
        }
        return false;
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 1000, .ExpectEarlyFinished = EarlyFinish };
        ConsumerSettings = TWorkerSettings{ .MessageCount = EarlyFinish ? 0 : 20, .MinMessageSize = 10, .MaxMessageSize = 1000, .EarlyFinish = EarlyFinish };

        auto channel = StartChannel(1, false);
        UNIT_ASSERT_C(WaitFor([&]() { return FinishQueued(); }, TDuration::Seconds(10)), "the finish did not arrive");

        StartConsumer(channel);
        WaitChannel("consumer last");
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    bool EarlyFinish = false;
};

// The consumer lets go of its buffer as soon as it popped the finish chunk, before the confirmation
// of the producer comes back. The confirmation finds no descriptor and is confirmed all the same, so
// that the session of the sender, which has no descriptors left to watch its queue for, does not keep
// it. The receiver is a debug session which holds the confirmation until the consumer is gone for sure.
struct TConfirmToGoneTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(200);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        UseDebugSessions = true;
        Init();
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0 && Debug1->Reconciliation.load() == 0; }, TDuration::Seconds(5)), "no session");

        ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 100, .LeaveAfterFinishChunk = true };

        // the data and the finish are let through, whatever follows - the confirmation - is held
        Debug1->PauseChannelData();
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return Debug1->PendingDataCount.load() >= 11; }, TDuration::Seconds(10)), "the messages did not arrive");
        Debug1->ProcessPending(11);

        auto consumer = WaitFinished(Control1, NodeIndex1, "the consumer");
        UNIT_ASSERT_C(!consumer.Error, consumer.Reason);
        auto producer = WaitFinished(Control0, NodeIndex0, "the producer");
        UNIT_ASSERT_C(!producer.Error, producer.Reason);
        UNIT_ASSERT_C(WaitFor([&]() { return Debug1->PendingDataCount.load() >= 1; }, TDuration::Seconds(10)), "no confirmation arrived");
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(Debug1) == 0; }, TDuration::Seconds(5)), "the input descriptor is still there");
        UNIT_ASSERT_VALUES_EQUAL(GetQueueSize(Debug0), 1);

        Debug1->ResumeChannelData();
        auto details = [&]() {
            return TStringBuilder() << "queue=" << GetQueueSize(Debug0) << ", InflightMessages=" << GetCounter(Service0, "OutputBuffer/InflightMessages")
                << ", log=" << GetReconciliationLog(Debug0);
        };
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(Debug0) == 0; }, TDuration::Seconds(5)),
            TStringBuilder() << "the confirmation was not confirmed, " << details());
        UNIT_ASSERT_VALUES_EQUAL_C(CountPings(Debug0), 0, details());
        UNIT_ASSERT_VALUES_EQUAL_C(AbortCount, 0, ErrorDetails());

        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// A flip of the memory pressure of the receiver while it has nothing to pop is still reported on the
// empty pop, both ways
struct TPressureFlipTest : public TSessionTest {

    void Prepare() override {
        Limits.EnableSpillingChannelBackpressure = true;
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        // the producer pauses after 5 messages, the consumer drains them and has nothing more to pop
        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 5, .PauseDelayMs = 2000 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100 };

        auto channel = StartChannel(1, true);
        std::shared_ptr<TNodeState> sender;
        std::shared_ptr<TOutputDescriptor> descriptor;
        UNIT_ASSERT_C(WaitFor([&]() {
            if (!sender) {
                sender = FindNodeState(Service0, Runtime->GetNodeId(1));
            }
            if (!sender) {
                return false;
            }
            auto descriptors = GetOutputDescriptors(sender);
            if (descriptors.empty()) {
                return false;
            }
            descriptor = descriptors.front();
            return descriptor->RemotePopBytes.load() == descriptor->PushBytes.load() && descriptor->PushBytes.load() > 0;
        }, TDuration::Seconds(10)), "the consumer did not drain the 1st messages");
        auto popBytes = descriptor->RemotePopBytes.load();
        UNIT_ASSERT_C(!descriptor->PeerMemoryPressure.load(), "pressure before it was set");

        // nothing to pop: the empty pop of the consumer carries the flip on its own
        InputQuotaManager->MemoryPressure = true;
        Runtime->Send(channel.second, Control1, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback}, NodeIndex1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return descriptor->PeerMemoryPressure.load(); }, TDuration::Seconds(5)), "pressure not reported");
        UNIT_ASSERT_VALUES_EQUAL_C(descriptor->RemotePopBytes.load(), popBytes, "something was popped");
        UNIT_ASSERT_VALUES_EQUAL(GetCounter(Service0, "OutputBuffer/ThrottledCount"), 1);

        InputQuotaManager->MemoryPressure = false;
        Runtime->Send(channel.second, Control1, new TEvDqCompute::TEvResumeExecution{EResumeSource::CAWakeupCallback}, NodeIndex1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return !descriptor->PeerMemoryPressure.load(); }, TDuration::Seconds(5)), "the release was not reported");
        UNIT_ASSERT_VALUES_EQUAL_C(descriptor->RemotePopBytes.load(), popBytes, "something was popped");
        UNIT_ASSERT_VALUES_EQUAL(GetCounter(Service0, "OutputBuffer/ThrottledCount"), 0);

        WaitChannel("pressure flip");
        descriptor.reset();
        sender.reset();
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

Y_UNIT_TEST_SUITE(Channels20Finish) {

    void LoadTest(int count, bool local, const TWorkerSettings& producerSettings, const TWorkerSettings& consumerSettings) {
        TLoadTest test;

        test.Count = count;
        test.Local = local;
        test.ProducerSettings = producerSettings;
        test.ConsumerSettings = consumerSettings;

        test.Run();
    }

    Y_UNIT_TEST(EarlyFinishUnblocksHardLimit2n) {
        TEarlyFinishAtHardLimitTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(EarlyFinishUnblocksHardLimit1n) {
        TEarlyFinishAtHardLimitTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(EarlyFinishBeforeProducerBinds2n) {
        TEarlyFinishFirstTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(EarlyFinishBeforeProducerBinds1n) {
        TEarlyFinishFirstTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(ConsumerBindsAfterProducerFinished2n) {
        TConsumerLastTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(ConsumerBindsAfterProducerFinished1n) {
        TConsumerLastTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(EarlyFinishAfterProducerFinished2n) {
        TConsumerLastTest test;
        test.Local = false;
        test.EarlyFinish = true;
        test.Run();
    }

    Y_UNIT_TEST(EarlyFinishAfterProducerFinished1n) {
        TConsumerLastTest test;
        test.Local = true;
        test.EarlyFinish = true;
        test.Run();
    }

    Y_UNIT_TEST(CheckpointsAndWatermarksInOrder2n) {
        LoadTest(10, false,
            TWorkerSettings{ .MessageCount = 200, .CheckpointEvery = 10, .WatermarkEvery = 7 },
            TWorkerSettings{ .MessageCount = 200, .CheckpointEvery = 10, .WatermarkEvery = 7 });
    }

    Y_UNIT_TEST(CheckpointsAndWatermarksInOrder1n) {
        LoadTest(10, true,
            TWorkerSettings{ .MessageCount = 200, .CheckpointEvery = 10, .WatermarkEvery = 7 },
            TWorkerSettings{ .MessageCount = 200, .CheckpointEvery = 10, .WatermarkEvery = 7 });
    }

    // a checkpoint may follow the finish and is still delivered; its pop is one more pop after the finish
    // chunk, which reports Finishing once more
    Y_UNIT_TEST(CheckpointAfterFinish2n) {
        LoadTest(10, false,
            TWorkerSettings{ .MessageCount = 20, .CheckpointAfterFinish = true },
            TWorkerSettings{ .MessageCount = 20, .CheckpointAfterFinish = true });
    }

    Y_UNIT_TEST(CheckpointAfterFinish1n) {
        LoadTest(10, true,
            TWorkerSettings{ .MessageCount = 20, .CheckpointAfterFinish = true },
            TWorkerSettings{ .MessageCount = 20, .CheckpointAfterFinish = true });
    }

    // data after the finish is dropped by the output side
    Y_UNIT_TEST(DataAfterFinishDropped2n) {
        LoadTest(10, false, TWorkerSettings{ .MessageCount = 20, .DataAfterFinish = 3 }, TWorkerSettings{ .MessageCount = 20 });
    }

    Y_UNIT_TEST(DataAfterFinishDropped1n) {
        LoadTest(10, true, TWorkerSettings{ .MessageCount = 20, .DataAfterFinish = 3 }, TWorkerSettings{ .MessageCount = 20 });
    }

    Y_UNIT_TEST(ConfirmFinishToGoneDescriptor2n) {
        TConfirmToGoneTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(PopEmptyReportsPressureFlip2n) {
        TPressureFlipTest test;
        test.Local = false;
        test.Run();
    }
}
