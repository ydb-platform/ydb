#include "dq_channel_test_harness.h"

// IMemoryQuotaManager accounting of the 3 buffer kinds: what happens when the quota runs out, and the
// quota manager which arrives after the descriptor - assigned late on the output side, reallocated on
// bind on the input side, reassigned on the local buffer.

// Both sides of the channel are aborted when a quota runs out, whichever side's it is; the quota must
// be back to 0 once the runtime is gone, with nothing freed twice
struct TQuotaAbortTest : public TSessionTest {

    void CheckAbort(const TEvTestPrivate::TFinishInfo& finished) {
        UNIT_ASSERT_C(finished.Aborted, finished.Reason);
        UNIT_ASSERT_C(finished.Reason.Contains("OVERLOADED"), finished.Reason);
        UNIT_ASSERT_C(finished.Reason.Contains("Channel memory limit exceeded"), finished.Reason);
        UNIT_ASSERT_C(!finished.Error, finished.Reason);
    }

    void CheckBothAborted() {
        CheckAbort(WaitFinished(Control0, NodeIndex0, "the producer"));
        CheckAbort(WaitFinished(Control1, NodeIndex1, "the consumer"));
    }
};

// The output quota is held from the push to the ack of the session; with the acks held the 3rd
// message of the channel does not fit
struct TOutputQuotaTest : public TQuotaAbortTest {

    void Run() override {
        Prepare();
        UseDebugSessions = true;
        Init();
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)), "no session");

        OutputQuotaManager->Limit = 100_KB;
        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 40000, .MaxMessageSize = 40000, .ExpectAbort = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 40000, .MaxMessageSize = 40000, .ExpectAbort = true };

        Debug0->PauseChannelAck();
        // the consumer binds first: aborted before its bind it would leave the descriptor the leading
        // message creates to the unbound cleanup
        auto producer = Runtime->Register(new TProducerActor(Service0, 1, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service1, 1, ConsumerSettings, InputQuotaManager), NodeIndex1);
        Actors.insert(producer);
        Actors.insert(consumer);
        Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetCounter(Service1, "InputBuffer/Count") == 1; }, TDuration::Seconds(10)), "the consumer did not bind");
        Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
        CheckBothAborted();
        // the producer keeps pushing into the void until the window is full, every push rejected
        UNIT_ASSERT_C(OutputQuotaManager->Rejected.load() >= 1, OutputQuotaManager->Rejected.load());

        Debug0->ResumeChannelAck();
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The input quota runs out with the consumer stalled, on a push of the receiver
struct TInputQuotaTest : public TQuotaAbortTest {

    void Run() override {
        Prepare();
        Init();

        InputQuotaManager->Limit = 100_KB;
        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 40000, .MaxMessageSize = 40000, .ExpectAbort = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 40000, .MaxMessageSize = 40000,
            .PauseMessageIndex = 0, .PauseDelayMs = 30000, .ExpectAbort = true };

        // the consumer binds first, so that the quota runs out on a push and not on the bind (see
        // TInputBindQuotaTest for that)
        auto producer = Runtime->Register(new TProducerActor(Service0, 1, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service1, 1, ConsumerSettings, InputQuotaManager), NodeIndex1);
        Actors.insert(producer);
        Actors.insert(consumer);
        Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetCounter(Service1, "InputBuffer/Count") == 1; }, TDuration::Seconds(10)), "the consumer did not bind");
        Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
        CheckBothAborted();
        UNIT_ASSERT_C(InputQuotaManager->Rejected.load() >= 1, InputQuotaManager->Rejected.load());

        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The local buffer: the push of the producer fails, both actors share the buffer
struct TLocalQuotaTest : public TQuotaAbortTest {

    void Run() override {
        Prepare();
        Init();

        // the buffer keeps the quota manager of whoever binds first, the same limit on both
        OutputQuotaManager->Limit = 100_KB;
        InputQuotaManager->Limit = 100_KB;
        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 40000, .MaxMessageSize = 40000, .ExpectAbort = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 40000, .MaxMessageSize = 40000,
            .PauseMessageIndex = 0, .PauseDelayMs = 30000, .ExpectAbort = true };

        StartChannel(1, true);
        // both on node 0, in whichever order
        CheckAbort(WaitFinished(Control0, NodeIndex0, "one side"));
        CheckAbort(WaitFinished(Control0, NodeIndex0, "the other side"));
        UNIT_ASSERT(FindFinished(1, TEvTestPrivate::ERole::Producer) && FindFinished(1, TEvTestPrivate::ERole::Consumer));
        UNIT_ASSERT_C(OutputQuotaManager->Rejected.load() + InputQuotaManager->Rejected.load() > 0, "nothing was rejected");

        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// An early finish of the consumer before the producer binds creates the output descriptor without a
// quota manager and pushes the finish chunk unquoted; the producer assigns its manager on bind
struct TLateQuotaTest : public TSessionTest {

    bool HasUnboundOutput() {
        auto session = FindNodeState(Service0, Runtime->GetNodeId(1));
        if (!session) {
            return false;
        }
        std::lock_guard lock(session->Mutex);
        for (const auto& [info, descriptor] : session->OutputDescriptors) {
            if (!descriptor->IsBound) {
                return true;
            }
        }
        return false;
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 100, .ExpectEarlyFinished = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true };

        // the producer is registered but not started, so the update of the consumer creates the descriptor
        auto producer = Runtime->Register(new TProducerActor(Service0, 1, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service1, 1, ConsumerSettings, InputQuotaManager), NodeIndex1);
        Actors.insert(producer);
        Actors.insert(consumer);
        Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return HasUnboundOutput(); }, TDuration::Seconds(10)),
            "the early finish did not create an output descriptor");

        Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
        WaitChannel("late quota assignment");
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The unquoted finish chunk of the scenario above lands in the WaitQueue of the descriptor when the
// session window is full, and the checkpoint the producer pushes after its own finish joins it quoted:
// SendFromWaiters has to account the unquoted prefix and the quoted rest apart
struct TWaitQueueQuotaTest : public TSessionTest {

    void Prepare() override {
        // one producer fills the session window before its own
        Limits.RemoteSessionInflightBytes = 1_MB;
        Limits.RemoteChannelColdInflightBytes = 4_MB;
        ExpectReconciliation = true;
        TSessionTest::Prepare();
    }

    std::shared_ptr<TOutputDescriptor> FindUnboundOutput() {
        std::lock_guard lock(Debug0->Mutex);
        for (const auto& [info, descriptor] : Debug0->OutputDescriptors) {
            if (!descriptor->IsBound) {
                return descriptor;
            }
        }
        return nullptr;
    }

    void Run() override {
        Prepare();
        UseDebugSessions = true;
        Init();
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0 && Debug1->Reconciliation.load() == 0; }, TDuration::Seconds(5)), "no session");

        // channel 1 fills the session window with the acks held
        Debug0->PauseChannelAck();
        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 50000, .MaxMessageSize = 50000 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetWaitersQueueSize(Debug0) > 0; }, TDuration::Seconds(10)), "the window did not fill up");

        // channel 2: the consumer early-finishes first, its finish chunk waits unquoted
        ProducerSettings = TWorkerSettings{ .MessageCount = 0, .CheckpointAfterFinish = true, .ExpectEarlyFinished = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true };
        auto producer = Runtime->Register(new TProducerActor(Service0, 2, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service1, 2, ConsumerSettings, InputQuotaManager), NodeIndex1);
        Actors.insert(producer);
        Actors.insert(consumer);
        Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);

        std::shared_ptr<TOutputDescriptor> descriptor;
        UNIT_ASSERT_C(WaitFor([&]() { return (descriptor = FindUnboundOutput()) && descriptor->WaitQueueSize.load() == 1; }, TDuration::Seconds(10)),
            "the finish chunk is not waiting");
        UNIT_ASSERT_C(!descriptor->IsQuotaAssigned(), "a quota manager before the producer bound");

        // the producer binds, assigns the quota and pushes a quoted checkpoint behind the finish
        Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
        UNIT_ASSERT_C(WaitFor([&]() { return descriptor->WaitQueueSize.load() == 2; }, TDuration::Seconds(10)),
            TStringBuilder() << "the checkpoint is not waiting, WaitQueueSize=" << descriptor->WaitQueueSize.load());
        UNIT_ASSERT_C(descriptor->IsQuotaAssigned(), "no quota manager after the producer bound");
        {
            std::lock_guard lock(descriptor->WaitQueueMutex);
            UNIT_ASSERT_VALUES_EQUAL(descriptor->UnquotedWaitBytes, 1);
            UNIT_ASSERT_VALUES_EQUAL(descriptor->WaitQueueBytes.load(), 2);
        }

        Debug0->ResumeChannelAck();
        WaitFinishes(Control0, NodeIndex0, 2, "producers");
        WaitFinishes(Control1, NodeIndex1, 2, "consumers");
        UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, ErrorDetails());
        {
            std::lock_guard lock(descriptor->WaitQueueMutex);
            UNIT_ASSERT_VALUES_EQUAL(descriptor->UnquotedWaitBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(descriptor->WaitQueueBytes.load(), 0);
        }
        descriptor.reset();
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// Data arriving before the consumer binds is queued without a quota; the bind moves the queue onto
// the manager of the consumer, or aborts if it does not fit
struct TInputBindQuotaTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10000, .MaxMessageSize = 10000, .ExpectAbort = Fails };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10000, .MaxMessageSize = 10000, .ExpectAbort = Fails };
        if (Fails) {
            InputQuotaManager->Limit = 100_KB;
        }

        auto channel = StartChannel(1, false);
        std::shared_ptr<TNodeState> receiver;
        UNIT_ASSERT_C(WaitFor([&]() {
            if (!receiver) {
                receiver = FindNodeState(Service1, Runtime->GetNodeId(0));
            }
            if (!receiver) {
                return false;
            }
            auto descriptors = GetInputDescriptors(receiver);
            return !descriptors.empty() && descriptors.front()->QueueSize.load() == 21;
        }, TDuration::Seconds(10)), "the messages did not queue up at the receiver");
        const ui64 queued = 20 * 10001 + 1;
        UNIT_ASSERT_VALUES_EQUAL(GetInputDescriptors(receiver).front()->QueueBytes.load(), queued);
        UNIT_ASSERT_VALUES_EQUAL(InputQuotaManager->Allocated.load(), 0);

        StartConsumer(channel);
        if (Fails) {
            for (auto [control, nodeIndex] : {std::make_pair(Control0, NodeIndex0), std::make_pair(Control1, NodeIndex1)}) {
                auto finished = WaitFinished(control, nodeIndex, "a side");
                UNIT_ASSERT_C(finished.Aborted && finished.Reason.Contains("OVERLOADED"), finished.Reason);
            }
            UNIT_ASSERT_VALUES_EQUAL(InputQuotaManager->Rejected.load(), 1);
            UNIT_ASSERT_VALUES_EQUAL(InputQuotaManager->Allocated.load(), 0);
            CheckSensors();
        } else {
            WaitChannel("input bind");
            UNIT_ASSERT_VALUES_EQUAL(InputQuotaManager->Allocated.load(), queued);
            UNIT_ASSERT_VALUES_EQUAL(InputQuotaManager->Freed.load(), queued);
            CheckSensors();
        }

        receiver.reset();
        Destroy();
        CheckQuota();
    }

    bool Fails = false;
};

// The local buffer takes the quota manager of whoever binds first and the one of the other side only if
// it had none: the reader without a manager (the way KqpExecuter reads its result) sees the writer's
struct TLocalReassignTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 1000, .MaxMessageSize = 10000 };
        ConsumerSettings = ProducerSettings;

        auto producer = Runtime->Register(new TProducerActor(Service0, 1, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->Register(new TConsumerActor(Service0, 1, ConsumerSettings, nullptr), NodeIndex0);
        Actors.insert(producer);
        Actors.insert(consumer);

        auto& registry = Service0->LocalBufferRegistry;
        auto bufferCount = [&]() {
            std::lock_guard lock(registry->Mutex);
            return registry->LocalBuffers.size();
        };

        if (ConsumerFirst) {
            Runtime->Send(consumer, Control0, new TEvTestPrivate::TEvStart(producer), NodeIndex0, true);
            UNIT_ASSERT_C(WaitFor([&]() { return bufferCount() == 1; }, TDuration::Seconds(10)), "the consumer did not bind");
            Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
        } else {
            Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
            UNIT_ASSERT_C(WaitFor([&]() { return OutputQuotaManager->Allocated.load() > 0; }, TDuration::Seconds(10)), "the producer did not push");
            Runtime->Send(consumer, Control0, new TEvTestPrivate::TEvStart(producer), NodeIndex0, true);
        }

        WaitFinishes(Control0, NodeIndex0, 2, "the channel");
        UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, ErrorDetails());
        // everything went through the manager of the producer, pushes and pops alike
        UNIT_ASSERT_C(OutputQuotaManager->Allocated.load() > 0, OutputQuotaManager->Allocated.load());
        UNIT_ASSERT_VALUES_EQUAL(OutputQuotaManager->Allocated.load(), OutputQuotaManager->Freed.load());
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    bool ConsumerFirst = true;
};

Y_UNIT_TEST_SUITE(Channels20Quota) {

    Y_UNIT_TEST(OutputQuotaExceededAbortsProducer2n) {
        TOutputQuotaTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(InputQuotaExceededAbortsPeer2n) {
        TInputQuotaTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(LocalQuotaExceededAbortsConsumer1n) {
        TLocalQuotaTest test;
        test.Local = true;
        test.Run();
    }

    Y_UNIT_TEST(LateQuotaAssignmentOnEarlyFinish2n) {
        TLateQuotaTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(WaitQueueQuotaAfterLateAssignment2n) {
        TWaitQueueQuotaTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(InputQuotaReallocatedOnBind2n) {
        TInputBindQuotaTest test;
        test.Local = false;
        test.Run();
    }

    // the bind aborts before the descriptor has a quota manager of its own
    Y_UNIT_TEST(InputQuotaReallocationFails2n) {
        TInputBindQuotaTest test;
        test.Local = false;
        test.Fails = true;
        test.Run();
    }

    Y_UNIT_TEST(LocalBufferReaderWithoutQuota1n) {
        TLocalReassignTest test;
        test.Local = true;
        test.ConsumerFirst = true;
        test.Run();
    }

    Y_UNIT_TEST(LocalBufferReaderWithoutQuotaBindsLast1n) {
        TLocalReassignTest test;
        test.Local = true;
        test.ConsumerFirst = false;
        test.Run();
    }
}
