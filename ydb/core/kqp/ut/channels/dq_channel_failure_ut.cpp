#include "dq_channel_test_harness.h"

// Message loss, paused acks and the reconciliation paths they drive: the gap RESEND, the idle ping, the
// session window and its waiters, the major and minor reconciliations started by undelivered data and
// the give-up of a session whose peer never answers.

// A load run with losses injected by the debug sessions; every channel must still complete in order
struct TLossTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(200);
        ExpectReconciliation = true;
        WaitTimeout = TDuration::Seconds(30);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();
        Start();
        Wait();
        Check();
        auto resent = GetCounter(Service0, "Session/MessagesResent", true) + GetCounter(Service1, "Session/MessagesResent", true);
        auto details = TStringBuilder() << "resent " << resent << ", log0=" << GetReconciliationLog(Debug0)
            << ", log1=" << GetReconciliationLog(Debug1);
        CheckSensors();
        if (ExpectResend) {
            UNIT_ASSERT_C(resent > 0, TStringBuilder() << "nothing was resent although messages were lost, " << details);
        }
        Destroy();
        CheckQuota();
    }

    bool ExpectResend = true;
};

// The outbound half of node 0 only, so that the sender session and its queue are the ones of Debug0
struct TOutboundTest : public TSessionTest {

    void Prepare() override {
        ExpectReconciliation = true;
        TSessionTest::Prepare();
    }

    void Init() override {
        UseDebugSessions = true;
        TSessionTest::Init();
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0 && Debug1->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
            "the debug sessions did not connect");
    }

    // count channels 1..count from node 0 to node 1, all started
    void StartOutbound(int count) {
        for (int i = 1; i <= count; i++) {
            StartChannel(i, true);
        }
    }

    // count finishes from each side, no errors
    void WaitOutbound(int count) {
        WaitFinishes(Control0, NodeIndex0, count, "producers");
        WaitFinishes(Control1, NodeIndex1, count, "consumers");
        UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, ErrorDetails());
    }

    ui64 SumWaitQueueBytes() {
        ui64 bytes = 0;
        for (const auto& descriptor : GetOutputDescriptors(Debug0)) {
            bytes += descriptor->WaitQueueBytes.load();
        }
        return bytes;
    }

    ui64 SumWaitQueueSize() {
        ui64 size = 0;
        for (const auto& descriptor : GetOutputDescriptors(Debug0)) {
            size += descriptor->WaitQueueSize.load();
        }
        return size;
    }

    TString SessionDetails() {
        return TStringBuilder() << "InflightBytes=" << Debug0->InflightBytes.load()
            << ", Queue=" << GetQueueSize(Debug0)
            << ", WaitersQueue=" << GetWaitersQueueSize(Debug0)
            << ", WaitQueueBytes=" << SumWaitQueueBytes()
            << ", WaitQueueSize=" << SumWaitQueueSize()
            << ", WaiterCount=" << GetCounter(Service0, "OutputBuffer/WaiterCount")
            << ", WaiterBytes=" << GetCounter(Service0, "OutputBuffer/WaiterBytes")
            << ", WaiterMessages=" << GetCounter(Service0, "OutputBuffer/WaiterMessages")
            << ", Reconciliation=" << Debug0->Reconciliation.load()
            << ", G=" << GetGenMajor(Debug0) << '.' << GetGenMinor(Debug0)
            << ", log=" << GetReconciliationLog(Debug0);
    }
};

// With the acks held at the sender, nothing leaves the session queue: the producers fill the session
// window and everything past it waits in the descriptors, on the waiters queue, accounted on the sensors.
// The producers push on while their waiters drain, which is where the numbering of a drained chunk and
// of a fresh push of the same descriptor once raced (SendFromWaiters).
struct TSessionWindowTest : public TOutboundTest {

    void Prepare() override {
        Limits.RemoteSessionInflightBytes = 1_MB;
        TOutboundTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 50000, .MaxMessageSize = 50000 };
        ConsumerSettings = ProducerSettings;

        Debug0->PauseChannelAck();
        StartOutbound(20);

        // the window is checked before the bytes are added, so it is exceeded by at most one message
        auto window = Limits.RemoteSessionInflightBytes;
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->InflightBytes.load() + 50001 >= window && GetWaitersQueueSize(Debug0) > 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "the session window did not fill up, " << SessionDetails());

        // every producer is blocked at the cold window of its channel by now, the numbers are settled
        Sleep(TDuration::MilliSeconds(500));
        auto details = SessionDetails();
        UNIT_ASSERT_LE_C(Debug0->InflightBytes.load(), window + 50001, details);
        UNIT_ASSERT_VALUES_EQUAL_C(Debug0->InflightBytes.load(), GetQueueBytes(Debug0), details);

        // whatever waits is on the sensors, and only that
        auto waitBytes = SumWaitQueueBytes();
        auto waitSize = SumWaitQueueSize();
        UNIT_ASSERT_C(waitBytes > 0 && waitSize > 0, details);
        UNIT_ASSERT_C(GetCounter(Service0, "OutputBuffer/WaiterCount") > 0, details);
        UNIT_ASSERT_VALUES_EQUAL_C(Debug0->WaiterBytes.load(), waitBytes, details);
        UNIT_ASSERT_VALUES_EQUAL_C(Debug0->WaiterMessages.load(), waitSize, details);
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service0, "OutputBuffer/WaiterBytes"), waitBytes, details);
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service0, "OutputBuffer/WaiterMessages"), waitSize, details);

        Debug0->ResumeChannelAck();
        WaitOutbound(20);
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// MaxInflightMessages bounds the session queue as the byte window does, for tiny messages
struct TMaxInflightMessagesTest : public TOutboundTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 20000, .MinMessageSize = 10, .MaxMessageSize = 10 };
        ConsumerSettings = ProducerSettings;

        Debug0->PauseChannelAck();
        StartOutbound(1);

        auto limit = Debug0->MaxInflightMessages;
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(Debug0) >= limit && GetWaitersQueueSize(Debug0) > 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "the session queue did not fill up to " << limit << ", " << SessionDetails());
        for (int i = 0; i < 20; i++) {
            UNIT_ASSERT_LE_C(GetQueueSize(Debug0), limit, SessionDetails());
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_C(SumWaitQueueSize() > 0, SessionDetails());

        Debug0->ResumeChannelAck();
        WaitOutbound(1);
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// SendFromWaiters holds still during a reconciliation; the waiters are drained once it is over
struct TWaitersAfterReconciliationTest : public TOutboundTest {

    void Prepare() override {
        Limits.RemoteSessionInflightBytes = 1_MB;
        TOutboundTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 50000, .MaxMessageSize = 50000 };
        ConsumerSettings = ProducerSettings;

        Debug0->PauseChannelAck();
        StartOutbound(10);
        UNIT_ASSERT_C(WaitFor([&]() { return GetWaitersQueueSize(Debug0) > 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "no waiters, " << SessionDetails());
        auto genMinor = GetGenMinor(Debug0);

        // the discovery reply is an ack, held like the others: the reconciliation stays open until the acks
        // are let go, and the acks of the old GenMinor which come first are ignored
        Runtime->Send(Debug0->NodeActorId, Control0, new NActors::TEvInterconnect::TEvNodeDisconnected(Runtime->GetNodeId(1)), NodeIndex0, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMinor(Debug0) == genMinor + 1; }, TDuration::Seconds(5)),
            TStringBuilder() << "no minor reconciliation, " << SessionDetails());
        UNIT_ASSERT_C(Debug0->Reconciliation.load() != 0, SessionDetails());
        UNIT_ASSERT_C(GetWaitersQueueSize(Debug0) > 0, SessionDetails());

        Debug0->ResumeChannelAck();
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
            TStringBuilder() << "not reconciled, " << SessionDetails());

        WaitOutbound(10);
        UNIT_ASSERT_VALUES_EQUAL_C(GetWaitersQueueSize(Debug0), 0, SessionDetails());
        UNIT_ASSERT_C(GetReconciliationLog(Debug0).Contains("D"), SessionDetails());
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The pop progress of the receiver is carried by updates alone; with them lost the producer stays cold
// and blocked. An idle ping brings them again, whichever side asks: the receiver answers the ping of
// the sender with them, and sends them with a ping of its own. The pings of the two sides refresh each
// other's activity, so either may be the only one to happen; each is staged here by making the
// discoveries of the other side go unanswered.
struct TLostUpdateTest : public TOutboundTest {

    void Prepare() override {
        Limits.RemoteChannelInflightBytes = 1_MB;
        Limits.RemoteChannelColdInflightBytes = 256_KB;
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(200);
        TOutboundTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 50000, .MaxMessageSize = 50000 };
        ConsumerSettings = ProducerSettings;

        // only the side which is to ping runs its cleanup: a ping of the other would refresh it, and one
        // of the receiver resends its updates as well, which would unblock the producer on its own
        auto& pinger = ReceiverPings ? Debug1 : Debug0;
        auto& other = ReceiverPings ? Debug0 : Debug1;
        Debug0->DropUpdateCount.store(1000000);
        other->CleanupPaused.store(true);
        StartOutbound(1);

        // the producer fills the cold window and the consumer drains all of it, the updates saying so lost
        std::shared_ptr<TOutputDescriptor> descriptor;
        UNIT_ASSERT_C(WaitFor([&]() {
            auto descriptors = GetOutputDescriptors(Debug0);
            if (descriptors.empty()) {
                return false;
            }
            descriptor = descriptors.front();
            return descriptor->PushBytes.load() >= Limits.RemoteChannelColdInflightBytes
                && GetInputPopBytes(Debug1) == descriptor->PushBytes.load();
        }, TDuration::Seconds(10)), TStringBuilder() << "the producer did not fill the cold window, " << SessionDetails());
        UNIT_ASSERT_VALUES_EQUAL_C(descriptor->RemotePopBytes.load(), 0, "an update got through");
        auto pingsBefore = CountPings(pinger);

        Debug0->DropUpdateCount.store(0);
        UNIT_ASSERT_C(WaitFor([&]() { return descriptor->RemotePopBytes.load() > 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "the producer was not unblocked, " << SessionDetails());
        other->CleanupPaused.store(false);

        WaitOutbound(1);
        auto details = TStringBuilder() << "pings " << pingsBefore << " -> " << CountPings(pinger) << ", " << SessionDetails();
        UNIT_ASSERT_C(CountPings(pinger) > pingsBefore, TStringBuilder() << "the producer was not unblocked by the idle ping, " << details);
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    bool ReceiverPings = false;
};

// A discovery lost on the way is retried by the timer of the reconciliation, once per attempt
struct TDroppedDiscoveryTest : public TOutboundTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 3, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        StartOutbound(1);
        WaitOutbound(1);
        WaitSettled(Debug0);
        auto genMinor = GetGenMinor(Debug0);

        Debug1->DropDiscoveryCount.store(1);
        Runtime->Send(Debug0->NodeActorId, Control0, new NActors::TEvInterconnect::TEvNodeDisconnected(Runtime->GetNodeId(1)), NodeIndex0, true);

        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMinor(Debug0) == genMinor + 1 && Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
            TStringBuilder() << "the retry did not reconcile, " << SessionDetails());
        // the 1st discovery is lost, so the retry of the timer is the only way this reconciled
        UNIT_ASSERT_C(GetReconciliationLog(Debug0).Contains("DT"), SessionDetails());

        StartChannel(2, true);
        WaitOutbound(1);
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// Undelivered data with an unknown actor means the peer session actor is gone: a major reconciliation.
// The receiver session is freed under a warm channel here; the next push of its producer bounces, the
// generation moves on and the producer is aborted for it. Its consumer is aborted with the session it
// is bound to, which nothing would reach any more. A new channel takes the fresh session and completes.
//
// With the reason Disconnected (an interconnect session drop) the reconciliation is a minor one: the
// queue is resent under the same generation and every channel goes on.
struct TUndeliveredTest : public TOutboundTest {

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);

        // B: warm, its producer pausing so that it pushes again only after the reconciliation
        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 1000, .MaxMessageSize = 1000,
            .PauseMessageIndex = 5, .PauseDelayMs = 2000, .ExpectAbort = Major };
        // its consumer stalls: for good in the major case, where its session goes under it, long enough
        // for the reconciliation in the minor one, where the channel completes
        ConsumerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 1000, .MaxMessageSize = 1000,
            .PauseMessageIndex = 1, .PauseDelayMs = Major ? 30000 : 3000, .ExpectAbort = Major };
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() {
            auto descriptors = GetOutputDescriptors(Debug0);
            return !descriptors.empty() && descriptors.front()->PushBytes.load() >= 5 * 1001
                && GetInputPopBytes(Debug1) > 0 && GetQueueSize(Debug0) == 0;
        }, TDuration::Seconds(10)), TStringBuilder() << "B did not warm up, " << SessionDetails());
        auto genMajor = GetGenMajor(Debug0);
        auto genMinor = GetGenMinor(Debug0);

        if (Major) {
            Debug1->Terminating.store(true);
            Service1->FreeNodeSession(senderNodeId, Debug1->NodeActorId);
            auto consumerB = WaitFinished(Control1, NodeIndex1, "the consumer of B");
            UNIT_ASSERT_C(consumerB.Aborted && consumerB.Reason.Contains("UNAVAILABLE"), consumerB.Reason);
            UNIT_ASSERT_C(consumerB.Reason.Contains("Node session freed with the channel still open"), consumerB.Reason);

            // the producer of B resumes into the void: the bounce is what starts the reconciliation
            UNIT_ASSERT_C(WaitFor([&]() { return GetGenMajor(Debug0) == genMajor + 1; }, TDuration::Seconds(10)),
                TStringBuilder() << "no major reconciliation, " << SessionDetails());
            UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
                TStringBuilder() << "not reconciled, " << SessionDetails());
            UNIT_ASSERT_C(GetReconciliationLog(Debug0).Contains("U"), SessionDetails());

            auto producerB = WaitFinished(Control0, NodeIndex0, "the producer of B");
            UNIT_ASSERT_C(producerB.Aborted && producerB.Reason.Contains("UNAVAILABLE"), producerB.Reason);
            UNIT_ASSERT_C(producerB.Reason.Contains("GenMajor") || producerB.Reason.Contains("Reconciliation"), producerB.Reason);
        } else {
            // A: everything it has stays queued, its consumer is not started yet
            Debug0->PauseChannelAck();
            ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 20 };
            ConsumerSettings = ProducerSettings;
            auto channelA = StartChannel(2, false);
            UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(Debug0) == 11; }, TDuration::Seconds(10)),
                TStringBuilder() << "A did not queue its messages, " << SessionDetails());

            Runtime->Send(Debug0->NodeActorId, Control0,
                new NActors::TEvents::TEvUndelivered(TEvDqCompute::TEvChannelDataV2::EventType, NActors::TEvents::TEvUndelivered::Disconnected),
                NodeIndex0, true);
            UNIT_ASSERT_C(WaitFor([&]() { return GetGenMinor(Debug0) == genMinor + 1; }, TDuration::Seconds(5)),
                TStringBuilder() << "no minor reconciliation, " << SessionDetails());
            UNIT_ASSERT_VALUES_EQUAL_C(GetGenMajor(Debug0), genMajor, SessionDetails());
            UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(Debug0), 11, SessionDetails());

            Debug0->ResumeChannelAck();
            UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
                TStringBuilder() << "not reconciled, " << SessionDetails());
            UNIT_ASSERT_C(GetReconciliationLog(Debug0).Contains("O"), SessionDetails());
            StartConsumer(channelA);
        }

        if (Major) {
            // a new channel on the fresh receiver session
            ProducerSettings = TWorkerSettings{ .MessageCount = 10, .MinMessageSize = 10, .MaxMessageSize = 20 };
            ConsumerSettings = ProducerSettings;
            StartChannel(2, true);
            WaitFinishes(Control0, NodeIndex0, 1, "the producer of A");
            WaitFinishes(Control1, NodeIndex1, 1, "the consumer of A");
        } else {
            WaitOutbound(2);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, ErrorDetails());
        auto producerA = FindFinished(2, TEvTestPrivate::ERole::Producer);
        auto consumerA = FindFinished(2, TEvTestPrivate::ERole::Consumer);
        UNIT_ASSERT(producerA && consumerA);
        UNIT_ASSERT_C(!producerA->Aborted && !consumerA->Aborted, "A was aborted");

        if (!Major) {
            auto producerB = FindFinished(1, TEvTestPrivate::ERole::Producer);
            auto consumerB = FindFinished(1, TEvTestPrivate::ERole::Consumer);
            UNIT_ASSERT(producerB && consumerB);
            UNIT_ASSERT_C(!producerB->Aborted && !consumerB->Aborted, "B was aborted by a minor reconciliation");
        }
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    bool Major = true;
};

// A bounce naming an actor the session does not address is the echo of a copy sent to an actor already
// superseded, see HandleUndelivered: taking it for the death of the live peer used to start a major
// reconciliation, and the receiver then failed every unfinished descriptor bound to the generation left
// behind.
//
// The setup is the moment where that costs the most: the consumer has popped everything, the finish chunk
// included, and waits for the confirmation of the finish, which is what the held update asks for. A stale
// bounce must leave all of this alone and the channel must complete; a genuine one must still reconcile,
// and the consumer bound to the generation left behind is aborted for it.
struct TFinishingStaleBounceTest : public TOutboundTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 20, .ExpectAbort = Genuine };

        // the updates of the receiver are held at the sender, the Finishing one among them
        Debug0->PauseChannelUpdate();
        StartOutbound(1);

        std::shared_ptr<TInputDescriptor> descriptor;
        UNIT_ASSERT_C(WaitFor([&]() {
            auto descriptors = GetInputDescriptors(Debug1);
            if (descriptors.empty()) {
                return false;
            }
            descriptor = descriptors.front();
            return descriptor->FinishPushed.load() && descriptor->PopStats.Bytes.load() == descriptor->PushStats.Bytes.load()
                && GetQueueSize(Debug0) == 0 && Debug0->Reconciliation.load() == 0;
        }, TDuration::Seconds(10)), TStringBuilder() << "the consumer did not pop the finish, " << SessionDetails());
        UNIT_ASSERT_C(!descriptor->Finished.load(), "the finish was confirmed with the updates held");
        auto genMajor = GetGenMajor(Debug0);
        ui64 boundGenMajor;
        {
            std::lock_guard lock(Debug1->Mutex);
            boundGenMajor = descriptor->OutputNodeGenMajor;
        }
        UNIT_ASSERT_VALUES_EQUAL_C(boundGenMajor, genMajor, SessionDetails());

        auto peerActorId = GetInputNodeActorId(Debug0);
        UNIT_ASSERT_C(peerActorId == Debug1->NodeActorId,
            TStringBuilder() << "InputNodeActorId=" << peerActorId << ", peer session actor " << Debug1->NodeActorId);

        // the receiver session actor is alive all along; only the actor the bounce names differs. The
        // stale one lives on the peer node, as the superseded session actor did, so a check on the node
        // alone would not tell the two apart
        Runtime->Send(Debug0->NodeActorId, Genuine ? peerActorId : Control1,
            new NActors::TEvents::TEvUndelivered(TEvDqCompute::TEvChannelDataV2::EventType, NActors::TEvents::TEvUndelivered::ReasonActorUnknown),
            NodeIndex0, true);

        if (Genuine) {
            UNIT_ASSERT_C(WaitFor([&]() { return GetGenMajor(Debug0) == genMajor + 1 && Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
                TStringBuilder() << "no major reconciliation, " << SessionDetails());
            UNIT_ASSERT_C(GetReconciliationLog(Debug0).Contains("U"), SessionDetails());

            Debug0->ResumeChannelUpdate();

            auto consumer = WaitFinished(Control1, NodeIndex1, "the consumer");
            UNIT_ASSERT_C(consumer.Aborted && consumer.Reason.Contains("UNAVAILABLE"), consumer.Reason);
            UNIT_ASSERT_C(consumer.Reason.Contains("advanced its generation"), consumer.Reason);
            UNIT_ASSERT_C(consumer.Reason.Contains("FP: 1, F: 0, EF: 0"), consumer.Reason);

            // the producer is not told: its descriptor stays at the old generation with nothing queued to
            // move it, so only the receiver side settles and the sender keeps the buffer of that producer
            UNIT_ASSERT_C(WaitFor([&]() {
                for (auto name : Gauges) {
                    if (GetCounter(Service1, name) != 0) {
                        return false;
                    }
                }
                return true;
            }, TDuration::Seconds(5)), TStringBuilder() << "sensors of node 1 not back to 0: " << SensorDetails(Service1));
            UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service0, "OutputBuffer/Count"), 1, SensorDetails(Service0));
        } else {
            // nothing is expected to happen, so the wait has to time out to mean anything
            UNIT_ASSERT_C(!WaitFor([&]() { return GetGenMajor(Debug0) != genMajor || GetReconciliationLog(Debug0).Contains("U"); },
                TDuration::Seconds(2)), TStringBuilder() << "the stale bounce started a major reconciliation, " << SessionDetails());
            UNIT_ASSERT_VALUES_EQUAL_C(Debug0->Reconciliation.load(), 0, SessionDetails());
            {
                std::lock_guard lock(Debug1->Mutex);
                boundGenMajor = descriptor->OutputNodeGenMajor;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(boundGenMajor, genMajor, SessionDetails());
            UNIT_ASSERT_C(!descriptor->Finished.load(), "the finish was confirmed with the updates still held");

            // the held update, let go, is answered under the generation it was sent for
            Debug0->ResumeChannelUpdate();
            auto consumer = WaitFinished(Control1, NodeIndex1, "the consumer");
            UNIT_ASSERT_C(!consumer.Aborted && !consumer.Error, consumer.Reason);
            auto producer = WaitFinished(Control0, NodeIndex0, "the producer");
            UNIT_ASSERT_C(!producer.Aborted && !producer.Error, producer.Reason);
            UNIT_ASSERT_C(descriptor->Finished.load(), "the consumer finished without the confirmation of the finish");
            UNIT_ASSERT_VALUES_EQUAL_C(ErrorCount, 0, ErrorDetails());
            CheckSensors();
        }

        descriptor.reset();
        Destroy();
        CheckQuota();
    }

    // the bounce names the session actor of the peer, as one from a peer which really died
    bool Genuine = false;
};

// A peer which never answers costs the session ReconciliationCount discoveries, then the session gives
// up: it fails its output descriptors - the producer is aborted - and frees itself
struct TGiveUpTest : public TSessionTest {

    void Prepare() override {
        // give up after 2 unanswered discoveries (~3s) instead of the default 3 (~7s)
        Limits.ReconciliationCount = 2;
        ExpectReconciliation = true;
        WaitTimeout = TDuration::Seconds(20);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto peerNodeId = Runtime->GetNodeId(1);

        ProducerSettings = TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        WaitChannel("warm up");

        auto session = FindNodeState(Service0, peerNodeId);
        UNIT_ASSERT_C(session, "node session not found");
        WaitSettled(session);

        // the consumer stalls, so the producer holds an output descriptor with data queued when the
        // handshake starts to fail
        ProducerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 10, .MaxMessageSize = 100, .ExpectAbort = true };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 50, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 30000 };
        StartChannel(2, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetOutputCount(session) == 1 && GetQueueSize(session) == 0; }, TDuration::Seconds(10)),
            "the producer did not send");

        // the peer cannot answer a discovery while its channel service is locked
        std::unique_lock serviceLock(Service1->Mutex);

        Runtime->Send(session->NodeActorId, Control0, new NActors::TEvInterconnect::TEvNodeDisconnected(peerNodeId), NodeIndex0, true);

        UNIT_ASSERT_C(WaitFor([&]() { return session->Terminating.load(); }, TDuration::Seconds(15)),
            TStringBuilder() << "the session did not give up, reconciliation log: " << GetReconciliationLog(session));
        auto log = GetReconciliationLog(session);
        UNIT_ASSERT_C(log.EndsWith("X"), log);

        auto producer = WaitFinished(Control0, NodeIndex0, "the producer");
        UNIT_ASSERT_C(producer.Aborted, producer.Reason);
        UNIT_ASSERT_C(producer.Reason.Contains("has not answered 2 discoveries"), producer.Reason);
        UNIT_ASSERT_VALUES_EQUAL_C(GetOutputCount(session), 0, "the output descriptor survived the give-up");

        serviceLock.unlock();
        UNIT_ASSERT_C(WaitFor([&]() { return FindNodeState(Service0, peerNodeId) == nullptr; }, TDuration::Seconds(5)),
            "the session was not freed");
        UNIT_ASSERT_C(WaitFor([&]() { return GetCounter(Service0, "OutputBuffer/Count") == 0; }, TDuration::Seconds(5)),
            TStringBuilder() << "OutputBuffer/Count=" << GetCounter(Service0, "OutputBuffer/Count"));

        // the consumer is stalled and never told, it goes with the runtime
        session.reset();
        Destroy();
        CheckQuota();
    }
};

// Data held at the receiver across a minor reconciliation arrives twice: the copies of the old GenMinor
// and the resent ones. The old ones are obsolete and dropped, the consumer sees each message once.
struct TStaleDataTest : public TOutboundTest {

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;

        Debug1->PauseChannelData();
        StartOutbound(1);
        UNIT_ASSERT_C(WaitFor([&]() { return Debug1->PendingDataCount.load() >= 21; }, TDuration::Seconds(10)),
            TStringBuilder() << "the messages did not reach the receiver, pending " << Debug1->PendingDataCount.load());
        auto genMinor = GetGenMinor(Debug0);

        Runtime->Send(Debug0->NodeActorId, Control0, new NActors::TEvInterconnect::TEvNodeDisconnected(Runtime->GetNodeId(1)), NodeIndex0, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMinor(Debug0) == genMinor + 1 && Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
            TStringBuilder() << "not reconciled, " << SessionDetails());
        // everything was resent, and the stale copies are still ahead of the resent ones
        UNIT_ASSERT_C(WaitFor([&]() { return Debug1->PendingDataCount.load() >= 42; }, TDuration::Seconds(10)),
            TStringBuilder() << "the resent messages did not reach the receiver, pending " << Debug1->PendingDataCount.load());

        Debug1->ResumeChannelData();
        WaitOutbound(1);
        // 20 messages, the finish and the confirmation of the finish
        UNIT_ASSERT_VALUES_EQUAL_C(GetConfirmedSeqNo(Debug1), 22, SessionDetails());
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service0, "Session/MessagesResent", true), 21, SessionDetails());
        CheckSensors();
        Destroy();
        CheckQuota();
    }
};

// The receiver in null mode confirms and reports every message as popped without any consumer: the
// flow control of the sender alone drives the producer to its finish
struct TNullModeTest : public TOutboundTest {

    void Run() override {
        Prepare();
        Init();

        Debug1->SetNullMode();
        ProducerSettings = TWorkerSettings{ .MessageCount = 1000, .MinMessageSize = 1000, .MaxMessageSize = 10000 };

        auto producer = Runtime->Register(new TProducerActor(Service0, 1, ProducerSettings, OutputQuotaManager), NodeIndex0);
        auto consumer = Runtime->AllocateEdgeActor(1);
        Actors.insert(producer);
        Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);

        auto finished = WaitFinished(Control0, NodeIndex0, "the producer");
        UNIT_ASSERT_C(!finished.Error, finished.Reason);
        // null mode keeps the input descriptors the leading messages created, they are not on a buffer
        UNIT_ASSERT_C(WaitFor([&]() { return GetCounter(Service0, "OutputBuffer/Count") == 0; }, TDuration::Seconds(5)),
            SessionDetails());
        Destroy();
        CheckQuota();
    }
};

Y_UNIT_TEST_SUITE(Channels20Failure) {

    void LossTest(int count, const TFailureSettings& failures, bool expectResend = true) {
        TLossTest test;

        test.Count = count;
        test.Local = false;
        test.ProducerSettings = TWorkerSettings{ .MessageCount = 100 };
        test.ConsumerSettings = TWorkerSettings{ .MessageCount = 100 };
        test.Failures = failures;
        test.ExpectResend = expectResend;

        test.Run();
    }

    Y_UNIT_TEST(DataLossUnderLoad2n) {
        LossTest(50, TFailureSettings{ .Data = 5, .DataCount = 3000 });
    }

    // an OK ack lost is covered by the next one, so a resend is not guaranteed
    Y_UNIT_TEST(AckLossUnderLoad2n) {
        LossTest(50, TFailureSettings{ .Ack = 5, .AckCount = 3000 }, false);
    }

    Y_UNIT_TEST(DataAndAckLoss2n) {
        LossTest(100, TFailureSettings{ .Data = 5, .DataCount = 3000, .Ack = 5, .AckCount = 3000 });
    }

    Y_UNIT_TEST(PausedAcksFillSessionWindow2n) {
        TSessionWindowTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(MaxInflightMessages2n) {
        TMaxInflightMessagesTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(WaitersDrainedAfterReconciliation2n) {
        TWaitersAfterReconciliationTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(LostUpdateRecoveredBySenderPing2n) {
        TLostUpdateTest test;
        test.Local = false;
        test.ReceiverPings = false;
        test.Run();
    }

    Y_UNIT_TEST(LostUpdateRecoveredByReceiverPing2n) {
        TLostUpdateTest test;
        test.Local = false;
        test.ReceiverPings = true;
        test.Run();
    }

    Y_UNIT_TEST(DroppedDiscoveryRetried2n) {
        TDroppedDiscoveryTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(PeerSessionGoneUnderWarmChannel2n) {
        TUndeliveredTest test;
        test.Local = false;
        test.Major = true;
        test.Run();
    }

    Y_UNIT_TEST(UndeliveredDataOtherReason2n) {
        TUndeliveredTest test;
        test.Local = false;
        test.Major = false;
        test.Run();
    }

    Y_UNIT_TEST(StaleBounceUnderFinishingChannel2n) {
        TFinishingStaleBounceTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(GenuineBounceStillReconciles2n) {
        TFinishingStaleBounceTest test;
        test.Local = false;
        test.Genuine = true;
        test.Run();
    }

    Y_UNIT_TEST(ReconciliationGiveUpFailsOutbound2n) {
        TGiveUpTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(StaleDataAfterReconciliationDropped2n) {
        TStaleDataTest test;
        test.Local = false;
        test.Run();
    }

    Y_UNIT_TEST(NullModeSenderOnly2n) {
        TNullModeTest test;
        test.Local = false;
        test.Run();
    }
}
