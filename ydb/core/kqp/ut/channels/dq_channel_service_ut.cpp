#include "dq_channel_test_harness.h"

// A 2nd attempt of a major reconciliation renumbered the queue from q+1 while the receiver was back at
// ConfirmedSeqNo 0, so it asked to resend a message the sender no longer had and the channel stalled.
struct TMajorReconRetryTest : public TSessionTest {

    void Run() override {
        ExpectReconciliation = true;
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 3, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        StartChannel(1, true);
        WaitChannel("warm up");

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);
        auto sender = FindNodeState(Service0, receiverNodeId);
        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(sender, "sender node session not found");
        UNIT_ASSERT_C(receiver, "receiver node session not found");
        WaitSettled(sender);
        auto genMajor = GetGenMajor(sender);

        receiver->Terminating.store(true);
        Service1->FreeNodeSession(senderNodeId, receiver->NodeActorId);
        receiver.reset();

        // the receiver's channel service does not answer the discovery until the lock is released
        std::unique_lock serviceLock(Service1->Mutex);

        auto channel = StartChannel(2, false);

        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMajor(sender) == genMajor + 1; }, TDuration::Seconds(5)),
            "the sender did not start a major reconciliation");
        auto queueSize = GetQueueSize(sender);
        auto frontSeqNo = GetFrontSeqNo(sender);
        UNIT_ASSERT_C(queueSize > 0, "nothing queued at the sender");
        UNIT_ASSERT_VALUES_EQUAL_C(frontSeqNo, 1, "queue is not renumbered from 1 by the major reconciliation");

        // the retry is what is under test, so it is waited for rather than assumed to have happened by
        // some time: a late timer would leave the sample looking at the 1st attempt on any version of the
        // code. DoReconciliation rebuilds the queue under the same lock it counts the attempt under, so a
        // count of 2 means the rebuild of the retry is complete
        UNIT_ASSERT_C(WaitFor([&]() { return GetReconciliationCount(sender) >= 2; }, TDuration::Seconds(5)),
            "the sender did not retry the discovery");
        auto retriedFrontSeqNo = GetFrontSeqNo(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(GetGenMajor(sender), genMajor + 1, "unexpected 2nd major reconciliation");
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(sender), queueSize, "queue size changed during reconciliation");

        serviceLock.unlock();
        StartConsumer(channel);

        auto details = TStringBuilder() << "queue front SeqNo after the reconciliation retry: " << retriedFrontSeqNo
            << " (expected 1), queue size: " << queueSize;
        WaitChannel(details);
        UNIT_ASSERT_VALUES_EQUAL_C(retriedFrontSeqNo, 1, "queue renumbered again by the reconciliation retry");
        CheckSensors();

        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// A gap RESEND asks to resend from ConfirmedSeqNo + 1, a discovery reply reports ConfirmedSeqNo itself,
// and HandleAck read both as the former: with the queue front at the confirmed message it restarted a
// reconciliation already running, which cleared nothing, so every retry got the same reply (TD-RT-RT-RTX)
// and the session died with its channels although the peer answered each time.
struct TDiscoveryResendTrapTest : public TSessionTest {

    void Run() override {
        ExpectReconciliation = true;
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 1, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        // must precede anything which would create a regular receiver session
        auto receiver = Service1->CreateDebugNodeState(senderNodeId);
        receiver->StartSession();

        StartChannel(1, true);
        WaitChannel("warm up");

        auto sender = FindNodeState(Service0, receiverNodeId);
        UNIT_ASSERT_C(sender, "sender node session not found");
        WaitSettled(sender);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(5)),
            "the warm up input descriptor is still there");
        auto genMinor = GetGenMinor(sender);

        receiver->PauseChannelData();
        auto channel = StartChannel(2, false);
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(sender) == 2; }, TDuration::Seconds(5)),
            "the sender did not send 2 messages");
        auto frontSeqNo = GetFrontSeqNo(sender);

        // both of them must have reached the receiver before the reconciliation below: the queue of the
        // sender only says they were sent, and the replay has nothing to replay until they are there
        UNIT_ASSERT_C(WaitFor([&]() { return receiver->PendingDataCount.load() >= 2; }, TDuration::Seconds(10)),
            "the messages did not reach the receiver");

        // the discovery of the minor reconciliation must reach the receiver after it has processed c
        std::unique_lock serviceLock(Service1->Mutex);

        Runtime->Send(sender->NodeActorId, Control0, new NActors::TEvInterconnect::TEvNodeDisconnected(receiverNodeId), NodeIndex0, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetGenMinor(sender) == genMinor + 1; }, TDuration::Seconds(5)),
            "the sender did not start a minor reconciliation");

        // c alone, its ack carrying the old GenMinor for the sender to ignore. The session stays paused:
        // unpausing it would deliver whatever else arrived meanwhile
        receiver->ProcessPending(1);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 1; }, TDuration::Seconds(5)),
            "the receiver did not process the 1st message");
        auto confirmedSeqNo = GetConfirmedSeqNo(receiver);
        UNIT_ASSERT_VALUES_EQUAL_C(confirmedSeqNo, frontSeqNo, "the receiver confirmed something else than the queue front");

        serviceLock.unlock();
        auto reconciled = WaitFor([&]() { return sender->Reconciliation.load() == 0 && GetFrontSeqNo(sender) == frontSeqNo + 1; },
            TDuration::Seconds(2));

        // the receiver drops the stale c+1 as obsolete (old GenMinor) and takes the resent one
        UNIT_ASSERT_C(WaitFor([&]() { return receiver->OutputNodeGenMinor.load() == genMinor + 1; }, TDuration::Seconds(5)),
            "the receiver did not get the discovery");
        receiver->ResumeChannelData();
        StartConsumer(channel);

        auto details = [&]() {
            return TStringBuilder() << "reconciled after the discovery reply: " << reconciled
                << ", queue front SeqNo: " << GetFrontSeqNo(sender) << " (confirmed by the receiver: " << confirmedSeqNo << ")"
                << ", reconciliation log: " << GetReconciliationLog(sender);
        };
        WaitChannel(details);
        UNIT_ASSERT_C(reconciled, details());
        CheckSensors();

        receiver.reset();
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// HandleAck popped the acknowledged prefix but adjusted InflightBytes only at its end, so every early
// return dropped the adjustment and the drift stood until the session stopped sending altogether. Staged
// on the gap RESEND path: the acks of the 1st two messages are lost and the 3rd message with them.
struct TInflightLeakTest : public TSessionTest {

    void Run() override {
        ExpectReconciliation = true;
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        // both must precede anything which would create a regular session for the same peer, and neither
        // may discover its peer before both are registered - a discovery makes the service of the peer
        // create a session of its own and CreateDebugNodeState refuses to replace one
        auto sender = Service0->CreateDebugNodeState(receiverNodeId);
        auto receiver = Service1->CreateDebugNodeState(senderNodeId);
        sender->StartSession();
        receiver->StartSession();

        ProducerSettings = TWorkerSettings{ .MessageCount = 1, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        StartChannel(1, true);
        WaitChannel("warm up");
        WaitSettled(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(sender->InflightBytes.load(), 0, "the warm up already leaked");

        ProducerSettings = TWorkerSettings{ .MessageCount = 4, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        // nothing is delivered until the whole batch is there, so the messages to lose can be named below
        // instead of being whichever happens to arrive next
        receiver->PauseChannelData();
        auto channel = StartChannel(2, false);

        // the batch is 4 messages plus the finish one; no ack can come back while the receiver is paused,
        // so the sender holds all 5 of them
        UNIT_ASSERT_C(WaitFor([&]() { return receiver->PendingDataCount.load() >= 5; }, TDuration::Seconds(10)),
            "the batch did not reach the receiver");
        auto seqNos = GetQueueSeqNos(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(seqNos.size(), 5, "the sender does not hold the whole batch");
        auto frontSeqNo = seqNos.front();
        auto queueBytes = GetQueueBytes(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(sender->InflightBytes.load(), queueBytes, "unexpected inflight bytes before the loss");

        // the 3rd message of the batch is lost and the acks of the 1st two with it, so the sender still
        // holds both when the receiver, which confirmed them, asks to resend from the 3rd - the ack which
        // pops a prefix and then leaves through StartReconciliation(false, 'R')
        receiver->DropDataSeqNo.store(seqNos[2]);
        sender->DropOkAckUpToSeqNo.store(seqNos[1]);

        receiver->ResumeChannelData();
        StartConsumer(channel);

        auto details = [&]() {
            return TStringBuilder() << "front SeqNo before the loss: " << frontSeqNo
                << ", queue bytes: " << queueBytes
                << ", inflight bytes now: " << sender->InflightBytes.load()
                << ", queue bytes now: " << GetQueueBytes(sender)
                << ", reconciliation log: " << GetReconciliationLog(sender);
        };

        WaitChannel(details);
        WaitSettled(sender);

        // the queue is empty now, so nothing at all is in flight
        UNIT_ASSERT_VALUES_EQUAL_C(sender->InflightBytes.load(), 0, details());
        CheckSensors();

        receiver.reset();
        sender.reset();
        Destroy();
        CheckQuota();
    }
};

// The give-up of a reconciliation fails the inbound channels too, so a session with nothing to deliver
// would destroy healthy ones as soon as the peer is slow to answer a handshake. A peer which is heard
// from - here it keeps streaming, replayed by the debug session while its channel service is locked -
// is alive, and the probe goes on instead of giving up. With something to deliver it gives up as
// before: a push during the probe waits in its descriptor, not in the queue of the session.
struct TSlowHandshakeTest : public TSessionTest {

    void Prepare() override {
        // the budget runs out after 2 unanswered discoveries (~3s) instead of the default 3 (~7s)
        Limits.ReconciliationCount = 2;
        ExpectReconciliation = true;
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        UseDebugSessions = true;
        Init();
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0 && Debug1->Reconciliation.load() == 0; }, TDuration::Seconds(5)), "no session");

        auto peerNodeId = Runtime->GetNodeId(1);

        // the peer streams to us, the data replayed by the test at its pace
        Debug0->PauseChannelData();
        ProducerSettings = TWorkerSettings{ .MessageCount = 100, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartInboundChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->PendingDataCount.load() >= 50; }, TDuration::Seconds(10)),
            "the data of the peer did not arrive");
        Debug0->ProcessPending(5);
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPopBytes(Debug0) > 0; }, TDuration::Seconds(10)),
            "the consumer did not bind and pop");
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(Debug0), 0, "the outbound half of the session is not empty");

        // the peer cannot answer a discovery while its channel service is locked, but the channels it has
        // already bound keep sending - it is alive and it never restarts
        std::unique_lock serviceLock(Service1->Mutex);
        Runtime->Send(Debug0->NodeActorId, Control0, new NActors::TEvInterconnect::TEvNodeDisconnected(peerNodeId), NodeIndex0, true);

        if (PushDuringProbe) {
            ProducerSettings = TWorkerSettings{ .MessageCount = 3, .MinMessageSize = 10, .MaxMessageSize = 100, .ExpectAbort = true };
            ConsumerSettings = ProducerSettings;
            StartChannel(2, true);
            UNIT_ASSERT_C(WaitFor([&]() { return GetWaitersQueueSize(Debug0) > 0; }, TDuration::Seconds(5)), "nothing waits");
        }

        // well past the budget, a message of the peer every 200ms all along
        auto deadline = TInstant::Now() + TDuration::Seconds(7);
        while (TInstant::Now() < deadline && !Debug0->Terminating.load()) {
            Debug0->ProcessPending(1);
            Sleep(TDuration::MilliSeconds(200));
        }
        auto details = TStringBuilder() << "reconciliation log: " << GetReconciliationLog(Debug0)
            << ", inbound traffic left: " << Debug0->PendingDataCount.load();

        if (PushDuringProbe) {
            UNIT_ASSERT_C(Debug0->Terminating.load(), TStringBuilder() << "the session did not give up with something to deliver, " << details);
            UNIT_ASSERT_C(GetReconciliationLog(Debug0).EndsWith("X"), details);
            // the give-up fails the descriptors of both halves
            auto producer = WaitFinished(Control0, NodeIndex0, "the producer");
            UNIT_ASSERT_C(producer.Aborted && producer.Reason.Contains("has not answered"), producer.Reason);
            serviceLock.unlock();
            Destroy();
            CheckQuota();
            return;
        }

        UNIT_ASSERT_C(!Debug0->Terminating.load(), TStringBuilder() << "the session gave up on a peer which is heard from, " << details);
        UNIT_ASSERT_C(GetReconciliationLog(Debug0).Contains("K"), details);
        UNIT_ASSERT_VALUES_EQUAL_C(GetInputCount(Debug0), 1, TStringBuilder() << "the inbound channel is gone, " << details);

        serviceLock.unlock();
        UNIT_ASSERT_C(WaitFor([&]() { return Debug0->Reconciliation.load() == 0; }, TDuration::Seconds(5)),
            TStringBuilder() << "not reconciled once the peer answers, " << details);
        Debug0->ResumeChannelData();
        WaitChannel(details);
        CheckSensors();
        Destroy();
        CheckQuota();
    }

    bool PushDuringProbe = false;
};

// Only a discovery, an ack and an update used to refresh LastPeerActivity, and a session whose channels
// all have this node as the receiver gets none of them, so it looked idle however much the peer streamed.
//
// The refresh is asserted rather than the absence of a ping: a ping is answered with an ack which
// refreshes the activity in turn, so a session which never pings looks like one just answered.
struct TPeerActivityTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        // long enough for no idle ping to interfere with the sampling below, short enough to keep it honest
        Limits.IdlePingPeriod = TDuration::MilliSeconds(1000);
        ExpectReconciliation = true;
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 2, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto senderNodeId = Runtime->GetNodeId(0);

        StartChannel(1, true);
        WaitChannel("warm up");

        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(receiver, "the receiving node session not found");

        // The pushed bytes waited for below are summed over every input descriptor of the session, and the
        // warm up channel leaves one carrying some until its consumer lets go of the buffer, which happens
        // after its TEvFinished has been grabbed. Waiting for it to go makes that wait mean the 2nd channel.
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(10)),
            "the input descriptor of the warm up channel is still there");

        // nothing refreshes the activity of that session while it is idle: it is well under the idle ping
        // period, so no discovery of its own goes out and no ack comes back
        Sleep(TDuration::MilliSeconds(200));
        auto before = receiver->LastPeerActivity.load();

        // the consumer stalls after the 1st message, so the input descriptor is still there to be sampled
        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 1500 };
        StartChannel(2, true);

        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPushBytes(receiver) > 0; }, TDuration::Seconds(10)),
            "no data of the peer arrived at the receiving session");
        auto after = receiver->LastPeerActivity.load();

        auto details = TStringBuilder() << "LastPeerActivity " << before << " -> " << after
            << ", pushed " << GetInputPushBytes(receiver) << " bytes"
            << ", reconciliation log: " << GetReconciliationLog(receiver);

        UNIT_ASSERT_C(after > before,
            TStringBuilder() << "the data of the peer did not refresh the activity of the session, " << details);

        WaitChannel(details);
        CheckSensors();

        receiver.reset();
        Destroy();
        CheckQuota();
    }
};

// TerminateInputDescriptor decremented InputBuffer/Count whatever its erase did, so an aborted channel,
// erased and accounted for where it was aborted, came off the shared gauge twice and drove it below zero.
// Staged through a peer session which is replaced while this node keeps its session and its consumer.
struct TBufferCountTest : public TSessionTest {

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        // the consumer stalls after the 1st message: a finished channel would be let go of, and a finished
        // descriptor is not failed at all
        ProducerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 20, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 30000 };
        StartChannel(1, true);

        std::shared_ptr<TNodeState> receiver;
        UNIT_ASSERT_C(WaitFor([&]() { return (receiver = FindNodeState(Service1, senderNodeId)) != nullptr; },
            TDuration::Seconds(10)), "the receiving node session not found");
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPopBytes(receiver) > 0; }, TDuration::Seconds(10)),
            "the consumer did not bind and pop");
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service1, "InputBuffer/Count"), 1,
            "the inbound channel is not on the sensor");

        std::shared_ptr<TNodeState> sender;
        UNIT_ASSERT_C(WaitFor([&]() { return (sender = FindNodeState(Service0, peerNodeId)) != nullptr; },
            TDuration::Seconds(10)), "the sending node session not found");
        sender->Terminating.store(true);
        Service0->FreeNodeSession(peerNodeId, sender->NodeActorId);
        sender.reset();
        {
            std::lock_guard lock(Service0->Mutex);
            Service0->GetOrCreateNodeState(peerNodeId);
        }

        // The discovery aborts the descriptor and takes it off the sensor, then the consumer is aborted in
        // turn and lets go of its buffer, terminating a descriptor which is gone. The 2 are not sampled
        // apart: the 2nd follows the 1st closely enough to race any poll.
        try {
            auto msg = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control1, TDuration::Seconds(10));
            Actors.erase(msg->Sender);
            UNIT_ASSERT_C(msg->Get()->Error, "the consumer of an aborted channel finished without an error");
        } catch (NActors::TEmptyEventQueueException&) {
            UNIT_ASSERT_C(false, "the consumer of the aborted channel did not finish");
        }

        auto negative = WaitFor([&]() { return GetCounter(Service1, "InputBuffer/Count") < 0; }, TDuration::Seconds(3));
        auto details = TStringBuilder() << "InputBuffer/Count=" << GetCounter(Service1, "InputBuffer/Count")
            << ", OutputBuffer/Count=" << GetCounter(Service0, "OutputBuffer/Count");
        UNIT_ASSERT_C(!negative, TStringBuilder() << "the descriptor of the aborted channel was counted off twice, " << details);
        UNIT_ASSERT_VALUES_EQUAL_C(GetCounter(Service1, "InputBuffer/Count"), 0, details);

        receiver.reset();
        Destroy();
    }
};

// The watchdog must not fire on a queue which is moving. On a slow link every message is older than the
// idle period by the time it is confirmed, so the age of the front says nothing; only the absence of
// progress does. Staged with a peer which confirms 1 message every 50ms against a 1s idle period - the
// slack a loaded machine needs to never leave a gap of a whole period between 2 confirmations.
struct TSlowQueueTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(1000);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        auto session = Service0->CreateDebugNodeState(peerNodeId);
        auto peer = Service1->CreateDebugNodeState(senderNodeId);
        session->StartSession();
        peer->StartSession();

        // the peer delivers, and so confirms, only what the test replays
        peer->PauseChannelData();

        ProducerSettings = TWorkerSettings{ .MessageCount = 150, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return peer->PendingDataCount.load() >= 100; }, TDuration::Seconds(10)),
            "the messages did not reach the peer");

        // the 1st confirmation proves the replay confirms at the current generation before anything is
        // sampled: a reconciliation which slipped in during the setup would make the sender resend at a
        // new GenMinor and the copies replayed here obsolete
        auto frontAtStart = GetFrontSeqNo(session);
        peer->ProcessPending(1);
        UNIT_ASSERT_C(WaitFor([&]() { return GetFrontSeqNo(session) > frontAtStart; }, TDuration::Seconds(5)),
            "the 1st replayed message was not confirmed");
        auto frontBefore = GetFrontSeqNo(session);
        auto pingsBefore = CountPings(session);
        auto genMinor = GetGenMinor(session);

        // 4 idle periods, the queue moving all along and its front older than the period after the 1st 20
        // confirmations
        auto deadline = TInstant::Now() + TDuration::Seconds(4);
        while (TInstant::Now() < deadline) {
            peer->ProcessPending(1);
            Sleep(TDuration::MilliSeconds(50));
        }

        auto details = TStringBuilder() << "the queue front moved from SeqNo " << frontBefore << " to "
            << GetFrontSeqNo(session) << ", " << GetQueueSize(session) << " message(s) still queued"
            << ", reconciliation log: " << GetReconciliationLog(session);
        UNIT_ASSERT_C(GetQueueSize(session) > 0, TStringBuilder() << "the queue ran dry, " << details);
        UNIT_ASSERT_C(GetGenMinor(session) == genMinor, TStringBuilder() << "the session reconciled, " << details);
        UNIT_ASSERT_C(GetFrontSeqNo(session) > frontBefore + 10, TStringBuilder() << "the queue did not move, " << details);
        UNIT_ASSERT_C(CountPings(session) == pingsBefore, TStringBuilder() << "a moving queue was pinged, " << details);

        peer->ResumeChannelData();
        WaitChannel(details);
        CheckSensors();

        session.reset();
        peer.reset();
        Destroy();
        CheckQuota();
    }
};

// A push into an empty queue starts the clock of the watchdog. Without that a session idle for longer than
// the period is pinged at the next cleanup tick for a message it has only just sent, and every restart
// from idle begins with a reconciliation and a resend.
struct TIdleRestartTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(1000);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        auto session = Service0->CreateDebugNodeState(peerNodeId);
        auto peer = Service1->CreateDebugNodeState(senderNodeId);
        session->StartSession();
        peer->StartSession();

        // the traffic of the peer, replayed by the test, keeps the liveness probe quiet throughout; the
        // queue of the session stays empty until the push under test
        session->PauseChannelData();
        ProducerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartInboundChannel(2, true);
        UNIT_ASSERT_C(WaitFor([&]() { return session->PendingDataCount.load() >= 100; }, TDuration::Seconds(10)),
            "the traffic of the peer did not arrive");

        // idle for longer than the period, with the peer heard from every 200ms
        for (int i = 0; i < 8; ++i) {
            session->ProcessPending(1);
            Sleep(TDuration::MilliSeconds(200));
        }
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(session), 0, "the session has something queued before the push");
        auto pingsBefore = CountPings(session);

        // nothing the session sends is confirmed, so the message pushed now stays queued
        peer->PauseChannelData();
        ProducerSettings = TWorkerSettings{ .MessageCount = 1, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(session) > 0; }, TDuration::Seconds(5)),
            "nothing was queued by the push");
        auto pushedAt = TInstant::Now();

        // No ping inside the period which starts with the push, then one once it is over - the queue is
        // stuck for real - and the next one a full period after that, not at the next tick: the resend of
        // a reconciliation restarts the clock as well. The peer keeps being heard from throughout, so none
        // of it can be the liveness probe.
        TInstant firstPing;
        TInstant secondPing;
        auto deadline = pushedAt + TDuration::Seconds(6);
        while (TInstant::Now() < deadline && !secondPing) {
            session->ProcessPending(1);
            auto pings = CountPings(session);
            if (!firstPing && pings > pingsBefore) {
                firstPing = TInstant::Now();
            } else if (firstPing && pings > pingsBefore + 1) {
                secondPing = TInstant::Now();
            }
            Sleep(TDuration::MilliSeconds(50));
        }

        auto details = TStringBuilder() << "pinged " << (firstPing ? firstPing - pushedAt : TDuration::Zero())
            << " after the push and again " << (secondPing ? secondPing - firstPing : TDuration::Zero()) << " later"
            << ", inbound traffic left: " << session->PendingDataCount.load()
            << ", reconciliation log: " << GetReconciliationLog(session);
        UNIT_ASSERT_C(firstPing, TStringBuilder() << "the stuck queue was never pinged, " << details);
        UNIT_ASSERT_C(firstPing - pushedAt >= TDuration::MilliSeconds(700),
            TStringBuilder() << "pinged right after a push into an idle queue, " << details);
        UNIT_ASSERT_C(secondPing, TStringBuilder() << "the stuck queue was not pinged again, " << details);
        UNIT_ASSERT_C(secondPing - firstPing >= TDuration::MilliSeconds(700),
            TStringBuilder() << "pinged again right after the resend, " << details);

        peer->ResumeChannelData();
        session->ResumeChannelData();
        session.reset();
        peer.reset();
        Destroy();
    }
};

// The watchdog of the outbound half: every other trigger needs an ack, a bounce or a dropped link, and one
// TNodeState covers both directions, so the traffic of the peer cannot answer for the half it is not
// sending on. Staged with an outbound channel whose messages never reach the peer while that peer streams
// on an inbound one, replayed here inside the idle period so that the liveness probe cannot be what fires.
struct TOutboundStallTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(200);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto senderNodeId = Runtime->GetNodeId(0);
        auto peerNodeId = Runtime->GetNodeId(1);

        // sending on one channel and receiving on another, as a session between nodes which run stages of
        // the same query does; both sides are registered before either discovers the other
        auto session = Service0->CreateDebugNodeState(peerNodeId);
        auto peer = Service1->CreateDebugNodeState(senderNodeId);
        session->StartSession();
        peer->StartSession();

        session->PauseChannelData();
        // Nothing the session sends reaches the peer, so nothing can confirm it and the queue cannot drain
        // until this test lets it. Losing the acks instead would leave the reply to a discovery able to
        // recover the queue, as it carries the SeqNo the peer confirmed, and a machine which delayed this
        // test past an idle period would find the stall already gone.
        peer->PauseChannelData();

        ProducerSettings = TWorkerSettings{ .MessageCount = 200, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartInboundChannel(2, true);
        UNIT_ASSERT_C(WaitFor([&]() { return session->PendingDataCount.load() >= 60; }, TDuration::Seconds(10)),
            "the peer did not fill the inbound channel");

        auto pingsBefore = CountPings(session);

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = ProducerSettings;
        StartChannel(1, true);
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(session) > 0; }, TDuration::Seconds(10)),
            "the session never had anything queued");

        // 1 message of the peer every 50ms against the 200ms idle period: the session never looks idle, so
        // the liveness probe cannot be what fires below
        bool pinged = false;
        auto deadline = TInstant::Now() + TDuration::Seconds(5);
        while (TInstant::Now() < deadline) {
            session->ProcessPending(1);
            if (CountPings(session) > pingsBefore) {
                pinged = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(50));
        }

        auto queueWhenPinged = GetQueueSize(session);
        auto details = [&]() {
            return TStringBuilder() << "the queue holds " << GetQueueSize(session) << " message(s), "
                << queueWhenPinged << " of them when it was pinged"
                << ", inbound traffic left: " << session->PendingDataCount.load()
                << ", reconciliation log: " << GetReconciliationLog(session);
        };

        UNIT_ASSERT_C(pinged, TStringBuilder() << "the stalled outbound queue was never pinged, " << details());
        UNIT_ASSERT_C(queueWhenPinged > 0, TStringBuilder() << "the queue was not stalled, " << details());

        peer->ResumeChannelData();
        UNIT_ASSERT_C(WaitFor([&]() { return GetQueueSize(session) == 0; }, TDuration::Seconds(10)),
            TStringBuilder() << "the ping did not recover the queue, " << details());

        session->ResumeChannelData();
        session.reset();
        peer.reset();
        Destroy();
    }
};

// The other reason HandleCleanup pings: a session with channels but nothing queued has no stall to watch,
// and a peer which freed its session with the link up sends no disconnect, so its inbound channels would
// hang. The discovery makes that peer announce itself for ConnectSession to fail them instead.
struct TLivenessProbeTest : public TSessionTest {

    void Prepare() override {
        Limits.CleanupPeriod = TDuration::MilliSeconds(50);
        Limits.IdlePingPeriod = TDuration::MilliSeconds(200);
        TSessionTest::Prepare();
    }

    void Run() override {
        Prepare();
        Init();

        auto peerNodeId = Runtime->GetNodeId(1);

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100 };
        ConsumerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 100,
            .PauseMessageIndex = 1, .PauseDelayMs = 30000 };
        StartInboundChannel(1, true);

        std::shared_ptr<TNodeState> session;
        UNIT_ASSERT_C(WaitFor([&]() { return (session = FindNodeState(Service0, peerNodeId)) != nullptr; },
            TDuration::Seconds(10)), "the node session not found");
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputPopBytes(session) > 0; }, TDuration::Seconds(10)),
            "the consumer did not bind and pop");

        // acks and updates do not go through the queue, so the watchdog of it has nothing to watch here
        UNIT_ASSERT_VALUES_EQUAL_C(GetQueueSize(session), 0, "the session has something queued");

        // The ping of either session counts: both go idle within microseconds of each other, both run the
        // same timers, and whichever asks first refreshes the other and can keep it from asking at all. The
        // session of the peer is in the same state, channels and an empty queue, so its ping is the same
        // branch. This side goes stale first, by the ack round trip, but a cleanup tick is far longer.
        auto peer = FindNodeState(Service1, Runtime->GetNodeId(0));
        auto pingsBefore = CountPings(session) + (peer ? CountPings(peer) : 0);
        auto pinged = WaitFor([&]() {
            return CountPings(session) + (peer ? CountPings(peer) : 0) > pingsBefore;
        }, TDuration::Seconds(5));

        auto details = TStringBuilder() << "the session holds " << GetInputCount(session)
            << " inbound channel(s) and an empty queue, its log: " << GetReconciliationLog(session)
            << ", the log of the peer: " << (peer ? GetReconciliationLog(peer) : "no session");
        UNIT_ASSERT_C(pinged, TStringBuilder() << "the peer went quiet and the session never asked, " << details);
        UNIT_ASSERT_C(GetInputCount(session) > 0, TStringBuilder() << "the channel is gone, " << details);

        session.reset();
        peer.reset();
        Destroy();
    }
};

// A bounce naming an actor the session does not address is the echo of a copy sent to an actor already
// superseded, see HandleUndelivered: taking it for the death of the live peer used to start a major
// reconciliation, and the peer then failed every unfinished channel bound to the generation left behind.
// Both halves are pinned: the stale bounce changes nothing, the genuine one still reconciles.
struct TStaleBounceTest : public TSessionTest {

    void SendBounce(const std::shared_ptr<TNodeState>& session, NActors::TActorId bouncedFrom) {
        Runtime->Send(session->NodeActorId, bouncedFrom,
            new NActors::TEvents::TEvUndelivered(TEvDqCompute::TEvChannelDataV2::EventType,
                NActors::TEvents::TEvUndelivered::ReasonActorUnknown),
            NodeIndex0, true);
    }

    void Run() override {
        Prepare();
        Init();

        ProducerSettings = TWorkerSettings{ .MessageCount = 5, .MinMessageSize = 10, .MaxMessageSize = 20 };
        ConsumerSettings = ProducerSettings;

        auto senderNodeId = Runtime->GetNodeId(0);
        auto receiverNodeId = Runtime->GetNodeId(1);

        StartChannel(1, true);
        WaitChannel("warm up");

        auto sender = FindNodeState(Service0, receiverNodeId);
        auto receiver = FindNodeState(Service1, senderNodeId);
        UNIT_ASSERT_C(sender, "sender node session not found");
        UNIT_ASSERT_C(receiver, "receiver node session not found");
        WaitSettled(sender);

        // the consumer lets go of the descriptor only after its TEvFinished has been grabbed, so the
        // bounce below starts from a receiver with nothing left of the warm up
        UNIT_ASSERT_C(WaitFor([&]() { return GetInputCount(receiver) == 0; }, TDuration::Seconds(10)),
            "the input descriptor of the warm up channel is still there");

        auto peerActorId = GetInputNodeActorId(sender);
        UNIT_ASSERT_C(peerActorId == receiver->NodeActorId, TStringBuilder() << "InputNodeActorId " << peerActorId
            << ", the session actor of the peer " << receiver->NodeActorId);

        auto genMajor = GetGenMajor(sender);
        auto details = [&]() {
            return TStringBuilder() << "GenMajor " << genMajor << " -> " << GetGenMajor(sender)
                << ", Reconciliation=" << sender->Reconciliation.load()
                << ", InputNodeActorId " << GetInputNodeActorId(sender) << " (the peer session actor " << peerActorId << ")"
                << ", reconciliation log: " << GetReconciliationLog(sender);
        };

        // the session actor of the peer is alive all along; only the actor the bounce names differs. The
        // stale one lives on the peer node, as the superseded session actor did, so a check on the node
        // alone would not tell the two apart
        SendBounce(sender, Genuine ? peerActorId : Control1);

        if (Genuine) {
            UNIT_ASSERT_C(WaitFor([&]() { return GetGenMajor(sender) == genMajor + 1 && sender->Reconciliation.load() == 0; },
                TDuration::Seconds(5)), TStringBuilder() << "the genuine bounce did not reconcile the session, " << details());
            UNIT_ASSERT_C(GetReconciliationLog(sender).Contains("U"),
                TStringBuilder() << "the major was started by something else than the bounce, " << details());
        } else {
            // nothing is expected to happen, so the wait has to time out to mean anything
            UNIT_ASSERT_C(!WaitFor([&]() { return GetGenMajor(sender) != genMajor || GetReconciliationLog(sender).Contains("U"); },
                TDuration::Seconds(2)), TStringBuilder() << "the stale bounce started a major reconciliation, " << details());
            UNIT_ASSERT_VALUES_EQUAL_C(sender->Reconciliation.load(), 0, details());
            UNIT_ASSERT_C(GetInputNodeActorId(sender) == peerActorId,
                TStringBuilder() << "the stale bounce forgot the peer, " << details());
        }

        // a channel started after the bounce still goes through, and at the generation it was left at:
        // an untouched session in the one case, a recovered one in the other
        StartChannel(2, true);
        WaitChannel(details);
        UNIT_ASSERT_VALUES_EQUAL_C(GetGenMajor(sender), genMajor + (Genuine ? 1 : 0), details());

        // a node session logs through the actor system from its destructor, so it may not outlive it
        sender.reset();
        receiver.reset();
        Destroy();
        CheckQuota();
    }

    // the bounce names the session actor of the peer, as one from a peer which really died
    bool Genuine = false;
};

Y_UNIT_TEST_SUITE(Channels20) {

    void LoadTest(int count, bool local, const TWorkerSettings& producerSettings, const TWorkerSettings& consumerSettings, const TFailureSettings& failureSettings = TFailureSettings{}) {
        TLoadTest test;

        test.Count = count;
        test.Local = local;
        test.ProducerSettings = producerSettings;
        test.ConsumerSettings = consumerSettings;
        test.Failures = failureSettings;
        test.ExpectReconciliation = failureSettings.Any();

        test.Run();
    }

    void LoadTest(int count, bool local, const TWorkerSettings& settings = TWorkerSettings{}, const TFailureSettings& failureSettings = TFailureSettings{}) {
        LoadTest(count, local, settings, settings, failureSettings);
    }

    Y_UNIT_TEST(EmptyFinish2n) {
        LoadTest(100, false);
    }

    Y_UNIT_TEST(SimpleFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 });
    }

    Y_UNIT_TEST(EarlyFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 50, .EarlyFinish = true });
    }

    Y_UNIT_TEST(InstantFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 10 }, TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true });
    }

    Y_UNIT_TEST(ConsumerPauseThenResume2n) {
        LoadTest(50, false,
            TWorkerSettings{ .MessageCount = 100 },
            TWorkerSettings{ .MessageCount = 100, .PauseMessageIndex = 20, .PauseDelayMs = 200 });
    }

    Y_UNIT_TEST(EmptyFinish1n) {
        LoadTest(100, true);
    }

    Y_UNIT_TEST(SimpleFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 100 });
    }

    Y_UNIT_TEST(EarlyFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 50, .EarlyFinish = true });
    }

    Y_UNIT_TEST(InstantFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 10 }, TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true });
    }

    // every 20th of the 1st 1000 data messages is lost on arrival, the gap RESEND recovers each
    Y_UNIT_TEST(MissedData) {
        LoadTest(50, false, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 100 }, TFailureSettings{ .Data = 5, .DataCount = 1000 });
    }

    void MemoryPressureTest(int count, bool local) {
        TMemoryPressureTest test;

        test.Count = count;
        test.Local = local;
        test.ProducerSettings = TWorkerSettings{ .MessageCount = 100 };
        test.ConsumerSettings = TWorkerSettings{ .MessageCount = 100 };

        test.Run();
    }

    Y_UNIT_TEST(MemoryPressure2n) {
        MemoryPressureTest(50, false);
    }

    Y_UNIT_TEST(MemoryPressure1n) {
        MemoryPressureTest(50, true);
    }

    void ThrottleTest(bool enabled) {
        TThrottleTest test;

        test.Enabled = enabled;
        test.Count = 1;
        test.Local = false;
        // 200 * ~64KB is way above the cold inflight window and still below RemoteChannelInflightBytes,
        // so an unthrottled producer would push all of it while the consumer sleeps
        test.ProducerSettings = TWorkerSettings{
            .MessageCount = 200, .MinMessageSize = 60000, .MaxMessageSize = 70000 };
        test.ConsumerSettings = TWorkerSettings{
            .MessageCount = 200, .MinMessageSize = 60000, .MaxMessageSize = 70000,
            .PauseMessageIndex = 5, .PauseDelayMs = 6000 };

        test.Run();
    }

    Y_UNIT_TEST(MemoryPressureThrottles2n) {
        ThrottleTest(true);
    }

    // the very same scenario must not throttle anything while the feature flag is off
    Y_UNIT_TEST(MemoryPressureDisabled2n) {
        ThrottleTest(false);
    }

    Y_UNIT_TEST(Reconciliation) {
        TReconTest test;

        test.Count = 1;
        test.Local = false;
        test.ProducerSettings.MessageCount = 100;
        test.ConsumerSettings.MessageCount = 100;

        test.Run();
    }

    Y_UNIT_TEST(MajorReconciliationRetry) {
        TMajorReconRetryTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(DiscoveryResendTrap) {
        TDiscoveryResendTrapTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(InflightLeakOnResend) {
        TInflightLeakTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(PeerActivityRefreshedByData) {
        TPeerActivityTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(BufferCountOfAnAbortedChannel) {
        TBufferCountTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(OutboundStallPingedWhilePeerStreams) {
        TOutboundStallTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(SlowQueueNotPinged) {
        TSlowQueueTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(IdleRestartNotPingedEarly) {
        TIdleRestartTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(QuietPeerProbed) {
        TLivenessProbeTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(StaleBounceIgnored) {
        TStaleBounceTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(GenuineBounceReconciles) {
        TStaleBounceTest test;

        test.Local = false;
        test.Genuine = true;

        test.Run();
    }

    Y_UNIT_TEST(InboundChannelSurvivesSlowHandshake) {
        TSlowHandshakeTest test;

        test.Local = false;

        test.Run();
    }

    Y_UNIT_TEST(PushDuringSlowHandshakeGivesUp) {
        TSlowHandshakeTest test;

        test.Local = false;
        test.PushDuringProbe = true;

        test.Run();
    }
}
