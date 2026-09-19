#include "retry_events_queue_ut_event.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/yql/dq/actors/common/retry_queue.h>

using namespace NActors;
using namespace NYql::NDq;

namespace {

const ui64 EventQueueId = 777;

struct TEvPrivate {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvSend = EvBegin + 10,
        EvData,
        EvDisconnect,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvSend : public TEventLocal<TEvSend, EvSend> {};
    struct TEvData : public TEventLocal<TEvData, EvData> {};
    struct TEvDisconnect : public TEventLocal<TEvDisconnect, EvDisconnect> {};
};

class ClientActor : public TActorBootstrapped<ClientActor> {
public:
    ClientActor(
        NActors::TActorId clientEdgeActorId,
        NActors::TActorId serverActorId)
     : ServerActorId(serverActorId)
     , ClientEdgeActorId(clientEdgeActorId) {}

    void Bootstrap() {
        Become(&ClientActor::StateFunc);
        Init();
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ) {
        EventsQueue.Retry();
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat::TPtr& ) {
        if (EventsQueue.Heartbeat()) {
            EventsQueue.Send(new TEvDqCompute::TEvInjectCheckpoint());
        }
    }

    void Handle(const TEvPrivate::TEvSend::TPtr& ) {
        EventsQueue.Send(new TEvDqCompute::TEvInjectCheckpoint());
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        if (EventsQueue.HandleUndelivered(ev) == NYql::NDq::TRetryEventsQueue::ESessionState::SessionClosed) {
            Send(ClientEdgeActorId, new TEvPrivate::TEvDisconnect());
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvEvHeartbeat, Handle);
        hFunc(TEvPrivate::TEvSend, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    )

    void Init() {
        EventsQueue.Init("TxId", SelfId(), SelfId(), EventQueueId, /* keepAlive */ true);
        EventsQueue.OnNewRecipientId(ServerActorId, /* unsubscribe */ false, /* connected */ true);
    }

    NYql::NDq::TRetryEventsQueue EventsQueue;
    NActors::TActorId ServerActorId;
    NActors::TActorId ClientEdgeActorId;
};

class ServerActor : public TActorBootstrapped<ServerActor> {
public:
    ServerActor(NActors::TActorId serverEdgeActorId)
        : ServerEdgeActorId(serverEdgeActorId) {}

    void Bootstrap() {
        Become(&ServerActor::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(TEvDqCompute::TEvInjectCheckpoint, Handle);
        hFunc(TEvents::TEvPoisonPill, Handle);
    )

    void Handle(const TEvents::TEvPoisonPill::TPtr& ) {
        PassAway();
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ) {
        EventsQueue.Retry();
    }

    void Handle(const TEvDqCompute::TEvInjectCheckpoint::TPtr& /*ev*/) {
        Send(ServerEdgeActorId, new TEvDqCompute::TEvInjectCheckpoint());
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        EventsQueue.HandleUndelivered(ev);
    }

    NYql::NDq::TRetryEventsQueue EventsQueue;
    NActors::TActorId ServerEdgeActorId;
};

struct TRuntime {
    TTestActorRuntimeBase Runtime;
    ClientActor* Client;
    ServerActor* Server;
    NActors::TActorId ClientActorId;
    NActors::TActorId ServerActorId;
    NActors::TActorId ClientEdgeActorId;
    NActors::TActorId ServerEdgeActorId;

    TRuntime()
        : Runtime(1, true)
    {
        Runtime.Initialize();

        ClientEdgeActorId = Runtime.AllocateEdgeActor(0);
        ServerEdgeActorId = Runtime.AllocateEdgeActor(0);

        Server = new ServerActor(ServerEdgeActorId);
        ServerActorId = Runtime.Register(Server, 0);
        Runtime.EnableScheduleForActor(ServerActorId, true);

        Client = new ClientActor(ClientEdgeActorId, ServerActorId);
        ClientActorId = Runtime.Register(Client, 0);
        Runtime.EnableScheduleForActor(ClientActorId, true);
    }
};

struct TRemoteQueueFixture : public NUnitTest::TBaseFixture {
    TTestActorRuntimeBase Runtime{2};
    TActorId SenderId;
    TActorId RecipientId;
    TRetryEventsQueue Queue;
    std::deque<THolder<IEventHandle>> SentEvents;
    std::deque<std::pair<THolder<IEventHandle>, TDuration>> ScheduledEvents;

    TRemoteQueueFixture() {
        Runtime.Initialize();
        SenderId = Runtime.AllocateEdgeActor(0);
        RecipientId = Runtime.AllocateEdgeActor(1);
        // Observe the queue's public transport output without running the interconnect protocol.
        Runtime.SetEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            if (event->Sender == SenderId) {
                SentEvents.emplace_back(event.Release());
                return true;
            }
            return false;
        });
        Runtime.SetScheduledEventFilter([&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event, TDuration delay, TInstant&) {
            if (event->Sender == SenderId) {
                ScheduledEvents.emplace_back(THolder<IEventHandle>(event.Release()), delay);
            }
            return true;
        });
        // Drive connection notifications explicitly to make replay deterministic.
        Queue.Init("TxId", SenderId, SenderId, EventQueueId, /* keepAlive */ false, /* useConnect */ false);
        Queue.OnNewRecipientId(RecipientId, /* unsubscribe */ false, /* connected */ true);
    }

    bool Receive(ui64 seqNo, ui64 confirmedSeqNo = 0) {
        TEvDqCompute::TEvInjectCheckpoint event;
        auto* meta = event.Record.MutableTransportMeta();
        meta->SetSeqNo(seqNo);
        meta->SetConfirmedSeqNo(confirmedSeqNo);
        return Queue.OnEventReceived(&event);
    }

    void Act(const std::function<void()>& action) {
        Runtime.RunCall([&] {
            action();
            return true;
        });
    }

    void Send(ui64 cookie = 0) {
        Act([&] {
            Queue.Send(new TEvDqCompute::TEvInjectCheckpoint(), cookie);
        });
    }

    auto ExpectEvent(ui64 seqNo, ui64 confirmedSeqNo = 0) {
        UNIT_ASSERT(!SentEvents.empty());
        auto event = std::move(SentEvents.front());
        SentEvents.pop_front();
        UNIT_ASSERT_VALUES_EQUAL(event->Type, TEvDqCompute::TEvInjectCheckpoint::EventType);
        UNIT_ASSERT_VALUES_EQUAL(event->Sender, SenderId);
        UNIT_ASSERT_VALUES_EQUAL(event->Recipient, RecipientId);
        UNIT_ASSERT(event->Flags & IEventHandle::FlagTrackDelivery);
        const auto& meta = event->Get<TEvDqCompute::TEvInjectCheckpoint>()->Record.GetTransportMeta();
        UNIT_ASSERT_VALUES_EQUAL(meta.GetSeqNo(), seqNo);
        UNIT_ASSERT_VALUES_EQUAL(meta.GetConfirmedSeqNo(), confirmedSeqNo);
        return event;
    }

    void Reconnect() {
        Act([&] {
            Queue.HandleNodeDisconnected(RecipientId.NodeId());
            Queue.HandleNodeConnected(RecipientId.NodeId());
        });
    }

    TRetryEventsQueue::ESessionState Undelivered(const TActorId& sender, ui32 reason) {
        TEvents::TEvUndelivered::TPtr event = reinterpret_cast<TEvents::TEvUndelivered::THandle*>(new IEventHandle(
            SenderId, sender, new TEvents::TEvUndelivered(TEvDqCompute::TEvInjectCheckpoint::EventType, reason)));
        return Runtime.RunCall([&] {
            return Queue.HandleUndelivered(event);
        });
    }
};

struct TUnorderedRemoteQueueFixture : public TRemoteQueueFixture {
    TUnorderedRemoteQueueFixture() {
        Queue.Init("TxId", SenderId, SenderId, EventQueueId, /* keepAlive */ false, /* useConnect */ false, /* ordered */ false);
    }
};

Y_UNIT_TEST_SUITE(TRetryEventsQueueTest) {
    Y_UNIT_TEST_F(SendAndReplayPreserveRecordPayloadsAndCookie, TRemoteQueueFixture) {
        Act([&] {
            auto event = MakeHolder<TEvDqCompute::TEvInjectCheckpoint>();
            event->Record.MutableCheckpoint()->SetId(42);
            event->Record.SetGeneration(7);
            event->AddPayload(TRope("first payload"));
            event->AddPayload(TRope("second payload"));
            Queue.Send(std::move(event), 123);
        });
        auto checkMessage = [](const auto& event) {
            UNIT_ASSERT_VALUES_EQUAL(event->Cookie, 123);
            const auto* message = event->template Get<TEvDqCompute::TEvInjectCheckpoint>();
            UNIT_ASSERT_VALUES_EQUAL(message->Record.GetCheckpoint().GetId(), 42);
            UNIT_ASSERT_VALUES_EQUAL(message->Record.GetGeneration(), 7);
            UNIT_ASSERT_VALUES_EQUAL(message->GetPayloadCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(message->GetPayload(0).ConvertToString(), "first payload");
            UNIT_ASSERT_VALUES_EQUAL(message->GetPayload(1).ConvertToString(), "second payload");
        };
        checkMessage(ExpectEvent(1));
        UNIT_ASSERT(Receive(1));
        Reconnect();
        checkMessage(ExpectEvent(1, 1));
        UNIT_ASSERT(SentEvents.empty());
    }

    Y_UNIT_TEST_F(DisconnectedQueueWaitsForMatchingConnection, TRemoteQueueFixture) {
        Queue.HandleNodeDisconnected(SenderId.NodeId());
        Send();
        ExpectEvent(1);

        Queue.HandleNodeDisconnected(RecipientId.NodeId());
        Send();
        Queue.HandleNodeConnected(SenderId.NodeId());
        Send();
        UNIT_ASSERT(SentEvents.empty());
        UNIT_ASSERT(ScheduledEvents.empty());

        Act([&] { Queue.HandleNodeConnected(RecipientId.NodeId()); });
        ExpectEvent(1);
        ExpectEvent(2);
        ExpectEvent(3);
        Act([&] { Queue.HandleNodeConnected(RecipientId.NodeId()); });
        UNIT_ASSERT(SentEvents.empty());
        Send();
        ExpectEvent(4);
    }

    Y_UNIT_TEST_F(RetrySchedulesOneConnectionAttemptAtATime, TRemoteQueueFixture) {
        Queue.Init("TxId", SenderId, SenderId, EventQueueId, /* keepAlive */ false, /* useConnect */ true);
        Act([&] {
            Queue.HandleNodeDisconnected(RecipientId.NodeId());
            Queue.HandleNodeDisconnected(RecipientId.NodeId());
        });
        Send();
        Send();
        UNIT_ASSERT(SentEvents.empty());
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.front().first->Get<TEvRetryQueuePrivate::TEvRetry>()->EventQueueId, EventQueueId);
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.front().second, TDuration::Zero());
        ScheduledEvents.clear();

        Act([&] { Queue.Retry(); });
        UNIT_ASSERT_VALUES_EQUAL(SentEvents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(SentEvents.front()->Type, TEvInterconnect::TEvConnectNode::EventType);
        UNIT_ASSERT_VALUES_EQUAL(SentEvents.front()->Recipient, Runtime.GetInterconnectProxy(0, 1));
        SentEvents.clear();

        Act([&] { Queue.HandleNodeDisconnected(RecipientId.NodeId()); });
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.size(), 1);
        UNIT_ASSERT(ScheduledEvents.front().second > TDuration::Zero());
        Act([&] { Queue.HandleNodeConnected(RecipientId.NodeId()); });
        ExpectEvent(1);
        ExpectEvent(2);
        Act([&] { Queue.Retry(); });
        UNIT_ASSERT(SentEvents.empty());
    }

    Y_UNIT_TEST_F(UndeliveredDistinguishesSessionsAndDisconnects, TRemoteQueueFixture) {
        UNIT_ASSERT(Undelivered(SenderId, TEvents::TEvUndelivered::Disconnected) == TRetryEventsQueue::ESessionState::WrongSession);
        Send();
        ExpectEvent(1);

        UNIT_ASSERT(Undelivered(RecipientId, TEvents::TEvUndelivered::Disconnected) == TRetryEventsQueue::ESessionState::Disconnected);
        Send();
        UNIT_ASSERT(SentEvents.empty());
        Act([&] { Queue.HandleNodeConnected(RecipientId.NodeId()); });
        ExpectEvent(1);
        ExpectEvent(2);

        UNIT_ASSERT(Undelivered(RecipientId, TEvents::TEvUndelivered::ReasonActorUnknown) == TRetryEventsQueue::ESessionState::SessionClosed);
    }

    Y_UNIT_TEST_F(UnsubscribeStopsSendingUntilReconnected, TRemoteQueueFixture) {
        Send();
        ExpectEvent(1);
        Act([&] { Queue.Unsubscribe(); });
        UNIT_ASSERT_VALUES_EQUAL(SentEvents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(SentEvents.front()->Type, TEvents::TEvUnsubscribe::EventType);
        UNIT_ASSERT_VALUES_EQUAL(SentEvents.front()->Recipient, Runtime.GetInterconnectProxy(0, 1));
        SentEvents.clear();

        Act([&] { Queue.Unsubscribe(); });
        Send();
        UNIT_ASSERT(SentEvents.empty());
        Act([&] { Queue.HandleNodeConnected(RecipientId.NodeId()); });
        ExpectEvent(1);
        ExpectEvent(2);
    }

    Y_UNIT_TEST_F(HeartbeatIsScheduledOnceAndStopsWhileDisconnected, TRemoteQueueFixture) {
        Queue.Init("TxId", SenderId, SenderId, EventQueueId, /* keepAlive */ true, /* useConnect */ false);
        Act([&] {
            Queue.HandleNodeConnected(RecipientId.NodeId());
            Queue.HandleNodeConnected(RecipientId.NodeId());
        });
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.front().first->Get<TEvRetryQueuePrivate::TEvEvHeartbeat>()->EventQueueId, EventQueueId);
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.front().second, TDuration::Seconds(2));
        ScheduledEvents.clear();

        Act([&] {
            Queue.Heartbeat();
        });
        UNIT_ASSERT_VALUES_EQUAL(ScheduledEvents.size(), 1);
        ScheduledEvents.clear();
        Queue.HandleNodeDisconnected(RecipientId.NodeId());
        Act([&] { UNIT_ASSERT(!Queue.Heartbeat()); });
        UNIT_ASSERT(ScheduledEvents.empty());
    }

    Y_UNIT_TEST_F(HasPendingEventsWaitsForPeerConfirmation, TRemoteQueueFixture) {
        UNIT_ASSERT(!Queue.HasPendingEvents());
        Send();
        ExpectEvent(1);
        Send();
        ExpectEvent(2);
        UNIT_ASSERT(Queue.HasPendingEvents());
        UNIT_ASSERT(Receive(1));
        UNIT_ASSERT(Queue.HasPendingEvents());
        UNIT_ASSERT(!Receive(1, 1));
        Reconnect();
        ExpectEvent(2, 1);
        UNIT_ASSERT(SentEvents.empty());
        UNIT_ASSERT(Receive(2, 2));
        UNIT_ASSERT(!Queue.HasPendingEvents());
        Reconnect();
        UNIT_ASSERT(SentEvents.empty());
    }

    Y_UNIT_TEST_F(LocalRecipientDoesNotUseTransportSequenceOrRetries, TRemoteQueueFixture) {
        RecipientId = Runtime.AllocateEdgeActor(0);
        Queue.OnNewRecipientId(RecipientId, /* unsubscribe */ false);
        UNIT_ASSERT(Receive(10));
        UNIT_ASSERT(Receive(10));
        UNIT_ASSERT(Receive(0));
        Send(123);
        UNIT_ASSERT_VALUES_EQUAL(ExpectEvent(0)->Cookie, 123);
        UNIT_ASSERT(!Queue.HasPendingEvents());
        Reconnect();
        UNIT_ASSERT(SentEvents.empty());
        UNIT_ASSERT(ScheduledEvents.empty());
    }

    Y_UNIT_TEST_F(FutureEventsDoNotAdvanceConfirmation, TRemoteQueueFixture) {
        UNIT_ASSERT(!Receive(3));
        UNIT_ASSERT(!Receive(2));
        UNIT_ASSERT(!Receive(2));
        Send();
        ExpectEvent(1, 0);

        UNIT_ASSERT(Receive(1));
        Send();
        ExpectEvent(2, 1);

        // Filling one gap must not implicitly confirm the previously rejected events.
        UNIT_ASSERT(!Receive(3));
        UNIT_ASSERT(Receive(2));
        UNIT_ASSERT(!Receive(1));
        Send();
        ExpectEvent(3, 2);

        UNIT_ASSERT(Receive(3));
        UNIT_ASSERT(!Receive(3));
        Send();
        ExpectEvent(4, 3);
    }

    Y_UNIT_TEST_F(UnorderedInputProcessesImmediatelyAndConfirmsContiguousPrefix, TUnorderedRemoteQueueFixture) {
        UNIT_ASSERT(Receive(3));
        UNIT_ASSERT(Receive(5));
        UNIT_ASSERT(!Receive(3));
        UNIT_ASSERT(!Receive(0));
        Send();
        ExpectEvent(1, 0);

        UNIT_ASSERT(Receive(1));
        Send();
        ExpectEvent(2, 1);

        // Closing the first gap confirms 2 and the already processed 3, but not 5.
        UNIT_ASSERT(Receive(2));
        UNIT_ASSERT(!Receive(5));
        Send();
        ExpectEvent(3, 3);

        UNIT_ASSERT(Receive(4));
        Send();
        ExpectEvent(4, 5);
        for (ui64 seqNo = 1; seqNo <= 5; ++seqNo) {
            UNIT_ASSERT(!Receive(seqNo));
        }
    }

    Y_UNIT_TEST_F(UnorderedInputPreservesDeduplicationAcrossReconnects, TUnorderedRemoteQueueFixture) {
        UNIT_ASSERT(Receive(2));
        Send();
        ExpectEvent(1, 0);

        Reconnect();
        ExpectEvent(1, 0);
        UNIT_ASSERT(!Receive(2));
        UNIT_ASSERT(Receive(1));
        Send();
        ExpectEvent(2, 2);

        // Receiving replies does not acknowledge the independent outgoing stream.
        UNIT_ASSERT(Queue.HasPendingEvents());
        UNIT_ASSERT(!Receive(2, 2));
        UNIT_ASSERT(!Queue.HasPendingEvents());
        Reconnect();
        UNIT_ASSERT(SentEvents.empty());
        UNIT_ASSERT(!Receive(1));
        UNIT_ASSERT(!Receive(2));
    }

    Y_UNIT_TEST_F(UnorderedInputStillProcessesPeerConfirmations, TUnorderedRemoteQueueFixture) {
        for (ui64 seqNo = 1; seqNo <= 3; ++seqNo) {
            Send();
            ExpectEvent(seqNo);
        }

        UNIT_ASSERT(Receive(2, 1));
        Reconnect();
        ExpectEvent(2, 0);
        ExpectEvent(3, 0);

        // A duplicate beyond the gap can carry a newer acknowledgment.
        UNIT_ASSERT(!Receive(2, 3));
        Reconnect();
        UNIT_ASSERT(SentEvents.empty());
        UNIT_ASSERT(Receive(1));
        Send();
        ExpectEvent(4, 2);
    }

    Y_UNIT_TEST_F(NewRecipientResetsUnorderedInput, TUnorderedRemoteQueueFixture) {
        UNIT_ASSERT(Receive(2));
        Send();
        ExpectEvent(1, 0);

        RecipientId = Runtime.AllocateEdgeActor(1);
        Queue.OnNewRecipientId(RecipientId, /* unsubscribe */ false, /* connected */ true);
        Reconnect();
        UNIT_ASSERT(SentEvents.empty());
        UNIT_ASSERT(Receive(1));
        Send();
        ExpectEvent(1, 1);
        UNIT_ASSERT(Receive(2));
        Send();
        ExpectEvent(2, 2);

        // The new recipient keeps the selected input ordering mode.
        UNIT_ASSERT(Receive(4));
        UNIT_ASSERT(Receive(3));
        Send();
        ExpectEvent(3, 4);
    }

    Y_UNIT_TEST_F(UnorderedInputLimitsUnconfirmedWindow, TUnorderedRemoteQueueFixture) {
        for (ui64 seqNo = 2; seqNo <= TEvRetryQueuePrivate::UNCONFIRMED_EVENTS_COUNT_LIMIT + 2; ++seqNo) {
            UNIT_ASSERT(Receive(seqNo));
        }
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            Receive(TEvRetryQueuePrivate::UNCONFIRMED_EVENTS_COUNT_LIMIT + 3),
            yexception, "Too wide window of reordered events");
        UNIT_ASSERT(Receive(1));
        Send();
        ExpectEvent(1, TEvRetryQueuePrivate::UNCONFIRMED_EVENTS_COUNT_LIMIT + 2);
        UNIT_ASSERT(Receive(TEvRetryQueuePrivate::UNCONFIRMED_EVENTS_COUNT_LIMIT + 3));
    }

    Y_UNIT_TEST_F(ReconnectReplaysDroppedEventsInOrder, TRemoteQueueFixture) {
        TRetryEventsQueue receiver;
        receiver.Init("TxId", RecipientId, RecipientId);
        receiver.OnNewRecipientId(SenderId, /* unsubscribe */ false);

        Send();
        auto first = ExpectEvent(1);
        Send();
        auto second = ExpectEvent(2);
        Send();
        auto third = ExpectEvent(3);

        // Later events reach the new session before the sender processes the reconnect.
        UNIT_ASSERT(!receiver.OnEventReceived(second->Get<TEvDqCompute::TEvInjectCheckpoint>()));
        UNIT_ASSERT(!receiver.OnEventReceived(third->Get<TEvDqCompute::TEvInjectCheckpoint>()));

        Reconnect();
        for (ui64 seqNo = 1; seqNo <= 3; ++seqNo) {
            auto event = ExpectEvent(seqNo);
            UNIT_ASSERT(receiver.OnEventReceived(event->Get<TEvDqCompute::TEvInjectCheckpoint>()));
        }
        UNIT_ASSERT(!receiver.OnEventReceived(first->Get<TEvDqCompute::TEvInjectCheckpoint>()));
        UNIT_ASSERT(!receiver.OnEventReceived(second->Get<TEvDqCompute::TEvInjectCheckpoint>()));
        UNIT_ASSERT(!receiver.OnEventReceived(third->Get<TEvDqCompute::TEvInjectCheckpoint>()));

        // Once acknowledged, the replayed events must leave the sender's queue.
        UNIT_ASSERT(Receive(1, 3));
        Reconnect();
        Send();
        ExpectEvent(4, 1);
    }

    Y_UNIT_TEST_F(RejectedEventsStillConfirmOutgoingMessages, TRemoteQueueFixture) {
        for (ui64 seqNo = 1; seqNo <= 3; ++seqNo) {
            Send();
            ExpectEvent(seqNo);
        }

        // The incoming payload has a gap, but its acknowledgment is still valid.
        UNIT_ASSERT(!Receive(2, 1));
        Reconnect();
        ExpectEvent(2, 0);
        ExpectEvent(3, 0);

        UNIT_ASSERT(Receive(1, 2));
        // A duplicate can also carry a newer acknowledgment.
        UNIT_ASSERT(!Receive(1, 3));
        Reconnect();
        Send();
        ExpectEvent(4, 1);
    }

    Y_UNIT_TEST_F(NewRecipientResetsSequenceAndPendingEvents, TRemoteQueueFixture) {
        UNIT_ASSERT(!Receive(2));
        UNIT_ASSERT(Receive(1));
        Send();
        ExpectEvent(1, 1);
        Send();
        ExpectEvent(2, 1);

        RecipientId = Runtime.AllocateEdgeActor(1);
        Queue.OnNewRecipientId(RecipientId, /* unsubscribe */ false, /* connected */ true);
        UNIT_ASSERT(!Receive(2));
        UNIT_ASSERT(Receive(1));
        Reconnect();
        UNIT_ASSERT(SentEvents.empty());
        Send();
        ExpectEvent(1, 1);
    }

    Y_UNIT_TEST(SendDisconnectAfterPoisonPill) {
        TRuntime runtime;

        runtime.Runtime.Send(new IEventHandle(
            runtime.ClientActorId,
            runtime.ClientEdgeActorId,
            new TEvPrivate::TEvSend()));

        TEvDqCompute::TEvInjectCheckpoint::TPtr event = runtime.Runtime.GrabEdgeEvent<TEvDqCompute::TEvInjectCheckpoint>(runtime.ServerEdgeActorId);
        UNIT_ASSERT(event);

        runtime.Runtime.Send(runtime.ServerActorId, runtime.ServerEdgeActorId, new TEvents::TEvPoisonPill());

        TEvPrivate::TEvDisconnect::TPtr disconnectEvent = runtime.Runtime.GrabEdgeEvent<TEvPrivate::TEvDisconnect>(runtime.ClientEdgeActorId);
        UNIT_ASSERT(disconnectEvent);
    }
}

} // namespace
