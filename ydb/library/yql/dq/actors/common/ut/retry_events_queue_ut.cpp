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
        EventsQueue.Init("TxId", SelfId(), SelfId(), EventQueueId, true /*KeepAlive*/);
        EventsQueue.OnNewRecipientId(ServerActorId, false, true /*connected*/);
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

Y_UNIT_TEST_SUITE(TRetryEventsQueueTest) {
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
