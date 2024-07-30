#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/yql/dq/actors/compute/retry_queue.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/core/fq/libs/row_dispatcher/events/data_plane.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <chrono>
#include <thread>


using namespace NActors;

namespace {

const ui64 EventQueueId = 777;

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvSend = EvBegin + 10,
        EvDisconnect,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvSend : public TEventLocal<TEvSend, EvSend> {};
    struct TEvDisconnect : public TEventLocal<TEvDisconnect, EvDisconnect> {};
};


class ClientActor : public TActorBootstrapped<ClientActor>, public NYql::NDq::TRetryEventsQueue::ICallbacks {
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

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvPing::TPtr& ) {
        EventsQueue.Ping();
    }

    void Handle(const TEvPrivate::TEvSend::TPtr& ) {
        EventsQueue.Send(new NFq::TEvRowDispatcher::TEvGetNextBatch());
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
        EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
        EventsQueue.HandleUndelivered(ev);
    }

    void SessionClosed(ui64 /*eventQueueId*/) override {
        Send(ClientEdgeActorId, new TEvPrivate::TEvDisconnect());
    }

    STRICT_STFUNC(StateFunc,
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvRetry, Handle);
        hFunc(NYql::NDq::TEvRetryQueuePrivate::TEvPing, Handle);
        hFunc(TEvPrivate::TEvSend, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, HandleConnected);
        hFunc(TEvInterconnect::TEvNodeDisconnected, HandleDisconnected);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
    )

    void Init() {
        EventsQueue.Init("TxId", SelfId(), SelfId(), EventQueueId, true /*KeepAlive*/, this);
        EventsQueue.OnNewRecipientId(ServerActorId);
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
        hFunc(NFq::TEvRowDispatcher::TEvGetNextBatch, Handle);
        hFunc(TEvents::TEvPoisonPill, Handle);
    )

    void Handle(const TEvents::TEvPoisonPill::TPtr& ) {
        PassAway();
    }

    void Handle(const NYql::NDq::TEvRetryQueuePrivate::TEvRetry::TPtr& ) {
        EventsQueue.Retry();
    }

    void Handle(const NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr& /*ev*/) {
        Send(ServerEdgeActorId, new NFq::TEvRowDispatcher::TEvGetNextBatch());
    }

    void HandleDisconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        EventsQueue.HandleNodeDisconnected(ev->Get()->NodeId);
    }

    void HandleConnected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
        EventsQueue.HandleNodeConnected(ev->Get()->NodeId);
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr &ev) {
        EventsQueue.HandleUndelivered(ev);
    }

    NYql::NDq::TRetryEventsQueue EventsQueue;
    NActors::TActorId ServerEdgeActorId;
};

struct TRuntime: public NActors::TTestBasicRuntime
{
public:
    TRuntime() 
    : NActors::TTestBasicRuntime(2, true){
        Initialize(NKikimr::TAppPrepare().Unwrap());
        SetLogPriority(NKikimrServices::YQ_ROW_DISPATCHER, NLog::PRI_DEBUG);

        ClientEdgeActorId = AllocateEdgeActor(0);
        ServerEdgeActorId = AllocateEdgeActor(1);
        
        Server = new ServerActor(ServerEdgeActorId);
        ServerActorId = Register(Server, 1);
        EnableScheduleForActor(ServerActorId, true);

        Client = new ClientActor(ClientEdgeActorId, ServerActorId);
        ClientActorId = Register(Client, 0);
        EnableScheduleForActor(ClientActorId, true);
    }

    ClientActor* Client;
    ServerActor* Server;
    NActors::TActorId ClientActorId;
    NActors::TActorId ServerActorId;
    NActors::TActorId ClientEdgeActorId;
    NActors::TActorId ServerEdgeActorId;
};


Y_UNIT_TEST_SUITE(TRetryEventsQueueTest) {
    Y_UNIT_TEST(SendDisconnectAfterPoisonPill) { 
        TRuntime runtime;

        runtime.Send(new IEventHandle(
            runtime.ClientActorId,
            runtime.ClientEdgeActorId,
            new TEvPrivate::TEvSend()));

        NFq::TEvRowDispatcher::TEvGetNextBatch::TPtr event = runtime.GrabEdgeEvent<NFq::TEvRowDispatcher::TEvGetNextBatch>(runtime.ServerEdgeActorId);
        UNIT_ASSERT(event);

        runtime.Send(runtime.ServerActorId, runtime.ServerEdgeActorId, new TEvents::TEvPoisonPill());

        TEvPrivate::TEvDisconnect::TPtr disconnectEvent = runtime.GrabEdgeEvent<TEvPrivate::TEvDisconnect>(runtime.ClientEdgeActorId);
        UNIT_ASSERT(disconnectEvent);
    }
}

}
