#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/network/sock.h>

#include <memory>

namespace {

enum class EConnectionPhase {
    Resolving,
    Connecting,
};

void CheckPoisonDuringRequest(EConnectionPhase phase) {
    NActors::TTestActorRuntimeBase runtime;
    runtime.Initialize();
    const auto owner = runtime.AllocateEdgeActor();
    const auto poller = runtime.AllocateEdgeActor();
    runtime.RegisterService(NActors::MakePollerActorId(), poller);

    using TOutgoing = NHttp::TEvHttpProxy::TEvHttpOutgoingRequest;
    TOutgoing::TPtr outgoing = reinterpret_cast<NActors::TEventHandle<TOutgoing>*>(new NActors::IEventHandle(
        NActors::TActorId(), owner,
        new TOutgoing(NHttp::THttpOutgoingRequest::CreateRequestGet("https://idp.example.com/"), TDuration::Hours(1))));
    const auto connection = runtime.Register(NHttp::CreateOutgoingConnectionActor(owner, outgoing));
    const auto resolve = runtime.GrabEdgeEventRethrow<NHttp::TEvHttpProxy::TEvResolveHostRequest>(owner);
    UNIT_ASSERT_VALUES_EQUAL(resolve->Sender, connection);

    if (phase == EConnectionPhase::Connecting) {
        // Hold poller registration before any connect/handshake can complete.
        runtime.Send(new NActors::IEventHandle(connection, owner,
            new NHttp::TEvHttpProxy::TEvResolveHostResponse("idp.example.com", std::make_shared<TSockAddrInet>("127.0.0.1", 443))));
        const auto registration = runtime.GrabEdgeEventRethrow<NActors::TEvPollerRegister>(poller);
        UNIT_ASSERT_VALUES_EQUAL(registration->Sender, connection);
    }

    runtime.Send(new NActors::IEventHandle(connection, owner, new NActors::TEvents::TEvPoison()));
    runtime.Schedule(new NActors::IEventHandle(owner, owner, new NActors::TEvents::TEvWakeup()), TDuration::Seconds(2));
    const auto closed = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingConnectionClosed>(owner, TDuration::Seconds(1));
    UNIT_ASSERT_C(closed, "Poison must close a pending connection without waiting for its inactivity timeout");
    UNIT_ASSERT_VALUES_EQUAL(closed->Get()->ConnectionID, connection);
    UNIT_ASSERT_C(runtime.FindActor(connection) == nullptr, "A stopped HTTP connection must detach");
}

} // namespace

Y_UNIT_TEST_SUITE(HttpProxyShutdown) {
    Y_UNIT_TEST(PoisonWhileResolving) {
        CheckPoisonDuringRequest(EConnectionPhase::Resolving);
    }

    Y_UNIT_TEST(PoisonWhileConnecting) {
        CheckPoisonDuringRequest(EConnectionPhase::Connecting);
    }
}
