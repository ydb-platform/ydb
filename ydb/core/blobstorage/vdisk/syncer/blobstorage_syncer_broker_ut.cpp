#include "blobstorage_syncer_broker.h"

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

TActorId StartBroker(TTestActorRuntime& runtime) {
    runtime.SetLogPriority(NKikimrServices::BS_SYNCER, NActors::NLog::PRI_DEBUG);
    runtime.Initialize(TAppPrepare().Unwrap());

    TControlWrapper control = 1;
    auto* broker = CreateSyncBrokerActor(control);
    auto brokerId = runtime.Register(broker);
    runtime.DispatchEvents();
    return brokerId;
}

Y_UNIT_TEST_SUITE(TSyncBrokerTests) {

Y_UNIT_TEST(ShouldReturnToken) {
    TTestActorRuntime runtime;
    auto brokerId = StartBroker(runtime);

    TActorId vDiskId(1, 1);
    auto clientId = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId, new TEvQuerySyncToken(vDiskId));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId);
}

Y_UNIT_TEST(ShouldReturnTokensWithSameVDiskId) {
    TTestActorRuntime runtime;
    auto brokerId = StartBroker(runtime);

    TActorId vDiskId(1, 1);
    auto clientId1 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId1, new TEvQuerySyncToken(vDiskId));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId1);

    auto clientId2 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId2, new TEvQuerySyncToken(vDiskId));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId2);
}

Y_UNIT_TEST(ShouldEnqueue) {
    TTestActorRuntime runtime;
    auto brokerId = StartBroker(runtime);

    TActorId vDiskId1(1, 1);
    auto clientId1 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId1, new TEvQuerySyncToken(vDiskId1));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId1);

    TActorId vDiskId2(1, 2);
    auto clientId2 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId2, new TEvQuerySyncToken(vDiskId2));
}

Y_UNIT_TEST(ShouldEnqueueWithSameVDiskId) {
    TTestActorRuntime runtime;
    auto brokerId = StartBroker(runtime);

    TActorId vDiskId1(1, 1);
    auto clientId1 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId1, new TEvQuerySyncToken(vDiskId1));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId1);

    TActorId vDiskId2(1, 2);
    auto clientId2 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId2, new TEvQuerySyncToken(vDiskId2));

    auto clientId3 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId3, new TEvQuerySyncToken(vDiskId2));
}

Y_UNIT_TEST(ShouldReleaseToken) {
    TTestActorRuntime runtime;
    auto brokerId = StartBroker(runtime);

    TActorId vDiskId(1, 1);
    auto clientId = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId, new TEvQuerySyncToken(vDiskId));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId);

    runtime.Send(brokerId, clientId, new TEvReleaseSyncToken(vDiskId));
}

Y_UNIT_TEST(ShouldProcessAfterRelease) {
    TTestActorRuntime runtime;
    auto brokerId = StartBroker(runtime);

    TActorId vDiskId1(1, 1);
    auto clientId1 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId1, new TEvQuerySyncToken(vDiskId1));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId1);

    TActorId vDiskId2(1, 2);
    auto clientId2 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId2, new TEvQuerySyncToken(vDiskId2));

    runtime.Send(brokerId, clientId1, new TEvReleaseSyncToken(vDiskId1));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId2);
}

Y_UNIT_TEST(ShouldReleaseInQueue) {
    TTestActorRuntime runtime;
    auto brokerId = StartBroker(runtime);

    TActorId vDiskId1(1, 1);
    auto clientId1 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId1, new TEvQuerySyncToken(vDiskId1));
    runtime.GrabEdgeEventRethrow<TEvSyncToken>(clientId1);

    TActorId vDiskId2(1, 2);
    auto clientId2 = runtime.AllocateEdgeActor();
    runtime.Send(brokerId, clientId2, new TEvQuerySyncToken(vDiskId2));

    runtime.Send(brokerId, clientId2, new TEvReleaseSyncToken(vDiskId2));
}

}
