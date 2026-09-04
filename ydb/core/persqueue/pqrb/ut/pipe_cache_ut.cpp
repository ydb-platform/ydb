#include "pqrb_ut_common.h"

namespace NKikimr::NPQ {

namespace {

TActorId RegisterReadSessionOnSharedPipe(
    const TString& session,
    TTestContext& tc,
    const TActorId& physicalPipe,
    const TActorId& logicalPipeClient)
{
    auto request = MakeHolder<TEvPersQueue::TEvRegisterReadSession>();
    auto& req = request->Record;
    req.SetSession(session);
    // Logical holder id (as pipe-cache clients put SelfId), not the physical ClientId.
    ActorIdToProto(logicalPipeClient, req.MutablePipeClient());
    req.SetClientId("user");

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        request.Release(),
        0,
        GetPipeConfigWithRetries(),
        physicalPipe);
    DispatchFor(tc);
    return logicalPipeClient;
}

ui32 CountNamedSessions(TTestContext& tc) {
    auto sessions = MakeHolder<TEvPersQueue::TEvGetReadSessionsInfo>();
    sessions->Record.SetClientId("user");
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, sessions.Release(), 0, GetPipeConfigWithRetries());
    auto info = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReadSessionsInfoResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(info);
    return info->Record.ReadSessionsSize();
}

} // namespace

Y_UNIT_TEST_SUITE(TPqrbPipeCache) {

Y_UNIT_TEST(TwoLogicalSessionsShareOnePhysicalPipe) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
        },
        .NextPartitionId = 2,
    });

    const TActorId physicalPipe = tc.Runtime->ConnectToPipe(
        tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
    UNIT_ASSERT(physicalPipe);

    const TActorId peer1 = tc.Runtime->AllocateEdgeActor();
    const TActorId peer2 = tc.Runtime->AllocateEdgeActor();

    RegisterReadSessionOnSharedPipe("session-pipe-cache-0", tc, physicalPipe, peer1);
    RegisterReadSessionOnSharedPipe("session-pipe-cache-1", tc, physicalPipe, peer2);

    UNIT_ASSERT_VALUES_EQUAL(CountNamedSessions(tc), 2u);

    // Physical disconnect must drop both logical sessions.
    tc.Runtime->ClosePipe(physicalPipe, tc.Edge, 0);
    DispatchFor(tc, TDuration::MilliSeconds(200));

    UNIT_ASSERT_VALUES_EQUAL(CountNamedSessions(tc), 0u);
}

Y_UNIT_TEST(UnregisterOneLogicalSessionKeepsTheOther) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
        },
        .NextPartitionId = 2,
    });

    const TActorId physicalPipe = tc.Runtime->ConnectToPipe(
        tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());
    UNIT_ASSERT(physicalPipe);

    const TActorId peer1 = tc.Runtime->AllocateEdgeActor();
    const TActorId peer2 = tc.Runtime->AllocateEdgeActor();

    RegisterReadSessionOnSharedPipe("session-pipe-cache-0", tc, physicalPipe, peer1);
    RegisterReadSessionOnSharedPipe("session-pipe-cache-1", tc, physicalPipe, peer2);
    UNIT_ASSERT_VALUES_EQUAL(CountNamedSessions(tc), 2u);

    auto unregister = MakeHolder<TEvPersQueue::TEvUnregisterClient>();
    unregister->Record.SetSession("session-pipe-cache-0");
    ActorIdToProto(peer1, unregister->Record.MutablePipeClient());
    unregister->Record.SetClientId("user");
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        unregister.Release(),
        0,
        GetPipeConfigWithRetries(),
        physicalPipe);
    DispatchFor(tc);

    UNIT_ASSERT_VALUES_EQUAL(CountNamedSessions(tc), 1u);

    auto sessions = MakeHolder<TEvPersQueue::TEvGetReadSessionsInfo>();
    sessions->Record.SetClientId("user");
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, sessions.Release(), 0, GetPipeConfigWithRetries());
    auto info = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReadSessionsInfoResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(info);
    UNIT_ASSERT_VALUES_EQUAL(info->Record.ReadSessionsSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(info->Record.GetReadSessions(0).GetSession(), TString("session-pipe-cache-1"));
}

Y_UNIT_TEST(DedicatedPipeSessionStillWorks) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
        },
        .NextPartitionId = 1,
    });

    auto pipe = RegisterReadSession("session-dedicated", tc);
    UNIT_ASSERT(pipe);
    UNIT_ASSERT_VALUES_EQUAL(CountNamedSessions(tc), 1u);

    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    UNIT_ASSERT_VALUES_EQUAL(lock->Record.GetSession(), TString("session-dedicated"));
}

} // Y_UNIT_TEST_SUITE(TPqrbPipeCache)

} // namespace NKikimr::NPQ
