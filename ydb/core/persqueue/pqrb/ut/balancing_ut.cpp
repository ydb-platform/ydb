#include "pqrb_ut_common.h"

#include <ydb/core/persqueue/pqrb/read_balancer__balancing.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqrbBalancing) {

Y_UNIT_TEST(UninitializedPartitionAcceptsFirstCommit) {
    NBalancing::TPartition partition;
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 0u);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionCookie, 0u);
    UNIT_ASSERT(partition.SetCommittedState(1, 1));
    UNIT_ASSERT(partition.Commited);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 1u);
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionCookie, 1u);
}

Y_UNIT_TEST(PartitionStateMachine) {
    NBalancing::TPartition partition;

    UNIT_ASSERT(!partition.IsInactive());
    UNIT_ASSERT(partition.NeedReleaseChildren());
    UNIT_ASSERT(!partition.BalanceToOtherPipe());

    UNIT_ASSERT(partition.SetCommittedState(1, 1));
    UNIT_ASSERT(partition.Commited);
    UNIT_ASSERT(partition.IsInactive());
    UNIT_ASSERT(!partition.NeedReleaseChildren());
    UNIT_ASSERT(!partition.SetCommittedState(2, 1));
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 2u);
    UNIT_ASSERT(!partition.SetCommittedState(1, 99));
    UNIT_ASSERT_VALUES_EQUAL(partition.PartitionGeneration, 2u);

    UNIT_ASSERT(partition.Reset());
    UNIT_ASSERT(!partition.Commited);
    UNIT_ASSERT(!partition.IsInactive());

    UNIT_ASSERT(partition.SetFinishedState(/*scaleAwareSDK=*/true, /*startedReadingFromEndOffset=*/false));
    UNIT_ASSERT(partition.IsInactive());
    UNIT_ASSERT(partition.NeedReleaseChildren());
    UNIT_ASSERT(partition.StartReading());
    UNIT_ASSERT(!partition.IsInactive());

    UNIT_ASSERT(!partition.SetFinishedState(/*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/false));
    UNIT_ASSERT(!partition.IsInactive());
    UNIT_ASSERT(partition.BalanceToOtherPipe());
    UNIT_ASSERT(!partition.NeedReleaseChildren());
    UNIT_ASSERT(partition.StopReading());
}

Y_UNIT_TEST(DestroyFreeFamilyOnRereadParentDoesNotCrash) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
            {2, {tc.TabletId, 3}},
        },
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .ParentPartitionIds = {{1, {0}}, {2, {0}}},
        .ChildPartitionIds = {{0, {1, 2}}},
        .NextPartitionId = 3,
    });

    auto pipe = RegisterReadSession("session-0", tc);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    UNIT_ASSERT_VALUES_EQUAL(lock->Record.GetPartition(), 0u);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe, "user", 0, /*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/true),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    DispatchFor(tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionStartedRequest(pipe, "user", 0),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    DispatchFor(tc);

    auto sessions = MakeHolder<TEvPersQueue::TEvGetReadSessionsInfo>();
    sessions->Record.SetClientId("user");
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, sessions.Release(), 0, GetPipeConfigWithRetries());
    auto info = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReadSessionsInfoResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(info);
    UNIT_ASSERT_VALUES_EQUAL(info->Record.ReadSessionsSize(), 1u);
}

Y_UNIT_TEST(SecondSessionTriggersRebalance) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
            {2, {tc.TabletId, 3}},
        },
        .NextPartitionId = 3,
    });

    auto pipe0 = RegisterReadSession("session-0", tc);
    absl::flat_hash_set<ui32> lockedByFirst;
    for (ui32 i = 0; i < 3; ++i) {
        auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
        UNIT_ASSERT(lock);
        lockedByFirst.insert(lock->Record.GetPartition());
    }
    UNIT_ASSERT_VALUES_EQUAL(lockedByFirst.size(), 3u);

    auto pipe1 = RegisterReadSession("session-1", tc);
    Y_UNUSED(pipe1);

    auto release = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvReleasePartition>(TDuration::Seconds(10));
    UNIT_ASSERT(release);
    UNIT_ASSERT_VALUES_EQUAL(release->Record.GetSession(), TString("session-0"));
    UNIT_ASSERT(release->Record.GetGroup() >= 1);
    const ui32 releasedPartition = release->Record.GetGroup() - 1;
    UNIT_ASSERT(lockedByFirst.contains(releasedPartition));

    auto released = MakeHolder<TEvPersQueue::TEvPartitionReleased>();
    released->Record.SetSession("session-0");
    released->Record.SetPartition(releasedPartition);
    released->Record.SetTopic("topic");
    released->Record.SetClientId("user");
    ActorIdToProto(pipe0, released->Record.MutablePipeClient());
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        released.Release(),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );

    auto lockOnSecond = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lockOnSecond);
    UNIT_ASSERT_VALUES_EQUAL(lockOnSecond->Record.GetSession(), TString("session-1"));
    UNIT_ASSERT_VALUES_EQUAL(lockOnSecond->Record.GetPartition(), releasedPartition);
}

Y_UNIT_TEST(BalancingUnsubscribeRemovesOnlyMatchingSubscription) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc, false, false);

    const TActorId subscriberA = tc.Runtime->AllocateEdgeActor();
    const TActorId subscriberB = tc.Runtime->AllocateEdgeActor();
    TActorId pipe = tc.Runtime->ConnectToPipe(tc.BalancerTabletId, tc.Edge, 0, GetPipeConfigWithRetries());

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvBalancingSubscribe(subscriberA, "/Root/topic", "user"),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvBalancingSubscribe(subscriberB, "/Root/topic", "user"),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );

    auto notifyA = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(subscriberA, TDuration::Seconds(10));
    auto notifyB = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(subscriberB, TDuration::Seconds(10));
    UNIT_ASSERT(notifyA);
    UNIT_ASSERT(notifyB);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvBalancingUnsubscribe(subscriberA, "/Root/topic", "user"),
        0,
        GetPipeConfigWithRetries(),
        pipe
    );
    DispatchFor(tc);

    auto pipeClient = RegisterReadSession("session-notify", tc);
    Y_UNUSED(pipeClient);

    auto afterUnsubscribeA = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(
        subscriberA,
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!afterUnsubscribeA, "Unsubscribed actor must not receive further notifications");

    auto afterUnsubscribeB = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvBalancingSubscribeNotify>(
        subscriberB,
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(afterUnsubscribeB);
    UNIT_ASSERT(afterUnsubscribeB->Get()->Record.GetStatus() == NKikimrPQ::TEvBalancingSubscribeNotify::BALANCING);
}

} // Y_UNIT_TEST_SUITE(TPqrbBalancing)

} // namespace NKikimr::NPQ
