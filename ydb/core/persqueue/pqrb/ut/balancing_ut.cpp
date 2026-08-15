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

} // Y_UNIT_TEST_SUITE(TPqrbBalancing)

} // namespace NKikimr::NPQ
