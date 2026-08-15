#include "pqrb_ut_common.h"

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqrbWritePartition) {

Y_UNIT_TEST(GetPartitionIdForWriteWithZeroGroupsDoesNotCrash) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvGetPartitionIdForWrite(),
        0,
        GetPipeConfigWithRetries()
    );

    auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionIdForWriteResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetPartitionId(), 0u);
}

Y_UNIT_TEST(GetPartitionIdForWriteRoundRobinsGroups) {
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

    absl::flat_hash_set<ui32> seen;
    for (ui32 i = 0; i < 3; ++i) {
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPersQueue::TEvGetPartitionIdForWrite(),
            0,
            GetPipeConfigWithRetries()
        );
        auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionIdForWriteResponse>(
            TDuration::Seconds(10)
        );
        UNIT_ASSERT(response);
        const ui32 partitionId = response->Record.GetPartitionId();
        UNIT_ASSERT_LT(partitionId, 3u);
        seen.insert(partitionId);
    }
    UNIT_ASSERT_VALUES_EQUAL(seen.size(), 3u);
}

} // Y_UNIT_TEST_SUITE(TPqrbWritePartition)

} // namespace NKikimr::NPQ
