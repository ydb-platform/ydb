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

} // Y_UNIT_TEST_SUITE(TPqrbWritePartition)

} // namespace NKikimr::NPQ
