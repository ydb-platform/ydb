#include "pqrb_ut_common.h"

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqrbMlp) {

Y_UNIT_TEST(GetPartitionIsQueuedUntilBalancerInited) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvMLPGetPartitionRequest("topic", "mlp-user"),
        0,
        GetPipeConfigWithRetries()
    );

    auto earlyError = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPErrorResponse>(TDuration::MilliSeconds(200));
    UNIT_ASSERT_C(!earlyError, "GetPartition must wait for init instead of failing with a missing consumer");

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"mlp-user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP}},
    });

    auto response = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPGetPartitionResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(response->GetPartitionId(), 0u);
    UNIT_ASSERT_VALUES_EQUAL(response->GetTabletId(), tc.TabletId);
}

Y_UNIT_TEST(GetRuntimeAttributesRespondsWhenPartitionTabletIsDown) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    const ui64 deadTabletId = MakeTabletID(false, 999);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {deadTabletId, 1}}},
        .Consumers = {{"mlp-user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP}},
    });

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvMLPGetRuntimeAttributesRequest("topic", "mlp-user"),
        0,
        GetPipeConfigWithRetries()
    );

    auto early = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPGetRuntimeAttributesResponse>(TDuration::MilliSeconds(200));
    UNIT_ASSERT_C(!early, "Runtime attributes wait for a stats round");

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(6));

    auto response = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPGetRuntimeAttributesResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(response);
}

} // Y_UNIT_TEST_SUITE(TPqrbMlp)

} // namespace NKikimr::NPQ
