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

Y_UNIT_TEST(DeletedMlpConsumerDoesNotRestoreReceiveAttemptAfterRestart) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}},
        .Consumers = {{"mlp-user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP}},
        .NextPartitionId = 2,
    });

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvMLPGetPartitionRequest("topic", "mlp-user"),
        0,
        GetPipeConfigWithRetries()
    );
    auto first = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPGetPartitionResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(first);
    UNIT_ASSERT_VALUES_EQUAL(first->GetPartitionId(), 0u);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvMLPGetPartitionRequest("topic", "mlp-user", "attempt-1"),
        0,
        GetPipeConfigWithRetries()
    );
    auto assigned = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPGetPartitionResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(assigned);
    UNIT_ASSERT_VALUES_EQUAL(assigned->GetStatus(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL(assigned->GetPartitionId(), 1u);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}},
        .Consumers = {{"other", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP}},
        .NextPartitionId = 2,
    });

    ForwardToTablet(*tc.Runtime, tc.BalancerTabletId, tc.Edge, new TEvents::TEvPoisonPill());
    TDispatchOptions rebootOptions;
    rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 2));
    tc.Runtime->DispatchEvents(rebootOptions);

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}},
        .Consumers = {{"mlp-user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP}},
        .NextPartitionId = 2,
    });

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvMLPGetPartitionRequest("topic", "mlp-user", "attempt-1"),
        0,
        GetPipeConfigWithRetries()
    );
    auto restored = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPGetPartitionResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(restored);
    UNIT_ASSERT_VALUES_EQUAL(restored->GetStatus(), Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_VALUES_EQUAL_C(
        restored->GetPartitionId(),
        0u,
        "Deleted consumer must not restore a sticky receive-attempt mapping from local DB"
    );
}

Y_UNIT_TEST(ReceiveAttemptIdIsStickyUntilExpiry) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}, {1, {tc.TabletId, 2}}},
        .Consumers = {{"mlp-user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_MLP}},
        .NextPartitionId = 2,
        .ReceiveAttemptIdPeriodMs = 200,
    });

    auto getPartition = [&](const TString& attemptId) {
        tc.Runtime->SendToPipe(
            tc.BalancerTabletId,
            tc.Edge,
            new TEvPQ::TEvMLPGetPartitionRequest("topic", "mlp-user", attemptId),
            0,
            GetPipeConfigWithRetries()
        );
        auto response = tc.Runtime->GrabEdgeEvent<TEvPQ::TEvMLPGetPartitionResponse>(TDuration::Seconds(10));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), Ydb::StatusIds::SUCCESS);
        return response->GetPartitionId();
    };

    const ui32 first = getPartition("attempt-1");
    UNIT_ASSERT_VALUES_EQUAL(getPartition("attempt-1"), first);

    const ui32 other = getPartition("attempt-2");
    UNIT_ASSERT_VALUES_UNEQUAL(other, first);
}

} // Y_UNIT_TEST_SUITE(TPqrbMlp)

} // namespace NKikimr::NPQ
