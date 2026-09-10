#include "pqrb_ut_common.h"

#include <ydb/library/actors/core/mon.h>

namespace NKikimr::NPQ {

namespace {

TString FetchBalancerHtml(TTestContext& tc) {
    ForwardToTablet(
        *tc.Runtime,
        tc.BalancerTabletId,
        tc.Edge,
        new NMon::TEvRemoteHttpInfo(TStringBuilder() << "/app?TabletID=" << tc.BalancerTabletId)
    );
    auto res = tc.Runtime->GrabEdgeEvent<NMon::TEvRemoteHttpInfoRes>(TDuration::Seconds(10));
    UNIT_ASSERT(res);
    return res->Html;
}

} // namespace

Y_UNIT_TEST_SUITE(TPqrbBalancingApp) {

Y_UNIT_TEST(RenderAppCoversFamilyPartitionAndSessionStates) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    // Finish/Commit are only valid on partitions that already have children.
    // 1 and 2 get grandchildren so later Finish events cover HTML description
    // branches without tripping the leaf-partition debug abort.
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {
            {0, {tc.TabletId, 1}},
            {1, {tc.TabletId, 2}},
            {2, {tc.TabletId, 3}},
            {3, {tc.TabletId, 4}},
            {4, {tc.TabletId, 5}},
        },
        .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
        .Consumers = {
            {"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING},
            {"other", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING},
        },
        .ParentPartitionIds = {{1, {0}}, {2, {0}}, {3, {1}}, {4, {2}}},
        .ChildPartitionIds = {{0, {1, 2}}, {1, {3}}, {2, {4}}},
        .NextPartitionId = 5,
    });
    WaitBalancerReady(tc);

    auto pipe0 = RegisterReadSession("session-0", tc);
    auto lock0 = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock0);
    UNIT_ASSERT_VALUES_EQUAL(lock0->Record.GetPartition(), 0u);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe0, "user", 0, /*scaleAwareSDK=*/true, /*startedReadingFromEndOffset=*/false),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );

    absl::flat_hash_set<ui32> lockedChildren;
    for (ui32 i = 0; i < 2; ++i) {
        auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
        UNIT_ASSERT(lock);
        lockedChildren.insert(lock->Record.GetPartition());
    }
    UNIT_ASSERT(lockedChildren.contains(1));
    UNIT_ASSERT(lockedChildren.contains(2));

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPQ::TEvReadingPartitionStatusRequest("user", 0, 1, 1),
        0,
        GetPipeConfigWithRetries()
    );
    DispatchFor(tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe0, "user", 1, /*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/true),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );
    DispatchFor(tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvReadingPartitionFinishedRequest(pipe0, "user", 2, /*scaleAwareSDK=*/false, /*startedReadingFromEndOffset=*/false),
        0,
        GetPipeConfigWithRetries(),
        pipe0
    );
    DispatchFor(tc);

    auto pipe1 = RegisterReadSession("session-1", tc);
    Y_UNUSED(pipe1);
    DispatchFor(tc, TDuration::MilliSeconds(200));

    const TString html = FetchBalancerHtml(tc);
    UNIT_ASSERT_C(html.Contains("Families"), html.substr(0, 2000));
    UNIT_ASSERT(html.Contains("Partitions"));
    UNIT_ASSERT(html.Contains("Statistics"));
    UNIT_ASSERT(html.Contains("Sessions"));
    UNIT_ASSERT(html.Contains("session-0"));
    UNIT_ASSERT(html.Contains("Total:"));
    UNIT_ASSERT(html.Contains("committed") || html.Contains("reading child") || html.Contains("finished"));
    UNIT_ASSERT(html.Contains("scheduled. iteration:") || html.Contains("iteration:"));
    UNIT_ASSERT(html.Contains("Free") || html.Contains("Ready") || html.Contains("Read") || html.Contains("Finished"));
    UNIT_ASSERT(html.Contains("Active"));
    UNIT_ASSERT(html.Contains("Inactive"));
    UNIT_ASSERT(html.Contains("?TabletID="));
}

Y_UNIT_TEST(RenderAppAfterSessionShowsConsumerTab) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .Consumers = {{"user", NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING}},
    });
    WaitBalancerReady(tc);

    auto pipe = RegisterReadSession("lonely-session", tc);
    auto lock = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvLockPartition>(TDuration::Seconds(10));
    UNIT_ASSERT(lock);
    Y_UNUSED(pipe);

    const TString html = FetchBalancerHtml(tc);
    UNIT_ASSERT_C(html.Contains("Families"), html.substr(0, 2000));
    UNIT_ASSERT(html.Contains("Ready") || html.Contains("Free") || html.Contains("Read"));
    UNIT_ASSERT(html.Contains("lonely-session"));
}

} // Y_UNIT_TEST_SUITE(TPqrbBalancingApp)

} // namespace NKikimr::NPQ
