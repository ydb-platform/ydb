#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NKikimr::NPQ {
namespace {

THolder<TEvPersQueue::TEvGetPartitionsLocationResponse> SendLocationRequest(
    TTestContext& tc,
    TEvPersQueue::TEvGetPartitionsLocation* request,
    TDuration timeout = TDuration::Seconds(10)
) {
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, request, 0, GetPipeConfigWithRetries());
    return tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(timeout);
}

void WaitBalancerReady(TTestContext& tc, ui32 retries = 20) {
    for (ui32 i = 0; i < retries; ++i) {
        auto response = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
        ASSERT_TRUE(response);
        if (response->Record.GetStatus()) {
            return;
        }
        tc.Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(100));
        tc.Runtime->DispatchEvents();
    }
    FAIL() << "Could not get positive response from balancer";
}

} // namespace

TEST(TPartitionsLocationQueue, AnswerAfterPipesBecomeReady) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    TVector<THolder<IEventHandle>> delayedConnects;
    tc.Runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
        if (auto* msg = ev->CastAsLocal<TEvTabletPipe::TEvClientConnected>()) {
            if (msg->TabletId == tc.TabletId && msg->Status == NKikimrProto::OK) {
                delayedConnects.emplace_back(ev.Release());
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
        }
        return TTestActorRuntimeBase::EEventAction::PROCESS;
    });

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc);

    ASSERT_FALSE(delayedConnects.empty());

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvGetPartitionsLocation(),
        0,
        GetPipeConfigWithRetries()
    );

    // Request must stay queued while PQ tablet pipe is not connected.
    auto earlyResponse = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::MilliSeconds(200)
    );
    ASSERT_FALSE(earlyResponse) << "Location response must not arrive before pipes are ready";

    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    for (auto& ev : delayedConnects) {
        tc.Runtime->Send(ev.Release());
    }

    auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    ASSERT_TRUE(response);
    ASSERT_TRUE(response->Record.GetStatus());
    ASSERT_EQ(response->Record.LocationsSize(), 1u);
    ASSERT_EQ(response->Record.GetLocations(0).GetPartitionId(), 0u);
    ASSERT_GT(response->Record.GetLocations(0).GetNodeId(), 0u);
}

TEST(TPartitionsLocationQueue, ExpireQueuedRequest) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    const ui64 deadTabletId = MakeTabletID(false, 999);
    PQBalancerPrepare("topic", {{0, {deadTabletId, 1}}}, /*ssId=*/1, tc);

    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvGetPartitionsLocation(),
        0,
        GetPipeConfigWithRetries()
    );

    // Request should be queued while pipe to a missing tablet never becomes ready.
    auto earlyResponse = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::MilliSeconds(200)
    );
    ASSERT_FALSE(earlyResponse) << "Location response must wait in queue before timeout";

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(5) + TDuration::MilliSeconds(1));

    auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    ASSERT_TRUE(response);
    ASSERT_FALSE(response->Record.GetStatus());
}

TEST(TPartitionsLocationQueue, HappyPathAfterPipesReady) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc);

    WaitBalancerReady(tc);

    auto response = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
    ASSERT_TRUE(response);
    ASSERT_TRUE(response->Record.GetStatus());
    ASSERT_EQ(response->Record.LocationsSize(), 1u);

    auto* specific = new TEvPersQueue::TEvGetPartitionsLocation();
    specific->Record.AddPartitions(0);
    response = SendLocationRequest(tc, specific);
    ASSERT_TRUE(response);
    ASSERT_TRUE(response->Record.GetStatus());
    ASSERT_EQ(response->Record.LocationsSize(), 1u);
    ASSERT_EQ(response->Record.GetLocations(0).GetPartitionId(), 0u);

    auto* unknown = new TEvPersQueue::TEvGetPartitionsLocation();
    unknown->Record.AddPartitions(50);
    response = SendLocationRequest(tc, unknown);
    ASSERT_TRUE(response);
    ASSERT_FALSE(response->Record.GetStatus());
}

} // namespace NKikimr::NPQ
