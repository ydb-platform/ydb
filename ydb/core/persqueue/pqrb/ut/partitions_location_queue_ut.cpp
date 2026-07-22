#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

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
        UNIT_ASSERT(response);
        if (response->Record.GetStatus()) {
            return;
        }
        tc.Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(100));
        tc.Runtime->DispatchEvents();
    }
    UNIT_FAIL("Could not get positive response from balancer");
}

} // namespace

Y_UNIT_TEST_SUITE(TPartitionsLocationQueue) {

Y_UNIT_TEST(AnswerAfterPipesBecomeReady) {
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

    UNIT_ASSERT(!delayedConnects.empty());

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
    UNIT_ASSERT_C(!earlyResponse, "Location response must not arrive before pipes are ready");

    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    for (auto& ev : delayedConnects) {
        tc.Runtime->Send(ev.Release());
    }

    auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LocationsSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetLocations(0).GetPartitionId(), 0u);
    UNIT_ASSERT_GT(response->Record.GetLocations(0).GetNodeId(), 0u);
}

Y_UNIT_TEST(ExpireQueuedRequest) {
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
    UNIT_ASSERT_C(!earlyResponse, "Location response must wait in queue before timeout");

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(5) + TDuration::MilliSeconds(1));

    auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(response);
    UNIT_ASSERT(!response->Record.GetStatus());
}

Y_UNIT_TEST(HappyPathAfterPipesReady) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc);

    WaitBalancerReady(tc);

    auto response = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LocationsSize(), 1u);

    auto* specific = new TEvPersQueue::TEvGetPartitionsLocation();
    specific->Record.AddPartitions(0);
    response = SendLocationRequest(tc, specific);
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LocationsSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetLocations(0).GetPartitionId(), 0u);

    auto* unknown = new TEvPersQueue::TEvGetPartitionsLocation();
    unknown->Record.AddPartitions(50);
    response = SendLocationRequest(tc, unknown);
    UNIT_ASSERT(response);
    UNIT_ASSERT(!response->Record.GetStatus());
}

Y_UNIT_TEST(SinglePartitionNotBlockedByAllPartitions) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    const ui64 deadTabletId = MakeTabletID(false, 999);

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
    PQBalancerPrepare(
        "topic",
        {{0, {tc.TabletId, 1}}, {1, {deadTabletId, 2}}},
        /*ssId=*/1,
        tc
    );

    UNIT_ASSERT(!delayedConnects.empty());

    // Head of queue: all partitions (blocked on dead tablet).
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        new TEvPersQueue::TEvGetPartitionsLocation(),
        0,
        GetPipeConfigWithRetries()
    );

    // Behind it: only partition 0 (same tablet as delayed pipe).
    auto* specific = new TEvPersQueue::TEvGetPartitionsLocation();
    specific->Record.AddPartitions(0);
    tc.Runtime->SendToPipe(
        tc.BalancerTabletId,
        tc.Edge,
        specific,
        0,
        GetPipeConfigWithRetries()
    );

    auto earlyResponse = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!earlyResponse, "Neither request can be answered before partition-0 pipe is ready");

    tc.Runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
    for (auto& ev : delayedConnects) {
        tc.Runtime->Send(ev.Release());
    }

    // Specific request must be answered without waiting for the all-partitions request.
    auto response = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LocationsSize(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetLocations(0).GetPartitionId(), 0u);

    auto stillWaiting = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!stillWaiting, "All-partitions request must stay queued while dead tablet is down");

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(5) + TDuration::MilliSeconds(1));

    auto expired = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(expired);
    UNIT_ASSERT(!expired->Record.GetStatus());
}

} // Y_UNIT_TEST_SUITE(TPartitionsLocationQueue)

} // namespace NKikimr::NPQ
