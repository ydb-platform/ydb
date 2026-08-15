#include "pqrb_ut_common.h"

#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr::NPQ {

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
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(5) + TDuration::MilliSeconds(25));

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
    tc.Runtime->AdvanceCurrentTime(TDuration::Seconds(5) + TDuration::MilliSeconds(25));

    auto expired = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(expired);
    UNIT_ASSERT(!expired->Record.GetStatus());
}

Y_UNIT_TEST(TimeoutQueueIsPolledEvery25ms) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    const ui64 deadTabletId = MakeTabletID(false, 999);
    PQBalancerPrepare("topic", {{0, {deadTabletId, 1}}}, /*ssId=*/1, tc);

    auto* req = new TEvPersQueue::TEvGetPartitionsLocation();
    req->Record.SetTimeoutMs(10);
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, req, 0, GetPipeConfigWithRetries());

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(10));
    auto earlyResponse = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::MilliSeconds(1)
    );
    UNIT_ASSERT_C(!earlyResponse, "Expired requests are collected on the 25ms poll, not immediately");

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(15));
    auto expired = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(expired);
    UNIT_ASSERT(!expired->Record.GetStatus());
}

Y_UNIT_TEST(ShorterTimeoutExpiresWhileLongerRequestIsHead) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    const ui64 deadTabletId = MakeTabletID(false, 999);
    PQBalancerPrepare("topic", {{0, {deadTabletId, 1}}}, /*ssId=*/1, tc);

    auto* longReq = new TEvPersQueue::TEvGetPartitionsLocation();
    longReq->Record.SetTimeoutMs(5000);
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, longReq, 0, GetPipeConfigWithRetries());

    auto* shortReq = new TEvPersQueue::TEvGetPartitionsLocation();
    shortReq->Record.SetTimeoutMs(200);
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, shortReq, 0, GetPipeConfigWithRetries());

    auto earlyResponse = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::MilliSeconds(50)
    );
    UNIT_ASSERT_C(!earlyResponse, "Requests must stay queued while the tablet is down");

    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(250));

    auto expired = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(expired);
    UNIT_ASSERT(!expired->Record.GetStatus());

    auto stillWaiting = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::MilliSeconds(200)
    );
    UNIT_ASSERT_C(!stillWaiting, "Longer request must stay queued until its own deadline");
}

Y_UNIT_TEST(StaleClientConnectedDoesNotOverridePipeLocation) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc);
    WaitBalancerReady(tc);

    auto baseline = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
    UNIT_ASSERT(baseline);
    UNIT_ASSERT(baseline->Record.GetStatus());
    const ui32 realNodeId = baseline->Record.GetLocations(0).GetNodeId();
    UNIT_ASSERT_GT(realNodeId, 0u);

    const TActorId staleClient(777, 1, 1, 1);
    const TActorId staleServer(999, 1, 1, 1);
    ForwardToTablet(
        *tc.Runtime,
        tc.BalancerTabletId,
        tc.Edge,
        new TEvTabletPipe::TEvClientConnected(
            tc.TabletId,
            NKikimrProto::OK,
            staleClient,
            staleServer,
            /*leader=*/true,
            /*dead=*/false,
            /*generation=*/42
        )
    );
    DispatchFor(tc);

    auto response = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.GetLocations(0).GetNodeId(), realNodeId);
    UNIT_ASSERT_VALUES_UNEQUAL(response->Record.GetLocations(0).GetNodeId(), 999u);
}

Y_UNIT_TEST(StaleClientDestroyedDoesNotDropLivePipe) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc);
    WaitBalancerReady(tc);

    ForwardToTablet(
        *tc.Runtime,
        tc.BalancerTabletId,
        tc.Edge,
        new TEvTabletPipe::TEvClientDestroyed(
            tc.TabletId,
            TActorId(777, 1, 1, 1),
            TActorId(999, 1, 1, 1)
        )
    );
    DispatchFor(tc);

    auto response = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LocationsSize(), 1u);
}

Y_UNIT_TEST(RemovedTabletsDoNotBlockAllPartitionsLocation) {
    TTestContext tc;
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);
    PQBalancerPrepare("topic", {{0, {tc.TabletId, 1}}}, /*ssId=*/1, tc, false, false);
    WaitBalancerReady(tc);

    const ui64 extraTabletId = MakeTabletID(false, 999);
    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
        .ExtraTablets = {extraTabletId},
    });

    auto* blocked = new TEvPersQueue::TEvGetPartitionsLocation();
    blocked->Record.SetTimeoutMs(200);
    tc.Runtime->SendToPipe(tc.BalancerTabletId, tc.Edge, blocked, 0, GetPipeConfigWithRetries());
    tc.Runtime->ResetScheduledCount();
    tc.Runtime->AdvanceCurrentTime(TDuration::MilliSeconds(250));
    auto blockedResponse = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvGetPartitionsLocationResponse>(
        TDuration::Seconds(10)
    );
    UNIT_ASSERT(blockedResponse);
    UNIT_ASSERT_C(!blockedResponse->Record.GetStatus(), "Extra tablet in TabletsInfo must block all-partitions location");

    SendBalancerUpdate(tc, TBalancerUpdate{
        .Partitions = {{0, {tc.TabletId, 1}}},
    });

    WaitBalancerReady(tc);
    auto response = SendLocationRequest(tc, new TEvPersQueue::TEvGetPartitionsLocation());
    UNIT_ASSERT(response);
    UNIT_ASSERT(response->Record.GetStatus());
    UNIT_ASSERT_VALUES_EQUAL(response->Record.LocationsSize(), 1u);
}

} // Y_UNIT_TEST_SUITE(TPartitionsLocationQueue)

} // namespace NKikimr::NPQ
