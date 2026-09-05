#include <ydb/core/persqueue/ut/common/pq_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(TPqTabletPipeCache) {

Y_UNIT_TEST(TwoOwnersShareOnePhysicalPipe) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);

    const TActorId physicalPipe = tc.Runtime->ConnectToPipe(tc.TabletId, tc.Edge, 0, GetPipeConfigWithRetries());
    UNIT_ASSERT(physicalPipe);

    const TActorId peer1 = tc.Runtime->AllocateEdgeActor();
    const TActorId peer2 = tc.Runtime->AllocateEdgeActor();

    auto sendOwnership = [&](const TActorId& peer, const TString& owner) {
        auto request = MakeHolder<TEvPersQueue::TEvRequest>();
        auto* req = request->Record.MutablePartitionRequest();
        req->SetPartition(0);
        req->MutableCmdGetOwnership()->SetOwner(owner);
        req->MutableCmdGetOwnership()->SetForce(true);
        // Logical holder id, as pipe-cache clients would set.
        ActorIdToProto(peer, req->MutablePipeClient());

        tc.Runtime->SendToPipe(
            tc.TabletId,
            tc.Edge,
            request.Release(),
            0,
            GetPipeConfigWithRetries(),
            physicalPipe);
    };

    sendOwnership(peer1, "owner-a");
    auto resp1 = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(resp1);
    UNIT_ASSERT_EQUAL(resp1->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
    UNIT_ASSERT(resp1->Record.GetPartitionResponse().HasCmdGetOwnershipResult());

    sendOwnership(peer2, "owner-b");
    auto resp2 = tc.Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(TDuration::Seconds(10));
    UNIT_ASSERT(resp2);
    UNIT_ASSERT_EQUAL(resp2->Record.GetErrorCode(), NPersQueue::NErrorCode::OK);
    UNIT_ASSERT(resp2->Record.GetPartitionResponse().HasCmdGetOwnershipResult());

    tc.Runtime->ClosePipe(physicalPipe, tc.Edge, 0);
    tc.Runtime->DispatchEvents();
}

Y_UNIT_TEST(DedicatedOwnershipStillWorks) {
    TTestContext tc;
    TFinalizer finalizer(tc);
    tc.Prepare();
    tc.Runtime->SetScheduledLimit(10000);

    PQTabletPrepare({}, {}, tc);

    auto [cookie, pipe] = CmdSetOwner(0, tc, "default-owner", true);
    UNIT_ASSERT(!cookie.empty());
    UNIT_ASSERT(pipe);
}

} // Y_UNIT_TEST_SUITE(TPqTabletPipeCache)

} // namespace NKikimr::NPQ
