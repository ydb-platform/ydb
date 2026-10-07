#include <ydb/public/sdk/cpp/src/client/topic/impl/deferred_publication_ack_tracker.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_session.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTopic;

Y_UNIT_TEST_SUITE(DeferredPublicationAckState) {

Y_UNIT_TEST(EmptyWaitDoesNotSeal) {
    TDeferredPublicationAckState state;
    auto wait = state.WaitAllAcks();
    UNIT_ASSERT(wait.HasValue());
    UNIT_ASSERT(wait.GetValue().IsSuccess());
    UNIT_ASSERT(!state.IsSealed());
    UNIT_ASSERT(state.TryOnWrite());
}

Y_UNIT_TEST(WaitAllAcksRetryAfterAbort) {
    TDeferredPublicationAckState state;
    UNIT_ASSERT(state.TryOnWrite());

    auto firstWait = state.WaitAllAcks();
    UNIT_ASSERT(state.IsSealed());
    UNIT_ASSERT(!firstWait.HasValue());

    state.OnUnackedAbort(1);
    UNIT_ASSERT(firstWait.HasValue());
    UNIT_ASSERT(!firstWait.GetValue().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(firstWait.GetValue().GetStatus(), EStatus::SESSION_EXPIRED);
    UNIT_ASSERT_STRING_CONTAINS(
        firstWait.GetValue().GetIssues().ToString(),
        "Cannot finalize deferred publication: associated write session was aborted");
    UNIT_ASSERT(!state.IsSealed());

    UNIT_ASSERT(state.TryOnWrite());
    auto secondWait = state.WaitAllAcks();
    UNIT_ASSERT_C(!secondWait.HasValue(), "retry must wait for the new backlog");
    UNIT_ASSERT(state.IsSealed());

    state.OnAck();
    UNIT_ASSERT(secondWait.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(secondWait.GetValue().IsSuccess());
}

Y_UNIT_TEST(WriteRejectedAfterSuccessfulWait) {
    TDeferredPublicationAckState state;
    UNIT_ASSERT(state.TryOnWrite());
    state.OnAck();

    auto wait = state.WaitAllAcks();
    UNIT_ASSERT(wait.HasValue());
    UNIT_ASSERT(wait.GetValue().IsSuccess());
    UNIT_ASSERT(state.IsSealed());
    UNIT_ASSERT(!state.TryOnWrite());
}

Y_UNIT_TEST(WaitAllAcksAfterSuccessStartsNewWait) {
    TDeferredPublicationAckState state;
    UNIT_ASSERT(state.TryOnWrite());
    state.OnAck();

    auto firstWait = state.WaitAllAcks();
    UNIT_ASSERT(firstWait.HasValue());
    UNIT_ASSERT(firstWait.GetValue().IsSuccess());

    // Second finalize wait is allowed; new writes are not.
    auto secondWait = state.WaitAllAcks();
    UNIT_ASSERT(secondWait.HasValue());
    UNIT_ASSERT(secondWait.GetValue().IsSuccess());
    UNIT_ASSERT(!state.TryOnWrite());
}

Y_UNIT_TEST(ConcurrentWaitersShareInFlightFuture) {
    TDeferredPublicationAckState state;
    UNIT_ASSERT(state.TryOnWrite());

    auto firstWait = state.WaitAllAcks();
    auto secondWait = state.WaitAllAcks();
    UNIT_ASSERT(!firstWait.HasValue());
    UNIT_ASSERT(!secondWait.HasValue());

    state.OnAck();
    UNIT_ASSERT(firstWait.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(secondWait.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(firstWait.GetValue().IsSuccess());
    UNIT_ASSERT(secondWait.GetValue().IsSuccess());
}

Y_UNIT_TEST(AbortWithoutWaitOnlyDropsUnackedCount) {
    TDeferredPublicationAckState state;
    UNIT_ASSERT(state.TryOnWrite());
    state.OnUnackedAbort(1);

    auto wait = state.WaitAllAcks();
    UNIT_ASSERT(wait.HasValue());
    UNIT_ASSERT(wait.GetValue().IsSuccess());
    UNIT_ASSERT(!state.IsSealed());
}

Y_UNIT_TEST(IndependentHandleDoesNotWaitForOtherWrites) {
    TDeferredPublication writeHandle(42);
    TDeferredPublication publishHandle(42);
    UNIT_ASSERT(TDeferredPublication::TAccess::AckState(writeHandle).get()
        != TDeferredPublication::TAccess::AckState(publishHandle).get());

    UNIT_ASSERT(TDeferredPublication::TAccess::AckState(writeHandle)->TryOnWrite());

    auto wait = TDeferredPublication::TAccess::AckState(publishHandle)->WaitAllAcks();
    UNIT_ASSERT(wait.HasValue());
    UNIT_ASSERT(wait.GetValue().IsSuccess());
    UNIT_ASSERT(!TDeferredPublication::TAccess::AckState(publishHandle)->IsSealed());
    UNIT_ASSERT(!TDeferredPublication::TAccess::AckState(writeHandle)->IsSealed());
}

Y_UNIT_TEST(CopySharesAckState) {
    TDeferredPublication a(42);
    TDeferredPublication b = a;
    UNIT_ASSERT(TDeferredPublication::TAccess::AckState(a)
        == TDeferredPublication::TAccess::AckState(b));

    UNIT_ASSERT(TDeferredPublication::TAccess::AckState(a)->TryOnWrite());
    auto wait = TDeferredPublication::TAccess::AckState(b)->WaitAllAcks();
    UNIT_ASSERT(!wait.HasValue());
    TDeferredPublication::TAccess::AckState(a)->OnAck();
    UNIT_ASSERT(wait.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(wait.GetValue().IsSuccess());
}

Y_UNIT_TEST(MoveTransfersAckState) {
    TDeferredPublication a(42, "ext-42");
    UNIT_ASSERT(TDeferredPublication::TAccess::AckState(a)->TryOnWrite());

    TDeferredPublication b = std::move(a);
    UNIT_ASSERT_VALUES_EQUAL(b.IntPublicationId, 42u);
    UNIT_ASSERT(b.ExtPublicationId.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*b.ExtPublicationId, "ext-42");
    UNIT_ASSERT(TDeferredPublication::TAccess::AckState(a).get()
        != TDeferredPublication::TAccess::AckState(b).get());

    auto waitB = TDeferredPublication::TAccess::AckState(b)->WaitAllAcks();
    UNIT_ASSERT(!waitB.HasValue());
    TDeferredPublication::TAccess::AckState(b)->OnAck();
    UNIT_ASSERT(waitB.Wait(TDuration::Seconds(1)));

    // Moved-from: ids cleared; ack state is created lazily on TAccess (empty, not sealed).
    UNIT_ASSERT_VALUES_EQUAL(a.IntPublicationId, 0u);
    UNIT_ASSERT(!a.ExtPublicationId.has_value());
    auto waitA = TDeferredPublication::TAccess::AckState(a)->WaitAllAcks();
    UNIT_ASSERT(waitA.HasValue());
    UNIT_ASSERT(waitA.GetValue().IsSuccess());
    UNIT_ASSERT(!TDeferredPublication::TAccess::AckState(a)->IsSealed());
}

} // Y_UNIT_TEST_SUITE
