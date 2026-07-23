#include <ydb/public/sdk/cpp/src/client/topic/impl/deferred_publish_ack_tracker.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NTopic;

Y_UNIT_TEST_SUITE(DeferredPublicationAckState) {

Y_UNIT_TEST(WaitAllAcksRetryAfterAbort) {
    TDeferredPublicationAckState state;
    state.OnWrite();

    auto firstWait = state.WaitAllAcks();
    UNIT_ASSERT(!firstWait.HasValue());

    state.OnUnackedAbort(1);
    UNIT_ASSERT(firstWait.HasValue());
    UNIT_ASSERT(!firstWait.GetValue().IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(firstWait.GetValue().GetStatus(), EStatus::SESSION_EXPIRED);

    state.OnWrite();
    auto secondWait = state.WaitAllAcks();
    UNIT_ASSERT_C(!secondWait.HasValue(), "retry must wait for the new backlog, not reuse failed future");

    state.OnAck();
    UNIT_ASSERT(secondWait.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(secondWait.GetValue().IsSuccess());
}

Y_UNIT_TEST(WaitAllAcksAfterSuccessStartsNewWait) {
    TDeferredPublicationAckState state;
    state.OnWrite();
    state.OnAck();

    auto firstWait = state.WaitAllAcks();
    UNIT_ASSERT(firstWait.HasValue());
    UNIT_ASSERT(firstWait.GetValue().IsSuccess());

    state.OnWrite();
    auto secondWait = state.WaitAllAcks();
    UNIT_ASSERT(!secondWait.HasValue());

    state.OnAck();
    UNIT_ASSERT(secondWait.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(secondWait.GetValue().IsSuccess());
}

Y_UNIT_TEST(WaitAllAcksEmptyThenWriteRequiresNewWait) {
    TDeferredPublicationAckState state;

    auto firstWait = state.WaitAllAcks();
    UNIT_ASSERT(firstWait.HasValue());
    UNIT_ASSERT(firstWait.GetValue().IsSuccess());

    state.OnWrite();
    auto secondWait = state.WaitAllAcks();
    UNIT_ASSERT(!secondWait.HasValue());

    state.OnAck();
    UNIT_ASSERT(secondWait.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(secondWait.GetValue().IsSuccess());
}

Y_UNIT_TEST(ConcurrentWaitersShareInFlightFuture) {
    TDeferredPublicationAckState state;
    state.OnWrite();

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
    state.OnWrite();
    state.OnUnackedAbort(1);

    auto wait = state.WaitAllAcks();
    UNIT_ASSERT(wait.HasValue());
    UNIT_ASSERT(wait.GetValue().IsSuccess());
}

} // Y_UNIT_TEST_SUITE
