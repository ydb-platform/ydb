#include <ydb/services/workload_manager/actors/classifier_metadata_tracker.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager::NPrivate {

namespace {

constexpr TDuration TIMEOUT = TDuration::Seconds(5);
const TInstant T0 = TInstant::Seconds(1000);

}

Y_UNIT_TEST_SUITE(ClassifierMetadataTracker) {
    // Metadata service disabled. The state:
    // - is ready at once,
    // - never in flight,
    // - never times out or requeries.
    Y_UNIT_TEST(TestServiceDisabledIsReady) {
        TClassifierMetadataTracker tracker(TIMEOUT);
        tracker.Init(false, T0);
        UNIT_ASSERT(!tracker.OnPoolsEnabled(true, T0));

        UNIT_ASSERT(tracker.GetState() == EMetadataState::Ready);
        UNIT_ASSERT(!tracker.IsInFlight());
        UNIT_ASSERT(!tracker.TimeOutPending(T0 + TIMEOUT * 2));
        UNIT_ASSERT(!tracker.NeedsRequery(T0 + TIMEOUT * 2));
    }

    // Metadata service enabled. The request:
    // - is not in flight and never times out while pools are off,
    // - starts its timeout when pools are enabled, not at init,
    // - becomes TimedOut once Pending past the limit.
    Y_UNIT_TEST(TestTimeoutAfterPoolsEnabled) {
        TClassifierMetadataTracker tracker(TIMEOUT);
        tracker.Init(true, T0);
        UNIT_ASSERT(!tracker.IsInFlight());
        UNIT_ASSERT(!tracker.TimeOutPending(T0 + TIMEOUT * 2));

        const TInstant enabledAt = T0 + TIMEOUT * 3;
        UNIT_ASSERT(tracker.OnPoolsEnabled(true, enabledAt));
        UNIT_ASSERT(!tracker.OnPoolsEnabled(true, enabledAt));
        UNIT_ASSERT(tracker.IsInFlight());

        UNIT_ASSERT(!tracker.TimeOutPending(enabledAt + TIMEOUT / 2));
        UNIT_ASSERT(tracker.GetState() == EMetadataState::Pending);

        UNIT_ASSERT(tracker.TimeOutPending(enabledAt + TIMEOUT * 2));
        UNIT_ASSERT(tracker.GetState() == EMetadataState::TimedOut);
        UNIT_ASSERT(!tracker.IsInFlight());
    }

    // Metadata request timed out. The state:
    // - requeries once per timeout, staying TimedOut,
    // - recovers to Ready on OnReady,
    // - never requeries once Ready.
    Y_UNIT_TEST(TestRequeryAndRecover) {
        TClassifierMetadataTracker tracker(TIMEOUT);
        tracker.Init(true, T0);
        UNIT_ASSERT(tracker.OnPoolsEnabled(true, T0));
        const TInstant timedOutAt = T0 + TIMEOUT * 2;
        UNIT_ASSERT(tracker.TimeOutPending(timedOutAt));

        UNIT_ASSERT(!tracker.NeedsRequery(timedOutAt + TIMEOUT / 2));

        const TInstant requeryAt = timedOutAt + TIMEOUT * 2;
        UNIT_ASSERT(tracker.NeedsRequery(requeryAt));
        UNIT_ASSERT(!tracker.NeedsRequery(requeryAt));
        UNIT_ASSERT(tracker.GetState() == EMetadataState::TimedOut);

        UNIT_ASSERT(tracker.OnReady(requeryAt));
        UNIT_ASSERT(tracker.GetState() == EMetadataState::Ready);
        UNIT_ASSERT(!tracker.OnReady(requeryAt));
        UNIT_ASSERT(!tracker.NeedsRequery(requeryAt + TIMEOUT * 2));
    }
}

}
