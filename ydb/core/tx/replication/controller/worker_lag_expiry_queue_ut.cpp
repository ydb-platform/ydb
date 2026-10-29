#include "worker_lag_expiry_queue.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NReplication::NController {

Y_UNIT_TEST_SUITE(WorkerLagExpiryQueue) {
    using NMonotonic::TMonotonic;

    Y_UNIT_TEST(RefreshReplacesDeadlineWithoutGrowingQueue) {
        TWorkerLagExpiryQueue queue;
        const TWorkerId worker(1, 1, 1);
        for (ui32 i = 0; i < 10000; ++i) {
            queue.SetDeadline(worker, TMonotonic::Seconds(60 + i));
            UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 1);
        }

        UNIT_ASSERT(!queue.PopExpired(TMonotonic::Seconds(10058)));
        UNIT_ASSERT(queue.PopExpired(TMonotonic::Seconds(10059)) == worker);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 0);
        UNIT_ASSERT(!queue.GetNextDeadline());
        UNIT_ASSERT(!queue.PopExpired(TMonotonic::Max()));
    }

    Y_UNIT_TEST(EqualDeadlinesSurviveRefreshAndRemoval) {
        TWorkerLagExpiryQueue queue;
        const TWorkerId first(1, 1, 1);
        const TWorkerId second(1, 1, 2);
        const TWorkerId third(1, 2, 1);
        const auto deadline = TMonotonic::Seconds(60);
        queue.SetDeadline(first, deadline);
        queue.SetDeadline(second, deadline);
        queue.SetDeadline(third, deadline);
        queue.SetDeadline(first, TMonotonic::Seconds(90));
        queue.Remove(second);

        UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 2);
        UNIT_ASSERT(queue.GetNextDeadline() == deadline);
        UNIT_ASSERT(queue.PopExpired(deadline) == third);
        UNIT_ASSERT(!queue.PopExpired(deadline));
        UNIT_ASSERT(queue.PopExpired(TMonotonic::Seconds(90)) == first);
        UNIT_ASSERT(!queue.GetNextDeadline());
    }

    Y_UNIT_TEST(RefreshMovesWorkerPastOtherDeadlines) {
        TWorkerLagExpiryQueue queue;
        const TWorkerId first(1, 1, 1);
        const TWorkerId second(1, 1, 2);
        const TWorkerId third(1, 1, 3);
        queue.SetDeadline(first, TMonotonic::Seconds(60));
        queue.SetDeadline(second, TMonotonic::Seconds(70));
        queue.SetDeadline(third, TMonotonic::Seconds(80));
        queue.SetDeadline(second, TMonotonic::Seconds(90));
        UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 3);
        UNIT_ASSERT(queue.PopExpired(TMonotonic::Seconds(60)) == first);
        UNIT_ASSERT(!queue.PopExpired(TMonotonic::Seconds(70)));
        UNIT_ASSERT(queue.PopExpired(TMonotonic::Seconds(80)) == third);
        UNIT_ASSERT(queue.PopExpired(TMonotonic::Seconds(90)) == second);
        UNIT_ASSERT(!queue.GetNextDeadline());
    }

    Y_UNIT_TEST(RemoveClearsBothIndicesBeforeWorkerIsReused) {
        TWorkerLagExpiryQueue queue;
        const TWorkerId worker(1, 1, 1);
        queue.SetDeadline(worker, TMonotonic::Seconds(60));
        queue.Remove(worker);
        queue.Remove(worker);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 0);
        UNIT_ASSERT(!queue.GetNextDeadline());
        queue.SetDeadline(worker, TMonotonic::Seconds(90));
        UNIT_ASSERT(!queue.PopExpired(TMonotonic::Seconds(60)));
        UNIT_ASSERT(queue.PopExpired(TMonotonic::Seconds(90)) == worker);
        UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 0);
    }

    Y_UNIT_TEST(PopsOnlyDueWorkersAndPreservesFutureEntries) {
        TWorkerLagExpiryQueue queue;
        for (ui32 i = 0; i < 1000; ++i) {
            queue.SetDeadline(TWorkerId(1, 1, i), TMonotonic::Seconds(i < 3 ? 60 : 120));
        }
        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT(queue.PopExpired(TMonotonic::Seconds(60)));
        }
        UNIT_ASSERT(!queue.PopExpired(TMonotonic::Seconds(60)));
        UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 997);
        UNIT_ASSERT(queue.GetNextDeadline() == TMonotonic::Seconds(120));
        queue.Clear();
        UNIT_ASSERT_VALUES_EQUAL(queue.GetSize(), 0);
        UNIT_ASSERT(!queue.PopExpired(TMonotonic::Max()));
        queue.SetDeadline(TWorkerId(1, 1, 0), TMonotonic::Seconds(180));
        UNIT_ASSERT(!queue.PopExpired(TMonotonic::Seconds(120)));
    }
}

}
