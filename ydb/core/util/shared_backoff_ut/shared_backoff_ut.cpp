#include <ydb/core/util/shared_backoff.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
using NActors::TMonotonic;

Y_UNIT_TEST_SUITE(SharedBackoff) {
    Y_UNIT_TEST(HealthyCallsRemainConcurrent) {
        TSharedBackoff gate;
        for (size_t i = 0; i < 100; ++i) {
            auto version = gate.TryAcquire(TMonotonic::Seconds(100), TDuration::Seconds(10));
            UNIT_ASSERT(version);
            UNIT_ASSERT_VALUES_EQUAL(*version, 0);
        }
    }

    Y_UNIT_TEST(ExactlyOneRecoveryCallAtTheBoundary) {
        TSharedBackoff gate;
        const auto now = TMonotonic::Seconds(100);
        UNIT_ASSERT(gate.OnFailure(0, now) == TSharedBackoff::EUpdate::Applied);
        const auto due = gate.GetNextAllowedAt();
        UNIT_ASSERT(!gate.TryAcquire(due - TDuration::MicroSeconds(1), TDuration::Seconds(10)));
        const auto probe = gate.TryAcquire(due, TDuration::Seconds(10));
        UNIT_ASSERT(probe);
        UNIT_ASSERT_VALUES_EQUAL(*probe, 2);
        UNIT_ASSERT(!gate.TryAcquire(due, TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(StaleOutcomesCannotChangeNewerFailure) {
        TSharedBackoff gate;
        const auto now = TMonotonic::Seconds(100);
        gate.OnFailure(0, now);
        const auto due = gate.GetNextAllowedAt();
        UNIT_ASSERT(gate.OnSuccess(0) == TSharedBackoff::EUpdate::Stale);
        UNIT_ASSERT(gate.OnFailure(0, now) == TSharedBackoff::EUpdate::Stale);
        UNIT_ASSERT_VALUES_EQUAL(gate.GetNextAllowedAt(), due);
        UNIT_ASSERT_VALUES_EQUAL(gate.GetVersion(), 1);
    }

    Y_UNIT_TEST(SuccessReleasesEveryWaitingCaller) {
        TSharedBackoff gate;
        gate.OnFailure(0, TMonotonic::Seconds(100));
        const auto now = gate.GetNextAllowedAt();
        const auto probe = gate.TryAcquire(now, TDuration::Seconds(10));
        UNIT_ASSERT(probe);
        UNIT_ASSERT(gate.OnSuccess(*probe) == TSharedBackoff::EUpdate::Applied);
        for (size_t i = 0; i < 100; ++i) {
            UNIT_ASSERT(gate.TryAcquire(now, TDuration::Seconds(10)));
        }
    }

    Y_UNIT_TEST(MissingOutcomeReservationIsFiniteAndFenced) {
        TSharedBackoff gate;
        gate.OnFailure(0, TMonotonic::Seconds(100));
        const auto first = gate.TryAcquire(gate.GetNextAllowedAt(), TDuration::Seconds(10));
        UNIT_ASSERT(first);
        const auto due = gate.GetNextAllowedAt();
        UNIT_ASSERT(!gate.TryAcquire(due - TDuration::MicroSeconds(1), TDuration::Seconds(10)));
        const auto next = gate.TryAcquire(due, TDuration::Seconds(10));
        UNIT_ASSERT(next);
        UNIT_ASSERT(*next > *first);
        UNIT_ASSERT(gate.OnSuccess(*first) == TSharedBackoff::EUpdate::Stale);
        UNIT_ASSERT(gate.OnFailure(*first, due) == TSharedBackoff::EUpdate::Stale);
    }

    Y_UNIT_TEST(RetiredWakeupCannotAdmitDuringNewerCooldown) {
        TSharedBackoff gate(TDuration::Seconds(20), TDuration::Seconds(20));
        gate.OnFailure(0, TMonotonic::Seconds(100));
        auto now = gate.GetNextAllowedAt();
        const auto first = gate.TryAcquire(now, TDuration::Seconds(10));
        UNIT_ASSERT(first);
        const auto retired = gate.GetNextAllowedAt();
        gate.OnFailure(*first, now);
        UNIT_ASSERT(gate.GetNextAllowedAt() > retired);
        UNIT_ASSERT(!gate.TryAcquire(retired, TDuration::Seconds(10)));
        UNIT_ASSERT(gate.TryAcquire(gate.GetNextAllowedAt(), TDuration::Seconds(10)));
    }

    Y_UNIT_TEST(SharedFailuresUseStandardJitterAndCap) {
        TSharedBackoff gate(TDuration::Seconds(1), TDuration::Seconds(8));
        auto now = TMonotonic::Seconds(100);
        ui64 version = 0;
        for (const ui64 milliseconds : {1000, 1000, 2000, 3000, 5000, 8000, 8000, 8000}) {
            gate.OnFailure(version, now);
            const auto delay = gate.GetNextAllowedAt() - now;
            UNIT_ASSERT(delay >= TDuration::MilliSeconds(milliseconds));
            UNIT_ASSERT(delay < TDuration::MilliSeconds(milliseconds + milliseconds / 4));
            now = gate.GetNextAllowedAt();
            const auto probe = gate.TryAcquire(now, TDuration::Seconds(10));
            UNIT_ASSERT(probe);
            version = *probe;
        }
    }
}
} // namespace NKikimr
