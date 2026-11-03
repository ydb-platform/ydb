#include <ydb/core/util/async_backoff.h>
#include <ydb/library/actors/async/cancellation.h>
#include <ydb/library/actors/async/timeout.h>
#include <ydb/library/actors/async/ut/common.h>

namespace NKikimr {
namespace {

using namespace NActors;
using namespace NAsyncTest;

struct TFixture {
    static constexpr TDuration RequestTimeout = TDuration::Seconds(10);

    TAsyncBackoff Backoff;
    TVector<ui64> Admitted;
    size_t ScheduledWakeups = 0;
    TAsyncTestActor::TState State;
    TAsyncTestActorRuntime Runtime;
    TAsyncTestActorRuntime::TAsyncActorOperations Actor;

    explicit TFixture(TDuration initial = TDuration::Seconds(1), TDuration maximum = TDuration::Seconds(8))
        : Backoff(initial, maximum)
        , Actor(Runtime.StartAsyncActor(State))
    {
        Runtime.SetScheduledEventFilter([this](auto&, auto& ev, auto, auto&) {
            if (ev->GetTypeRewrite() == TEvents::TSystem::ResumeRunnable) {
                ++ScheduledWakeups;
            }
            return false;
        });
    }

    ~TFixture() {
        if (!State.Destroyed) {
            Actor.Poison();
        }
    }

    void Fail() {
        Actor.RunSync([this] {
            UNIT_ASSERT(Backoff.OnFailure(Backoff.GetVersion()) == TAsyncBackoff::EUpdate::Applied);
        });
    }

    void Wait() {
        Actor.RunAsync([this]() -> async<void> {
            Admitted.push_back(co_await Backoff.Wait(RequestTimeout));
        });
    }

    void AdvanceTo(TMonotonic when) {
        UNIT_ASSERT(when >= Runtime.GetCurrentMonotonicTime());
        Runtime.SimulateSleep(when - Runtime.GetCurrentMonotonicTime());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(AsyncBackoff) {
    Y_UNIT_TEST(HealthyCallsRemainConcurrent) {
        TFixture f;
        for (size_t i = 0; i < 20; ++i) {
            f.Wait();
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 20);
        for (ui64 version : f.Admitted) {
            UNIT_ASSERT_VALUES_EQUAL(version, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.ScheduledWakeups, 0);
    }

    Y_UNIT_TEST(CooldownAdmitsOnlyOneCallAtTheExactBoundary) {
        TFixture f;
        f.Fail();
        const ui64 failedVersion = f.Backoff.GetVersion();
        const auto due = f.Backoff.GetNextAllowedAt();
        for (size_t i = 0; i < 100; ++i) {
            f.Wait();
        }
        UNIT_ASSERT(f.Admitted.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.ScheduledWakeups, 1);
        f.AdvanceTo(due - TDuration::MicroSeconds(1));
        UNIT_ASSERT(f.Admitted.empty());
        f.AdvanceTo(due);
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.front(), failedVersion + 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Backoff.GetNextAllowedAt(), due + TFixture::RequestTimeout);
    }

    Y_UNIT_TEST(StaleAndDuplicateOutcomesCannotRescheduleRecovery) {
        TFixture f;
        f.Fail();
        const auto version = f.Backoff.GetVersion();
        const auto due = f.Backoff.GetNextAllowedAt();
        f.Actor.RunSync([&] {
            UNIT_ASSERT(f.Backoff.OnFailure(0) == TAsyncBackoff::EUpdate::Stale);
            UNIT_ASSERT(f.Backoff.OnSuccess(0) == TAsyncBackoff::EUpdate::Stale);
        });
        UNIT_ASSERT_VALUES_EQUAL(f.Backoff.GetVersion(), version);
        UNIT_ASSERT_VALUES_EQUAL(f.Backoff.GetNextAllowedAt(), due);
        f.Wait();
        f.AdvanceTo(due);
        f.Fail();
        const auto next = f.Backoff.GetNextAllowedAt();
        f.Actor.RunSync([&] {
            UNIT_ASSERT(f.Backoff.OnFailure(f.Admitted.front()) == TAsyncBackoff::EUpdate::Stale);
            UNIT_ASSERT(f.Backoff.OnSuccess(f.Admitted.front()) == TAsyncBackoff::EUpdate::Stale);
        });
        UNIT_ASSERT_VALUES_EQUAL(f.Backoff.GetNextAllowedAt(), next);
    }

    Y_UNIT_TEST(SuccessWakesAllWaitersAndResetsTheStandardBackoff) {
        TFixture f;
        f.Fail();
        f.Wait();
        f.AdvanceTo(f.Backoff.GetNextAllowedAt());
        const auto probe = f.Admitted.front();
        f.Wait();
        f.Wait();
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
        f.Actor.RunSync([&] {
            UNIT_ASSERT(f.Backoff.OnSuccess(probe) == TAsyncBackoff::EUpdate::Applied);
        });
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 3);
        f.Actor.RunSync([&] {
            const auto version = f.Backoff.GetVersion();
            UNIT_ASSERT(f.Backoff.OnSuccess(version) == TAsyncBackoff::EUpdate::Applied);
            UNIT_ASSERT_VALUES_EQUAL(f.Backoff.GetVersion(), version);
        });
        f.Fail();
        const auto delay = f.Backoff.GetNextAllowedAt() - f.Runtime.GetCurrentMonotonicTime();
        UNIT_ASSERT(delay >= TDuration::Seconds(1));
        UNIT_ASSERT(delay < TDuration::MilliSeconds(1250));
    }

    Y_UNIT_TEST(MissingOutcomeAllowsAnotherCallAfterTheRequestTimeout) {
        TFixture f;
        f.Fail();
        f.Wait();
        f.Wait();
        f.AdvanceTo(f.Backoff.GetNextAllowedAt());
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
        const auto next = f.Backoff.GetNextAllowedAt();
        f.AdvanceTo(next - TDuration::MicroSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
        f.AdvanceTo(next);
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 2);
        UNIT_ASSERT(f.Admitted[1] > f.Admitted[0]);
        f.Actor.RunSync([&] {
            UNIT_ASSERT(f.Backoff.OnSuccess(f.Admitted[0]) == TAsyncBackoff::EUpdate::Stale);
        });
    }

    Y_UNIT_TEST(CancellingTheTimerWaiterTransfersTheWakeup) {
        TFixture f;
        f.Fail();
        TAsyncCancellationScope cancel;
        bool cancelled = false;
        f.Actor.RunAsync([&]() -> async<void> {
            cancelled = !(co_await cancel.Wrap([&]() -> async<void> {
                f.Admitted.push_back(co_await f.Backoff.Wait(TFixture::RequestTimeout));
            }));
        });
        f.Wait();
        f.Actor.RunSync([&] { cancel.Cancel(); });
        UNIT_ASSERT(cancelled);
        UNIT_ASSERT(f.Admitted.empty());
        f.AdvanceTo(f.Backoff.GetNextAllowedAt());
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
    }

    Y_UNIT_TEST(CallerDeadlineDoesNotAdmitOrStrandOtherWaiters) {
        TFixture f;
        f.Fail();
        std::optional<ui64> result;
        bool finished = false;
        f.Actor.RunAsync([&]() -> async<void> {
            result = co_await WithTimeout(TDuration::MilliSeconds(100),
                f.Backoff.Wait(TFixture::RequestTimeout));
            finished = true;
        });
        f.Wait();
        f.Runtime.SimulateSleep(TDuration::MilliSeconds(100));
        UNIT_ASSERT(finished);
        UNIT_ASSERT(!result);
        UNIT_ASSERT(f.Admitted.empty());
        f.AdvanceTo(f.Backoff.GetNextAllowedAt());
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
    }

    Y_UNIT_TEST(ActorDeathDisarmsLateWakeups) {
        TFixture f;
        f.Fail();
        f.Wait();
        f.Wait();
        f.Actor.Poison();
        UNIT_ASSERT(f.State.Destroyed);
        f.Runtime.SimulateSleep(TDuration::Minutes(1));
        UNIT_ASSERT(f.Admitted.empty());
    }

    Y_UNIT_TEST(HardMailboxTeardownDestroysWaitersWithoutActorContext) {
        TFixture f;
        f.Fail();
        f.Wait();
        f.Wait();
        f.Runtime.CleanupNode();
        UNIT_ASSERT(f.State.Destroyed);
        UNIT_ASSERT(f.Admitted.empty());
    }

    Y_UNIT_TEST(CancellingAllWaitersAllowsAFreshWaiter) {
        TFixture f;
        f.Fail();
        TAsyncCancellationScope cancel;
        for (size_t i = 0; i < 3; ++i) {
            f.Actor.RunAsync([&]() -> async<void> {
                co_await cancel.Wrap([&]() -> async<void> {
                    f.Admitted.push_back(co_await f.Backoff.Wait(TFixture::RequestTimeout));
                });
            });
        }
        f.Actor.RunSync([&] { cancel.Cancel(); });
        f.Wait();
        UNIT_ASSERT(f.Admitted.empty());
        f.AdvanceTo(f.Backoff.GetNextAllowedAt());
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
    }

    Y_UNIT_TEST(RetiredTimerCannotAdmitACallInANewerCooldown) {
        TFixture f(TDuration::Seconds(20), TDuration::Seconds(20));
        f.Fail();
        f.Wait();
        f.Wait();
        f.AdvanceTo(f.Backoff.GetNextAllowedAt());
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
        const auto retiredDeadline = f.Backoff.GetNextAllowedAt();
        f.Fail();
        const auto due = f.Backoff.GetNextAllowedAt();
        UNIT_ASSERT(due > retiredDeadline);
        f.AdvanceTo(retiredDeadline);
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 1);
        f.AdvanceTo(due);
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 2);
    }

    Y_UNIT_TEST(SharedFailuresUseStandardJitterAndCap) {
        TFixture f;
        const ui64 baseMilliseconds[] = {1000, 1000, 2000, 3000, 5000, 8000, 8000, 8000};
        for (auto base : baseMilliseconds) {
            f.Fail();
            const auto delay = f.Backoff.GetNextAllowedAt() - f.Runtime.GetCurrentMonotonicTime();
            UNIT_ASSERT(delay >= TDuration::MilliSeconds(base));
            UNIT_ASSERT(delay < TDuration::MilliSeconds(base + base / 4));
            f.Wait();
            f.AdvanceTo(f.Backoff.GetNextAllowedAt());
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Admitted.size(), 8);
    }
}

} // namespace NKikimr
