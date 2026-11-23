#include <ydb/library/lockfree_bucket/light_bucket.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <barrier>
#include <limits>
#include <thread>
#include <vector>

namespace NKikimr {
namespace {

constexpr ui64 Second = 1'000'000'000;
struct TTestClock final : ITimeProvider {
    std::atomic<ui64> TimeUs{0};

    TInstant Now() override {
        return TInstant::MicroSeconds(TimeUs.load(std::memory_order_relaxed));
    }

    void SetTimeNs(ui64 nowNs) {
        TimeUs.store(nowNs / 1000, std::memory_order_relaxed);
    }
};

void CheckStats(TLockFreeLightBucket& bucket, ui64 expectedGreen, ui64 expectedRed) {
    const auto [green, red] = bucket.OverflowStats();
    UNIT_ASSERT_VALUES_EQUAL(green, expectedGreen);
    UNIT_ASSERT_VALUES_EQUAL(red, expectedRed);
}

} // namespace

Y_UNIT_TEST_SUITE(TLockFreeLightBucketTest) {
    Y_UNIT_TEST(SplitsAnIdleIntervalAtRecovery) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        clock->SetTimeNs(Second);
        TLockFreeLightBucket bucket(20, 10, clock);
        clock->SetTimeNs(5 * Second);
        bucket.Take(50); // +20 to -30 at the end of four green seconds.
        CheckStats(bucket, 4 * Second, 0);
        clock->SetTimeNs(10 * Second);
        CheckStats(bucket, 6 * Second, 3 * Second);
        CheckStats(bucket, 6 * Second, 3 * Second);
        // A delayed clock reading cannot count a previously claimed interval.
        clock->SetTimeNs(9 * Second);
        bucket.Take(20);
        clock->SetTimeNs(11 * Second);
        CheckStats(bucket, 7 * Second, 3 * Second);
    }

    Y_UNIT_TEST(RefillCanRecoverBeforeAnotherWithdrawal) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(20, 10, clock);
        bucket.Take(35); // -15
        clock->SetTimeNs(4 * Second);
        bucket.Take(25); // Refill reaches the cap, then the take leaves -5.
        CheckStats(bucket, 5 * Second / 2, 3 * Second / 2);
        clock->SetTimeNs(8 * Second);
        bucket.Take(5); // -5 to +15, with half a second spent red.
        CheckStats(bucket, 6 * Second, 2 * Second);
    }

    Y_UNIT_TEST(RefillUsesOnlyTheElapsedInterval) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(10, 3, clock);
        bucket.Take(11);
        clock->SetTimeNs(250'000'000);
        CheckStats(bucket, 0, 250'000'000);
        // The first poll discarded 0.75 tokens. Recovery takes another 1/3 second.
        clock->SetTimeNs(750'000'000);
        CheckStats(bucket, 166'666'666, 583'333'334);
    }

    Y_UNIT_TEST(ConfigurationSettlesTheOldInterval) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(12, 0, clock);
        bucket.Take(12);
        clock->SetTimeNs(Second);
        bucket.Take(6);
        clock->SetTimeNs(2 * Second);
        bucket.SetReplenishmentRate(3);
        clock->SetTimeNs(3 * Second);
        bucket.SetReplenishmentRate(6);
        clock->SetTimeNs(4 * Second);
        CheckStats(bucket, Second + Second / 2, 2 * Second + Second / 2);
        bucket.SetCapacity(1);
        bucket.SetCapacity(10);
        bucket.Take(2);
        bucket.SetReplenishmentRate(0);
        clock->SetTimeNs(5 * Second);
        CheckStats(bucket, Second + Second / 2, 3 * Second + Second / 2);
    }

    Y_UNIT_TEST(DebtLimitFollowsCapacity) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(100, 1'000, clock, TLockFreeLightBucket::EDebtLimit::TwiceCapacity);
        bucket.Take(10'000);
        bucket.Take(10'000);
        clock->SetTimeNs(Second);
        CheckStats(bucket, 800'000'000, 200'000'000);

        bucket.Take(std::numeric_limits<ui64>::max());
        clock->SetTimeNs(Second + 50'000'000);
        bucket.SetCapacity(25); // The outstanding debt falls from 150 to 50.
        clock->SetTimeNs(2 * Second);
        CheckStats(bucket, 1'700'000'000, 300'000'000);

        bucket.SetCapacity(0);
        bucket.Take(10'000);
        clock->SetTimeNs(3 * Second);
        CheckStats(bucket, 2'700'000'000, 300'000'000);
    }

    Y_UNIT_TEST(SaturatesAtSignedBalanceLimits) {
        constexpr ui64 maxVolume = std::numeric_limits<ui64>::max();
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(maxVolume, maxVolume, clock);
        bucket.Take(maxVolume); // exactly i64 minimum
        bucket.Take(maxVolume); // saturates there
        clock->SetTimeNs(maxVolume);
        const auto [greenNs, redNs] = bucket.OverflowStats();
        UNIT_ASSERT_VALUES_EQUAL(greenNs, maxVolume - redNs);
        // At UINT64_MAX the double rate can round the crossing by one nanosecond.
        UNIT_ASSERT_GE(redNs, Second / 2);
        UNIT_ASSERT_LE(redNs, Second / 2 + 1);
    }

    Y_UNIT_TEST(OneTokenOverLargeCapacityIsRed) {
        constexpr ui64 capacity = std::numeric_limits<i64>::max();
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(capacity, 0, clock);
        bucket.Take(capacity + 1);
        clock->SetTimeNs(Second);
        CheckStats(bucket, 0, Second);
    }

    Y_UNIT_TEST(ConcurrentTakesAccountForEveryInterval) {
        constexpr size_t writerCount = 8;
        constexpr ui64 withdrawalsPerWriter = 2000;
        constexpr ui64 totalWithdrawals = writerCount * withdrawalsPerWriter;
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(0, 1'000'000, clock);
        bucket.Take(totalWithdrawals);
        std::barrier start(static_cast<std::ptrdiff_t>(writerCount));
        std::vector<std::thread> writers;
        for (size_t writer = 0; writer < writerCount; ++writer) {
            writers.emplace_back([&] {
                start.arrive_and_wait();
                for (ui64 withdrawal = 0; withdrawal < withdrawalsPerWriter; ++withdrawal) {
                    clock->TimeUs.fetch_add(1, std::memory_order_relaxed);
                    bucket.Take(2);
                }
            });
        }
        for (std::thread& writer : writers) {
            writer.join();
        }
        // The initial debt alone covers the entire concurrent part.
        constexpr ui64 concurrentNs = 1000 * totalWithdrawals;
        CheckStats(bucket, 0, concurrentNs);
        clock->SetTimeNs(5 * concurrentNs);
        const auto [greenNs, redNs] = bucket.OverflowStats();
        UNIT_ASSERT_VALUES_EQUAL(greenNs + redNs, 5 * concurrentNs);

        struct alignas(16) TStatePair {
            i64 Balance;
            ui64 TimestampNs;
        };
        if constexpr (std::atomic_ref<TStatePair>::is_always_lock_free) {
            UNIT_ASSERT_VALUES_EQUAL(redNs, 3 * concurrentNs);
        } else {
            // Even if every refill were lost, the final debt is at most three tokens per take.
            UNIT_ASSERT_GE(redNs, concurrentNs);
            UNIT_ASSERT_LE(redNs, 4 * concurrentNs);
        }
    }
}

} // namespace NKikimr
