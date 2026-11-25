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
struct alignas(16) TFullState {
    i64 Balance;
    ui64 Timestamp;
};
constexpr bool HasFullState = std::atomic_ref<TFullState>::is_always_lock_free;

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
        TLockFreeLightBucket bucket(20'000, 10'000, clock);
        clock->SetTimeNs(5 * Second);
        bucket.Take(50'000); // +20k to -30k at the end of four green seconds.
        CheckStats(bucket, 4 * Second, 0);
        clock->SetTimeNs(10 * Second);
        CheckStats(bucket, 6 * Second, 3 * Second);
        CheckStats(bucket, 6 * Second, 3 * Second);
        // A delayed clock reading cannot count a previously claimed interval.
        clock->SetTimeNs(9 * Second);
        bucket.Take(20'000);
        clock->SetTimeNs(11 * Second);
        CheckStats(bucket, 7 * Second, 3 * Second);
    }

    Y_UNIT_TEST(RefillCanRecoverBeforeAnotherWithdrawal) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(20'000, 10'000, clock);
        bucket.Take(35'000); // -15k
        clock->SetTimeNs(4 * Second);
        bucket.Take(25'000); // Refill reaches the cap, then the take leaves -5k.
        CheckStats(bucket, 5 * Second / 2, 3 * Second / 2);
        clock->SetTimeNs(8 * Second);
        bucket.Take(5'000); // -5k to +15k, with half a second spent red.
        CheckStats(bucket, 6 * Second, 2 * Second);
    }

    Y_UNIT_TEST(RefillUsesOnlyTheElapsedInterval) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(10'000, 3'000, clock);
        bucket.Take(11'000);
        clock->SetTimeNs(100'000);
        CheckStats(bucket, 0, 100'000);
        // The first poll discarded 0.3 tokens. Recovery takes another 1/3 second.
        clock->SetTimeNs(500'100'000);
        CheckStats(bucket, 166'666'666, 333'433'334);
    }

    Y_UNIT_TEST(ConfigurationSettlesTheOldInterval) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(12'000, 0, clock);
        bucket.Take(12'000);
        clock->SetTimeNs(Second);
        bucket.Take(6'000);
        clock->SetTimeNs(2 * Second);
        bucket.SetReplenishmentRate(3'000);
        clock->SetTimeNs(3 * Second);
        bucket.SetReplenishmentRate(6'000);
        clock->SetTimeNs(4 * Second);
        CheckStats(bucket, Second + Second / 2, 2 * Second + Second / 2);
        bucket.SetCapacity(1'000);
        bucket.SetCapacity(10'000);
        bucket.Take(2'000);
        bucket.SetReplenishmentRate(0);
        clock->SetTimeNs(5 * Second);
        CheckStats(bucket, Second + Second / 2, 3 * Second + Second / 2);
    }

    Y_UNIT_TEST(DebtLimitFollowsCapacity) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(100'000, 1'000'000, clock, TLockFreeLightBucket::EDebtLimit::TwiceCapacity);
        bucket.Take(10'000'000);
        bucket.Take(10'000'000);
        clock->SetTimeNs(Second);
        CheckStats(bucket, 800'000'000, 200'000'000);

        bucket.Take(std::numeric_limits<ui64>::max());
        clock->SetTimeNs(Second + 50'000'000);
        bucket.SetCapacity(25'000); // The outstanding debt falls from 150k to 50k.
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
        bucket.Take(maxVolume);
        bucket.Take(maxVolume);
        if constexpr (HasFullState) {
            clock->SetTimeNs(maxVolume);
        } else {
            clock->SetTimeNs(Second);
        }
        const auto [greenNs, redNs] = bucket.OverflowStats();
        if constexpr (HasFullState) {
            UNIT_ASSERT_VALUES_EQUAL(greenNs, maxVolume - redNs);
            // At UINT64_MAX the double rate can round the crossing by one nanosecond.
            UNIT_ASSERT_GE(redNs, Second / 2);
            UNIT_ASSERT_LE(redNs, Second / 2 + 1);
        } else {
            // Repaying 2^31 * 1,000 tokens at UINT64_MAX tokens/s takes 117 ns.
            UNIT_ASSERT_VALUES_EQUAL(redNs, 117);
            UNIT_ASSERT_VALUES_EQUAL(greenNs, Second - 117);
        }
    }

    Y_UNIT_TEST(OneTokenOverLargeCapacityIsRed) {
        constexpr ui64 capacity = std::numeric_limits<i64>::max();
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(capacity, 0, clock);
        bucket.Take(capacity + 1);
        clock->SetTimeNs(Second);
        CheckStats(bucket, 0, Second);
    }

    Y_UNIT_TEST(ChargesAndRefillPreserveTokenPrecision) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(1'999, 500, clock);
        bucket.Take(999);
        bucket.Take(1'999);
        bucket.Take(1'000);
        clock->SetTimeNs(5 * Second);
        CheckStats(bucket, 1'002'000'000, 3'998'000'000);
    }

    Y_UNIT_TEST(SmallChargesExactlyExhaustCapacity) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(850'000, 0, clock);
        for (ui32 write = 0; write < 1'000; ++write) {
            bucket.Take(850);
        }
        clock->SetTimeNs(Second);
        CheckStats(bucket, Second, 0);
        bucket.Take(1);
        clock->SetTimeNs(2 * Second);
        CheckStats(bucket, Second, Second);
    }

    Y_UNIT_TEST(ShortRefillsCoverRepeatedWrites) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(2'000, 100'000'000, clock);
        bucket.Take(1'000);
        ui64 nowNs = 0;
        for (ui32 pair = 0; pair < 100; ++pair) {
            nowNs += 5'000; // Refill 500 tokens.
            clock->SetTimeNs(nowNs);
            bucket.Take(1'000);
            nowNs += 17'000; // Refill 1,700 tokens.
            clock->SetTimeNs(nowNs);
            bucket.Take(1'000);
        }
        CheckStats(bucket, nowNs, 0);
    }

    Y_UNIT_TEST(SmallCapacityAllowsBoundedDebt) {
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(499, 1'000, clock, TLockFreeLightBucket::EDebtLimit::TwiceCapacity);
        bucket.Take(500); // One token of debt takes a millisecond to repay.
        clock->SetTimeNs(Second);
        CheckStats(bucket, Second - 1'000'000, 1'000'000);
        bucket.Take(std::numeric_limits<ui64>::max(), 500); // New debt limit is exactly 1,000 tokens.
        clock->SetTimeNs(3 * Second);
        CheckStats(bucket, 2 * Second - 1'000'000, Second + 1'000'000);
    }

    Y_UNIT_TEST(TimestampWrapAndStaleReading) {
        constexpr ui64 wrapNs = (ui64{1} << 32) * 1'000;
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        clock->SetTimeNs(wrapNs - 2'000);
        TLockFreeLightBucket bucket(0, 1'000'000'000, clock);
        bucket.Take(3'000);
        clock->SetTimeNs(wrapNs + 1'000);
        CheckStats(bucket, 0, 3'000);
        clock->SetTimeNs(wrapNs - 1'000);
        bucket.Take(1'000);
        clock->SetTimeNs(wrapNs + 4'000);
        CheckStats(bucket, 2'000, 4'000);

        constexpr ui64 maxGapNs = static_cast<ui64>(std::numeric_limits<i32>::max()) * 1'000;
        clock->SetTimeNs(wrapNs + 4'000 + maxGapNs);
        CheckStats(bucket, 2'000 + maxGapNs, 4'000);
    }

    Y_UNIT_TEST(ConcurrentTakesAccountForEveryInterval) {
        constexpr size_t writerCount = 8;
        constexpr ui64 withdrawalsPerWriter = 2000;
        constexpr ui64 totalWithdrawals = writerCount * withdrawalsPerWriter;
        const TIntrusivePtr<TTestClock> clock = MakeIntrusive<TTestClock>();
        TLockFreeLightBucket bucket(0, 1'000'000'000, clock);
        bucket.Take(1'000 * totalWithdrawals);
        std::barrier start(static_cast<std::ptrdiff_t>(writerCount));
        std::vector<std::thread> writers;
        for (size_t writer = 0; writer < writerCount; ++writer) {
            writers.emplace_back([&] {
                start.arrive_and_wait();
                for (ui64 withdrawal = 0; withdrawal < withdrawalsPerWriter; ++withdrawal) {
                    clock->TimeUs.fetch_add(1, std::memory_order_relaxed);
                    bucket.Take(1'731);
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
        constexpr ui64 debtRepaidNs = (1'000 + 1'731) * totalWithdrawals;
        CheckStats(bucket, 5 * concurrentNs - debtRepaidNs, debtRepaidNs);
    }
}

} // namespace NKikimr
