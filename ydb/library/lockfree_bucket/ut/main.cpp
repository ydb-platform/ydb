#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/lockfree_bucket/lockfree_bucket.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>
#include <util/system/types.h>

#include <thread>

struct TTestTimerMs {
    using TTime = TInstant;
    static constexpr ui64 Resolution = 1000ull; // milliseconds

    static TTime Now() {
        return TInstant::Zero() + TDuration::MilliSeconds(Time.load());
    }

    static ui64 Duration(TTime from, TTime to) {
        return (to - from).MilliSeconds();
    }

    static std::atomic<ui64> Time;

    static void Reset() {
        Time.store(0);
    }

    static void Advance(TDuration delta) {
        Time.fetch_add(delta.MilliSeconds());
    }
};

std::atomic<ui64> TTestTimerMs::Time = {};

Y_UNIT_TEST_SUITE(TLockFreeBucket) {
    struct TTestContext {
        TTestContext() {
            MaxTokens.store(1'000'000);
            MinTokens.store(-1'000'000);
            Inflow.store(1'000'000);
            TTestTimerMs::Reset();
        }

        template<class TCallback>
        void Initialize(TCallback callback, ui32 threadCount) {
            for (ui32 i = 0; i < threadCount; ++i) {
                Threads.emplace_back(callback);
            }
        }

        ~TTestContext() {
            JoinAll();
        }

        void JoinAll() {
            for (std::thread& t : Threads) {
                t.join();
            }
            Threads.clear();
        }

        std::atomic<i64> MaxTokens;
        std::atomic<i64> MinTokens;
        std::atomic<ui64> Inflow;
        std::vector<std::thread> Threads;
    };

    void TestLowerLimit(ui32 threadCount) {
        TTestContext ctx;
        TLockFreeBucket<TTestTimerMs> bucket(ctx.MaxTokens, ctx.MinTokens, ctx.Inflow);

        auto worker = [&]() {
            for (ui32 i = 0; i < 100; ++i) {
                TTestTimerMs::Advance(TDuration::MilliSeconds(10));
                bucket.FillAndTake(123'456);
            }
        };

        ctx.Initialize(worker, threadCount);
        ctx.JoinAll();

        TTestTimerMs::Advance(TDuration::Seconds(1));
        TTestTimerMs::Advance(TDuration::MilliSeconds(1));

        UNIT_ASSERT(!bucket.IsEmpty());
    }

    void TestUpperLimit(ui32 tokensTaken, bool isEmpty, ui32 threadCount) {
        TTestContext ctx;
        TLockFreeBucket<TTestTimerMs> bucket(ctx.MaxTokens, ctx.MinTokens, ctx.Inflow);
        TTestTimerMs::Advance(TDuration::Seconds(100500));

        auto worker = [&]() {
            for (ui32 i = 0; i < 100; ++i) {
                TTestTimerMs::Advance(TDuration::MilliSeconds(10));
                bucket.FillAndTake(tokensTaken);
            }
        };

        ctx.Initialize(worker, threadCount);
        ctx.JoinAll();

        UNIT_ASSERT_VALUES_EQUAL(bucket.IsEmpty(), isEmpty);
    }

    Y_UNIT_TEST(LowerLimitSingleThreaded) {
        TestLowerLimit(1);
    }

    Y_UNIT_TEST(LowerLimitMultiThreaded) {
        TestLowerLimit(20);
    }

    Y_UNIT_TEST(UpperLimitSingleThreaded) {
        TestUpperLimit(123'456, true, 1);
    }

    Y_UNIT_TEST(UpperLimitMultiThreaded) {
        TestUpperLimit(123'456, true, 20);
    }

    Y_UNIT_TEST(LowIntakeSingleThreaded) {
        TestUpperLimit(1, false, 1);
    }

    Y_UNIT_TEST(LowIntakeMultiThreaded) {
        TestUpperLimit(1, false, 20);
    }
}
