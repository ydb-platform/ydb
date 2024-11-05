#include "throttler.h"

#include <util/generic/scope.h>
#include <util/system/thread.h>

#include <library/cpp/time_provider/time_provider.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJaegerTracing {

class TTimeProviderMock : public ITimeProvider {
public:
    TTimeProviderMock(TInstant now) : CurrentTimeUS(now.GetValue()) {}

    void Advance(TDuration delta) {
        CurrentTimeUS.fetch_add(delta.GetValue());
    }

    TInstant Now() final {
        return TInstant::FromValue(CurrentTimeUS.load());
    }

private:
    std::atomic<ui64> CurrentTimeUS;
};

Y_UNIT_TEST_SUITE(ThrottlerControlTests) {
    void CheckAtLeast(TThrottler& throttler, ui32 n) {
        for (ui32 i = 0; i < n; ++i) {
            UNIT_ASSERT(!throttler.Throttle());
        }
    }

    void CheckExact(TThrottler& throttler, ui32 n) {
        CheckAtLeast(throttler, n);
        UNIT_ASSERT(throttler.Throttle());
    }

    Y_UNIT_TEST(Simple) {
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(6, 2, timeProvider);
        CheckExact(throttler, 3);
        CheckExact(throttler, 0);

        timeProvider->Advance(TDuration::Seconds(9));
        CheckExact(throttler, 0);
        timeProvider->Advance(TDuration::Seconds(1));
        CheckExact(throttler, 1);

        timeProvider->Advance(TDuration::Seconds(15));
        CheckExact(throttler, 1);

        timeProvider->Advance(TDuration::Seconds(15));
        CheckExact(throttler, 2);
    }

    Y_UNIT_TEST(LongIdle) {
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(10, 2, timeProvider);
        CheckAtLeast(throttler, 3);

        timeProvider->Advance(TDuration::Hours(1));
        CheckExact(throttler, 3);
    }

    Y_UNIT_TEST(Overflow_1) {
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(1'000'000'000'000'000'000, 20'000, timeProvider);

        // TODO(pumpurum): switch back to CheckExact when we figure out how to limit properly
        CheckAtLeast(throttler, 20'001);

        timeProvider->Advance(TDuration::Days(365 * 10));

        CheckAtLeast(throttler, 20'001);
    }

    Y_UNIT_TEST(Overflow_2) {
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(1'000'000'000'000'000'000, 1, timeProvider);
        CheckAtLeast(throttler, 1);

        timeProvider->Advance(TDuration::Days(365 * 10));

        CheckAtLeast(throttler, 1);
    }

    void TestMultiThreaded(ui32 threads, ui64 ticks, ui64 init, ui64 step) {
        constexpr std::array<TDuration, 4> delays = {
            TDuration::MilliSeconds(1),
            TDuration::MilliSeconds(10),
            TDuration::MilliSeconds(100),
            TDuration::Seconds(1)
        };

        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(60, init - 1, timeProvider);

        auto shouldStop = std::make_shared<std::atomic<bool>>(false);
        TVector<THolder<TThread>> workers;
        Y_SCOPE_EXIT(shouldStop, &workers) {
            shouldStop->store(true);

            try {
                for (auto& worker : workers) {
                    worker->Join();
                }
            } catch (yexception& e) {
                Cerr << "Failed to join worker:" << Endl;
                Cerr << e.what() << Endl;
            }
        };

        std::atomic<ui64> totalConsumed{0};
        workers.reserve(threads);
        for (size_t i = 0; i < threads; ++i) {
            workers.push_back(MakeHolder<TThread>([&]() {
                while (!shouldStop->load(std::memory_order_relaxed)) {
                    if (!throttler.Throttle()) {
                        totalConsumed.fetch_add(1);
                    }
                }
            }));
        }
        for (auto& worker : workers) {
            worker->Start();
        }

        auto waitForIncrease = [&](ui64 expected) -> bool {
            for (const TDuration& delay : delays) {
                Sleep(delay);
                if (totalConsumed.load() == expected) {
                    return true;
                }
            }
            return false;
        };

        ui64 expected = init;
        UNIT_ASSERT(waitForIncrease(expected));

        auto advance = [&](ui64 seconds, ui64 expectedIncrease) {
            timeProvider->Advance(TDuration::Seconds(seconds));
            expected += expectedIncrease;
            UNIT_ASSERT(waitForIncrease(expected));
        };

        advance(1, 1);

        for (size_t i = 0; i < ticks; ++i) {
            advance(step, step);
        }

        advance(init + 1000, init);
    }

    #define TEST_MULTI_THREADED(threads, ticks, init, step)                                 \
    Y_UNIT_TEST(MultiThreaded##threads##Threads##ticks##Ticks##init##Init##step##Step) {    \
        TestMultiThreaded(threads, ticks, init, step);                                      \
    }

    TEST_MULTI_THREADED(2, 200, 30, 7);
    TEST_MULTI_THREADED(5, 150, 500, 15);
    TEST_MULTI_THREADED(10, 100, 1000, 22);

    #undef TEST_MULTI_THREADED
}

} // namespace NKikimr::NJaegerTracing
