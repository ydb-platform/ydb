#include "throttler.h"
#include "util/generic/scope.h"
#include "util/system/thread.h"

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

    Y_UNIT_TEST(MultiThreaded) {
        auto check = []<size_t Threads, ui64 Ticks, ui64 Init, ui64 Step>() {
            constexpr TDuration delay = TDuration::MilliSeconds(1);
            auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

            TThrottler throttler(60, Init - 1, timeProvider);

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
            workers.reserve(Threads);
            for (size_t i = 0; i < Threads; ++i) {
                workers.push_back(MakeHolder<TThread>([&, shouldStop]() {
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

            Sleep(delay);
            ui64 expected = Init;
            UNIT_ASSERT_EQUAL(totalConsumed.load(), expected);

            auto advance = [&, delay](ui64 seconds, ui64 expectedIncreace) {
                timeProvider->Advance(TDuration::Seconds(seconds));
                expected += expectedIncreace;
                Sleep(delay);
                UNIT_ASSERT_EQUAL(totalConsumed.load(), expected);
            };

            advance(1, 1);

            for (size_t i = 0; i < Ticks; ++i) {
                advance(Step, Step);
                Sleep(delay);
                UNIT_ASSERT_EQUAL(totalConsumed.load(), expected);
            }

            advance(Init + 1000, Init);
        };

        check.operator()<2, 200, 30, 7>();
        check.operator()<5, 150, 500, 15>();
        check.operator()<10, 100, 1000, 22>();
    }
}

} // namespace NKikimr::NJaegerTracing
