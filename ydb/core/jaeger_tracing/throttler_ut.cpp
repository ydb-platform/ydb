#include "throttler.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJaegerTracing {

class TTimeProviderMock : public ITimeProvider {
public:
    TTimeProviderMock(TInstant now) : CurrentTime(now) {}

    void Advance(TDuration delta) {
        CurrentTime += delta;
    }

    TInstant Now() final {
        return CurrentTime;
    }

private:
    TInstant CurrentTime;
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
        TControlWrapper maxPerMinute(6);
        TControlWrapper maxBurst(2);

        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(maxPerMinute, maxBurst, timeProvider);
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
        TControlWrapper maxPerMinute(10);
        TControlWrapper maxBurst(2);

        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(maxPerMinute, maxBurst, timeProvider);
        CheckAtLeast(throttler, 3);

        timeProvider->Advance(TDuration::Hours(1));
        CheckExact(throttler, 3);
    }

    Y_UNIT_TEST(Overflow) {
        TControlWrapper maxPerMinute(6'000);
        TControlWrapper maxBurst(6'000);

        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(maxPerMinute, maxBurst, timeProvider);
        CheckExact(throttler, 6'001);

        timeProvider->Advance(TDuration::Days(365 * 10));

        CheckExact(throttler, 6'001);
    }

    Y_UNIT_TEST(ChangingControls) {
        TControlWrapper maxPerMinute(6);
        TControlWrapper maxBurst(2);

        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());

        TThrottler throttler(maxPerMinute, maxBurst, timeProvider);
        CheckExact(throttler, 3);

        maxBurst.Set(4);
        CheckExact(throttler, 2);

        maxBurst.Set(0);
        CheckExact(throttler, 0);

        timeProvider->Advance(TDuration::Seconds(9));
        CheckExact(throttler, 0);
        timeProvider->Advance(TDuration::Seconds(1));
        CheckExact(throttler, 1);

        maxPerMinute.Set(12 * 60);
        timeProvider->Advance(TDuration::Seconds(1));
        CheckExact(throttler, 1);

        maxBurst.Set(20);

        timeProvider->Advance(TDuration::Seconds(3));
        CheckExact(throttler, 21);

        maxBurst.Set(0);
        timeProvider->Advance(TDuration::Seconds(59));
        CheckAtLeast(throttler, 1);
        maxPerMinute.Set(1);
        CheckExact(throttler, 0);
        timeProvider->Advance(TDuration::Minutes(1));
        CheckExact(throttler, 1);

        maxBurst.Set(2);
        CheckExact(throttler, 2);
    }
}

} // namespace NKikimr::NJaegerTracing
