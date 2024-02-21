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
}

} // namespace NKikimr::NJaegerTracing
