#include <library/cpp/tvmauth/client/misc/exponential_backoff.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(PasspUtilsExpBackoff) {
    Y_UNIT_TEST(common) {
        TExponentialBackoff b({TDuration::Seconds(1), TDuration::Seconds(60), 2, 0.01});

        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), b.GetCurrentValue());

        TDuration dur = b.GetCurrentValue();
        for (size_t idx = 0; idx < 6; ++idx) {
            TDuration newValue = b.Increase();
            UNIT_ASSERT_LT(dur, newValue);
            dur = newValue;
        }

        UNIT_ASSERT_LT(TDuration::Seconds(60) - TDuration::Seconds(3), dur);
        UNIT_ASSERT_LT(dur, TDuration::Seconds(60) + TDuration::Seconds(3));
    }

    Y_UNIT_TEST(sleep) {
        TExponentialBackoff b({TDuration::Seconds(60), TDuration::Seconds(600), 2, 0.01});

        const TInstant start = TInstant::Now();

        TAutoEvent started;
        std::thread t([&b, &started]() {
            started.Signal();
            b.Sleep();
        });

        started.WaitT(TDuration::Seconds(30));
        b.Interrupt();
        t.join();
        TDuration dur = TInstant::Now() - start;

        UNIT_ASSERT_LT(dur, TDuration::Seconds(60));
    }
}
