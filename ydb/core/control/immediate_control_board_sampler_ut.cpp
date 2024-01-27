#include "immediate_control_board_sampler.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(SamplingControlTests) {
    ui32 RunTrials(TSampler& sampler, ui32 trials) {
        ui32 cnt = 0;
        for (ui32 i = 0; i < trials; ++i) {
            if (sampler.Sample()) {
                ++cnt;
            }
        }
        return cnt;
    }

    Y_UNIT_TEST(Simple) {
        TControlWrapper control(500'000, 0, 1'000'000);
        TSampler sampler(control, 42);

        auto samples = RunTrials(sampler, 100'000);
        UNIT_ASSERT_GE(samples, 48'000);
        UNIT_ASSERT_LE(samples, 52'000);
    }

    Y_UNIT_TEST(EdgeCaseLower) {
        TControlWrapper control(0, 0, 1'000'000);
        TSampler sampler(control, 42);

        auto samples = RunTrials(sampler, 100'000);
        UNIT_ASSERT_EQUAL(samples, 0);
    }

    Y_UNIT_TEST(EdgeCaseUpper) {
        TControlWrapper control(1'000'000, 0, 1'000'000);
        TSampler sampler(control, 42);

        auto samples = RunTrials(sampler, 100'000);
        UNIT_ASSERT_EQUAL(samples, 100'000);
    }

    Y_UNIT_TEST(ChangingControl) {
        TControlWrapper control(250'000, 0, 1'000'000);
        TSampler sampler(control, 42);

        {
            auto samples = RunTrials(sampler, 100'000);
            UNIT_ASSERT_GE(samples, 23'000);
            UNIT_ASSERT_LE(samples, 27'000);
        }

        control = 750'000;
        {
            auto samples = RunTrials(sampler, 100'000);
            UNIT_ASSERT_GE(samples, 73'000);
            UNIT_ASSERT_LE(samples, 77'000);
        }
    }
}

} // namespace NKikimr
