#include "sampler.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NJaegerTracing {

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
        TSampler sampler(0.5, 42);

        auto samples = RunTrials(sampler, 100'000);
        UNIT_ASSERT_GE(samples, 48'000);
        UNIT_ASSERT_LE(samples, 52'000);
    }

    Y_UNIT_TEST(EdgeCaseLower) {
        TSampler sampler(0, 42);

        auto samples = RunTrials(sampler, 100'000);
        UNIT_ASSERT_EQUAL(samples, 0);
    }

    Y_UNIT_TEST(EdgeCaseUpper) {
        TSampler sampler(1, 42);

        auto samples = RunTrials(sampler, 100'000);
        UNIT_ASSERT_EQUAL(samples, 100'000);
    }
}

} // namespace NKikimr::NJaegerTracing
