#include "helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THelpersTest)
{
    Y_UNIT_TEST(ShouldRecalculateIOpsWithSiteFormula)
    {
        // Zeroes
        UNIT_ASSERT_VALUES_EQUAL(0, CalculateThrottlerC1(0.0, 0.0));

        // Zero bandwidth
        UNIT_ASSERT_VALUES_EQUAL(100, CalculateThrottlerC1(100.0, 0.0));

        // Zero IOps
        UNIT_ASSERT_VALUES_EQUAL(1, CalculateThrottlerC1(0.0, 100.0));

        // Random
        UNIT_ASSERT_VALUES_EQUAL(13, CalculateThrottlerC1(12.34, 567890));

        // Very big IOps
        UNIT_ASSERT_VALUES_EQUAL(
            1'000'000'000,
            CalculateThrottlerC1(1e9, 6473.783));

        // Very big bandwidth
        UNIT_ASSERT_VALUES_EQUAL(154, CalculateThrottlerC1(154.76, 1e9));
    }

    Y_UNIT_TEST(ShouldRecalculateBandwidthWithSiteFormula)
    {
        // Zeroes
        UNIT_ASSERT_VALUES_EQUAL(0, CalculateThrottlerC2(0.0, 0.0));

        // Zero bandwidth
        UNIT_ASSERT_VALUES_EQUAL(0, CalculateThrottlerC2(100.0, 0.0));

        // Zero IOps
        UNIT_ASSERT_VALUES_EQUAL(Max<ui64>(), CalculateThrottlerC2(0.0, 100.0));

        // Random
        UNIT_ASSERT_VALUES_EQUAL(573'629, CalculateThrottlerC2(12.34, 567890));

        // Very big IOps
        UNIT_ASSERT_VALUES_EQUAL(6'467, CalculateThrottlerC2(1e9, 6473.783));

        // Very big bandwidth
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            CalculateThrottlerC2(154.76, 1e9));

        // Overflow with 32bit types
        // result is   7313817600d -> 110110011111100000000000000000000b
        // but returns 3018850304d ->  10110011111100000000000000000000b
        // should return Max<ui32>()
        // Okay with 64bit
        UNIT_ASSERT_VALUES_EQUAL(7313817600, CalculateThrottlerC2(300, 1_GB));
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateSecondsToDuration)
    {
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), SecondsToDuration(1.0));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(500),
            SecondsToDuration(0.5));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(525),
            SecondsToDuration(0.000525));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MicroSeconds(134),
            SecondsToDuration(0.000133658));
    }

    Y_UNIT_TEST(ShouldCorrectlyCalculateCostPerIO)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            SecondsToDuration(9. / 128),
            CostPerIO(128, 1024, 64));

        UNIT_ASSERT_VALUES_EQUAL(
            SecondsToDuration(1. / 128),
            CostPerIO(128, 0, 64));
    }
}

}   // namespace NYdb::NBS
