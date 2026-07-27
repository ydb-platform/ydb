#include "backoff_delay_provider.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS {

Y_UNIT_TEST_SUITE(TBackoffDelayProvider)
{
    Y_UNIT_TEST(ExampleUsage)
    {
        TBackoffDelayProvider provider(
            TDuration::Seconds(1),
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), provider.GetDelay());
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(2), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(4), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(8), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(16), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(30), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(30), provider.GetDelay());
    }

    Y_UNIT_TEST(Reset)
    {
        TBackoffDelayProvider provider(
            TDuration::Seconds(10),
            TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(10), provider.GetDelay());
        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(20), provider.GetDelay());
        provider.Reset();
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(10), provider.GetDelay());
    }

    Y_UNIT_TEST(GetAndIncrease)
    {
        TBackoffDelayProvider provider(
            TDuration::Seconds(1),
            TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(2),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(4),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(8),
            provider.GetDelayAndIncrease());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(10),
            provider.GetDelayAndIncrease());
    }

    Y_UNIT_TEST(FirstStepExampleUsage)
    {
        TBackoffDelayProvider provider(
            TDuration::Zero(),
            TDuration::MilliSeconds(100),
            TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), provider.GetDelay());

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            provider.GetDelay());

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(200),
            provider.GetDelay());

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(400),
            provider.GetDelay());
    }

    Y_UNIT_TEST(NonZeroInitialDelayWithCustomFirstStep)
    {
        TBackoffDelayProvider provider(
            TDuration::MilliSeconds(50),
            TDuration::MilliSeconds(200),
            TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(50),
            provider.GetDelay());

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            provider.GetDelay());

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(200),
            provider.GetDelay());

        provider.Reset();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(50),
            provider.GetDelay());
    }

    Y_UNIT_TEST(MaxDelayConstraintWithCustomFirstStep)
    {
        TBackoffDelayProvider provider(
            TDuration::Zero(),
            TDuration::MilliSeconds(100),
            TDuration::MilliSeconds(150));

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(100),
            provider.GetDelay());

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(150),
            provider.GetDelay());

        provider.IncreaseDelay();
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(150),
            provider.GetDelay());
    }
}

}   // namespace NYdb::NBS
