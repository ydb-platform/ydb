#include "max_calculator.h"

#include "monitoring.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMaxCalculatorTest)
{
    Y_UNIT_TEST(ShouldCorrectlyCountMaxValueWithoutTime)
    {
        TMaxCalculator<DEFAULT_BUCKET_COUNT> calculator(
            std::make_shared<TTestTimer>());

        ui64 value = DEFAULT_BUCKET_COUNT + 100;

        for (size_t i = 1; i <= DEFAULT_BUCKET_COUNT; ++i) {
            calculator.Add(value);
            UNIT_ASSERT_VALUES_EQUAL(value, calculator.NextValue());
        }

        for (size_t i = 1; i < DEFAULT_BUCKET_COUNT; ++i) {
            calculator.Add(value - i);
            UNIT_ASSERT_VALUES_EQUAL(value, calculator.NextValue());
        }

        for (size_t i = value - DEFAULT_BUCKET_COUNT; i > 0; --i) {
            calculator.Add(i);
            UNIT_ASSERT_VALUES_EQUAL(
                i + DEFAULT_BUCKET_COUNT - 1,
                calculator.NextValue());
        }

        calculator.Add(0);
        UNIT_ASSERT_VALUES_EQUAL(
            DEFAULT_BUCKET_COUNT - 1,
            calculator.NextValue());
    }

    Y_UNIT_TEST(ShouldCorrectlyCountMaxValueWithTime)
    {
        auto timer = std::make_shared<TTestTimer>();
        TMaxPerSecondCalculator<DEFAULT_BUCKET_COUNT> calculator(timer);

        ui64 value = 1;

        for (size_t i = 1; i <= DEFAULT_BUCKET_COUNT; ++i) {
            for (size_t j = 0; j < i; ++j) {
                calculator.Add(value);
            }
            UNIT_ASSERT_VALUES_EQUAL(value * i, calculator.NextValue());
            timer->AdvanceTime(TDuration::Seconds(1));
        }

        for (size_t i = 1; i < DEFAULT_BUCKET_COUNT; ++i) {
            calculator.Add(1);
            UNIT_ASSERT_VALUES_EQUAL(
                DEFAULT_BUCKET_COUNT,
                calculator.NextValue());
            timer->AdvanceTime(TDuration::Seconds(i));
        }

        calculator.Add(0);
        UNIT_ASSERT_VALUES_EQUAL(1, calculator.NextValue());
    }

    Y_UNIT_TEST(ShouldRoundUpValues)
    {
        auto timer = std::make_shared<TTestTimer>();
        TMaxPerSecondCalculator<DEFAULT_BUCKET_COUNT> calculator(timer);

        calculator.Add(1);
        timer->AdvanceTime(TDuration::MilliSeconds(1'001));

        UNIT_ASSERT_VALUES_EQUAL(1, calculator.NextValue());
    }
}

}   // namespace NYdb::NBS
