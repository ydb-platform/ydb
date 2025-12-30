#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(ParseDurationTests) {
    Y_UNIT_TEST(ParseDuration_WithUnit) {
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("5s"), TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("100ms"), TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("2m"), TDuration::Minutes(2));
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("1h"), TDuration::Hours(1));
    }

    Y_UNIT_TEST(ParseDuration_ThrowsOnPlainNumber) {
        UNIT_ASSERT_EXCEPTION(ParseDuration("100"), TMisuseException);
    }

    Y_UNIT_TEST(ParseDurationMilliseconds_WithUnit) {
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("5s"), TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("100ms"), TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("2m"), TDuration::Minutes(2));
    }

    Y_UNIT_TEST(ParseDurationMilliseconds_PlainNumber) {
        // Plain number should be interpreted as milliseconds
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("1000"), TDuration::MilliSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("500"), TDuration::MilliSeconds(500));
    }

    Y_UNIT_TEST(ParseDurationWithDefaultUnit_Hours) {
        auto hoursParser = [](double hours) { return TDuration::Seconds(hours * 3600); };
        
        // With unit - should use the unit
        UNIT_ASSERT_VALUES_EQUAL(
            ParseDurationWithDefaultUnit("1h", hoursParser),
            TDuration::Hours(1)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            ParseDurationWithDefaultUnit("30m", hoursParser),
            TDuration::Minutes(30)
        );
        
        // Plain number - should use default unit (hours)
        UNIT_ASSERT_VALUES_EQUAL(
            ParseDurationWithDefaultUnit("2", hoursParser),
            TDuration::Hours(2)
        );
        UNIT_ASSERT_VALUES_EQUAL(
            ParseDurationWithDefaultUnit("0.5", hoursParser),
            TDuration::Minutes(30)
        );
    }

    Y_UNIT_TEST(ParseDurationWithDefaultUnit_Negative) {
        auto msParser = [](double ms) { return TDuration::MilliSeconds(static_cast<ui64>(ms)); };
        UNIT_ASSERT_EXCEPTION(ParseDurationWithDefaultUnit("-5", msParser), TMisuseException);
    }

    Y_UNIT_TEST(ParseDurationWithDefaultUnit_Empty) {
        auto msParser = [](double ms) { return TDuration::MilliSeconds(static_cast<ui64>(ms)); };
        UNIT_ASSERT_EXCEPTION(ParseDurationWithDefaultUnit("", msParser), TMisuseException);
    }
}
