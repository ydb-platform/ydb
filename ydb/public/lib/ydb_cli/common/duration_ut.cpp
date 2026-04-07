#include <ydb/public/lib/ydb_cli/common/duration.h>

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

    Y_UNIT_TEST(ParseDuration_FractionalValues) {
        // TDuration::Parse supports fractional values
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("0.5s"), TDuration::MilliSeconds(500));
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("0.5m"), TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("1.5h"), TDuration::Minutes(90));
        UNIT_ASSERT_VALUES_EQUAL(ParseDuration("0.5d"), TDuration::Hours(12));
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

    Y_UNIT_TEST(ParseDurationMilliseconds_FractionalPlainNumber) {
        // Fractional plain number in milliseconds (rounds to microseconds)
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("500.5"), TDuration::MicroSeconds(500500));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("100.4"), TDuration::MicroSeconds(100400));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMilliseconds("1.0001"), TDuration::MicroSeconds(1000));
    }

    Y_UNIT_TEST(ParseDurationSeconds_WithUnit) {
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("5s"), TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("100ms"), TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("2m"), TDuration::Minutes(2));
    }

    Y_UNIT_TEST(ParseDurationSeconds_PlainNumber) {
        // Plain number should be interpreted as seconds
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("60"), TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("30"), TDuration::Seconds(30));
    }

    Y_UNIT_TEST(ParseDurationSeconds_FractionalPlainNumber) {
        // Fractional plain number in seconds (rounds to microseconds)
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("30.5"), TDuration::MicroSeconds(30500000));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("60.400001"), TDuration::MicroSeconds(60400001));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationSeconds("1.000001"), TDuration::MicroSeconds(1000001));
    }

    Y_UNIT_TEST(ParseDurationHours_WithUnit) {
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("2h"), TDuration::Hours(2));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("30m"), TDuration::Minutes(30));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("1d"), TDuration::Hours(24));
    }

    Y_UNIT_TEST(ParseDurationHours_PlainNumber) {
        // Plain number should be interpreted as hours
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("2"), TDuration::Hours(2));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("24"), TDuration::Hours(24));
    }

    Y_UNIT_TEST(ParseDurationHours_FractionalPlainNumber) {
        // Fractional plain number in hours (rounds to microseconds)
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("0.5"), TDuration::MicroSeconds(1800000000)); // 0.5h = 30 min = 1800000000 us
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("1.5"), TDuration::MicroSeconds(5400000000)); // 1.5h = 90 minutes = 5400000000 us
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationHours("0.000001"), TDuration::MicroSeconds(3600)); // 0.000001h = 3600 us = 3.6 ms
    }

    Y_UNIT_TEST(ParseDurationMicroseconds_WithUnit) {
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMicroseconds("5s"), TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMicroseconds("100ms"), TDuration::MilliSeconds(100));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMicroseconds("500us"), TDuration::MicroSeconds(500));
    }

    Y_UNIT_TEST(ParseDurationMicroseconds_PlainNumber) {
        // Plain number should be interpreted as microseconds
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMicroseconds("1000"), TDuration::MicroSeconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMicroseconds("500"), TDuration::MicroSeconds(500));
    }

    Y_UNIT_TEST(ParseDurationMicroseconds_FractionalPlainNumber) {
        // Fractional plain number in microseconds (should round)
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMicroseconds("500.5"), TDuration::MicroSeconds(501));
        UNIT_ASSERT_VALUES_EQUAL(ParseDurationMicroseconds("100.4"), TDuration::MicroSeconds(100));
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
