#include <ydb/core/fq/libs/compute/common/utils.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/text_format.h>

Y_UNIT_TEST_SUITE(FormatTimes) {
    Y_UNIT_TEST(DurationUs) {
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(         0),  "0.00s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(         1),     "1us");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(        10),    "10us");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(       111),   "111us");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(     1'000),     "1ms");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(    10'000),    "10ms");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(   100'000),  "0.10s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs( 1'000'000),  "1.00s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(10'000'000), "10.00s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs( 1'099'000),  "1.09s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(99'888'000), "1m 39s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationUs(6'999'999'999), "1h 56m");
    }
    Y_UNIT_TEST(DurationMs) {
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(        0),   "0.00s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(        1),      "1ms");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(       10),     "10ms");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(       99),     "99ms");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(      111),   "0.11s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(        2),      "2ms");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(       20),     "20ms");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(      200),   "0.20s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(    2'000),   "2.00s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(   20'000),  "20.00s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(    7'009),   "7.00s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(   99'999),  "1m 39s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(3'599'000), "59m 59s");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(3'600'000),  "1h 00m");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(6'001'000),  "1h 40m");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(6'000'000'000), "1666h");
        UNIT_ASSERT_EQUAL(NFq::FormatDurationMs(1'000'000'000'000), "277777h");
    }
}
