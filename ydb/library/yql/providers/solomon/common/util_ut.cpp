#include "util.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NSo {

Y_UNIT_TEST_SUITE(TestSolomonParseSelectors) {
    Y_UNIT_TEST(Basic) {
        TString selectors = "{a = \"a\", b = \"b\"}";
        std::map<TString, TString> result;

        std::map<TString, TString> expectedResult = {
            { "b", "b" },
            { "a", "a" }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(NewFormat) {
        TString selectors = "\"sensor_name\"{a = \"a\", b = \"b\"}";
        std::map<TString, TString> result;

        std::map<TString, TString> expectedResult = {
            { "a", "a" },
            { "b", "b" },
            { "name", "sensor_name" }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(Empty) {
        TString selectors = "{}";
        std::map<TString, TString> result;

        std::map<TString, TString> expectedResult = {};

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(EmptyNewFormat) {
        TString selectors = "\"sensor_name\"{}";
        std::map<TString, TString> result;

        std::map<TString, TString> expectedResult = {
            { "name", "sensor_name" }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }

    Y_UNIT_TEST(NoBrackets) {
        TString selectors = "a = \"a\", b = \"b\"";
        std::map<TString, TString> result;

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format");
    }

    Y_UNIT_TEST(NoQuotes) {
        TString selectors = "{a = a, b = b}";
        std::map<TString, TString> result;

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format");
    }

    Y_UNIT_TEST(NoQuotesOnSensorName) {
        TString selectors = "sensor_name{a = \"a\", b = \"b\"}";
        std::map<TString, TString> result;

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), "Selectors should be specified in [\"sensor_name\"]{[label_name1 = \"label_value1\", ...]} format");
    }

    Y_UNIT_TEST(ValidLabelValues) {
        TString selectors = "\"{\"{a = \",\", b = \"}\"}";
        std::map<TString, TString> result;

        std::map<TString, TString> expectedResult = {
            { "name", "{" },
            { "a", "," },
            { "b", "}" }
        };

        UNIT_ASSERT_EQUAL(ParseSelectorValues(selectors, result), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(result, expectedResult);
    }
}

}  // namespace NYql::NSo
