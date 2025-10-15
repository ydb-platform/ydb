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

Y_UNIT_TEST_SUITE(TestSolomonParseLabelNames) {
    Y_UNIT_TEST(Basic) {
        TString labelNames = "label1, label2";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2"
        };
        TVector<TString> expectedAliases = {
            "label1", "label2"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(WithAliases) {
        TString labelNames = "label1 as alias1, label2 as alias2";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2"
        };
        TVector<TString> expectedAliases = {
            "alias1", "alias2"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(OneAlias) {
        TString labelNames = "label1, label2 as alias2, label3";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2", "label3"
        };
        TVector<TString> expectedAliases = {
            "label1", "alias2", "label3"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(CaseSensitivity) {
        TString labelNames = "label1, label2 AS alias2, label3";
        TVector<TString> names;
        TVector<TString> aliases;

        TVector<TString> expectedNames = {
            "label1", "label2", "label3"
        };
        TVector<TString> expectedAliases = {
            "label1", "alias2", "label3"
        };

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), TMaybe<TString>{});
        UNIT_ASSERT_EQUAL(names, expectedNames);
        UNIT_ASSERT_EQUAL(aliases, expectedAliases);
    }

    Y_UNIT_TEST(InvalidLabelName) {
        TString labelNames = "{}, {}";
        TVector<TString> names;
        TVector<TString> aliases;

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format");
    }

    Y_UNIT_TEST(NoAs) {
        TString labelNames = "label1 alias1";
        TVector<TString> names;
        TVector<TString> aliases;

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format");
    }

    Y_UNIT_TEST(EmptyAlias) {
        TString labelNames = "label1 as, label2";
        TVector<TString> names;
        TVector<TString> aliases;

        UNIT_ASSERT_EQUAL(ParseLabelNames(labelNames, names, aliases), "Label names should be specified in \"label1 [as alias1], label2 [as alias2], ...\" format");
    }
}

}  // namespace NYql::NSo
