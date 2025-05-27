#include "test_base.h"

class TJsonPathLaxTest : public TJsonPathTestBase {
public:
    TJsonPathLaxTest()
        : TJsonPathTestBase()
    {
    }

    UNIT_TEST_SUITE(TJsonPathLaxTest);
        UNIT_TEST(TestArrayUnwrap);
        UNIT_TEST(TestArrayWrap);
        UNIT_TEST(TestInvalidArrayIndices);
        UNIT_TEST(TestStructuralErrorsHandling);
        UNIT_TEST(TestCompareOperations);
        UNIT_TEST(TestFilter);
        UNIT_TEST(TestNumericMethods);
        UNIT_TEST(TestDoubleMethod);
        UNIT_TEST(TestKeyValueMethod);
        UNIT_TEST(TestExistsPredicate);
        UNIT_TEST(TestLikeRegexPredicate);
        UNIT_TEST(TestStartsWithPredicate);
    UNIT_TEST_SUITE_END();

    void TestArrayUnwrap() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([
                {"key": 1},
                {"key": 2}
            ])", "$.key", {"1", "2"}},
            {R"([
                {"key": 1},
                {"key": 2}
            ])", "$.*", {"1", "2"}},
            {R"({
                "first": {"key": 1},
                "second": []
            })", "$.*.key", {"1"}},
            {R"({
                "first": {"key": 1},
                "second": []
            })", "$.*.*", {"1"}},
            {R"({"another_key": 123})", "$.key", {}},
            {R"([
                {"key": [{"nested": 28}]},
                {"key": [{"nested": 29}]}
            ])", "$.key.nested", {"28", "29"}},
            {R"([
                {"key": [{"nested": 28}]},
                {"key": [{"nested": 29}]}
            ])", "$.*.*", {"28", "29"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestArrayWrap() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([1, 2])", "$[*][0]", {"1", "2"}},
            {R"([[1], 2, [3]])", "$[*][0]", {"1", "2", "3"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestInvalidArrayIndices() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"({
                "idx": -1,
                "array": [1, 2, 3]
            })", "$.array[$.idx]", {}},
            {R"({
                "from": -1,
                "to": 3,
                "array": [1, 2, 3]
            })", "$.array[$.from to $.to]", {}},
            {R"({
                "from": 0,
                "to": -1,
                "array": [1, 2, 3]
            })", "$.array[$.from to $.to]", {}},
            {R"([1, 2, 3, 4, 5])", "$[3 to 0]", {}},
            {R"({
                "idx": -1,
                "array": [1, 2, 3]
            })", "$.array[$.idx, 1 to 2]", {"2", "3"}},
            {R"({
                "from": -1,
                "to": 3,
                "array": [1, 2, 3]
            })", "$.array[0, $.from to $.to, 2 to 2]", {"1", "3"}},
            {R"({
                "from": 0,
                "to": -1,
                "array": [1, 2, 3]
            })", "$.array[0, $.from to $.to, 1 to 1]", {"1", "2"}},
            {R"([1, 2, 3, 4, 5])", "$[0, 3 to 0, 1]", {"1", "2"}},
            {R"([[1, 2], [3, 4, 5], []])", "$[*][2]", {"5"}},
            {"[]", "$[last]", {}},
            {"[]", "$[last to 0]", {}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStructuralErrorsHandling() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([[{"key": 1}]])", "$.key", {}},
            {R"([[{"key": 1}]])", "$.*", {}},
            {R"([
                {"key": 1},
                {"not_key": 2},
                {"key": 3}
            ])", "$[*].key", {"1", "3"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestCompareOperations() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {R"({
                "left": [1, 2, 3],
                "right": [4, 5, 6]
            })", "$.left < $.right", {"true"}},
            // Check incomparable types
            // NOTE: Even though values of types string and number are incomparable,
            //       pair 1 < 4 is true and was found first, so the overall result is true
            {R"({
                "left": [1, 2, "string"],
                "right": [4, 5, 6]
            })", "$.left < $.right", {"true"}},
            // NOTE: In this example pair "string" < 4 results in error and was found first,
            //       so overall result is null
            {R"({
                "left": ["string", 2, 3],
                "right": [4, 5, 6]
            })", "$.left < $.right", {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestFilter() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])", "$ ? (@.age >= 18 && @.age <= 30) . age", {"18", "25"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestNumericMethods() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {"[-1.23, 4.56, 3, 0]", "$.abs()", {"1.23", "4.56", "3", "0"}},
            {"[-1.23, 4.56, 3, 0]", "$.floor()", {"-2", "4", "3", "0"}},
            {"[-1.23, 4.56, 3, 0]", "$.ceiling()", {"-1", "5", "3", "0"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestDoubleMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {R"([
                "123", "123.4", "0.567", "1234e-1", "567e-3", "123.4e-1",
                "123e3", "123e+3", "1.23e+1", "1.23e1",
                "12e0", "12.3e0", "0", "0.0", "0.0e0"
            ])", "$.double()", {
                "123", "123.4", "0.567", "123.4", "0.567", "12.34",
                "123000", "123000", "12.3", "12.3",
                "12", "12.3", "0", "0", "0",
            }},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestKeyValueMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {R"([{
                "one": 1,
                "two": 2,
                "three": 3
            }])", "$.keyvalue().name", {"\"one\"", "\"three\"", "\"two\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestExistsPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"({
                "key": 123
            })", "exists ($.another_key)", {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLikeRegexPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrapping
            {R"(["string", "123", "456"])", R"($ like_regex "[0-9]+")", {"true"}},

            // Check early stopping
            {R"([123, "123", "456"])", R"($ like_regex "[0-9]+")", {"null"}},
            {R"(["123", "456", 123])", R"($ like_regex "[0-9]+")", {"true"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStartsWithPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"(["a", "b", "c"])", R"("abcd" starts with $[*])", {"true"}},
            {R"(["a", 1.45, 50])", R"("abcd" starts with $[*])", {"true"}},
            {R"([1.45, 50, "a"])", R"("abcd" starts with $[*])", {"null"}},
            {R"(["b", "c"])", R"("abcd" starts with $[*])", {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LAX_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TJsonPathLaxTest);