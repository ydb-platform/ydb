#include "test_base.h"

class TJsonPathLaxTest: public TJsonPathTestBase {
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
            {.Json = R"([
                {"key": 1},
                {"key": 2}
            ])",
             .JsonPath = "$.key",
             .Result = {"1", "2"}},
            {.Json = R"([
                {"key": 1},
                {"key": 2}
            ])",
             .JsonPath = "$.*",
             .Result = {"1", "2"}},
            {.Json = R"({
                "first": {"key": 1},
                "second": []
            })",
             .JsonPath = "$.*.key",
             .Result = {"1"}},
            {.Json = R"({
                "first": {"key": 1},
                "second": []
            })",
             .JsonPath = "$.*.*",
             .Result = {"1"}},
            {.Json = R"({"another_key": 123})", .JsonPath = "$.key", .Result = {}},
            {.Json = R"([
                {"key": [{"nested": 28}]},
                {"key": [{"nested": 29}]}
            ])",
             .JsonPath = "$.key.nested",
             .Result = {"28", "29"}},
            {.Json = R"([
                {"key": [{"nested": 28}]},
                {"key": [{"nested": 29}]}
            ])",
             .JsonPath = "$.*.*",
             .Result = {"28", "29"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestArrayWrap() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"([1, 2])", .JsonPath = "$[*][0]", .Result = {"1", "2"}},
            {.Json = R"([[1], 2, [3]])", .JsonPath = "$[*][0]", .Result = {"1", "2", "3"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestInvalidArrayIndices() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"({
                "idx": -1,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.idx]",
             .Result = {}},
            {.Json = R"({
                "from": -1,
                "to": 3,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.from to $.to]",
             .Result = {}},
            {.Json = R"({
                "from": 0,
                "to": -1,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.from to $.to]",
             .Result = {}},
            {.Json = R"([1, 2, 3, 4, 5])", .JsonPath = "$[3 to 0]", .Result = {}},
            {.Json = R"({
                "idx": -1,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.idx, 1 to 2]",
             .Result = {"2", "3"}},
            {.Json = R"({
                "from": -1,
                "to": 3,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[0, $.from to $.to, 2 to 2]",
             .Result = {"1", "3"}},
            {.Json = R"({
                "from": 0,
                "to": -1,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[0, $.from to $.to, 1 to 1]",
             .Result = {"1", "2"}},
            {.Json = R"([1, 2, 3, 4, 5])", .JsonPath = "$[0, 3 to 0, 1]", .Result = {"1", "2"}},
            {.Json = R"([[1, 2], [3, 4, 5], []])", .JsonPath = "$[*][2]", .Result = {"5"}},
            {.Json = "[]", .JsonPath = "$[last]", .Result = {}},
            {.Json = "[]", .JsonPath = "$[last to 0]", .Result = {}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStructuralErrorsHandling() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"([[{"key": 1}]])", .JsonPath = "$.key", .Result = {}},
            {.Json = R"([[{"key": 1}]])", .JsonPath = "$.*", .Result = {}},
            {.Json = R"([
                {"key": 1},
                {"not_key": 2},
                {"key": 3}
            ])",
             .JsonPath = "$[*].key",
             .Result = {"1", "3"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestCompareOperations() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {.Json = R"({
                "left": [1, 2, 3],
                "right": [4, 5, 6]
            })",
             .JsonPath = "$.left < $.right",
             .Result = {"true"}},
            // Check incomparable types
            // NOTE: Even though values of types string and number are incomparable,
            // pair 1 < 4 is true and was found first, so the overall result is true
            {.Json = R"({
                "left": [1, 2, "string"],
                "right": [4, 5, 6]
            })",
             .JsonPath = "$.left < $.right",
             .Result = {"true"}},
            // NOTE: In this example pair "string" < 4 results in error and was found first,
            // so overall result is null
            {.Json = R"({
                "left": ["string", 2, 3],
                "right": [4, 5, 6]
            })",
             .JsonPath = "$.left < $.right",
             .Result = {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestFilter() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {.Json = R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])",
             .JsonPath = "$ ? (@.age >= 18 && @.age <= 30) . age",
             .Result = {"18", "25"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestNumericMethods() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {.Json = "[-1.23, 4.56, 3, 0]", .JsonPath = "$.abs()", .Result = {"1.23", "4.56", "3", "0"}},
            {.Json = "[-1.23, 4.56, 3, 0]", .JsonPath = "$.floor()", .Result = {"-2", "4", "3", "0"}},
            {.Json = "[-1.23, 4.56, 3, 0]", .JsonPath = "$.ceiling()", .Result = {"-1", "5", "3", "0"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestDoubleMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {.Json = R"([
                "123", "123.4", "0.567", "1234e-1", "567e-3", "123.4e-1",
                "123e3", "123e+3", "1.23e+1", "1.23e1",
                "12e0", "12.3e0", "0", "0.0", "0.0e0"
            ])",
             .JsonPath = "$.double()",
             .Result = {
                 "123",
                 "123.4",
                 "0.567",
                 "123.4",
                 "0.567",
                 "12.34",
                 "123000",
                 "123000",
                 "12.3",
                 "12.3",
                 "12",
                 "12.3",
                 "0",
                 "0",
                 "0",
             }},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestKeyValueMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrap
            {.Json = R"([{
                "one": 1,
                "two": 2,
                "three": 3
            }])",
             .JsonPath = "$.keyvalue().name",
             .Result = {"\"one\"", "\"three\"", "\"two\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestExistsPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"({
                "key": 123
            })",
             .JsonPath = "exists ($.another_key)",
             .Result = {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLikeRegexPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Check unwrapping
            {.Json = R"(["string", "123", "456"])", .JsonPath = R"($ like_regex "[0-9]+")", .Result = {"true"}},

            // Check early stopping
            {.Json = R"([123, "123", "456"])", .JsonPath = R"($ like_regex "[0-9]+")", .Result = {"null"}},
            {.Json = R"(["123", "456", 123])", .JsonPath = R"($ like_regex "[0-9]+")", .Result = {"true"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStartsWithPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"(["a", "b", "c"])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"true"}},
            {.Json = R"(["a", 1.45, 50])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"true"}},
            {.Json = R"([1.45, 50, "a"])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"null"}},
            {.Json = R"(["b", "c"])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : LaxModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TJsonPathLaxTest);
