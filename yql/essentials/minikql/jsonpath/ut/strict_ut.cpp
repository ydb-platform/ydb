#include "test_base.h"

class TJsonPathStrictTest: public TJsonPathTestBase {
public:
    TJsonPathStrictTest()
        : TJsonPathTestBase()
    {
    }

    UNIT_TEST_SUITE(TJsonPathStrictTest);
    UNIT_TEST(TestRuntimeErrors);
    UNIT_TEST(TestIncomparableTypes);
    UNIT_TEST(TestLikeRegexPredicate);
    UNIT_TEST(TestStartsWithPredicate);
    UNIT_TEST_SUITE_END();

    void TestRuntimeErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"([
                {"key": 1},
                {"key": 2}
            ])",
             .JsonPath = "$.key",
             .Error = C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {.Json = R"([
                {"key": 1},
                {"key": 2}
            ])",
             .JsonPath = "$.*",
             .Error = C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {.Json = R"({
                "first": {"key": 1},
                "second": []
            })",
             .JsonPath = "$.*.key",
             .Error = C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {.Json = R"({
                "first": {"key": 1},
                "second": []
            })",
             .JsonPath = "$.*.*",
             .Error = C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {.Json = R"({"another_key": 123})", .JsonPath = "$.key", .Error = C(TIssuesIds::JSONPATH_MEMBER_NOT_FOUND)},
            {.Json = R"([1, 2])", .JsonPath = "$[*][0]", .Error = C(TIssuesIds::JSONPATH_EXPECTED_ARRAY)},
            {.Json = R"([[1], 2, [3]])", .JsonPath = "$[*][0]", .Error = C(TIssuesIds::JSONPATH_EXPECTED_ARRAY)},
            {.Json = R"({
                "idx": -1,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.idx]",
             .Error = C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {.Json = R"({
                "from": -1,
                "to": 3,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.from to $.to]",
             .Error = C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {.Json = R"({
                "from": 0,
                "to": -1,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.from to $.to]",
             .Error = C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {.Json = R"({
                "from": -20,
                "to": -10,
                "array": [1, 2, 3]
            })",
             .JsonPath = "$.array[$.from to $.to]",
             .Error = C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {.Json = R"([1, 2, 3, 4, 5])", .JsonPath = "$[3 to 0]", .Error = C(TIssuesIds::JSONPATH_INVALID_ARRAY_INDEX_RANGE)},
            {.Json = R"([[1, 2], [3, 4, 5], []])", .JsonPath = "$[*][2]", .Error = C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {.Json = "[]", .JsonPath = "$[last]", .Error = C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {.Json = "[]", .JsonPath = "$[last to 0]", .Error = C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : StrictModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestIncomparableTypes() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"({
                "left": [1, 2, "string"],
                "right": [4, 5, 6]
            })",
             .JsonPath = "$.left < $.right",
             .Result = {"null"}},
            {.Json = R"({
                "left": ["string", 2, 3],
                "right": [4, 5, 6]
            })",
             .JsonPath = "$.left < $.right",
             .Result = {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : StrictModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLikeRegexPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"(["123", 123])", .JsonPath = R"($[*] like_regex "[0-9]+")", .Result = {"null"}},
            {.Json = R"([123, "123"])", .JsonPath = R"($[*] like_regex "[0-9]+")", .Result = {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : StrictModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStartsWithPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"(["a", "b", "c"])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"true"}},
            {.Json = R"(["a", 1.45, 50])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"null"}},
            {.Json = R"([1.45, 50, "a"])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"null"}},
            {.Json = R"(["b", "c"])", .JsonPath = R"("abcd" starts with $[*])", .Result = {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : StrictModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TJsonPathStrictTest);
