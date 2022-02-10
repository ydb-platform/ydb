#include "test_base.h"

class TJsonPathStrictTest : public TJsonPathTestBase {
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
            {R"([
                {"key": 1},
                {"key": 2}
            ])", "$.key", C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {R"([
                {"key": 1},
                {"key": 2}
            ])", "$.*", C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {R"({
                "first": {"key": 1},
                "second": []
            })", "$.*.key", C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {R"({
                "first": {"key": 1},
                "second": []
            })", "$.*.*", C(TIssuesIds::JSONPATH_EXPECTED_OBJECT)},
            {R"({"another_key": 123})", "$.key", C(TIssuesIds::JSONPATH_MEMBER_NOT_FOUND)},
            {R"([1, 2])", "$[*][0]", C(TIssuesIds::JSONPATH_EXPECTED_ARRAY)},
            {R"([[1], 2, [3]])", "$[*][0]", C(TIssuesIds::JSONPATH_EXPECTED_ARRAY)},
            {R"({
                "idx": -1,
                "array": [1, 2, 3]
            })", "$.array[$.idx]", C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {R"({
                "from": -1,
                "to": 3,
                "array": [1, 2, 3]
            })", "$.array[$.from to $.to]", C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {R"({
                "from": 0,
                "to": -1,
                "array": [1, 2, 3]
            })", "$.array[$.from to $.to]", C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {R"({
                "from": -20,
                "to": -10,
                "array": [1, 2, 3]
            })", "$.array[$.from to $.to]", C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {R"([1, 2, 3, 4, 5])", "$[3 to 0]", C(TIssuesIds::JSONPATH_INVALID_ARRAY_INDEX_RANGE)},
            {R"([[1, 2], [3, 4, 5], []])", "$[*][2]", C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {"[]", "$[last]", C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
            {"[]", "$[last to 0]", C(TIssuesIds::JSONPATH_ARRAY_INDEX_OUT_OF_BOUNDS)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : STRICT_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestIncomparableTypes() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"({
                "left": [1, 2, "string"],
                "right": [4, 5, 6]
            })", "$.left < $.right", {"null"}},
            {R"({
                "left": ["string", 2, 3],
                "right": [4, 5, 6]
            })", "$.left < $.right", {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : STRICT_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLikeRegexPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"(["123", 123])", R"($[*] like_regex "[0-9]+")", {"null"}},
            {R"([123, "123"])", R"($[*] like_regex "[0-9]+")", {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : STRICT_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStartsWithPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"(["a", "b", "c"])", R"("abcd" starts with $[*])", {"true"}},
            {R"(["a", 1.45, 50])", R"("abcd" starts with $[*])", {"null"}},
            {R"([1.45, 50, "a"])", R"("abcd" starts with $[*])", {"null"}},
            {R"(["b", "c"])", R"("abcd" starts with $[*])", {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : STRICT_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TJsonPathStrictTest);