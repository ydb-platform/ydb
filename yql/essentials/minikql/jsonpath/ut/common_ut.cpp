#include "test_base.h"

#include <util/string/builder.h>

#include <cmath>

class TJsonPathCommonTest: public TJsonPathTestBase {
public:
    TJsonPathCommonTest()
        : TJsonPathTestBase()
    {
    }

    UNIT_TEST_SUITE(TJsonPathCommonTest);
    UNIT_TEST(TestPrimary);
    UNIT_TEST(TestMemberAccess);
    UNIT_TEST(TestWildcardMemberAccess);
    UNIT_TEST(TestArrayAccess);
    UNIT_TEST(TestLastArrayIndex);
    UNIT_TEST(TestLastArrayIndexInvalid);
    UNIT_TEST(TestNonIntegerArrayIndex);
    UNIT_TEST(TestWildcardArrayAccess);
    UNIT_TEST(TestUnaryOperations);
    UNIT_TEST(TestUnaryOperationsErrors);
    UNIT_TEST(TestBinaryArithmeticOperations);
    UNIT_TEST(TestBinaryArithmeticOperationsErrors);
    UNIT_TEST(TestParseErrors);
    UNIT_TEST(TestVariables);
    UNIT_TEST(TestDivisionByZero);
    UNIT_TEST(TestInfinityResult);
    UNIT_TEST(TestLogicalOperations);
    UNIT_TEST(TestCompareOperations);
    UNIT_TEST(TestFilter);
    UNIT_TEST(TestFilterInvalid);
    UNIT_TEST(TestNumericMethods);
    UNIT_TEST(TestNumericMethodsErrors);
    UNIT_TEST(TestDoubleMethod);
    UNIT_TEST(TestDoubleMethodErrors);
    UNIT_TEST(TestTypeMethod);
    UNIT_TEST(TestSizeMethod);
    UNIT_TEST(TestKeyValueMethod);
    UNIT_TEST(TestKeyValueMethodErrors);
    UNIT_TEST(TestStartsWithPredicate);
    UNIT_TEST(TestStartsWithPredicateErrors);
    UNIT_TEST(TestExistsPredicate);
    UNIT_TEST(TestIsUnknownPredicate);
    UNIT_TEST(TestLikeRegexPredicate);
    UNIT_TEST_SUITE_END();

    void TestPrimary() {
        const TVector<TMultiOutputTestCase> testCases = {
            // Context object $ must return whole JSON when used alone
            {.Json = R"({"key": 123})", .JsonPath = "$", .Result = {R"({"key":123})"}},
            {.Json = R"([1, 2, 3])", .JsonPath = "$", .Result = {R"([1,2,3])"}},
            {.Json = "1.234", .JsonPath = "$", .Result = {"1.234"}},
            {.Json = R"("some string")", .JsonPath = "$", .Result = {R"("some string")"}},

            // Literal must not depend on input
            {.Json = R"({"key": 123})", .JsonPath = "123", .Result = {"123"}},
            {.Json = R"([1, 2, 3])", .JsonPath = "123", .Result = {"123"}},
            {.Json = "1.234", .JsonPath = "123", .Result = {"123"}},
            {.Json = R"("some string")", .JsonPath = "123", .Result = {"123"}},

            // Check various ways to define number literal
            {.Json = "1", .JsonPath = "123.4", .Result = {"123.4"}},
            {.Json = "1", .JsonPath = "0.567", .Result = {"0.567"}},

            {.Json = "1", .JsonPath = "1234e-1", .Result = {"123.4"}},
            {.Json = "1", .JsonPath = "567e-3", .Result = {"0.567"}},
            {.Json = "1", .JsonPath = "123.4e-1", .Result = {"12.34"}},

            {.Json = "1", .JsonPath = "123e3", .Result = {"123000"}},
            {.Json = "1", .JsonPath = "123e+3", .Result = {"123000"}},
            {.Json = "1", .JsonPath = "1.23e+1", .Result = {"12.3"}},
            {.Json = "1", .JsonPath = "1.23e1", .Result = {"12.3"}},

            {.Json = "1", .JsonPath = "12e0", .Result = {"12"}},
            {.Json = "1", .JsonPath = "12.3e0", .Result = {"12.3"}},

            {.Json = "1", .JsonPath = "0", .Result = {"0"}},
            {.Json = "1", .JsonPath = "0.0", .Result = {"0"}},
            {.Json = "1", .JsonPath = "0.0e0", .Result = {"0"}},

            // Check boolean and null literals
            {.Json = "1", .JsonPath = "null", .Result = {"null"}},
            {.Json = "1", .JsonPath = "false", .Result = {"false"}},
            {.Json = "1", .JsonPath = "true", .Result = {"true"}},

            // Check string literals
            {.Json = "1", .JsonPath = "\"string\"", .Result = {"\"string\""}},
            {.Json = "1", .JsonPath = "\"  space  another space  \"", .Result = {"\"  space  another space  \""}},
            {.Json = "1", .JsonPath = "\"привет\"", .Result = {"\"привет\""}},
            // NOTE: escaping is added by library/cpp/json
            {.Json = "1", .JsonPath = "\"\r\n\t\"", .Result = {R"("\r\n\t")"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestMemberAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"({"key": 123, "another_key": 456})", .JsonPath = "$.key", .Result = {"123"}},
            {.Json = R"({"key": 123, "_another_28_key_$_": 456})", .JsonPath = "$._another_28_key_$_", .Result = {"456"}},
            {.Json = R"({"key": 123, "another_key": 456})", .JsonPath = "           $.another_key    ", .Result = {"456"}},

            {.Json = R"({"key": 123, "another_key": 456})", .JsonPath = "$.key", .Result = {"123"}},
            {.Json = R"({"k\"ey": 123, "another_key": 456})", .JsonPath = R"($."k\"ey")", .Result = {"123"}},
            {.Json = R"({"k\"ey": 123, "another_key": 456})", .JsonPath = "$.'k\\\"ey'", .Result = {"123"}},

            {.Json = R"({"key": 123, "another_key": 456})", .JsonPath = "$.'key'", .Result = {"123"}},
            {.Json = R"({"key": 123, "_another_28_key_$_": 456})", .JsonPath = "$.'_another_28_key_$_'", .Result = {"456"}},
            {.Json = R"({"key": 123, "another_key": 456})", .JsonPath = "           $.'another_key'    ", .Result = {"456"}},

            {.Json = R"({"key": 123, "another_key": 456})", .JsonPath = "$.\"key\"", .Result = {"123"}},
            {.Json = R"({"key": 123, "_another_28_key_$_": 456})", .JsonPath = "$.\"_another_28_key_$_\"", .Result = {"456"}},
            {.Json = R"({"key": 123, "another_key": 456})", .JsonPath = "           $.\"another_key\"    ", .Result = {"456"}},

            {.Json = R"({"key": 123, "another key": 456})", .JsonPath = "$.'another key'", .Result = {"456"}},
            {.Json = R"({"key": 123, "another key": 456})", .JsonPath = "$.\"another key\"", .Result = {"456"}},

            {.Json = R"({"key": 123, "прием отбой": 456})", .JsonPath = "$.'прием отбой'", .Result = {"456"}},
            {.Json = R"({"key": 123, "прием отбой": 456})", .JsonPath = "$.\"прием отбой\"", .Result = {"456"}},

            {.Json = R"({"key": {"another": 456}})", .JsonPath = "$.key.another", .Result = {"456"}},
            {.Json = R"({"key": {"another key": 456}})", .JsonPath = "$.'key'.\"another key\"", .Result = {"456"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestWildcardMemberAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"({
                "first": 12,
                "second": 72
            })",
             .JsonPath = "$.*",
             .Result = {"12", "72"}},
            {.Json = R"({
                "friends": {
                    "Nik": {"age": 18},
                    "Kate": {"age": 72}
                }
            })",
             .JsonPath = "$.friends.*.age",
             .Result = {"72", "18"}},
            {.Json = R"({
                "friends": {
                    "Nik": {"age": 18},
                    "Kate": {"age": 72}
                }
            })",
             .JsonPath = "$.*.*.*",
             .Result = {"72", "18"}},
            {.Json = R"({})", .JsonPath = "$.*.key", .Result = {}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestArrayAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"([1, 2, 3])", .JsonPath = "$[0]", .Result = {"1"}},
            {.Json = R"([1, 2, 3, 4, 5, 6])", .JsonPath = "$[0 to 2]", .Result = {"1", "2", "3"}},
            {.Json = R"([1, 2, 3, 4, 5, 6])", .JsonPath = "$[5, 0 to 2, 0, 0, 3 to 5, 2]", .Result = {"6", "1", "2", "3", "1", "1", "4", "5", "6", "3"}},
            {.Json = R"({
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })",
             .JsonPath = "$.friends[1 to 3, 0].age",
             .Result = {"72", "50", "60", "18"}},
            {.Json = R"({
                "range": {
                    "from": 1,
                    "to": 2
                },
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })",
             .JsonPath = "$.friends[$.range.from to $.range.to].age",
             .Result = {"72", "50"}},
            {.Json = R"({
                "range": {
                    "from": [1, 3, 4],
                    "to": {"key1": 1, "key2": 2, "key3": 3}
                },
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })",
             .JsonPath = "$.friends[$.range.from[1] to $.range.to.key3].age",
             .Result = {"60"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLastArrayIndex() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"([1, 2, 3])", .JsonPath = "$[last]", .Result = {"3"}},
            {.Json = R"([1, 2, 3])", .JsonPath = "$[1 to last]", .Result = {"2", "3"}},
            {.Json = R"([1, 2, 3])", .JsonPath = "$[last to last]", .Result = {"3"}},
            {.Json = R"([1, 2, 3, 5, 6])", .JsonPath = "$[1, last, last, 0, 2 to last, 3]", .Result = {"2", "6", "6", "1", "3", "5", "6", "5"}},
            {.Json = R"([
                [1, 2, 3, 4],
                [5, 6, 7, 8]
            ])",
             .JsonPath = "$[*][last]",
             .Result = {"4", "8"}},
            {.Json = R"({
                "ranges": [
                    {"from": 1, "to": 3},
                    {"from": 0, "to": 1}
                ],
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })",
             .JsonPath = "$.friends[last, $.ranges[last].from to $.ranges[last].to, 2 to last].age",
             .Result = {"60", "18", "72", "50", "60"}},
            {.Json = R"({
                "ranges": [
                    {"from": 1.23, "to": 3.75},
                    {"from": 0.58, "to": 1.00001}
                ],
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })",
             .JsonPath = "$.friends[last, $.ranges[last].from to $.ranges[last].to, 2 to last].age",
             .Result = {"60", "18", "72", "50", "60"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLastArrayIndexInvalid() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"({})", .JsonPath = "last", .Error = C(TIssuesIds::JSONPATH_LAST_OUTSIDE_OF_ARRAY_SUBSCRIPT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestNonIntegerArrayIndex() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"({
                "range": {
                    "from": [1, 3, 4],
                    "to": {"key1": 1, "key2": 2, "key3": 3}
                },
                "friends": [1, 2, 3]
            })",
             .JsonPath = "$.friends[$.range.from[*] to $.range.to.*]",
             .Error = C(TIssuesIds::JSONPATH_INVALID_ARRAY_INDEX)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestWildcardArrayAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"([1, 2, 3])", .JsonPath = "$[*]", .Result = {"1", "2", "3"}},
            {.Json = R"([[1], [2], [3, 4, 5]])", .JsonPath = "$[*][*]", .Result = {"1", "2", "3", "4", "5"}},
            {.Json = R"({
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })",
             .JsonPath = "$.friends[*].age",
             .Result = {"18", "72", "50", "60"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestUnaryOperations() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"([])", .JsonPath = "-3", .Result = {"-3"}},
            {.Json = R"([])", .JsonPath = "+3", .Result = {"3"}},
            {.Json = R"(-1)", .JsonPath = "-$", .Result = {"1"}},
            {.Json = R"(-1)", .JsonPath = "+$", .Result = {"-1"}},
            {.Json = R"({
                "range": {
                    "from": -1,
                    "to": -2
                },
                "array": [1, 2, 3, 4]
            })",
             .JsonPath = "$.array[-$.range.from to -$.range.to]",
             .Result = {"2", "3"}},
            {.Json = R"({
                "range": {
                    "from": 1,
                    "to": -2
                },
                "array": [1, 2, 3, 4]
            })",
             .JsonPath = "$.array[+$.range.from to -$.range.to]",
             .Result = {"2", "3"}},
            {.Json = R"({
                "range": {
                    "from": -1,
                    "to": 2
                },
                "array": [1, 2, 3, 4]
            })",
             .JsonPath = "$.array[-$.range.from to +$.range.to]",
             .Result = {"2", "3"}},
            {.Json = R"({
                "range": {
                    "from": 1,
                    "to": 2
                },
                "array": [1, 2, 3, 4]
            })",
             .JsonPath = "$.array[+$.range.from to +$.range.to]",
             .Result = {"2", "3"}},
            {.Json = R"([1, 2, 3])", .JsonPath = "-$[*]", .Result = {"-1", "-2", "-3"}},
            {.Json = "30000000000000000000000000", .JsonPath = "-$", .Result = {"-3e+25"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestUnaryOperationsErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"({})", .JsonPath = "-$", .Error = C(TIssuesIds::JSONPATH_INVALID_UNARY_OPERATION_ARGUMENT_TYPE)},
            {.Json = R"([1, 2, [], 4])", .JsonPath = "-$[*]", .Error = C(TIssuesIds::JSONPATH_INVALID_UNARY_OPERATION_ARGUMENT_TYPE)},
            {.Json = R"([1, 2, {}, 4])", .JsonPath = "-$[*]", .Error = C(TIssuesIds::JSONPATH_INVALID_UNARY_OPERATION_ARGUMENT_TYPE)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestBinaryArithmeticOperations() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = "[]", .JsonPath = "1 + 2", .Result = {"3"}},
            {.Json = "[]", .JsonPath = "1 - 2", .Result = {"-1"}},
            {.Json = "[]", .JsonPath = "10 * 5", .Result = {"50"}},
            {.Json = "[]", .JsonPath = "10 / 5", .Result = {"2"}},
            {.Json = "[]", .JsonPath = "13 % 5", .Result = {"3"}},

            {.Json = "[]", .JsonPath = "20 * 2 + 5", .Result = {"45"}},
            {.Json = "[]", .JsonPath = "20 / 2 + 5", .Result = {"15"}},
            {.Json = "[]", .JsonPath = "20 % 2 + 5", .Result = {"5"}},

            {.Json = "[]", .JsonPath = "20 * (2 + 5)", .Result = {"140"}},
            {.Json = "[]", .JsonPath = "20 / (2 + 3)", .Result = {"4"}},
            {.Json = "[]", .JsonPath = "20 % (2 + 5)", .Result = {"6"}},

            {.Json = "[]", .JsonPath = "5 / 2", .Result = {"2.5"}},
            {.Json = "[5.24 , 2.62]", .JsonPath = "$[0] / $[1]", .Result = {"2"}},
            {.Json = "[5.24, 2.62]", .JsonPath = "$[0] % $[1]", .Result = {"0"}},
            {.Json = "[3.753, 2.35]", .JsonPath = "$[0] % $[1]", .Result = {"1.403"}},

            {.Json = "[]", .JsonPath = "- 1 + 1", .Result = {"0"}},
            {.Json = "[]", .JsonPath = "+ 1 + 1", .Result = {"2"}},

            {.Json = "[1, 2, 3, 4]", .JsonPath = "$[last, last-1, last-2, last-3]", .Result = {"4", "3", "2", "1"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestBinaryArithmeticOperationsErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = "[1, 2, 3]", .JsonPath = "$[*] + 1", .Error = C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT)},
            {.Json = "[1, 2, 3]", .JsonPath = "1 + $[*]", .Error = C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT)},
            {.Json = "[1, 2, 3]", .JsonPath = "$[*] + $[*]", .Error = C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT)},

            {.Json = "[1, 2, 3]", .JsonPath = "$ + 1", .Error = C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT_TYPE)},
            {.Json = "[1, 2, 3]", .JsonPath = "1 + $", .Error = C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT_TYPE)},
            {.Json = "[1, 2, 3]", .JsonPath = "$ + $", .Error = C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT_TYPE)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestParseErrors() {
        const TVector<TString> testCases = {
            "strict",
            "strict smth.key",
            "strict $.",
            "strict $.$key",
            "strict $.28key",
            "strict $.ke^y",
            "strict $.привет",
            "strict $._пока_28_ключ_$_",
            "     strict       $.пока    ",
            "lax",
            "lax smth.key",
            "lax $.",
            "lax $.$key",
            "lax $.28key",
            "lax $.ke^y",
            "lax $.привет",
            "lax $._пока_28_ключ_$_",
            "     lax       $.пока    ",
            "12.",
            "12..3",
            "12.3e",
            "12.3e++1",
            "12.3e--1",
            "1e100000000000000000000000000000000",
            "true || false",
            "1 && (true == true)",
            "!true",
            "$[*] ? (@.active) . id",
            "!(1 > 2).type()",
            "(null) is unknown",
            "(12 * 12) is unknown",
            R"($ like_regex "[[[")",
            R"($ like_regex "[0-9]+" flag "x")",
            "$.first fjrfrfq fqijrhfqiwrjhfqrf qrfqr",
        };

        for (const auto& testCase : testCases) {
            RunParseErrorTestCase(testCase);
        }
    }

    void TestVariables() {
        TVector<TVariablesTestCase> testCases = {
            {.Json = "123", .Variables = {{"var", "456"}}, .JsonPath = "$ + $var", .Result = {"579"}},
            {.Json = "123", .Variables = {{"var", "456"}}, .JsonPath = "$var", .Result = {"456"}},
            {.Json = "123", .Variables = {{"var", R"({"key": [1, 2, 3, 4, 5]})"}}, .JsonPath = "$var.key[2 to last]", .Result = {"3", "4", "5"}},
            {.Json = "123", .Variables = {{"to", "1"}, {"strict", "2"}}, .JsonPath = "$to + $strict", .Result = {"3"}},
        };
        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunVariablesTestCase(testCase.Json, testCase.Variables, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestDivisionByZero() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = "0", .JsonPath = "1 / $", .Error = C(TIssuesIds::JSONPATH_DIVISION_BY_ZERO)},
            {.Json = "0.00000000000000000001", .JsonPath = "1 / $", .Error = C(TIssuesIds::JSONPATH_DIVISION_BY_ZERO)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestInfinityResult() {
        const double step = 1000000000;
        double current = step;
        TStringBuilder literal;
        TStringBuilder query;
        literal << '"' << step;
        query << step;
        while (!std::isinf(current)) {
            query << " * " << step;
            literal << "000000000";
            current *= step;
        }
        literal << '"';

        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = "0", .JsonPath = TString(query), .Error = C(TIssuesIds::JSONPATH_BINARY_OPERATION_RESULT_INFINITY)},
            {.Json = TString(literal), .JsonPath = "$.double()", .Error = C(TIssuesIds::JSONPATH_INFINITE_NUMBER_STRING)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestLogicalOperations() {
        const TVector<TMultiOutputTestCase> testCases = {
            // JsonPath does not allow to use boolean literals in boolean operators.
            // Here we use their replacements:
            // 1. "(1 < true)" for "null"
            // 2. "(true == true)" for "true"
            // 3. "(true != true)" for "false"
            {.Json = "1", .JsonPath = "(1 < true) || (1 < true)", .Result = {"null"}},
            {.Json = "1", .JsonPath = "(1 < true) || (true != true)", .Result = {"null"}},
            {.Json = "1", .JsonPath = "(1 < true) || (true == true)", .Result = {"true"}},
            {.Json = "1", .JsonPath = "(true != true) || (1 < true)", .Result = {"null"}},
            {.Json = "1", .JsonPath = "(true != true) || (true != true)", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(true != true) || (true == true)", .Result = {"true"}},
            {.Json = "1", .JsonPath = "(true == true) || (1 < true)", .Result = {"true"}},
            {.Json = "1", .JsonPath = "(true == true) || (true != true)", .Result = {"true"}},
            {.Json = "1", .JsonPath = "(true == true) || (true == true)", .Result = {"true"}},

            {.Json = "1", .JsonPath = "(1 < true) && (1 < true)", .Result = {"null"}},
            {.Json = "1", .JsonPath = "(1 < true) && (true != true)", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(1 < true) && (true == true)", .Result = {"null"}},
            {.Json = "1", .JsonPath = "(true != true) && (1 < true)", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(true != true) && (true != true)", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(true != true) && (true == true)", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(true == true) && (1 < true)", .Result = {"null"}},
            {.Json = "1", .JsonPath = "(true == true) && (true != true)", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(true == true) && (true == true)", .Result = {"true"}},

            {.Json = "1", .JsonPath = "(true != true) && (true != true) || (true == true)", .Result = {"true"}},
            {.Json = "1", .JsonPath = "(true != true) && ((true != true) || (true == true))", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(true != true) || (true != true) || (true == true)", .Result = {"true"}},
            {.Json = "1", .JsonPath = "(true == true) && (true == true) && (true == true) && (true != true)", .Result = {"false"}},

            {.Json = "1", .JsonPath = "!(1 < true)", .Result = {"null"}},
            {.Json = "1", .JsonPath = "!(true != true)", .Result = {"true"}},
            {.Json = "1", .JsonPath = "!(true == true)", .Result = {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestCompareOperations() {
        const TVector<TString> operations = {"==", "<", "<=", ">", ">=", "!=", "<>"};
        // All compare operations between null and non-null operands are false
        for (const auto& op : operations) {
            RunTestCase("1", TStringBuilder() << "null " << op << " 1", {"false"});
            RunTestCase("1", TStringBuilder() << "1 " << op << " null", {"false"});
        }

        // If one of the operands is not scalar, comparison results to null
        for (const auto& op : operations) {
            RunTestCase("[[]]", TStringBuilder() << "$ " << op << " 1", {"null"});
            RunTestCase("[[]]", TStringBuilder() << "1 " << op << " $", {"null"});
            RunTestCase("[[]]", TStringBuilder() << "$ " << op << " $", {"null"});

            RunTestCase("{}", TStringBuilder() << "$ " << op << " 1", {"null"});
            RunTestCase("{}", TStringBuilder() << "1 " << op << " $", {"null"});
            RunTestCase("{}", TStringBuilder() << "$ " << op << " $", {"null"});
        }

        // If both operands are null, only == is true
        for (const auto& op : operations) {
            const TString result = op == "==" ? "true" : "false";
            RunTestCase("1", TStringBuilder() << "null " << op << " null", {result});
        }

        const TVector<TMultiOutputTestCase> testCases = {
            // Check comparison of numbers
            {.Json = "1", .JsonPath = "1.23 < 4.56", .Result = {"true"}},
            {.Json = "1", .JsonPath = "1.23 > 4.56", .Result = {"false"}},
            {.Json = "1", .JsonPath = "1.23 <= 4.56", .Result = {"true"}},
            {.Json = "1", .JsonPath = "1.23 >= 4.56", .Result = {"false"}},
            {.Json = "1", .JsonPath = "1.23 == 1.23", .Result = {"true"}},
            {.Json = "1", .JsonPath = "1.23 != 1.23", .Result = {"false"}},
            {.Json = "1", .JsonPath = "1.23 <> 4.56", .Result = {"true"}},
            {.Json = "1", .JsonPath = "1.00000000000000000001 == 1.00000000000000000002", .Result = {"true"}},

            // Check numbers of different kinds (int64 vs double)
            {.Json = "1", .JsonPath = "1 < 2.33", .Result = {"true"}},
            {.Json = "1", .JsonPath = "1 > 4.56", .Result = {"false"}},
            {.Json = "1", .JsonPath = "1 <= 4.56", .Result = {"true"}},
            {.Json = "1", .JsonPath = "1 >= 4.56", .Result = {"false"}},
            {.Json = "1", .JsonPath = "1 == 1.23", .Result = {"false"}},
            {.Json = "1", .JsonPath = "1 != 1.23", .Result = {"true"}},
            {.Json = "1", .JsonPath = "1 <> 4.56", .Result = {"true"}},

            // Check comparison of strings
            {.Json = "1", .JsonPath = R"("abc" < "def")", .Result = {"true"}},
            {.Json = "1", .JsonPath = R"("abc" > "def")", .Result = {"false"}},
            {.Json = "1", .JsonPath = R"("abc" <= "def")", .Result = {"true"}},
            {.Json = "1", .JsonPath = R"("abc" >= "def")", .Result = {"false"}},
            {.Json = "1", .JsonPath = R"("abc" == "abc")", .Result = {"true"}},
            {.Json = "1", .JsonPath = R"("abc" != "abc")", .Result = {"false"}},
            {.Json = "1", .JsonPath = R"("abc" <> "def")", .Result = {"true"}},

            // Check comparison of UTF8 strings
            // First string is U+00e9 (LATIN SMALL LETTER E WITH ACUTE), "é"
            // Second string is U+0065 (LATIN SMALL LETTER E) U+0301 (COMBINING ACUTE ACCENT), "é"
            {.Json = "1", .JsonPath = R"("é" < "é")", .Result = {"false"}},
            {.Json = "1", .JsonPath = R"("é" > "é")", .Result = {"true"}},
            {.Json = "1", .JsonPath = R"("привет" == "привет")", .Result = {"true"}},

            // Check cross-product comparison
            {.Json = R"({
                "left": [1],
                "right": [4, 5, 6]
            })",
             .JsonPath = "$.left[*] < $.right[*]",
             .Result = {"true"}},
            {.Json = R"({
                "left": [4, 5, 6],
                "right": [1]
            })",
             .JsonPath = "$.left[*] < $.right[*]",
             .Result = {"false"}},
            {.Json = R"({
                "left": [1, 2, 3],
                "right": [4, 5, 6]
            })",
             .JsonPath = "$.left[*] < $.right[*]",
             .Result = {"true"}},
            {.Json = R"({
                "left": [10, 30, 40],
                "right": [1, 2, 15]
            })",
             .JsonPath = "$.left[*] < $.right[*]",
             .Result = {"true"}},
            {.Json = R"({
                "left": [10, 30, 40],
                "right": [1, 2, 3]
            })",
             .JsonPath = "$.left[*] < $.right[*]",
             .Result = {"false"}},

            // Check incomparable types
            {.Json = "1", .JsonPath = "1 < true", .Result = {"null"}},
            {.Json = "1", .JsonPath = R"(true <> "def")", .Result = {"null"}},

            // Check error in arguments
            {.Json = R"({
                "array": [1, 2, 3, 4, 5],
                "invalid_index": {
                    "key": 1
                }
            })",
             .JsonPath = "$.array[$.invalid_index] < 3",
             .Result = {"null"}},
            {.Json = R"({
                "array": [1, 2, 3, 4, 5],
                "invalid_index": {
                    "key": 1
                }
            })",
             .JsonPath = "5 >= $.array[$.invalid_index]",
             .Result = {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestFilter() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = "[1, 2, 3]", .JsonPath = "$[*] ? (@ > 2)", .Result = {"3"}},
            {.Json = R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])",
             .JsonPath = "$[*] ? (@.age >= 18)",
             .Result = {R"({"age":18})", R"({"age":25})", R"({"age":50})"}},
            {.Json = R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])",
             .JsonPath = "$[*] ? (@.age >= 18) ? (@.age <= 30)",
             .Result = {R"({"age":18})", R"({"age":25})"}},
            {.Json = R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])",
             .JsonPath = "$[*] ? (@.age >= 18) ? (@.age <= 30) . age",
             .Result = {"18", "25"}},
            {.Json = R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])",
             .JsonPath = "$[*] ? (@.age >= 18 && @.age <= 30) . age",
             .Result = {"18", "25"}},
            {.Json = R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])",
             .JsonPath = "$[*] ? (@.age >= 18 || @.age <= 30) . age",
             .Result = {"18", "25", "50", "5"}},
            {.Json = R"([
                {
                    "id": 1,
                    "is_valid": false,
                    "days_till_doom": 11,
                    "age_estimation": 4
                },
                {
                    "id": 2,
                    "is_valid": true,
                    "days_till_doom": 5,
                    "age_estimation": 3
                },
                {
                    "id": 3,
                    "is_valid": true,
                    "days_till_doom": 20,
                    "age_estimation": 10
                },
                {
                    "id": 4,
                    "is_valid": true,
                    "days_till_doom": 30,
                    "age_estimation": 2
                }
            ])",
             .JsonPath = "$[*] ? (@.is_valid == true && @.days_till_doom > 10 && 2 * @.age_estimation <= 12).id",
             .Result = {"4"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestFilterInvalid() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"({})", .JsonPath = "@", .Error = C(TIssuesIds::JSONPATH_FILTER_OBJECT_OUTSIDE_OF_FILTER)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestNumericMethods() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = "[-1.23, 4.56, 3, 0]", .JsonPath = "$[*].abs()", .Result = {"1.23", "4.56", "3", "0"}},
            {.Json = "[-1.23, 4.56, 3, 0]", .JsonPath = "$[*].floor()", .Result = {"-2", "4", "3", "0"}},
            {.Json = "[-1.23, 4.56, 3, 0]", .JsonPath = "$[*].ceiling()", .Result = {"-1", "5", "3", "0"}},
            {.Json = "-123.45", .JsonPath = "$.ceiling().abs().floor()", .Result = {"123"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestNumericMethodsErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"(["1", true, null])", .JsonPath = "$[*].abs()", .Error = C(TIssuesIds::JSONPATH_INVALID_NUMERIC_METHOD_ARGUMENT)},
            {.Json = R"(["1", true, null])", .JsonPath = "$[*].floor()", .Error = C(TIssuesIds::JSONPATH_INVALID_NUMERIC_METHOD_ARGUMENT)},
            {.Json = R"(["1", true, null])", .JsonPath = "$[*].ceiling()", .Error = C(TIssuesIds::JSONPATH_INVALID_NUMERIC_METHOD_ARGUMENT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestDoubleMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"([
                "123", "123.4", "0.567", "1234e-1", "567e-3", "123.4e-1",
                "123e3", "123e+3", "1.23e+1", "1.23e1",
                "12e0", "12.3e0", "0", "0.0", "0.0e0"
            ])",
             .JsonPath = "$[*].double()",
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
            {.Json = R"("-123.45e1")", .JsonPath = "$.double().abs().floor()", .Result = {"1234"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestDoubleMethodErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"(["1", true, null])", .JsonPath = "$[*].double()", .Error = C(TIssuesIds::JSONPATH_INVALID_DOUBLE_METHOD_ARGUMENT)},
            {.Json = R"("hi stranger")", .JsonPath = "$.double()", .Error = C(TIssuesIds::JSONPATH_INVALID_NUMBER_STRING)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestTypeMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = "null", .JsonPath = "$.type()", .Result = {"\"null\""}},
            {.Json = "true", .JsonPath = "$.type()", .Result = {"\"boolean\""}},
            {.Json = "false", .JsonPath = "$.type()", .Result = {"\"boolean\""}},
            {.Json = "1", .JsonPath = "$.type()", .Result = {"\"number\""}},
            {.Json = "-1", .JsonPath = "$.type()", .Result = {"\"number\""}},
            {.Json = "4.56", .JsonPath = "$.type()", .Result = {"\"number\""}},
            {.Json = "-4.56", .JsonPath = "$.type()", .Result = {"\"number\""}},
            {.Json = "\"some string\"", .JsonPath = "$.type()", .Result = {"\"string\""}},
            {.Json = "[]", .JsonPath = "$.type()", .Result = {"\"array\""}},
            {.Json = "[1, 2, 3, 4]", .JsonPath = "$.type()", .Result = {"\"array\""}},
            {.Json = "{}", .JsonPath = "$.type()", .Result = {"\"object\""}},
            {.Json = "{\"key\": 123}", .JsonPath = "$.type()", .Result = {"\"object\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestSizeMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = "null", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "true", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "false", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "1", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "-1", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "4.56", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "-4.56", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "\"some string\"", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "[]", .JsonPath = "$.size()", .Result = {"0"}},
            {.Json = "[1, 2, 3, 4]", .JsonPath = "$.size()", .Result = {"4"}},
            {.Json = "{}", .JsonPath = "$.size()", .Result = {"1"}},
            {.Json = "{\"key\": 123}", .JsonPath = "$.size()", .Result = {"1"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestKeyValueMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"({
                "one": 1,
                "two": 2,
                "three": 3
            })",
             .JsonPath = "$.keyvalue()",
             .Result = {
                 R"({"name":"one","value":1})",
                 R"({"name":"three","value":3})",
                 R"({"name":"two","value":2})",
             }},
            {.Json = R"({
                "one": "string",
                "two": [1, 2, 3, 4],
                "three": [4, 5]
            })",
             .JsonPath = R"($.keyvalue() ? (@.value.type() == "array" && @.value.size() > 2).name)",
             .Result = {"\"two\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestKeyValueMethodErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = "\"string\"", .JsonPath = "$.keyvalue()", .Error = C(TIssuesIds::JSONPATH_INVALID_KEYVALUE_METHOD_ARGUMENT)},
            {.Json = "[1, 2, 3, 4]", .JsonPath = "$.keyvalue()", .Error = C(TIssuesIds::JSONPATH_INVALID_KEYVALUE_METHOD_ARGUMENT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestStartsWithPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = "1", .JsonPath = R"("some string" starts with "some")", .Result = {"true"}},
            {.Json = "1", .JsonPath = R"("some string" starts with "string")", .Result = {"false"}},
            {.Json = R"(["some string", "string"])", .JsonPath = R"($[*] ? (@ starts with "string"))", .Result = {"\"string\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStartsWithPredicateErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {.Json = R"(["first", "second"])", .JsonPath = R"($[*] starts with "first")", .Error = C(TIssuesIds::JSONPATH_INVALID_STARTS_WITH_ARGUMENT)},
            {.Json = "1", .JsonPath = R"(1 starts with "string")", .Error = C(TIssuesIds::JSONPATH_INVALID_STARTS_WITH_ARGUMENT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestExistsPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"({
                "key": 123
            })",
             .JsonPath = "exists ($.key)",
             .Result = {"true"}},
            {.Json = "\"string\"", .JsonPath = "exists ($ * 2)", .Result = {"null"}},
            {.Json = R"(["some string", 2])", .JsonPath = "$[*] ? (exists (@ * 2))", .Result = {"2"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestIsUnknownPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = "1", .JsonPath = "(1 < true) is unknown", .Result = {"true"}},
            {.Json = "1", .JsonPath = "(true == true) is unknown", .Result = {"false"}},
            {.Json = "1", .JsonPath = "(true == false) is unknown", .Result = {"false"}},
            {.Json = R"(["some string", -20])", .JsonPath = "$[*] ? ((1 < @) is unknown)", .Result = {"\"some string\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLikeRegexPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {.Json = R"(["string", "123", "456"])", .JsonPath = R"($[*] like_regex "[0-9]+")", .Result = {"true"}},
            {.Json = R"(["string", "another string"])", .JsonPath = R"($[*] like_regex "[0-9]+")", .Result = {"false"}},

            // Case insensitive flag
            {.Json = R"("AbCd")", .JsonPath = R"($ like_regex "abcd")", .Result = {"false"}},
            {.Json = R"("AbCd")", .JsonPath = R"($ like_regex "abcd" flag "i")", .Result = {"true"}},

            {.Json = R"(["string", "123", "456"])", .JsonPath = R"($[*] ? (@ like_regex "[0-9]+"))", .Result = {"\"123\"", "\"456\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : AllModes_) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TJsonPathCommonTest);
