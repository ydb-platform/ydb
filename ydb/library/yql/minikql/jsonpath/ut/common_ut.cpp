#include "test_base.h"

#include <util/string/builder.h>

#include <cmath>

class TJsonPathCommonTest : public TJsonPathTestBase {
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
            {R"({"key": 123})", "$", {R"({"key":123})"}},
            {R"([1, 2, 3])", "$", {R"([1,2,3])"}},
            {"1.234", "$", {"1.234"}},
            {R"("some string")", "$", {R"("some string")"}},

            // Literal must not depend on input
            {R"({"key": 123})", "123", {"123"}},
            {R"([1, 2, 3])", "123", {"123"}},
            {"1.234", "123", {"123"}},
            {R"("some string")", "123", {"123"}},

            // Check various ways to define number literal
            {"1", "123.4", {"123.4"}},
            {"1", "0.567", {"0.567"}},

            {"1", "1234e-1", {"123.4"}},
            {"1", "567e-3", {"0.567"}},
            {"1", "123.4e-1", {"12.34"}},

            {"1", "123e3", {"123000"}},
            {"1", "123e+3", {"123000"}},
            {"1", "1.23e+1", {"12.3"}},
            {"1", "1.23e1", {"12.3"}},

            {"1", "12e0", {"12"}},
            {"1", "12.3e0", {"12.3"}},

            {"1", "0", {"0"}},
            {"1", "0.0", {"0"}},
            {"1", "0.0e0", {"0"}},

            // Check boolean and null literals
            {"1", "null", {"null"}},
            {"1", "false", {"false"}},
            {"1", "true", {"true"}},

            // Check string literals
            {"1", "\"string\"", {"\"string\""}},
            {"1", "\"  space  another space  \"", {"\"  space  another space  \""}},
            {"1", "\"привет\"", {"\"привет\""}},
            // NOTE: escaping is added by library/cpp/json
            {"1", "\"\r\n\t\"", {"\"\\r\\n\\t\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestMemberAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"({"key": 123, "another_key": 456})", "$.key", {"123"}},
            {R"({"key": 123, "_another_28_key_$_": 456})", "$._another_28_key_$_", {"456"}},
            {R"({"key": 123, "another_key": 456})", "           $.another_key    ", {"456"}},

            {R"({"key": 123, "another_key": 456})", "$.key", {"123"}},
            {R"({"k\"ey": 123, "another_key": 456})", "$.\"k\\\"ey\"", {"123"}},
            {R"({"k\"ey": 123, "another_key": 456})", "$.'k\\\"ey'", {"123"}},

            {R"({"key": 123, "another_key": 456})", "$.'key'", {"123"}},
            {R"({"key": 123, "_another_28_key_$_": 456})", "$.'_another_28_key_$_'", {"456"}},
            {R"({"key": 123, "another_key": 456})", "           $.'another_key'    ", {"456"}},

            {R"({"key": 123, "another_key": 456})", "$.\"key\"", {"123"}},
            {R"({"key": 123, "_another_28_key_$_": 456})", "$.\"_another_28_key_$_\"", {"456"}},
            {R"({"key": 123, "another_key": 456})", "           $.\"another_key\"    ", {"456"}},

            {R"({"key": 123, "another key": 456})", "$.'another key'", {"456"}},
            {R"({"key": 123, "another key": 456})", "$.\"another key\"", {"456"}},

            {R"({"key": 123, "прием отбой": 456})", "$.'прием отбой'", {"456"}},
            {R"({"key": 123, "прием отбой": 456})", "$.\"прием отбой\"", {"456"}},

            {R"({"key": {"another": 456}})", "$.key.another", {"456"}},
            {R"({"key": {"another key": 456}})", "$.'key'.\"another key\"", {"456"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestWildcardMemberAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"({
                "first": 12,
                "second": 72
            })", "$.*", {"12", "72"}},
            {R"({
                "friends": {
                    "Nik": {"age": 18},
                    "Kate": {"age": 72}
                }
            })", "$.friends.*.age", {"72", "18"}},
            {R"({
                "friends": {
                    "Nik": {"age": 18},
                    "Kate": {"age": 72}
                }
            })", "$.*.*.*", {"72", "18"}},
            {R"({})", "$.*.key", {}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestArrayAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([1, 2, 3])", "$[0]", {"1"}},
            {R"([1, 2, 3, 4, 5, 6])", "$[0 to 2]", {"1", "2", "3"}},
            {R"([1, 2, 3, 4, 5, 6])", "$[5, 0 to 2, 0, 0, 3 to 5, 2]", {"6", "1", "2", "3", "1", "1", "4", "5", "6", "3"}},
            {R"({
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })", "$.friends[1 to 3, 0].age", {"72", "50", "60", "18"}},
            {R"({
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
            })", "$.friends[$.range.from to $.range.to].age", {"72", "50"}},
            {R"({
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
            })", "$.friends[$.range.from[1] to $.range.to.key3].age", {"60"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLastArrayIndex() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([1, 2, 3])", "$[last]", {"3"}},
            {R"([1, 2, 3])", "$[1 to last]", {"2", "3"}},
            {R"([1, 2, 3])", "$[last to last]", {"3"}},
            {R"([1, 2, 3, 5, 6])", "$[1, last, last, 0, 2 to last, 3]", {"2", "6", "6", "1", "3", "5", "6", "5"}},
            {R"([
                [1, 2, 3, 4],
                [5, 6, 7, 8]
            ])", "$[*][last]", {"4", "8"}},
            {R"({
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
            })", "$.friends[last, $.ranges[last].from to $.ranges[last].to, 2 to last].age", {"60", "18", "72", "50", "60"}},
            {R"({
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
            })", "$.friends[last, $.ranges[last].from to $.ranges[last].to, 2 to last].age", {"60", "18", "72", "50", "60"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLastArrayIndexInvalid() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {R"({})", "last", C(TIssuesIds::JSONPATH_LAST_OUTSIDE_OF_ARRAY_SUBSCRIPT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestNonIntegerArrayIndex() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {R"({
                "range": {
                    "from": [1, 3, 4],
                    "to": {"key1": 1, "key2": 2, "key3": 3}
                },
                "friends": [1, 2, 3]
            })", "$.friends[$.range.from[*] to $.range.to.*]", C(TIssuesIds::JSONPATH_INVALID_ARRAY_INDEX)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestWildcardArrayAccess() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([1, 2, 3])", "$[*]", {"1", "2", "3"}},
            {R"([[1], [2], [3, 4, 5]])", "$[*][*]", {"1", "2", "3", "4", "5"}},
            {R"({
                "friends": [
                    {"name": "Nik", "age": 18},
                    {"name": "Kate", "age": 72},
                    {"name": "Foma", "age": 50},
                    {"name": "Jora", "age": 60}
                ]
            })", "$.friends[*].age", {"18", "72", "50", "60"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestUnaryOperations() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([])", "-3", {"-3"}},
            {R"([])", "+3", {"3"}},
            {R"(-1)", "-$", {"1"}},
            {R"(-1)", "+$", {"-1"}},
            {R"({
                "range": {
                    "from": -1,
                    "to": -2
                },
                "array": [1, 2, 3, 4]
            })", "$.array[-$.range.from to -$.range.to]", {"2", "3"}},
            {R"({
                "range": {
                    "from": 1,
                    "to": -2
                },
                "array": [1, 2, 3, 4]
            })", "$.array[+$.range.from to -$.range.to]", {"2", "3"}},
            {R"({
                "range": {
                    "from": -1,
                    "to": 2
                },
                "array": [1, 2, 3, 4]
            })", "$.array[-$.range.from to +$.range.to]", {"2", "3"}},
            {R"({
                "range": {
                    "from": 1,
                    "to": 2
                },
                "array": [1, 2, 3, 4]
            })", "$.array[+$.range.from to +$.range.to]", {"2", "3"}},
            {R"([1, 2, 3])", "-$[*]", {"-1", "-2", "-3"}},
            {"10000000000000000000000000", "-$", {"-9.999999999999999e+24"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestUnaryOperationsErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {R"({})", "-$", C(TIssuesIds::JSONPATH_INVALID_UNARY_OPERATION_ARGUMENT_TYPE)},
            {R"([1, 2, [], 4])", "-$[*]", C(TIssuesIds::JSONPATH_INVALID_UNARY_OPERATION_ARGUMENT_TYPE)},
            {R"([1, 2, {}, 4])", "-$[*]", C(TIssuesIds::JSONPATH_INVALID_UNARY_OPERATION_ARGUMENT_TYPE)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestBinaryArithmeticOperations() {
        const TVector<TMultiOutputTestCase> testCases = {
            {"[]", "1 + 2", {"3"}},
            {"[]", "1 - 2", {"-1"}},
            {"[]", "10 * 5", {"50"}},
            {"[]", "10 / 5", {"2"}},
            {"[]", "13 % 5", {"3"}},

            {"[]", "20 * 2 + 5", {"45"}},
            {"[]", "20 / 2 + 5", {"15"}},
            {"[]", "20 % 2 + 5", {"5"}},

            {"[]", "20 * (2 + 5)", {"140"}},
            {"[]", "20 / (2 + 3)", {"4"}},
            {"[]", "20 % (2 + 5)", {"6"}},

            {"[]", "5 / 2", {"2.5"}},
            {"[5.24 , 2.62]", "$[0] / $[1]", {"2"}},
            {"[5.24, 2.62]", "$[0] % $[1]", {"0"}},
            {"[3.753, 2.35]", "$[0] % $[1]", {"1.403"}},

            {"[]", "- 1 + 1", {"0"}},
            {"[]", "+ 1 + 1", {"2"}},

            {"[1, 2, 3, 4]", "$[last, last-1, last-2, last-3]", {"4", "3", "2", "1"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestBinaryArithmeticOperationsErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {"[1, 2, 3]", "$[*] + 1", C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT)},
            {"[1, 2, 3]", "1 + $[*]", C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT)},
            {"[1, 2, 3]", "$[*] + $[*]", C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT)},

            {"[1, 2, 3]", "$ + 1", C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT_TYPE)},
            {"[1, 2, 3]", "1 + $", C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT_TYPE)},
            {"[1, 2, 3]", "$ + $", C(TIssuesIds::JSONPATH_INVALID_BINARY_OPERATION_ARGUMENT_TYPE)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
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
            {"123", {{"var", "456"}}, "$ + $var", {"579"}},
            {"123", {{"var", "456"}}, "$var", {"456"}},
            {"123", {{"var", R"({"key": [1, 2, 3, 4, 5]})"}}, "$var.key[2 to last]", {"3", "4", "5"}},
            {"123", {{"to", "1"}, {"strict", "2"}}, "$to + $strict", {"3"}},
        };
        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunVariablesTestCase(testCase.Json, testCase.Variables, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestDivisionByZero() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {"0", "1 / $", C(TIssuesIds::JSONPATH_DIVISION_BY_ZERO)},
            {"0.00000000000000000001", "1 / $", C(TIssuesIds::JSONPATH_DIVISION_BY_ZERO)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
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
            {"0", TString(query), C(TIssuesIds::JSONPATH_BINARY_OPERATION_RESULT_INFINITY)},
            {TString(literal), "$.double()", C(TIssuesIds::JSONPATH_INFINITE_NUMBER_STRING)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
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
            {"1", "(1 < true) || (1 < true)", {"null"}},
            {"1", "(1 < true) || (true != true)", {"null"}},
            {"1", "(1 < true) || (true == true)", {"true"}},
            {"1", "(true != true) || (1 < true)", {"null"}},
            {"1", "(true != true) || (true != true)", {"false"}},
            {"1", "(true != true) || (true == true)", {"true"}},
            {"1", "(true == true) || (1 < true)", {"true"}},
            {"1", "(true == true) || (true != true)", {"true"}},
            {"1", "(true == true) || (true == true)", {"true"}},

            {"1", "(1 < true) && (1 < true)", {"null"}},
            {"1", "(1 < true) && (true != true)", {"false"}},
            {"1", "(1 < true) && (true == true)", {"null"}},
            {"1", "(true != true) && (1 < true)", {"false"}},
            {"1", "(true != true) && (true != true)", {"false"}},
            {"1", "(true != true) && (true == true)", {"false"}},
            {"1", "(true == true) && (1 < true)", {"null"}},
            {"1", "(true == true) && (true != true)", {"false"}},
            {"1", "(true == true) && (true == true)", {"true"}},

            {"1", "(true != true) && (true != true) || (true == true)", {"true"}},
            {"1", "(true != true) && ((true != true) || (true == true))", {"false"}},
            {"1", "(true != true) || (true != true) || (true == true)", {"true"}},
            {"1", "(true == true) && (true == true) && (true == true) && (true != true)", {"false"}},

            {"1", "!(1 < true)", {"null"}},
            {"1", "!(true != true)", {"true"}},
            {"1", "!(true == true)", {"false"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
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
            {"1", "1.23 < 4.56", {"true"}},
            {"1", "1.23 > 4.56", {"false"}},
            {"1", "1.23 <= 4.56", {"true"}},
            {"1", "1.23 >= 4.56", {"false"}},
            {"1", "1.23 == 1.23", {"true"}},
            {"1", "1.23 != 1.23", {"false"}},
            {"1", "1.23 <> 4.56", {"true"}},
            {"1", "1.00000000000000000001 == 1.00000000000000000002", {"true"}},

            // Check numbers of different kinds (int64 vs double)
            {"1", "1 < 2.33", {"true"}},
            {"1", "1 > 4.56", {"false"}},
            {"1", "1 <= 4.56", {"true"}},
            {"1", "1 >= 4.56", {"false"}},
            {"1", "1 == 1.23", {"false"}},
            {"1", "1 != 1.23", {"true"}},
            {"1", "1 <> 4.56", {"true"}},

            // Check comparison of strings
            {"1", R"("abc" < "def")", {"true"}},
            {"1", R"("abc" > "def")", {"false"}},
            {"1", R"("abc" <= "def")", {"true"}},
            {"1", R"("abc" >= "def")", {"false"}},
            {"1", R"("abc" == "abc")", {"true"}},
            {"1", R"("abc" != "abc")", {"false"}},
            {"1", R"("abc" <> "def")", {"true"}},

            // Check comparison of UTF8 strings
            // First string is U+00e9 (LATIN SMALL LETTER E WITH ACUTE), "é"
            // Second string is U+0065 (LATIN SMALL LETTER E) U+0301 (COMBINING ACUTE ACCENT), "é"
            {"1", R"("é" < "é")", {"false"}},
            {"1", R"("é" > "é")", {"true"}},
            {"1", R"("привет" == "привет")", {"true"}},

            // Check cross-product comparison
            {R"({
                "left": [1],
                "right": [4, 5, 6]
            })", "$.left[*] < $.right[*]", {"true"}},
            {R"({
                "left": [4, 5, 6],
                "right": [1]
            })", "$.left[*] < $.right[*]", {"false"}},
            {R"({
                "left": [1, 2, 3],
                "right": [4, 5, 6]
            })", "$.left[*] < $.right[*]", {"true"}},
            {R"({
                "left": [10, 30, 40],
                "right": [1, 2, 15]
            })", "$.left[*] < $.right[*]", {"true"}},
            {R"({
                "left": [10, 30, 40],
                "right": [1, 2, 3]
            })", "$.left[*] < $.right[*]", {"false"}},

            // Check incomparable types
            {"1", "1 < true", {"null"}},
            {"1", R"(true <> "def")", {"null"}},

            // Check error in arguments
            {R"({
                "array": [1, 2, 3, 4, 5],
                "invalid_index": {
                    "key": 1
                }
            })", "$.array[$.invalid_index] < 3", {"null"}},
            {R"({
                "array": [1, 2, 3, 4, 5],
                "invalid_index": {
                    "key": 1
                }
            })", "5 >= $.array[$.invalid_index]", {"null"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestFilter() {
        const TVector<TMultiOutputTestCase> testCases = {
            {"[1, 2, 3]", "$[*] ? (@ > 2)", {"3"}},
            {R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])", "$[*] ? (@.age >= 18)", {R"({"age":18})", R"({"age":25})", R"({"age":50})"}},
            {R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])", "$[*] ? (@.age >= 18) ? (@.age <= 30)", {R"({"age":18})", R"({"age":25})"}},
            {R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])", "$[*] ? (@.age >= 18) ? (@.age <= 30) . age", {"18", "25"}},
            {R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])", "$[*] ? (@.age >= 18 && @.age <= 30) . age", {"18", "25"}},
            {R"([
                {"age": 18},
                {"age": 25},
                {"age": 50},
                {"age": 5}
            ])", "$[*] ? (@.age >= 18 || @.age <= 30) . age", {"18", "25", "50", "5"}},
            {R"([
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
            ])", "$[*] ? (@.is_valid == true && @.days_till_doom > 10 && 2 * @.age_estimation <= 12).id", {"4"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestFilterInvalid() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {R"({})", "@", C(TIssuesIds::JSONPATH_FILTER_OBJECT_OUTSIDE_OF_FILTER)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestNumericMethods() {
        const TVector<TMultiOutputTestCase> testCases = {
            {"[-1.23, 4.56, 3, 0]", "$[*].abs()", {"1.23", "4.56", "3", "0"}},
            {"[-1.23, 4.56, 3, 0]", "$[*].floor()", {"-2", "4", "3", "0"}},
            {"[-1.23, 4.56, 3, 0]", "$[*].ceiling()", {"-1", "5", "3", "0"}},
            {"-123.45", "$.ceiling().abs().floor()", {"123"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestNumericMethodsErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {R"(["1", true, null])", "$[*].abs()", C(TIssuesIds::JSONPATH_INVALID_NUMERIC_METHOD_ARGUMENT)},
            {R"(["1", true, null])", "$[*].floor()", C(TIssuesIds::JSONPATH_INVALID_NUMERIC_METHOD_ARGUMENT)},
            {R"(["1", true, null])", "$[*].ceiling()", C(TIssuesIds::JSONPATH_INVALID_NUMERIC_METHOD_ARGUMENT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestDoubleMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"([
                "123", "123.4", "0.567", "1234e-1", "567e-3", "123.4e-1",
                "123e3", "123e+3", "1.23e+1", "1.23e1",
                "12e0", "12.3e0", "0", "0.0", "0.0e0"
            ])", "$[*].double()", {
                "123", "123.4", "0.567", "123.4", "0.567", "12.34",
                "123000", "123000", "12.3", "12.3",
                "12", "12.3", "0", "0", "0",
            }},
            {R"("-123.45e1")", "$.double().abs().floor()", {"1234"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestDoubleMethodErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {R"(["1", true, null])", "$[*].double()", C(TIssuesIds::JSONPATH_INVALID_DOUBLE_METHOD_ARGUMENT)},
            {R"("hi stranger")", "$.double()", C(TIssuesIds::JSONPATH_INVALID_NUMBER_STRING)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestTypeMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {"null", "$.type()", {"\"null\""}},
            {"true", "$.type()", {"\"boolean\""}},
            {"false", "$.type()", {"\"boolean\""}},
            {"1", "$.type()", {"\"number\""}},
            {"-1", "$.type()", {"\"number\""}},
            {"4.56", "$.type()", {"\"number\""}},
            {"-4.56", "$.type()", {"\"number\""}},
            {"\"some string\"", "$.type()", {"\"string\""}},
            {"[]", "$.type()", {"\"array\""}},
            {"[1, 2, 3, 4]", "$.type()", {"\"array\""}},
            {"{}", "$.type()", {"\"object\""}},
            {"{\"key\": 123}", "$.type()", {"\"object\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestSizeMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {"null", "$.size()", {"1"}},
            {"true", "$.size()", {"1"}},
            {"false", "$.size()", {"1"}},
            {"1", "$.size()", {"1"}},
            {"-1", "$.size()", {"1"}},
            {"4.56", "$.size()", {"1"}},
            {"-4.56", "$.size()", {"1"}},
            {"\"some string\"", "$.size()", {"1"}},
            {"[]", "$.size()", {"0"}},
            {"[1, 2, 3, 4]", "$.size()", {"4"}},
            {"{}", "$.size()", {"1"}},
            {"{\"key\": 123}", "$.size()", {"1"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestKeyValueMethod() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"({
                "one": 1,
                "two": 2,
                "three": 3
            })", "$.keyvalue()", {
                R"({"name":"one","value":1})",
                R"({"name":"three","value":3})",
                R"({"name":"two","value":2})",
            }},
            {R"({
                "one": "string",
                "two": [1, 2, 3, 4],
                "three": [4, 5]
            })", R"($.keyvalue() ? (@.value.type() == "array" && @.value.size() > 2).name)", {"\"two\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestKeyValueMethodErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {"\"string\"", "$.keyvalue()", C(TIssuesIds::JSONPATH_INVALID_KEYVALUE_METHOD_ARGUMENT)},
            {"[1, 2, 3, 4]", "$.keyvalue()", C(TIssuesIds::JSONPATH_INVALID_KEYVALUE_METHOD_ARGUMENT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestStartsWithPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {"1", R"("some string" starts with "some")", {"true"}},
            {"1", R"("some string" starts with "string")", {"false"}},
            {R"(["some string", "string"])", R"($[*] ? (@ starts with "string"))", {"\"string\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestStartsWithPredicateErrors() {
        const TVector<TRuntimeErrorTestCase> testCases = {
            {R"(["first", "second"])", R"($[*] starts with "first")", C(TIssuesIds::JSONPATH_INVALID_STARTS_WITH_ARGUMENT)},
            {"1", R"(1 starts with "string")", C(TIssuesIds::JSONPATH_INVALID_STARTS_WITH_ARGUMENT)},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunRuntimeErrorTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Error);
            }
        }
    }

    void TestExistsPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"({
                "key": 123
            })", "exists ($.key)", {"true"}},
            {"\"string\"", "exists ($ * 2)", {"null"}},
            {R"(["some string", 2])", "$[*] ? (exists (@ * 2))", {"2"}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestIsUnknownPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {"1", "(1 < true) is unknown", {"true"}},
            {"1", "(true == true) is unknown", {"false"}},
            {"1", "(true == false) is unknown", {"false"}},
            {R"(["some string", -20])", "$[*] ? ((1 < @) is unknown)", {"\"some string\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }

    void TestLikeRegexPredicate() {
        const TVector<TMultiOutputTestCase> testCases = {
            {R"(["string", "123", "456"])", R"($[*] like_regex "[0-9]+")", {"true"}},
            {R"(["string", "another string"])", R"($[*] like_regex "[0-9]+")", {"false"}},

            // Case insensitive flag
            {R"("AbCd")", R"($ like_regex "abcd")", {"false"}},
            {R"("AbCd")", R"($ like_regex "abcd" flag "i")", {"true"}},

            {R"(["string", "123", "456"])", R"($[*] ? (@ like_regex "[0-9]+"))", {"\"123\"", "\"456\""}},
        };

        for (const auto& testCase : testCases) {
            for (const auto mode : ALL_MODES) {
                RunTestCase(testCase.Json, mode + testCase.JsonPath, testCase.Result);
            }
        }
    }
};

UNIT_TEST_SUITE_REGISTRATION(TJsonPathCommonTest);
