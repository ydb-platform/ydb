#include "test_base.h"

/*
    These examples are taken from [ISO/IEC TR 19075-6:2017] standard (https://www.iso.org/standard/67367.html)
*/

class TJsonPathExamplesTest : public TJsonPathTestBase {
public:
    TJsonPathExamplesTest()
        : TJsonPathTestBase()
    {
    }

    UNIT_TEST_SUITE(TJsonPathExamplesTest);
        UNIT_TEST(TestMemberAccessExamples);
        UNIT_TEST(TestElementAccessExamples);
        UNIT_TEST(TestFilterExamples);
    UNIT_TEST_SUITE_END();

    void TestMemberAccessExamples() {
        TString input = R"({
            "phones": [
                {"type": "cell", "number": "abc-defg"},
                {"number": "pqr-wxyz"},
                {"type": "home", "number": "hij-klmn"}
            ]
        })";

        RunTestCase(input, "lax $.phones.type", {"\"cell\"", "\"home\""});
        RunRuntimeErrorTestCase(input, "strict $.phones[*].type", C(TIssuesIds::JSONPATH_MEMBER_NOT_FOUND));
        // NOTE: Example in standard has different order of elements. This is okay because order of elements after
        // wildcard member access is implementation-defined
        RunTestCase(input, "lax $.phones.*", {"\"abc-defg\"", "\"cell\"", "\"pqr-wxyz\"", "\"hij-klmn\"", "\"home\""});
    }

    void TestElementAccessExamples() {
        // NOTE: Example in standard has different order of elements. This is okay because order of elements after
        // wildcard member access is implementation-defined
        RunTestCase(R"({
            "sensors": {
                "SF": [10, 11, 12, 13, 15, 16, 17],
                "FC": [20, 22, 24],
                "SJ": [30, 33]
            }
        })", "lax $.sensors.*[0, last, 2]", {"20", "24", "24", "10", "17", "12", "30", "33"});

        RunTestCase(R"({
            "x": [12, 30],
            "y": [8],
            "z": ["a", "b", "c"]
        })", "lax $.*[1 to last]", {"30", "\"b\"", "\"c\""});
    }

    void TestFilterExamples() {
        RunParseErrorTestCase("$ ? (@.skilled)");

        TString json = R"({"name":"Portia","skilled":true})";
        RunTestCase(json, "$ ? (@.skilled == true)", {json});

        // Standard also mentions this example in lax mode. It is invalid because
        // in this case automatic unwrapping on arrays before filters will be performed
        // and query will finish with error
        RunTestCase(R"({
            "x": [1, "one"]
        })", "strict $.x ? (2 > @[*])", {});

        RunTestCase(R"({
            "name": {
                "first": "Manny",
                "last": "Moe"
            },
            "points": 123
        })", "strict $ ? (exists (@.name)).name", {R"({"first":"Manny","last":"Moe"})"});

        RunTestCase(R"({
            "points": 41
        })", "strict $ ? (exists (@.name)).name", {});
    }
};

UNIT_TEST_SUITE_REGISTRATION(TJsonPathExamplesTest);