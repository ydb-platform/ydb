#include <yql/essentials/minikql/dom/json.h>

#include <yql/essentials/minikql/dom/test_support/fixtures.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
using namespace NYql::NDom;

Y_UNIT_TEST_SUITE(TJsonTests) {
Y_UNIT_TEST(TestValidate) {
    UNIT_ASSERT(IsValidJson(NTest::GetJsonDocument()));

    UNIT_ASSERT(!IsValidJson("[123}"));
    UNIT_ASSERT(!IsValidJson("[123],[456]"));
    UNIT_ASSERT(!IsValidJson(R"({"c" : "scm"])"));
    UNIT_ASSERT(!IsValidJson(""));
    UNIT_ASSERT(!IsValidJson(R"({"c",})"));
    UNIT_ASSERT(!IsValidJson(R"({null : "scm"})"));
    UNIT_ASSERT(!IsValidJson(R"({'one': 1})"));
}
} // Y_UNIT_TEST_SUITE(TJsonTests)
