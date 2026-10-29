#include <ydb/library/naming_conventions/naming_conventions.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

using namespace NKikimr::NNaming;

Y_UNIT_TEST_SUITE(NamingConventionsSuite) {

    Y_UNIT_TEST(TestCamelToSnake) {
        Cerr << "DIR_OUTPUTS_PROBE: naming successful test" << Endl;
        UNIT_ASSERT_EQUAL("camel_case", CamelToSnakeCase("CamelCase"));
        UNIT_ASSERT_EQUAL("snakesnake", CamelToSnakeCase("snakesnake"));
        UNIT_ASSERT_EQUAL("b_f_g", CamelToSnakeCase("BFG"));
    }

    Y_UNIT_TEST(TestSnakeToCamel) {
        Cout << "DIR_OUTPUTS_PROBE: stdout from the intentional failure" << Endl;
        Cerr << "DIR_OUTPUTS_PROBE: naming intentional failure; preserve this stderr in the log ZIP" << Endl;
        UNIT_ASSERT_EQUAL("CamelCase", SnakeToCamelCase("camel_case"));
        UNIT_ASSERT_EQUAL("Snakesnake", SnakeToCamelCase("snakesnake"));
        UNIT_ASSERT_EQUAL("Bfg", SnakeToCamelCase("bfg_"));
        UNIT_FAIL("DIR_OUTPUTS_PROBE: intentional failure to inspect CI artifacts; do not merge");
    }

}
