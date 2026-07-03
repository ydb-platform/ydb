#include "run_ydb.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/strip.h>

Y_UNIT_TEST_SUITE(SqlResourcePool) {
    Y_UNIT_TEST(ShouldWorkOnDefaultPool) {
        // The "default" resource pool always exists when workload manager is enabled.
        TString output = RunYdb({"sql", "--resource-pool", "default", "-s", "SELECT 42 AS value"}, {});
        UNIT_ASSERT_STRING_CONTAINS(output, "42");
    }

    Y_UNIT_TEST(ShouldFailOnInvalidPool) {
        // A non-existent pool must cause the query to fail.
        // RunYdb with default expectedExitCode=0 throws yexception whose message
        // includes stderr, so we can check the error text.
        try {
            RunYdb(
                {"sql", "--resource-pool", "nonexistent_pool_12345", "-s", "SELECT 42 AS value"},
                {}
            );
            UNIT_FAIL("Expected RunYdb to throw for non-existent resource pool");
        } catch (const yexception& e) {
            UNIT_ASSERT_STRING_CONTAINS(e.what(), "exitcode: 1");
            UNIT_ASSERT_STRING_CONTAINS(e.what(), "Resource pool nonexistent_pool_12345 not found");
        }
    }

    Y_UNIT_TEST(ShouldFailWithoutValue) {
        // --resource-pool at the end without a value triggers "must have arg" error.
        try {
            RunYdb({"sql", "-s", "SELECT 1", "--resource-pool"}, {});
            UNIT_FAIL("Expected RunYdb to throw when --resource-pool has no value");
        } catch (const yexception& e) {
            UNIT_ASSERT_STRING_CONTAINS(e.what(), "option --resource-pool must have arg");
        }
    }
}
