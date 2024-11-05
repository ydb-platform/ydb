#include "udf_tz.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql::NUdf;

Y_UNIT_TEST_SUITE(TUdfTz) {
    Y_UNIT_TEST(Count) {
        UNIT_ASSERT_VALUES_EQUAL_C(GetTimezones().size(), 600, "Please run arcadia/ydb/library/yql/public/udf/tz/gen");
    }

    Y_UNIT_TEST(Gmt) {
        UNIT_ASSERT(GetTimezones().size() > 0);
        UNIT_ASSERT_VALUES_EQUAL(GetTimezones()[0], "GMT");
    }
}
