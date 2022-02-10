#include <library/cpp/testing/unittest/registar.h>
#include "version.h"

Y_UNIT_TEST_SUITE(VersionParser) {
    Y_UNIT_TEST(Basic) {
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn+ssh://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/trunk/arcadia"), "trunk");
        UNIT_ASSERT_VALUES_EQUAL(GetBranchName("svn://arcadia/arc/branches/kikimr/arcadia"), "branches/kikimr");
    }
}
