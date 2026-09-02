#include "rpc_calls.h"
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(SplitPathTests) {
    Y_UNIT_TEST(WithoutDatabaseShouldSuccess) {
        {
            auto pair = NGRpcService::SplitPath("/root/table");
            UNIT_ASSERT_VALUES_EQUAL(pair.first, "/root");
            UNIT_ASSERT_VALUES_EQUAL(pair.second, "table");
        }
        {
            auto pair = NGRpcService::SplitPath("/root/dir/table");
            UNIT_ASSERT_VALUES_EQUAL(pair.first, "/root/dir");
            UNIT_ASSERT_VALUES_EQUAL(pair.second, "table");
        }
    }

    Y_UNIT_TEST(WithDatabaseShouldSuccess) {
        for (auto db : TVector<TString>{"/root/db", "/root/db/"}) {
            for (auto subpath : TVector<TString>{"table", "dir/table"}) {
                auto pair = NGRpcService::SplitPath(db, "/root/db/" + subpath);
                UNIT_ASSERT_VALUES_EQUAL(pair.first, "/root/db");
                UNIT_ASSERT_VALUES_EQUAL(pair.second, subpath);
            }
        }
    }

    Y_UNIT_TEST(WithDatabaseShouldFail) {
        for (auto path : TVector<TString>{"/root/db1/table", "/root/db1/dir/table"}) {
            UNIT_ASSERT_EXCEPTION(NGRpcService::SplitPath("/root/db2", path), yexception);
        }
    }
}

} // namespace NKikimr
