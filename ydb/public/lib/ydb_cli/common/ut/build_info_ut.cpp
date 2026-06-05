#include <ydb/public/lib/ydb_cli/common/build_info.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(YdbCliBuildInfo) {
    Y_UNIT_TEST(AppendWithCommandTag) {
        TDriverConfig config;
        TYdbCliBuildInfo buildInfo{"test-app", "1.2.3"};
        UNIT_ASSERT_NO_EXCEPTION(AppendYdbCliBuildInfo(config, buildInfo, "ydb-import-file-csv"));
    }

    Y_UNIT_TEST(AppendWithoutCommandTag) {
        TDriverConfig config;
        TYdbCliBuildInfo buildInfo{"test-app", "1.2.3"};
        UNIT_ASSERT_NO_EXCEPTION(AppendYdbCliBuildInfo(config, buildInfo, ""));
    }

    Y_UNIT_TEST(AppendWithEmptyBuildInfo) {
        TDriverConfig config;
        TYdbCliBuildInfo buildInfo{"", ""};
        UNIT_ASSERT_NO_EXCEPTION(AppendYdbCliBuildInfo(config, buildInfo, "ydb-test"));
    }
}
