#include <ydb/public/lib/ydb_cli/commands/ydb_root_common.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/array_size.h>

using namespace NYdb::NConsoleClient;

namespace {

TClientSettings MakeClientSettings() {
    TClientSettings settings;
    settings.EnableSsl = false;
    settings.UseAccessToken = false;
    settings.UseDefaultTokenFile = false;
    settings.UseIamAuth = false;
    settings.UseExportToYt = false;
    settings.UseStaticCredentials = false;
    settings.MentionUserAccount = false;
    settings.UseOauth2TokenExchange = false;
    settings.YdbDir = "ydb-relative-database-ut";
    return settings;
}

} // namespace

Y_UNIT_TEST_SUITE(RelativeDatabase) {
    Y_UNIT_TEST(IsPreservedByCliConfiguration) {
        char arg0[] = "ydb";
        char arg1[] = "-e";
        char arg2[] = "localhost:2135";
        char arg3[] = "-d";
        char arg4[] = "mydb";
        char* argv[] = {arg0, arg1, arg2, arg3, arg4};

        TClientCommand::TConfig config(Y_ARRAY_SIZE(argv), argv);
        const auto settings = MakeClientSettings();
        TClientCommandRootCommon root("ydb", settings);

        root.Prepare(config);
        root.ExtractParams(config);

        UNIT_ASSERT_NO_EXCEPTION(root.Validate(config));
        UNIT_ASSERT_VALUES_EQUAL(config.Database, "mydb");
        UNIT_ASSERT_VALUES_EQUAL(config.CreateDriverConfig().GetDatabase(), "mydb");
    }
}
