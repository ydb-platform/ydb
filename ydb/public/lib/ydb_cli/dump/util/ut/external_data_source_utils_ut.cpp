#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ExternalDataSourceBackup) {
    Y_UNIT_TEST(IamBackupKeepsResolvedIdentityWithoutInitialToken) {
        Ydb::Table::DescribeExternalDataSourceResult description;
        description.mutable_self()->set_name("source");
        description.set_source_type("Ydb");
        description.set_location("localhost:2135");
        auto &properties = *description.mutable_properties();
        properties["AUTH_METHOD"] = "IAM";
        properties["SERVICE_ACCOUNT_ID"] = "service-account";
        properties["RESOURCE_ID"] = "cloud-id";
        properties["INITIAL_TOKEN_SECRET_PATH"] = "/Root/initial-token";

        const auto query = NYdb::NDump::BuildCreateExternalDataSourceQuery(description, "/Root", NYdb::NDump::EIamInitialTokenBackupMode::Omit);
        UNIT_ASSERT_STRING_CONTAINS(query, "SERVICE_ACCOUNT_ID");
        UNIT_ASSERT_STRING_CONTAINS(query, "service-account");
        UNIT_ASSERT_STRING_CONTAINS(query, "RESOURCE_ID");
        UNIT_ASSERT_STRING_CONTAINS(query, "cloud-id");
        UNIT_ASSERT_C(!query.Contains("INITIAL_TOKEN_SECRET"), query);

        // The legacy overload is also used by local dumps and must keep its format.
        const auto legacy = NYdb::NDump::BuildCreateExternalDataSourceQuery(description, "/Root");
        UNIT_ASSERT_STRING_CONTAINS(legacy, "INITIAL_TOKEN_SECRET_PATH");

        properties.erase("INITIAL_TOKEN_SECRET_PATH");
        properties["INITIAL_TOKEN_SECRET_NAME"] = "legacy-initial-token";
        const auto named = NYdb::NDump::BuildCreateExternalDataSourceQuery(description, "/Root", NYdb::NDump::EIamInitialTokenBackupMode::Omit);
        UNIT_ASSERT_C(!named.Contains("INITIAL_TOKEN_SECRET"), named);
        UNIT_ASSERT_STRING_CONTAINS(named, "cloud-id");
    }

    Y_UNIT_TEST(NonIamBackupKeepsItsCredentialReference) {
        Ydb::Table::DescribeExternalDataSourceResult description;
        description.mutable_self()->set_name("source");
        description.set_source_type("ClickHouse");
        description.set_location("example.com");
        auto &properties = *description.mutable_properties();
        properties["AUTH_METHOD"] = "BASIC";
        properties["PASSWORD_SECRET_PATH"] = "/Root/password";
        const auto query = NYdb::NDump::BuildCreateExternalDataSourceQuery(description, "/Root", NYdb::NDump::EIamInitialTokenBackupMode::Omit);
        UNIT_ASSERT_STRING_CONTAINS(query, "PASSWORD_SECRET_PATH");
        UNIT_ASSERT_STRING_CONTAINS(query, "/Root/password");
    }
}
