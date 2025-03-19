#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

const TString UserName = "user0@builtin";

void AddPermissions(const TKikimrRunner& kikimr, const TString& path, const TString& subject, const std::vector<std::string>& permissionNames) {
    auto driver = NYdb::TDriver(NYdb::TDriverConfig()
    .SetEndpoint(kikimr.GetEndpoint())
    .SetDatabase("/Root")
    .SetAuthToken("root@builtin"));
    auto schemeClient = NYdb::NScheme::TSchemeClient(driver);
    auto result = schemeClient.ModifyPermissions(path,
        NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(NYdb::NScheme::TPermissions(subject, permissionNames))
    ).ExtractValueSync();
    AssertSuccessResult(result);

    Tests::TClient::RefreshPathCache(kikimr.GetTestServer().GetRuntime(), path);    
}

void WaitForProxy(const TKikimrRunner& kikimr, const TString& subject) {
    auto driver = NYdb::TDriver(NYdb::TDriverConfig()
    .SetEndpoint(kikimr.GetEndpoint())
    .SetDatabase("/Root")
    .SetAuthToken(subject));

    NYdb::NQuery::TQueryClient client(driver);            
    while(true) {
        auto result = client.ExecuteScript("SELECT 1").ExtractValueSync();
        NYdb::EStatus scriptStatus = result.Status().GetStatus();
        UNIT_ASSERT_C(scriptStatus == NYdb::EStatus::UNAVAILABLE || scriptStatus == NYdb::EStatus::SUCCESS || scriptStatus == NYdb::EStatus::UNAUTHORIZED, result.Status().GetIssues().ToString());
        if (scriptStatus == NYdb::EStatus::SUCCESS)
            return;
        Sleep(TDuration::Seconds(1));
    };
}

void AddConnectPermission(const TKikimrRunner& kikimr, const TString& subject) {
    AddPermissions(kikimr, "/Root", subject, {"ydb.database.connect"});
    WaitForProxy(kikimr, subject);
}

Y_UNIT_TEST_SUITE(KqpAcl) {
    Y_UNIT_TEST(FailNavigate) {
        TKikimrRunner kikimr(UserName);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(FailResolve) {
        TKikimrRunner kikimr;
        AddConnectPermission(kikimr, UserName);
        AddPermissions(kikimr, "/Root/TwoShard", UserName, {"ydb.deprecated.describe_schema"});

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserName);
        auto driver = TDriver(driverConfig);
        auto db = NYdb::NTable::TTableClient(driver);

        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteDataQuery(R"(
                SELECT * FROM `/Root/TwoShard`;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            // TODO: Should be UNAUTHORIZED
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);
        }
        {
            auto result = session.ExecuteDataQuery(R"(
                UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES
                    (10u, "One", -10);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            // TODO: Should be UNAUTHORIZED
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);
        }

        driver.Stop(true);
    }

    Y_UNIT_TEST(ReadSuccess) {
        TKikimrRunner kikimr;
        AddConnectPermission(kikimr, UserName);
        AddPermissions(kikimr, "/Root/TwoShard", UserName, {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"});

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserName);
        auto driver = TDriver(driverConfig);
        auto db = NYdb::NTable::TTableClient(driver);

        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);
        driver.Stop(true);
    }

    Y_UNIT_TEST(FailedReadAccessDenied) {
        TKikimrRunner kikimr;
        AddConnectPermission(kikimr, UserName);
        AddPermissions(kikimr, "/Root/TwoShard", UserName, {});

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserName);
        auto driver = TDriver(driverConfig);
        auto db = NYdb::NTable::TTableClient(driver);
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/TwoShard`;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        Cerr << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        const auto expectedIssueMessage = "Cannot find table 'db.[/Root/TwoShard]' because it does not exist or you do not have access permissions.";
        UNIT_ASSERT_VALUES_EQUAL(result.GetIssues().ToString().contains(expectedIssueMessage), true);
        driver.Stop(true);
    }

    Y_UNIT_TEST(WriteSuccess) {
        TKikimrRunner kikimr;
        AddConnectPermission(kikimr, UserName);
        AddPermissions(kikimr, "/Root/TwoShard", UserName, {"ydb.deprecated.describe_schema", "ydb.deprecated.update_row"});

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserName);
        auto driver = TDriver(driverConfig);
        auto db = NYdb::NTable::TTableClient(driver);

        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES
                (10u, "One", -10);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);
        driver.Stop(true);
    }

    Y_UNIT_TEST(FailedWriteAccessDenied) {
        TKikimrRunner kikimr;
        AddConnectPermission(kikimr, UserName);
        AddPermissions(kikimr, "/Root/TwoShard", UserName, {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"});

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserName);
        auto driver = TDriver(driverConfig);
        auto db = NYdb::NTable::TTableClient(driver);
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1, Value2) VALUES
                (10u, "One", -10);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::ABORTED);
        const auto expectedIssueMessage = "Failed to resolve table `/Root/TwoShard` status: AccessDenied.";
        UNIT_ASSERT_VALUES_EQUAL(result.GetIssues().ToString().contains(expectedIssueMessage), true);
        driver.Stop(true);
    }

    Y_UNIT_TEST(RecursiveCreateTableShouldSuccess) {
        TKikimrRunner kikimr;
        AddConnectPermission(kikimr, UserName);

        {
            auto schemeClient = kikimr.GetSchemeClient();
            AssertSuccessResult(schemeClient.MakeDirectory("/Root/PQ").ExtractValueSync());
        }
        AddPermissions(kikimr, "/Root/PQ", UserName, {"ydb.deprecated.create_table"});

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserName);
        auto driver = TDriver(driverConfig);
        auto db = NYdb::NTable::TTableClient(driver);

        auto session = db.CreateSession().GetValueSync().GetSession();

        const char* queryTmpl = R"(
            CREATE TABLE `/Root/PQ/%s` (
                id Int64,
                name String,
                primary key (id)
            );
        )";

        AssertSuccessResult(session.ExecuteSchemeQuery(Sprintf(queryTmpl, "table")).ExtractValueSync());
        AssertSuccessResult(session.ExecuteSchemeQuery(Sprintf(queryTmpl, "a/b/c/table")).ExtractValueSync());

        driver.Stop(true);
    }

    Y_UNIT_TEST_TWIN(AclForOltpAndOlap, isOlap) {
        const TString query = Sprintf(R"(
            CREATE TABLE `/Root/test_acl` (
                id Int64 NOT NULL,
                name String,
                primary key (id)
            ) WITH (STORE=%s);
        )", isOlap ? "COLUMN" : "ROW");
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = NKqp::TKikimrSettings().SetAppConfig(appConfig);
        TKikimrRunner kikimr(appConfig);

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("root@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            AssertSuccessResult(client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync());

            driver.Stop(true);
        }

        {
            auto schemeClient = kikimr.GetSchemeClient();
            NYdb::NScheme::TPermissions permissions("user0@builtin", {});
            AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync()
            );
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/test_acl`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            const auto expectedIssueMessage = "Cannot find table 'db.[/Root/test_acl]' because it does not exist or you do not have access permissions.";
            UNIT_ASSERT_C(result.GetIssues().ToString().contains(expectedIssueMessage), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES (1, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());
            UNIT_ASSERT_C(resultWrite.GetIssues().ToString().contains(expectedIssueMessage), resultWrite.GetIssues().ToString());

            driver.Stop(true);
        }

        {
            auto schemeClient = kikimr.GetSchemeClient();
            NYdb::NScheme::TPermissions permissions("user0@builtin", {"ydb.deprecated.describe_schema"});
            AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync()
            );
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/test_acl`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            const auto expectedIssueMessage = "Failed to resolve table `/Root/test_acl` status: AccessDenied., code: 2028";
            UNIT_ASSERT_C(result.GetIssues().ToString().contains(expectedIssueMessage), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES (1, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());
            UNIT_ASSERT_C(resultWrite.GetIssues().ToString().contains(expectedIssueMessage), resultWrite.GetIssues().ToString());

            driver.Stop(true);
        }

        {
            auto schemeClient = kikimr.GetSchemeClient();
            NYdb::NScheme::TPermissions permissions("user0@builtin", {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"});
            AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync()
            );
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/test_acl`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES (1, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());
            const auto expectedIssueMessage = "Failed to resolve table `/Root/test_acl` status: AccessDenied., code: 2028";
            UNIT_ASSERT_C(resultWrite.GetIssues().ToString().contains(expectedIssueMessage), resultWrite.GetIssues().ToString());

            driver.Stop(true);
        }

        {
            auto schemeClient = kikimr.GetSchemeClient();
            NYdb::NScheme::TPermissions permissions("user0@builtin", {"ydb.deprecated.update_row"});
            AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync()
            );
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/test_acl`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES (1, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());

            auto resultDelete = client.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` WHERE 1=1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!resultDelete.IsSuccess(), resultDelete.GetIssues().ToString());
            const auto expectedIssueMessage = "Failed to resolve table `/Root/test_acl` status: AccessDenied., code: 2028";
            UNIT_ASSERT_C(resultDelete.GetIssues().ToString().contains(expectedIssueMessage), resultDelete.GetIssues().ToString());

            driver.Stop(true);
        }

        {
            auto schemeClient = kikimr.GetSchemeClient();
            NYdb::NScheme::TPermissions permissions("user0@builtin", {"ydb.deprecated.erase_row"});
            NYdb::NScheme::TPermissions revokePermissions("user0@builtin", {"ydb.deprecated.update_row"});
            AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                    NYdb::NScheme::TModifyPermissionsSettings()
                        .AddGrantPermissions(permissions)
                        .AddRevokePermissions(revokePermissions)
                ).ExtractValueSync()
            );
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/test_acl`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES (1, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());
            const auto expectedIssueMessage = "Failed to resolve table `/Root/test_acl` status: AccessDenied., code: 2028";
            UNIT_ASSERT_C(resultWrite.GetIssues().ToString().contains(expectedIssueMessage), resultWrite.GetIssues().ToString());

            auto resultDelete = client.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` WHERE 1=1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultDelete.IsSuccess(), resultDelete.GetIssues().ToString());

            driver.Stop(true);
        }

        {
            auto schemeClient = kikimr.GetSchemeClient();
            NYdb::NScheme::TPermissions permissions("user0@builtin", {"ydb.deprecated.erase_row", "ydb.deprecated.update_row"});
            AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync()
            );
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/test_acl`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES (1, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());

            auto resultDelete = client.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` WHERE 1=1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultDelete.IsSuccess(), resultDelete.GetIssues().ToString());

            driver.Stop(true);
        }

        {
            auto schemeClient = kikimr.GetSchemeClient();
            NYdb::NScheme::TPermissions revokePermissions("user0@builtin", {"ydb.deprecated.select_row"});
            AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                    NYdb::NScheme::TModifyPermissionsSettings()
                        .AddRevokePermissions(revokePermissions)
                ).ExtractValueSync()
            );
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/test_acl`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            const auto expectedIssueMessage = "Failed to resolve table `/Root/test_acl` status: AccessDenied., code: 2028";
            UNIT_ASSERT_C(result.GetIssues().ToString().contains(expectedIssueMessage), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES (1, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());

            auto resultDelete = client.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` WHERE 1=1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!resultDelete.IsSuccess(), resultDelete.GetIssues().ToString());
            UNIT_ASSERT_C(resultDelete.GetIssues().ToString().contains(expectedIssueMessage), resultDelete.GetIssues().ToString());

            driver.Stop(true);
        }

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto result = client.ExecuteQuery(R"(
                INSERT INTO `/Root/test_acl` (id, name) VALUES (100, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            auto resultWrite = client.ExecuteQuery(R"(
                UPDATE `/Root/test_acl` ON SELECT 100 AS id, 'new test' AS name;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultWrite.IsSuccess(), resultWrite.GetIssues().ToString());

            auto resultDelete = client.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` ON SELECT 100 AS id;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultDelete.IsSuccess(), resultDelete.GetIssues().ToString());

            driver.Stop(true);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
