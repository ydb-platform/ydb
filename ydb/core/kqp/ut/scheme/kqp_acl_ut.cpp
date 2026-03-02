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

void AddConnectPermission(const TKikimrRunner& kikimr, const TString& subject) {
    AddPermissions(kikimr, "/Root", subject, {"ydb.database.connect"});
    WaitForProxy(kikimr, subject);
}

Y_UNIT_TEST_SUITE(KqpAcl) {
    Y_UNIT_TEST(FailNavigate) {
        TKikimrRunner kikimr;

        auto db = kikimr.GetTableClient(NYdb::NTable::TClientSettings().AuthToken(UserName));
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
        NKqp::TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        TKikimrRunner kikimr(settings);

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

    Y_UNIT_TEST_QUAD(AclDml, UseSink, IsOlap) {
        NKqp::TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(IsOlap);
        TKikimrRunner kikimr(settings);

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("root@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/test_acl` (
                    id Uint64 NOT NULL,
                    name String,
                    primary key (id)
                ) WITH (STORE=%s);
            )", IsOlap ? "COLUMN" : "ROW");

            AssertSuccessResult(client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync());

            driver.Stop(true);
        }

        AddConnectPermission(kikimr, UserName);
        AddPermissions(kikimr, "/Root/test_acl", UserName, {"ydb.deprecated.describe_schema", "ydb.deprecated.update_row"});

        auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserName);
        auto driver = TDriver(driverConfig);
        auto client = NYdb::NQuery::TQueryClient(driver);

        auto session = client.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                UPSERT INTO `/Root/test_acl` (id, name) VALUES
                    (10u, "One");
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        {
            auto result = session.ExecuteQuery(R"(
                REPLACE INTO `/Root/test_acl` (id, name) VALUES
                    (11u, "Two");
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        {
            auto result = session.ExecuteQuery(R"(
                INSERT INTO `/Root/test_acl` (id, name) VALUES
                    (12u, "Three");
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        {
            auto result = session.ExecuteQuery(R"(
                UPDATE `/Root/test_acl` SET name = "test" WHERE id = 10u;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                UPDATE `/Root/test_acl` ON SELECT 11u AS id, "test" AS name;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        {
            auto result = session.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` WHERE id = 10u;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` ON SELECT 11u AS id;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::ABORTED, result.GetIssues().ToString());
        }

        AddPermissions(kikimr, "/Root/test_acl", UserName, {"ydb.deprecated.describe_schema", "ydb.deprecated.update_row", "ydb.deprecated.erase_row"});

        {
            auto result = session.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` WHERE id = 10u;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        {
            auto result = session.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` ON SELECT 11u AS id;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        AddPermissions(kikimr, "/Root/test_acl", UserName, {"ydb.deprecated.describe_schema", "ydb.deprecated.update_row", "ydb.deprecated.erase_row", "ydb.deprecated.select_row"});

        {
            auto result = session.ExecuteQuery(R"(
                UPDATE `/Root/test_acl` SET name = "test" WHERE id = 10u;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        {
            auto result = session.ExecuteQuery(R"(
                DELETE FROM `/Root/test_acl` WHERE id = 10u;
            )", NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            AssertSuccessResult(result);
        }

        driver.Stop(true);
    }

    Y_UNIT_TEST_QUAD(AclRevoke, UseSink, IsOlap) {
        NKqp::TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(IsOlap);
        TKikimrRunner kikimr(settings);

        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("root@builtin");
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/test_acl` (
                    id Uint64 NOT NULL,
                    name String,
                    primary key (id)
                ) WITH (STORE=%s);
            )", IsOlap ? "COLUMN" : "ROW");

            AssertSuccessResult(client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync());

            driver.Stop(true);
        }

        AddConnectPermission(kikimr, UserName);

        for (const auto permission : {"ydb.deprecated.describe_schema", "ydb.deprecated.update_row"}) {
            AddPermissions(kikimr, "/Root/test_acl", UserName, {"ydb.deprecated.describe_schema", "ydb.deprecated.update_row"});

            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken(UserName);
            auto driver = TDriver(driverConfig);
            auto client = NYdb::NQuery::TQueryClient(driver);

            auto session = client.GetSession().GetValueSync().GetSession();

            const TString query = R"(UPSERT INTO `/Root/test_acl` (id, name) VALUES (10u, "One");)";

            for (size_t index = 0; index < 10; ++index) {
                auto result = session.ExecuteQuery(
                    query,
                    NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                AssertSuccessResult(result);
            }

            {
                auto schemeClient = kikimr.GetSchemeClient();
                NYdb::NScheme::TPermissions permissions("user0@builtin", {permission});
                AssertSuccessResult(schemeClient.ModifyPermissions("/Root/test_acl",
                        NYdb::NScheme::TModifyPermissionsSettings().AddRevokePermissions(permissions)
                    ).ExtractValueSync()
                );
            }

            {
                auto result = session.ExecuteQuery(
                    query,
                    NYdb::NQuery::TTxControl::BeginTx(NYdb::NQuery::TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            }

            driver.Stop(true);
        }
    }

    NQuery::TSession CreateSession(NQuery::TQueryClient& client) {
        auto result = client.GetSession().GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result.GetSession();
    }

    Y_UNIT_TEST_TWIN(AlterDatabasePrivilegesRequiredToChangeSchemeLimits, AsClusterAdmin) {
        /* Default Kikimr runner can not create extsubdomain. */
        TTestExtEnv::TEnvSettings settings;
        settings.FeatureFlags.SetEnableAlterDatabase(true);

        TTestExtEnv env(settings);
        env.CreateDatabase("Test");

        auto& runtime = *env.GetServer().GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);

        // make cluster_admin@builtin a cluster admin
        {
            // The order is important here, because grants from anonymous user are possible
            // only while AdministrationAllowedSIDs is empty (which means that anyone is an admin).
            env.GetClient().TestGrant("/", "Root", "cluster_admin@builtin", NACLib::EAccessRights::GenericFull);
            runtime.GetAppData().AdministrationAllowedSIDs.emplace_back("cluster_admin@builtin");
        }
        NQuery::TQueryClient clusterAdmin(env.GetDriver(), NQuery::TClientSettings().AuthToken("cluster_admin@builtin"));
        auto clusterAdminSession = CreateSession(clusterAdmin);

        {
            env.GetClient().SetSecurityToken("cluster_admin@builtin"); // must be a cluster admin

            auto result = clusterAdminSession.ExecuteQuery(R"(
                    CREATE USER databaseadmin ENCRYPTED PASSWORD 'secret_password';
                )", NQuery::TTxControl::NoTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        NQuery::TQueryClient databaseAdmin(env.GetDriver(), NQuery::TClientSettings().CredentialsProviderFactory(
            CreateLoginCredentialsProviderFactory({
                .User = "databaseadmin",
                .Password = "secret_password",
            })
        ));
        auto databaseAdminSession = CreateSession(databaseAdmin);

        {
            auto result = clusterAdminSession.ExecuteQuery(R"(
                    ALTER DATABASE `/Root/Test` OWNER TO databaseadmin;
                )", NQuery::TTxControl::NoTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            TTableClient client(env.GetDriver());
            auto session = client.CreateSession().GetValueSync().GetSession();
            CheckOwner(session, "/Root/Test", "databaseadmin");
        }

        {
            auto result = (AsClusterAdmin ? clusterAdminSession : databaseAdminSession).ExecuteQuery(R"(
                    ALTER DATABASE `/Root/Test` SET (MAX_PATHS = 10, MAX_SHARDS = 20);
                )", NQuery::TTxControl::NoTx()
            ).ExtractValueSync();

            if (AsClusterAdmin) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToString());
            }
        }
    }

    Y_UNIT_TEST_TWIN(AclTemporary, IsOlap) {
        auto settings = NKqp::TKikimrSettings().SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableTempTablesForUser(true);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestClient().GrantConnect("user_write@builtin");
        kikimr.GetTestClient().GrantConnect("user_read@builtin");
        kikimr.GetTestServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back("root@builtin");

        auto driverWriteConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken("user_write@builtin")
            .SetDatabase("/Root");
        auto driverWrite = TDriver(driverWriteConfig);
        auto clientWrite = NYdb::NQuery::TQueryClient(driverWrite);

        auto sessionWrite = CreateSession(clientWrite);
        auto sessionWriteOther = CreateSession(clientWrite);

        auto driverReadConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken("user_read@builtin")
            .SetDatabase("/Root");
        auto driverRead = TDriver(driverReadConfig);
        auto clientRead = NYdb::NQuery::TQueryClient(driverRead);

        auto sessionRead = CreateSession(clientRead);

        auto clientRoot = kikimr.GetQueryClient();
        auto sessionRoot = CreateSession(clientRoot);


        {
            const TString queryWrite = Sprintf(R"(
                CREATE TEMPORARY TABLE `/Root/Test` (
                    id Uint64 NOT NULL,
                    name String,
                    primary key (id)
                ) WITH (STORE=%s);
            )", IsOlap ? "COLUMN" : "ROW");

            auto result = sessionWrite.ExecuteQuery(queryWrite, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // tmp table by session alias
        const TString queryWithAlias = R"(
            UPSERT INTO `/Root/Test` (id, name) VALUES
                (10u, "One");
        )";

        {
            auto result = sessionWrite.ExecuteQuery(queryWithAlias, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = sessionWriteOther.ExecuteQuery(queryWithAlias, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = sessionRead.ExecuteQuery(queryWithAlias, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = sessionRoot.ExecuteQuery(queryWithAlias, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }

        TString tempDirName;
        {
            auto schemeClient = kikimr.GetSchemeClient();
            auto listResult = schemeClient.ListDirectory(
                "/Root/.tmp/sessions"
                ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS, listResult.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren().size(), 1);
            tempDirName = listResult.GetChildren()[0].Name;
        }

        const TString sessionTmpDir = TStringBuilder()
            << "/Root/.tmp/sessions/"
            << tempDirName;

        {
            auto result = sessionWrite.ExecuteQuery(Sprintf(R"(
                CREATE TABLE `%s/Test` (
                    id Uint64 NOT NULL,
                    name String,
                    primary key (id)
                );
            )", sessionTmpDir.c_str()), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetIssues().ToString().contains("path is temporary"), result.GetIssues().ToString());
        }

        const TString queryWithRealPath = Sprintf(R"(
            UPSERT INTO `%s/Root/Test` (id, name) VALUES
                (10u, "One");
        )", sessionTmpDir.c_str());

        const TString queryWithFakePath = Sprintf(R"(
            UPSERT INTO `%s/Root/Test2` (id, name) VALUES
                (10u, "One");
        )", sessionTmpDir.c_str());

        {
            auto result = sessionWrite.ExecuteQuery(queryWithRealPath, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = sessionWriteOther.ExecuteQuery(queryWithRealPath, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = sessionRead.ExecuteQuery(queryWithRealPath, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetIssues().ToString().contains("because it does not exist or you do not have access permissions. Please check correctness of table path and user permissions."), result.GetIssues().ToString());
        }

        {
            auto result = sessionRead.ExecuteQuery(queryWithFakePath, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetIssues().ToString().contains("because it does not exist or you do not have access permissions. Please check correctness of table path and user permissions."), result.GetIssues().ToString());
        }

        {
            auto result = sessionRoot.ExecuteQuery(queryWithRealPath, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = sessionRoot.ExecuteQuery(queryWithFakePath, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetIssues().ToString().contains("because it does not exist or you do not have access permissions. Please check correctness of table path and user permissions."), result.GetIssues().ToString());
        }

        auto schemeClientWrite = NYdb::NScheme::TSchemeClient(driverWrite);
        auto schemeClientRead = NYdb::NScheme::TSchemeClient(driverRead);
        auto schemeClientRoot = kikimr.GetSchemeClient();

        {
            auto result = schemeClientWrite.ListDirectory(sessionTmpDir).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = schemeClientRead.ListDirectory(sessionTmpDir).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.ListDirectory(sessionTmpDir).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.DescribePath(sessionTmpDir).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = schemeClientRead.DescribePath(sessionTmpDir).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.DescribePath(sessionTmpDir).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.ListDirectory("/Root/.tmp").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRead.ListDirectory("/Root/.tmp").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.ListDirectory("/Root/.tmp").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.DescribePath("/Root/.tmp").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRead.DescribePath("/Root/.tmp").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.DescribePath("/Root/.tmp").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.ListDirectory("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRead.ListDirectory("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.ListDirectory("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.DescribePath("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRead.DescribePath("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.DescribePath("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.MakeDirectory("/Root/.tmp/test1").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRead.MakeDirectory("/Root/.tmp/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.MakeDirectory("/Root/.tmp/test2").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.MakeDirectory("/Root/.tmp/sessions/test1").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRead.MakeDirectory("/Root/.tmp/sessions/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRoot.MakeDirectory("/Root/.tmp/sessions/test2").GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = schemeClientWrite.MakeDirectory(sessionTmpDir + "/test1").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "path is temporary",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientRead.MakeDirectory(sessionTmpDir + "/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            // Nobody can create objects in temporary directory
            auto result = schemeClientRoot.MakeDirectory(sessionTmpDir + "/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "path is temporary",
                result.GetIssues().ToString()
            );
        }

        driverWrite.Stop(true);
        driverRead.Stop(true);
    }

    Y_UNIT_TEST_TWIN(AclTemporaryInterruptInheritance, IsOlap) {
        auto settings = NKqp::TKikimrSettings().SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableTempTablesForUser(true);
        TKikimrRunner kikimr(settings);

        const TString UserWriteName = "user_write@builtin";
        const TString UserReadName = "root@builtin";

        auto driverWriteConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserWriteName)
            .SetDatabase("/Root");
        auto driverWrite = TDriver(driverWriteConfig);
        auto clientWrite = NYdb::NQuery::TQueryClient(driverWrite);

        auto sessionWrite = CreateSession(clientWrite);
        auto sessionWriteOther = CreateSession(clientWrite);

        auto driverReadConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserReadName)
            .SetDatabase("/Root");
        auto driverRead = TDriver(driverReadConfig);
        auto clientRead = NYdb::NQuery::TQueryClient(driverRead);

        auto sessionRead = CreateSession(clientRead);

        auto schemeClientWrite = NYdb::NScheme::TSchemeClient(driverWrite);
        auto schemeClientRead = NYdb::NScheme::TSchemeClient(driverRead);


        {
            const TString queryWrite = Sprintf(R"(
                CREATE TEMPORARY TABLE `/Root/Test` (
                    id Uint64 NOT NULL,
                    name String,
                    primary key (id)
                ) WITH (STORE=%s);
            )", IsOlap ? "COLUMN" : "ROW");

            auto result = sessionWrite.ExecuteQuery(queryWrite, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString tempDirName;
        {
            auto schemeClient = kikimr.GetSchemeClient();
            auto listResult = schemeClient.ListDirectory(
                "/Root/.tmp/sessions"
                ).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS, listResult.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren().size(), 1);
            tempDirName = listResult.GetChildren()[0].Name;
        }

        kikimr.GetTestClient().TestGrant("/", "Root", UserReadName, NACLib::EAccessRights::GenericRead);

        {
            auto result = schemeClientRead.ListDirectory("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        kikimr.GetTestClient().TestGrant("/Root/.tmp", "sessions", UserReadName, NACLib::EAccessRights::GenericRead);

        {
            auto result = schemeClientRead.ListDirectory("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(result.GetChildren().size(), 1);
        }

        {
            auto result = schemeClientRead.ListDirectory(TStringBuilder()
                << "/Root/.tmp/sessions/" << tempDirName  << "/").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNAUTHORIZED);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Access denied",
                result.GetIssues().ToString()
            );
        }

        {
            auto result = schemeClientWrite.ListDirectory(TStringBuilder()
                << "/Root/.tmp/sessions/" << tempDirName << "/").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        driverWrite.Stop(true);
        driverRead.Stop(true);
    }

    Y_UNIT_TEST(AclInterruptInheritance) {
        auto settings = NKqp::TKikimrSettings().SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableTempTablesForUser(true);
        TKikimrRunner kikimr(settings);
        kikimr.GetTestClient().GrantConnect("write@builtin");
        kikimr.GetTestClient().GrantConnect("read@builtin");

        const TString UserWriteName = "write@builtin";
        const TString UserReadName = "read@builtin";

        auto driverWriteConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserWriteName)
            .SetDatabase("/Root");
        auto driverWrite = TDriver(driverWriteConfig);
        auto clientWrite = NYdb::NQuery::TQueryClient(driverWrite);

        auto sessionWrite = CreateSession(clientWrite);

        auto driverReadConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserReadName)
            .SetDatabase("/Root");
        auto driverRead = TDriver(driverReadConfig);
        auto clientRead = NYdb::NQuery::TQueryClient(driverRead);

        auto sessionRead = CreateSession(clientRead);

        kikimr.GetTestClient().TestGrant("/", "Root", "write@builtin",
            NACLib::EAccessRights::GenericWrite);
        kikimr.GetTestClient().TestGrant("/", "Root", "write@builtin", 
            NACLib::EAccessRights::GenericRead);

        {
            auto result = sessionWrite.ExecuteQuery(R"(
                CREATE TABLE `/Root/test1/test2/Test` (
                    id Uint64 NOT NULL,
                    primary key (id)
                );
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto schemeClientWrite = NYdb::NScheme::TSchemeClient(driverWrite);
            auto result = schemeClientWrite.ListDirectory("/Root/test1/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto schemeClientRead = NYdb::NScheme::TSchemeClient(driverRead);
            auto result = schemeClientRead.ListDirectory("/Root/test1/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToString());
        }

        kikimr.GetTestClient().TestGrant("/", "Root", "read@builtin", NACLib::EAccessRights::GenericRead);

        {
            auto schemeClientWrite = NYdb::NScheme::TSchemeClient(driverWrite);
            auto result = schemeClientWrite.ListDirectory("/Root/test1/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto schemeClientRead = NYdb::NScheme::TSchemeClient(driverRead);
            auto result = schemeClientRead.ListDirectory("/Root/test1/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NACLib::TDiffACL diffAcl;
        diffAcl.SetInterruptInheritance(true);
        kikimr.GetTestClient().ModifyACL("/Root", "test1", diffAcl.SerializeAsString());


        {
            auto schemeClientWrite = NYdb::NScheme::TSchemeClient(driverWrite);
            auto result = schemeClientWrite.ListDirectory("/Root/test1/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto schemeClientRead = NYdb::NScheme::TSchemeClient(driverRead);
            auto result = schemeClientRead.ListDirectory("/Root/test1/test2").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToString()); // Bug?
        }
    }

    Y_UNIT_TEST_QUAD(AclCreateTableAs, IsOlap, UseAdmin) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableMoveColumnTable(true);
        auto settings = NKqp::TKikimrSettings().SetFeatureFlags(featureFlags).SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableDataShardCreateTableAs(true);
        TKikimrRunner kikimr(settings);
        if (UseAdmin) {
            kikimr.GetTestClient().GrantConnect("user_write@builtin");
            kikimr.GetTestServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.emplace_back("root@builtin");
        }

        const TString UserWriteName = "user_write@builtin";
        const TString UserReadName = "root@builtin";

        AddPermissions(kikimr, "/Root", UserWriteName, {"ydb.deprecated.create_table"});

        auto driverWriteConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserWriteName)
            .SetDatabase("/Root");
        auto driverWrite = TDriver(driverWriteConfig);
        auto clientWrite = NYdb::NQuery::TQueryClient(driverWrite);

        auto sessionWrite = CreateSession(clientWrite);
        auto sessionWriteOther = CreateSession(clientWrite);

        auto driverReadConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetAuthToken(UserReadName)
            .SetDatabase("/Root");
        auto driverRead = TDriver(driverReadConfig);
        auto clientRead = NYdb::NQuery::TQueryClient(driverRead);

        auto sessionRead = CreateSession(clientRead);

        {
            const TString queryWrite = Sprintf(R"(
                CREATE TABLE `/Root/TestSimple` (
                    id Uint64 NOT NULL,
                    name String,
                    primary key (id)
                ) WITH (STORE=%s);
            )", IsOlap ? "COLUMN" : "ROW");

            auto result = sessionWrite.ExecuteQuery(queryWrite, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const TString queryWrite = Sprintf(R"(
                CREATE TABLE `/Root/Test` (
                    primary key (id)
                ) WITH (STORE=%s)
                AS SELECT 1 As id, "test" As name;
            )", IsOlap ? "COLUMN" : "ROW");

            auto result = sessionWrite.ExecuteQuery(queryWrite, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // Writer can access table
            auto result = sessionWriteOther.ExecuteQuery("SELECT * FROM `/Root/Test`", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // Another user can't access table
            auto result = sessionRead.ExecuteQuery("SELECT * FROM `/Root/Test`", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }

        AddPermissions(kikimr, "/Root", UserReadName, {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"});

        {
            auto result = sessionRead.ExecuteQuery("SELECT * FROM `/Root/Test`", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        driverWrite.Stop(true);
        driverRead.Stop(true);
    }
}

} // namespace NKqp
} // namespace NKikimr
