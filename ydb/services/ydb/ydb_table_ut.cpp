#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/core/issue/protos/issue_id.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <util/thread/factory.h>

using namespace NYdb;
using namespace NYdb::NTable;

TSession CreateSession(TDriver driver, const TString& token = "", const TString& discoveryEndpoint = "") {
    NYdb::NTable::TClientSettings settings;
    if (token)
        settings.AuthToken(token);
    if (discoveryEndpoint)
        settings.DiscoveryEndpoint(discoveryEndpoint);
    NYdb::NTable::TTableClient client(driver, settings);
    auto session = client.CreateSession().ExtractValueSync();
    UNIT_ASSERT_EQUAL(session.IsTransportError(), false);
    return session.GetSession();
}


void EnsureTablePartitions(NYdb::NTable::TTableClient& client, TString table, ui32 expectedPartitions) {
    auto session = client.CreateSession().ExtractValueSync().GetSession();
    auto describeTableSettings = TDescribeTableSettings()
        .WithTableStatistics(true).WithPartitionStatistics(true).WithKeyShardBoundary(true);
    auto description = session.DescribeTable(table, describeTableSettings).ExtractValueSync();

    UNIT_ASSERT_C(description.IsSuccess(), description.GetIssues().ToString());
    UNIT_ASSERT_VALUES_EQUAL(description.GetTableDescription().GetPartitionsCount(), expectedPartitions);
    UNIT_ASSERT_VALUES_EQUAL(description.GetTableDescription().GetPartitionStats().size(), expectedPartitions);
}

bool HasIssue(const NYql::TIssues& issues, ui32 code, std::string_view message,
    std::function<bool(const NYql::TIssue& issue)> predicate = {})
{
    bool hasIssue = false;

    for (auto& issue : issues) {
        NYql::WalkThroughIssues(issue, false, [&] (const NYql::TIssue& issue, int) {
            if (!hasIssue && issue.GetCode() == code && (message.empty() || message == issue.GetMessage())) {
                hasIssue = !predicate || predicate(issue);
            }
        });
    }

    return hasIssue;
}

static void MultiTenantSDK(bool asyncDiscovery) {
    TKikimrWithGrpcAndRootSchemaWithAuthAndSsl server;
    ui16 grpc = server.GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;

    auto driver = NYdb::TDriver(
        TDriverConfig()
            .SetAuthToken("badguy@builtin")
            .UseSecureConnection(NYdbSslTestData::CaCrt)
            .SetEndpoint(location)
            .SetDiscoveryMode(asyncDiscovery ? EDiscoveryMode::Async : EDiscoveryMode::Sync));


    NYdb::NTable::TClientSettings settings;
    settings.AuthToken("root@builtin");

    NYdb::NTable::TTableClient clientgood(driver, settings);
    NYdb::NTable::TTableClient clientbad(driver);
//TODO: No discovery in ut
/*
    NYdb::NTable::TClientSettings settings2;
    settings2.AuthToken("root@builtin");
    settings2.Database_ = "/balabla";
    NYdb::NTable::TTableClient clientbad2(driver, settings2);
*/
    const TString sql = R"__(
        CREATE TABLE `Root/Test` (
            Key Uint32,
            Value String,
            PRIMARY KEY (Key)
        );)__";

    clientbad.CreateSession().Apply([sql](const TAsyncCreateSessionResult& future) {
        const auto& sessionValue = future.GetValue();
        UNIT_ASSERT(!sessionValue.IsTransportError());
        auto session = sessionValue.GetSession();
        session.ExecuteSchemeQuery(sql).Apply([](const TAsyncStatus& future) {
            const auto& status = future.GetValue();
            UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::UNAUTHORIZED);
        }).Wait();
    }).Wait();


    clientgood.CreateSession().Apply([sql](const TAsyncCreateSessionResult& future) {
        const auto& sessionValue = future.GetValue();
        UNIT_ASSERT(!sessionValue.IsTransportError());
        auto session = sessionValue.GetSession();
        session.ExecuteSchemeQuery(sql).Apply([](const TAsyncStatus& future) {
            const auto& status = future.GetValue();
            UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
        }).Wait();
    }).Wait();
/*
    clientbad2.CreateSession().Subscribe([sql](const TAsyncCreateSessionResult& future) {
        const auto& sessionValue = future.GetValue();
        UNIT_ASSERT_EQUAL(sessionValue.GetStatus(), EStatus::CLIENT_DISCOVERY_FAILED);
        UNIT_ASSERT_EQUAL(sessionValue.GetIssues().ToString(), "<main>: Error: Endpoint list is empty for database /balabla");
        UNIT_ASSERT(sessionValue.IsTransportError());
    });
*/
    driver.Stop(true);
}

Y_UNIT_TEST_SUITE(YdbYqlClient) {
    Y_UNIT_TEST(TestYqlWrongTable) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto session = CreateSession(connection);

        {
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Json,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Yson,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(TestYqlIssues) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto session = CreateSession(connection);

        {
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Uint32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        auto result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Value)
                VALUES("foo", "bar");
            )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        auto ref = R"___(<main>: Error: Type annotation, code: 1030
    <main>:2:25: Error: At function: KiWriteTable!
        <main>:2:43: Error: Failed to convert type: Struct<'Key':String,'Value':String> to Struct<'Key':Uint32?,'Value':String?>
            <main>:2:43: Error: Failed to convert 'Key': String to Optional<Uint32>
        <main>:2:43: Error: Failed to convert input columns types to scheme types, code: 2031
)___";
        UNIT_ASSERT_EQUAL(result.GetIssues().Size(), 1);
        UNIT_ASSERT_NO_DIFF(result.GetIssues().ToString(), ref);
    }

    Y_UNIT_TEST(TestYqlSessionClosed) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto session = CreateSession(connection);
        auto status = session.Close().ExtractValueSync();
        UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);

        auto result = session.ExecuteDataQuery("SELECT 42;",
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::BAD_SESSION);
    }

    Y_UNIT_TEST(DiscoveryLocationOverride) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint("wrongLocation"));
        auto session = CreateSession(connection, "", location);
        auto status = session.Close().ExtractValueSync();
        UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(TestColumnOrder) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto session = CreateSession(connection);

        {
            auto status = session.ExecuteSchemeQuery(R"__(
            CREATE TABLE `Root/Test` (
                Column1 Uint32,
                Column2 Uint32,
                Column3 Uint32,
                Column4 Uint32,
                PRIMARY KEY (Column1)
            );)__").ExtractValueSync();

            UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
        }

        session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Column1, Column2, Column3, Column4)
            VALUES(1u, 12u, 13u, 14u);
            UPSERT INTO `Root/Test` (Column1, Column2, Column3, Column4)
            VALUES(2u, 22u, 23u, 24u);
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        auto result = session.ExecuteDataQuery(R"___(
            SELECT Column4, Column2, Column3, Column1 FROM `Root/Test`;
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        TVector<TResultSet> resultSets = result.GetResultSets();
        UNIT_ASSERT_EQUAL(resultSets.size(), 1);
        UNIT_ASSERT_EQUAL(resultSets[0].ColumnsCount(), 4);
        auto columnMeta = resultSets[0].GetColumnsMeta();
        const TString ref[] = { "Column4", "Column2", "Column3", "Column1" };
        for (size_t i = 0; i < columnMeta.size(); ++i) {
            UNIT_ASSERT_NO_DIFF(columnMeta[i].Name, ref[i]);
        }
    }

    Y_UNIT_TEST(TestDecimal) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto session = CreateSession(connection);

        auto result = session.ExecuteDataQuery(R"___(
            SELECT CAST("184467440737.12345678" as Decimal(22,9));
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        TVector<TResultSet> resultSets = result.GetResultSets();
        UNIT_ASSERT_EQUAL(resultSets.size(), 1);
        UNIT_ASSERT_EQUAL(resultSets[0].ColumnsCount(), 1);
        UNIT_ASSERT_EQUAL(resultSets[0].GetColumnsMeta().size(), 1);
        auto column = resultSets[0].GetColumnsMeta()[0];
        TTypeParser typeParser(column.Type);
        typeParser.OpenOptional();
        UNIT_ASSERT_EQUAL(typeParser.GetKind(), TTypeParser::ETypeKind::Decimal);

        TResultSetParser rsParser(resultSets[0]);
        while (rsParser.TryNextRow()) {
            auto columnParser = std::move(rsParser.ColumnParser(0));
            columnParser.OpenOptional();
            auto decimalString = columnParser.GetDecimal().ToString();
            UNIT_ASSERT_EQUAL(decimalString, "184467440737.12345678");
        }
    }

    Y_UNIT_TEST(TestDecimalFullStack) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto sessionResponse = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_EQUAL(sessionResponse.IsTransportError(), false);

        auto session = sessionResponse.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Int32)
                .AddNullableColumn("Value", TDecimalType(22,9));
            tableBuilder.SetPrimaryKeyColumn("Key");
            auto result = session.CreateTable("/Root/FooTable", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            TString query = R"___(
                DECLARE $Value AS Decimal(22,9);
                DECLARE $Key AS Int32;
                UPSERT INTO `Root/FooTable` (Key, Value) VALUES
                    ($Key, $Value);
             )___";

            constexpr int records = 5;
            int count = records;
            const TString decimalParams[records] = {
                "123",
                "4.56",
                "0",
                "-4.56",
                "-123"
            };
            while (count--) {
                auto paramsBuilder = client.GetParamsBuilder();
                auto params = paramsBuilder
                    .AddParam("$Key")
                        .Int32(count)
                        .Build()
                    .AddParam("$Value")
                        .Decimal(TDecimalValue(decimalParams[count]))
                        .Build()
                    .Build();
                auto result = session
                    .ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW())
                        .CommitTx(), std::move(params))
                    .ExtractValueSync();

                UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            }

        }

        {
            TString query = R"___(SELECT SUM(Value),MIN(Value),MAX(Value) FROM `Root/FooTable`)___";
            auto result = session
                .ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW())
                    .CommitTx())
                .ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            TVector<TResultSet> resultSets = result.GetResultSets();
            UNIT_ASSERT_EQUAL(resultSets.size(), 1);
            UNIT_ASSERT_EQUAL(resultSets[0].ColumnsCount(), 3);
            UNIT_ASSERT_EQUAL(resultSets[0].GetColumnsMeta().size(), 3);

            for (auto column : resultSets[0].GetColumnsMeta()) {
                TTypeParser typeParser(column.Type);
                UNIT_ASSERT_EQUAL(typeParser.GetKind(), TTypeParser::ETypeKind::Optional);
                typeParser.OpenOptional();
                UNIT_ASSERT_EQUAL(typeParser.GetKind(), TTypeParser::ETypeKind::Decimal);
            }

            TResultSetParser rsParser(resultSets[0]);
            const TString expected[3] = {
                "0",
                "-123",
                "123"
            };
            while (rsParser.TryNextRow()) {
                for (size_t i = 0; i < resultSets[0].ColumnsCount(); i++) {
                    auto columnParser = std::move(rsParser.ColumnParser(i));
                    columnParser.OpenOptional();
                    auto decimal = columnParser.GetDecimal();

                    UNIT_ASSERT_EQUAL(decimal.ToString(), expected[i]);
                }
            }
        }
        {
            auto res = session.DescribeTable("Root/FooTable").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_EQUAL(res.GetTableDescription().GetColumns().size(), 2);

            TTypeParser::ETypeKind kinds[2] = {TTypeParser::ETypeKind::Primitive, TTypeParser::ETypeKind::Decimal};
            int i = 0;
            for (const auto& column : res.GetTableDescription().GetColumns()) {
                auto tParser = TTypeParser(column.Type);
                tParser.OpenOptional();
                UNIT_ASSERT_EQUAL(kinds[i++], tParser.GetKind());
            }
        }
    }

    Y_UNIT_TEST(TestTzTypesFullStack) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        NYdb::NTable::TTableClient client(connection);
        auto sessionResponse = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_EQUAL(sessionResponse.IsTransportError(), false);

        auto session = sessionResponse.GetSession();

        {
            TString query = R"___(
                DECLARE $x AS TzDate;
                DECLARE $y AS TzDatetime;
                DECLARE $z AS TzTimestamp;
                SELECT $x, $y, $z;
            )___";

            auto paramsBuilder = client.GetParamsBuilder();
            auto params = paramsBuilder
                    .AddParam("$x")
                        .TzDate("2020-09-22,Europe/Moscow")
                        .Build()
                    .AddParam("$y")
                        .TzDatetime("2020-09-22T15:00:00,Europe/Moscow")
                        .Build()
                    .AddParam("$z")
                        .TzTimestamp("2020-09-22T15:00:00,Europe/Moscow")
                        .Build()
                    .Build();
            auto result = session
                .ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW())
                    .CommitTx(), std::move(params))
                .ExtractValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            auto yson = FormatResultSetYson(result.GetResultSet(0));

            UNIT_ASSERT_VALUES_EQUAL(yson, "[[\"2020-09-22,Europe/Moscow\";\"2020-09-22T15:00:00,Europe/Moscow\";\"2020-09-22T15:00:00,Europe/Moscow\"]]");
        }
    }

    Y_UNIT_TEST(TestVariant) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto session = CreateSession(connection);

        const TString query = R"___(
            $struct = AsStruct(5 as foo, true as bar);
            $var_type = VariantType(TypeOf($struct));
            select Variant(42,"foo",$var_type) as Variant1;
        )___";

        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx(TTxSettings::SerializableRW()
        ).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        TVector<TResultSet> resultSets = result.GetResultSets();
        UNIT_ASSERT_EQUAL(resultSets.size(), 1);
        UNIT_ASSERT_EQUAL(resultSets[0].ColumnsCount(), 1);
        UNIT_ASSERT_EQUAL(resultSets[0].GetColumnsMeta().size(), 1);
        auto column = resultSets[0].GetColumnsMeta()[0];
        TTypeParser typeParser(column.Type);
        UNIT_ASSERT_EQUAL(typeParser.GetKind(), TTypeParser::ETypeKind::Variant);

        TResultSetParser rsParser(resultSets[0]);
        while (rsParser.TryNextRow()) {
            auto columnParser = std::move(rsParser.ColumnParser(0));
            columnParser.OpenVariant();
            UNIT_ASSERT_EQUAL(columnParser.GetInt32(), 42);
        }
    }

    Y_UNIT_TEST(TestDescribeDirectory) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto scheme = NYdb::NScheme::TSchemeClient(connection);
        auto session = CreateSession(connection);
        {
            auto status = scheme.MakeDirectory("Root/Foo").ExtractValueSync();
            UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
        }
        {
            auto status = session.ExecuteSchemeQuery(R"__(
            CREATE TABLE `Root/Foo/Test` (
                Column1 Uint32,
                Column2 Uint32,
                Column3 Uint32,
                Column4 Uint32,
                PRIMARY KEY (Column1)
            );)__").ExtractValueSync();

            UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto status = scheme.ListDirectory("Root/Foo").ExtractValueSync();
            UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
            auto children = status.GetChildren();
            UNIT_ASSERT_EQUAL(children[0].Name, "Test");
            UNIT_ASSERT_EQUAL(children[0].Type,  NYdb::NScheme::ESchemeEntryType::Table);
            UNIT_ASSERT_EQUAL(children[0].Owner, "root@builtin");
        }
        {
            auto status = scheme.ListDirectory("Root/BadPath").ExtractValueSync();
            UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SCHEME_ERROR);
            const TString expected = R"___(<main>: Error: Path not found
)___";
            UNIT_ASSERT_EQUAL(status.GetIssues().ToString(), expected);
        }

    }

    Y_UNIT_TEST(SecurityTokenAuth) {
        TKikimrWithGrpcAndRootSchemaWithAuthAndSsl server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetAuthToken("root@builtin")
                .UseSecureConnection(NYdbSslTestData::CaCrt)
                .SetEndpoint(location));

        {
            auto session = CreateSession(connection, "root@builtin");
            {
                auto status = session.ExecuteSchemeQuery(R"__(
                CREATE TABLE `Root/Test` (
                    Key Uint32,
                    Value String,
                    PRIMARY KEY (Key)
                );)__").ExtractValueSync();

                UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
                UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
            }
            {
                auto scheme = NYdb::NScheme::TSchemeClient(connection);
                auto status = scheme.ModifyPermissions("Root/Test",
                    NYdb::NScheme::TModifyPermissionsSettings()
                        .AddGrantPermissions(
                            NYdb::NScheme::TPermissions("pupkin@builtin", TVector<TString>{"ydb.tables.modify"})
                        )
                        .AddSetPermissions(
                            NYdb::NScheme::TPermissions("root@builtin", TVector<TString>{"ydb.tables.modify"}) //This permission should be ignored - last set win
                        )
                        .AddSetPermissions(
                            NYdb::NScheme::TPermissions("root@builtin", TVector<TString>{"ydb.tables.read"})
                        )
                    ).ExtractValueSync();
                UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
                UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
            }
            {
                auto scheme = NYdb::NScheme::TSchemeClient(connection);
                auto status = scheme.DescribePath("Root/Test").ExtractValueSync();
                UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
                UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
                auto entry = status.GetEntry();
                UNIT_ASSERT_EQUAL(entry.Owner, "root@builtin");
                UNIT_ASSERT_EQUAL(entry.Permissions.size(), 2);
                UNIT_ASSERT_EQUAL(entry.Permissions[0].PermissionNames.size(), 1);
                UNIT_ASSERT_EQUAL(entry.Permissions[0].Subject, "pupkin@builtin");
                UNIT_ASSERT_EQUAL(entry.Permissions[0].PermissionNames[0], "ydb.tables.modify");
                UNIT_ASSERT_EQUAL(entry.Permissions[1].Subject, "root@builtin");
                UNIT_ASSERT_EQUAL(entry.Permissions[1].PermissionNames.size(), 1);
                UNIT_ASSERT_EQUAL(entry.Permissions[1].PermissionNames[0], "ydb.tables.read");
            }
        }
        {
            auto session = CreateSession(connection, "test_user@builtin");

            {
                auto status = session.ExecuteDataQuery(R"__(
                    SELECT * FROM `Root/Test`;
                )__",TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

                UNIT_ASSERT_EQUAL(status.IsTransportError(), false);
                UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SCHEME_ERROR);
            }
        }
    }

    Y_UNIT_TEST(ConnectDbAclIsStrictlyChecked) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetCheckDatabaseAccessPermission(true);
        appConfig.MutableFeatureFlags()->SetAllowYdbRequestsWithoutDatabase(false);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->AddDefaultUserSIDs("test_user_no_rights@builtin");
        TKikimrWithGrpcAndRootSchemaWithAuth server(appConfig);

        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NActors::NLog::PRI_DEBUG);

        ui16 grpc = server.GetPort();

        { // no db
            TString location = TStringBuilder() << "localhost:" << grpc;
            auto driver = NYdb::TDriver(
                TDriverConfig()
                    .SetEndpoint(location));

            NYdb::NTable::TClientSettings settings;
            settings.AuthToken("root@builtin");

            NYdb::NTable::TTableClient client(driver, settings);
            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                Cerr << "Call\n";
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            // KIKIMR-14509 - reslore old behaviour allow requests without database for storage nodes
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());

        }
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location)
                .SetDatabase("/Root"));

        { // no token
            NYdb::NTable::TClientSettings settings;
            NYdb::NTable::TTableClient client(driver, settings);
            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                Cerr << "Call\n";
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::CLIENT_UNAUTHENTICATED, status.GetIssues().ToString());
        }


        { // empty token
            NYdb::NTable::TClientSettings settings;
            settings.AuthToken("");
            NYdb::NTable::TTableClient client(driver, settings);

            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                Cerr << "Call\n";
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::CLIENT_UNAUTHENTICATED, status.GetIssues().ToString());
        }

        { // no connect right
            TString location = TStringBuilder() << "localhost:" << grpc;
            auto driver = NYdb::TDriver(
                TDriverConfig()
                    .SetEndpoint(location)
                    .SetDatabase("/Root"));

            NYdb::NTable::TClientSettings settings;
            settings.AuthToken("test_user@builtin");
            NYdb::NTable::TTableClient client(driver, settings);

            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::UNAUTHORIZED, status.GetIssues().ToString());
        }

        { // set connect
            NYdb::TCommonClientSettings settings;
            settings.AuthToken("root@builtin");
            auto scheme = NYdb::NScheme::TSchemeClient(driver, settings);
            auto status = scheme.ModifyPermissions("/Root",
                NYdb::NScheme::TModifyPermissionsSettings()
                    .AddGrantPermissions(
                        NYdb::NScheme::TPermissions("test_user@builtin", TVector<TString>{"ydb.database.connect"})
                    )
                ).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL_C(status.IsTransportError(), false, status.GetIssues().ToString());
        }

        ui32 attemps = 2; // system is notified asynchronously, so it may see old acl for awhile
        while (attemps) { // accept connect right
            --attemps;

            NYdb::NTable::TClientSettings settings;
            settings.AuthToken("test_user@builtin");
            NYdb::NTable::TTableClient client(driver, settings);

            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            if (attemps && status.GetStatus() == EStatus::UNAUTHORIZED) {
                continue;
            }

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ConnectDbAclIsOffWhenYdbRequestsWithoutDatabase) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetCheckDatabaseAccessPermission(true);
        appConfig.MutableFeatureFlags()->SetAllowYdbRequestsWithoutDatabase(true);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(false);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->AddDefaultUserSIDs("test_user_no_rights@builtin");
        TKikimrWithGrpcAndRootSchema server(appConfig);

        ui16 grpc = server.GetPort();
        {
            TString location = TStringBuilder() << "localhost:" << grpc;
            auto driver = NYdb::TDriver(
                TDriverConfig()
                    .SetEndpoint(location)
                    .SetDatabase("/Root"));

            // with db
            NYdb::NTable::TClientSettings settings;
            settings.AuthToken("test_user@builtin");
            NYdb::NTable::TTableClient client(driver, settings);

            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::UNAUTHORIZED, status.GetIssues().ToString());
        }

        {
            TString location = TStringBuilder() << "localhost:" << grpc;
            auto driver = NYdb::TDriver(
                TDriverConfig()
                    .SetEndpoint(location));

            // without db
            NYdb::NTable::TClientSettings settings;
            settings.AuthToken("test_user@builtin");
            NYdb::NTable::TTableClient client(driver, settings);

            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ConnectDbAclIsOffWhenTokenIsOptionalAndNull) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetCheckDatabaseAccessPermission(true);
        appConfig.MutableFeatureFlags()->SetAllowYdbRequestsWithoutDatabase(false);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(false);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->AddDefaultUserSIDs("test_user_no_rights@builtin");
        TKikimrWithGrpcAndRootSchema server(appConfig);

        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto driver = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));

        { // no token
            NYdb::NTable::TClientSettings settings;
            NYdb::NTable::TTableClient client(driver, settings);

            auto call = [] (NYdb::NTable::TTableClient& client) -> NYdb::TStatus {
                return client.CreateSession().ExtractValueSync();
            };
            auto status = client.RetryOperationSync(call);

            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::SUCCESS, status.GetIssues().ToString());
        }
    }
/*
    Y_UNIT_TEST(SecurityTokenError) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
        TKikimrWithGrpcAndRootSchema server(appConfig, true, true);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetAuthToken("@error")
                .UseSecureConnection(NYdbSslTestData::CaCrt)
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);

        {
            auto session = client.GetSession().GetValueSync();
            UNIT_ASSERT_EQUAL(session.GetStatus(), EStatus::UNAVAILABLE);
            UNIT_ASSERT_EQUAL(client.GetActiveSessionCount(), 1);
        }
        UNIT_ASSERT_EQUAL(client.GetActiveSessionCount(), 0);
    }
*/

    Y_UNIT_TEST(SecurityTokenAuthMultiTenantSDK) {
        MultiTenantSDK(false);
    }

    Y_UNIT_TEST(SecurityTokenAuthMultiTenantSDKAsync) {
        MultiTenantSDK(true);
    }

    Y_UNIT_TEST(TraceId) {
        TStringStream logStream;
        TAutoPtr<TLogBackend> logBackend(new TStreamLogBackend(&logStream));

        TString traceId = "CppUtTestQuery";

        {
            NKikimrConfig::TAppConfig appConfig;
            auto& logConfig = *appConfig.MutableLogConfig();
            auto& entry = *logConfig.AddEntry();
            entry.SetComponent(NKikimrServices::EServiceKikimr_Name(NKikimrServices::KQP_YQL));
            entry.SetLevel(NActors::NLog::PRI_DEBUG);

            TKikimrWithGrpcAndRootSchema server(appConfig, {}, logBackend);

            server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
            server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
            server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_WORKER, NActors::NLog::PRI_DEBUG);
            server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_DEBUG);

            ui16 grpc = server.GetPort();

            TString location = TStringBuilder() << "localhost:" << grpc;

            auto connection = NYdb::TDriver(
                TDriverConfig()
                    .SetEndpoint(location));
            auto session = CreateSession(connection);

            auto result = session
                .ExecuteSchemeQuery(R"___(
                    CREATE TABLE `Root/Test` (
                        Key Uint32,
                        Value String,
                        PRIMARY KEY (Key)
                    );
                )___", TExecSchemeQuerySettings().TraceId(traceId)).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            result = session
                .ExecuteDataQuery(R"___(
                    UPSERT INTO `Root/Test` (Key, Value)
                    VALUES(2u, "Two");
                )___",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                TExecDataQuerySettings().TraceId(traceId)).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
        bool grpcHasTraceId = false;
        bool proxyHasTraceId = false;
        bool workerHasTraceId = false;
        bool yqlHasTraceId = false;

        TString line;
        while (logStream.ReadLine(line)) {
            if (line.Contains(traceId)) {
                grpcHasTraceId = grpcHasTraceId || line.Contains("GRPC_SERVER");
                proxyHasTraceId = proxyHasTraceId || line.Contains("KQP_PROXY");
                workerHasTraceId = workerHasTraceId || line.Contains("KQP_WORKER");
                yqlHasTraceId = yqlHasTraceId || line.Contains("KQP_YQL");
            }
        }

        UNIT_ASSERT(grpcHasTraceId);
        UNIT_ASSERT(proxyHasTraceId);
        UNIT_ASSERT(workerHasTraceId);
        UNIT_ASSERT(yqlHasTraceId);
    }

    Y_UNIT_TEST(BuildInfo) {
        TStringStream logStream;
        TAutoPtr<TLogBackend> logBackend(new TStreamLogBackend(&logStream));

        {
            TKikimrWithGrpcAndRootSchema server({}, {}, logBackend);

            server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);

            ui16 grpc = server.GetPort();

            TString location = TStringBuilder() << "localhost:" << grpc;

            auto connection = NYdb::TDriver(
                TDriverConfig()
                    .SetEndpoint(location)
                    .SetDatabase("/Root"));
            Y_UNUSED(connection);
        }
        bool grpcHasBuildInfo = false;

        TString line;
        const TString expectedBuildInfo = Sprintf("ydb-cpp-sdk/%s", GetSdkSemver().c_str());
        while (logStream.ReadLine(line)) {
            if (line.Contains(expectedBuildInfo)) {
                grpcHasBuildInfo = grpcHasBuildInfo || line.Contains("GRPC_SERVER");
            }
        }
        UNIT_ASSERT(grpcHasBuildInfo);
    }

    Y_UNIT_TEST(Utf8DatabasePassViaHeader) {
        TStringStream logStream;
        TAutoPtr<TLogBackend> logBackend(new TStreamLogBackend(&logStream));

        TString utf8Database = "/йцукен";
        {
            TKikimrWithGrpcAndRootSchema server({}, {}, logBackend);

            server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);

            ui16 grpc = server.GetPort();

            TString location = TStringBuilder() << "localhost:" << grpc;

            auto connection = NYdb::TDriver(
                TDriverConfig()
                    .SetEndpoint(location)
                    .SetDatabase(utf8Database));
            Y_UNUSED(connection);
        }
        bool found = false;

        TString line;
        while (logStream.ReadLine(line)) {
            if (line.Contains(utf8Database)) {
                 found = true;
            }
        }
        UNIT_ASSERT(found);
    }


    Y_UNIT_TEST(TestTransactionQueryError) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto session = CreateSession(connection);

        {
            auto status = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (Key Int32, Value String, PRIMARY KEY (Key));
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(status.GetStatus(), EStatus::SUCCESS);
        }

        auto result1 = session.ExecuteDataQuery(R"___(
            INSERT INTO `Root/Test` (Key, Value) VALUES(1u, "One");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result1.GetStatus(), EStatus::SUCCESS);

        {
            auto session2 = CreateSession(connection);
            auto result = session2.ExecuteDataQuery(R"___(
                UPSERT INTO `Root/Test` (Key, Value) VALUES(1u, "Two");
            )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result2 = session.ExecuteDataQuery(R"___(
                SELECT 42;
            )___", TTxControl::Tx(*result1.GetTransaction()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result2.GetStatus(), EStatus::NOT_FOUND);
            auto issueString = result2.GetIssues().ToString();
            TString expected =
R"___(<main>: Error: Transaction not found: , code: 2015
)___";
            UNIT_ASSERT_NO_DIFF(issueString, expected);
        }
    }

    Y_UNIT_TEST(TestDoubleKey) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
            CREATE TABLE `Root/Test` (
                Key Double,
                Value String,
                PRIMARY KEY (Key)
            );
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);

        auto ref = R"___(<main>: Error: Execution, code: 1060
    <main>:5:30: Error: Executing CREATE TABLE
        <main>: Error: Column Key has wrong key type Double, code: 2003
)___";
        UNIT_ASSERT_NO_DIFF(result.GetIssues().ToString(), ref);
    }

    Y_UNIT_TEST(TestBusySession) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();
        TVector<NYdb::NTable::TAsyncDataQueryResult> futures;

        for (ui32 i = 0; i < 10; ++i) {
            auto query = session.ExecuteDataQuery(R"___(
                SELECT 1;
            )___", TTxControl::BeginTx().CommitTx());
            futures.push_back(query);
        }

        for (auto& future : futures) {
            auto result = future.ExtractValueSync();
            if (result.GetStatus() != EStatus::SUCCESS) {
                UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SESSION_BUSY);
            }
        }
    }

    Y_UNIT_TEST(TestMultipleModifications) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;
        const ui32 sessionsCount = 10;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        TVector<TSession> sessions;
        for (ui32 i = 0; i < sessionsCount; ++i) {
            sessions.push_back(client.CreateSession().ExtractValueSync().GetSession());
        }

        TVector<NYdb::TAsyncStatus> futures;
        for (ui32 i = 0; i < sessionsCount; ++i) {
            auto query = sessions[i].ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Int,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___");
            futures.push_back(query);
        }

        for (auto& future : futures) {
            auto result = future.ExtractValueSync();
            if (result.GetStatus() != EStatus::SUCCESS) {
                UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::OVERLOADED);
            }
        }

        // Make sure table exists
        auto result = sessions[0].ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Value) VALUES (1, "One");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(TestYqlLongSessionPrepareError) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Uint32,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        auto result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Value) VALUES(1u, "One");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        Cerr << result.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto prepareResult = session.PrepareDataQuery(R"___(
            SELECT * FROM `Root/BadTable`;
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(prepareResult.GetStatus(), EStatus::SCHEME_ERROR);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Value) VALUES(2u, "Two");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            SELECT * FROM `Root/Test`;
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        TResultSet resultSet = result.GetResultSet(0);
        TResultSetParser rsParser(resultSet);
        int c = 0;
        while (rsParser.TryNextRow()) {
            c++;
        }

        UNIT_ASSERT_VALUES_EQUAL(c, 2);
    }

    Y_UNIT_TEST(TestYqlLongSessionMultipleErrors) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
               CREATE TABLE `Root/Test` (
                   Key Uint32,
                   Value String,
                   PRIMARY KEY (Key)
               );
           )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/BadTable1` (Key, Value) VALUES(1u, "One");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/BadTable2` (Key, Value) VALUES(2u, "Two");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW())).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(TestYqlTypesFromPreparedQuery) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.PrepareDataQuery(R"___(
            DECLARE $paramName AS String;
            SELECT $paramName;
            )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto query = result.GetQuery();
        auto paramsBuilder = query.GetParamsBuilder();
        paramsBuilder.AddParam("$paramName").String("someString").Build();
        auto params = paramsBuilder.Build();
        {
            auto result = query.Execute(TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(),
                params).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto resultSets = result.GetResultSets();
            UNIT_ASSERT_EQUAL(resultSets.size(), 1);
            auto& resultSet = resultSets[0];
            UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 1);
            auto meta = resultSet.GetColumnsMeta();
            UNIT_ASSERT_EQUAL(meta.size(), 1);
            TTypeParser parser(meta[0].Type);
            UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT(parser.GetPrimitive() == EPrimitiveType::String);

            TResultSetParser rsParser(resultSet);
            while (rsParser.TryNextRow()) {
                UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetString(), "someString");
            }
        }
        // Test params is not destructed during previous execution
        {
            auto result = query.Execute(TTxControl::BeginTx(TTxSettings::OnlineRO()).CommitTx(),
                params).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto resultSets = result.GetResultSets();
            UNIT_ASSERT_EQUAL(resultSets.size(), 1);
            auto& resultSet = resultSets[0];
            UNIT_ASSERT_EQUAL(resultSet.ColumnsCount(), 1);
            auto meta = resultSet.GetColumnsMeta();
            UNIT_ASSERT_EQUAL(meta.size(), 1);
            TTypeParser parser(meta[0].Type);
            UNIT_ASSERT(parser.GetKind() == TTypeParser::ETypeKind::Primitive);
            UNIT_ASSERT(parser.GetPrimitive() == EPrimitiveType::String);

            TResultSetParser rsParser(resultSet);
            while (rsParser.TryNextRow()) {
                UNIT_ASSERT_EQUAL(rsParser.ColumnParser(0).GetString(), "someString");
            }
        }
    }

    Y_UNIT_TEST(TestConstraintViolation) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
            CREATE TABLE `Root/Test` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            INSERT INTO `Root/Test` (Key, Value) VALUES (1u, "One");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            INSERT INTO `Root/Test` (Key, Value) VALUES (1u, "Two");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(TestReadTableOneBatch) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
            CREATE TABLE `Root/Test` (
                Key Uint64,
                Value String,
                SomeJson Json,
                SomeYson Yson,
                PRIMARY KEY (Key)
            );
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Value, SomeJson, SomeYson) VALUES (1u, "One", "[1]", "[1]");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        {
            auto selectResult = session.ExecuteDataQuery(R"(
                SELECT Key, Value, SomeJson, SomeYson FROM `Root/Test`;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(selectResult.GetStatus(), EStatus::SUCCESS);
            auto text = FormatResultSetYson(selectResult.GetResultSet(0));
            UNIT_ASSERT_VALUES_EQUAL("[[[1u];[\"One\"];[\"[1]\"];[\"[1]\"]]]", text);
        }

        {
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .Uint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Inclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            TValueBuilder valueTo;
            valueTo.BeginTuple()
                .AddElement()
                    .Uint64(1000)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .To(TKeyBound::Inclusive(valueTo.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);
        }

        {
            auto it = session.ReadTable("Root/Test").ExtractValueSync();
            bool read = false;
            while (true) {
                TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
                if (streamPart.EOS()) {
                    break;
                }
                UNIT_ASSERT_VALUES_EQUAL(streamPart.IsSuccess(), true);

                auto rsParser = TResultSetParser(streamPart.ExtractPart());

                while (rsParser.TryNextRow()) {
                    auto columnParser1 = std::move(rsParser.ColumnParser(0));
                    columnParser1.OpenOptional();
                    auto key = columnParser1.GetUint64();
                    UNIT_ASSERT_VALUES_EQUAL(key, 1);

                    auto& columnParser2 = rsParser.ColumnParser(1);
                    columnParser2.OpenOptional();
                    auto val = columnParser2.GetString();
                    UNIT_ASSERT_VALUES_EQUAL(val, "One");

                    auto& columnParser3 = rsParser.ColumnParser(2);
                    columnParser3.OpenOptional();
                    auto json = columnParser3.GetJson();
                    UNIT_ASSERT_VALUES_EQUAL(json, "[1]");

                    auto& columnParser4 = rsParser.ColumnParser(3);
                    columnParser4.OpenOptional();
                    auto yson = columnParser4.GetYson();
                    UNIT_ASSERT_VALUES_EQUAL(yson, "[1]");

                    read = true;
                }
            }
            UNIT_ASSERT(read);

            // Attempt to call ReadNext on finished iterator causes ContractViolation
            UNIT_ASSERT_EXCEPTION(it.ReadNext().GetValueSync().EOS(), NYdb::TContractViolation);
        }
    }

    Y_UNIT_TEST(TestReadTableNotNullBorder) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
            CREATE TABLE `Root/Test` (
                Key Uint64 NOT NULL,
                Value String,
                Amount Decimal(22,9) NOT NULL,
                PRIMARY KEY (Key)
            );
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Value, Amount) VALUES (0u, "Zero", UNWRAP(CAST("0.11" AS Decimal(22, 9))));
            UPSERT INTO `Root/Test` (Key, Value, Amount) VALUES (1u, "One", UNWRAP(CAST("1.11" AS Decimal(22, 9))));
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        {
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .Uint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Inclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[[[1u];[\"One\"];[\"1.11\"]]]");
        }

        {
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .Uint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .ReturnNotNullAsOptional(false)
                .From(TKeyBound::Inclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[[1u;[\"One\"];\"1.11\"]]");
        }

        {
            // Allow to use Optional values for NOT NULL columns
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .OptionalUint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Inclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), EStatus::SUCCESS, streamPart.GetIssues().ToString());

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[[[1u];[\"One\"];[\"1.11\"]]]");
        }

        {
            // Allow to use Optional values for NOT NULL columns
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .OptionalUint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Inclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), EStatus::SUCCESS, streamPart.GetIssues().ToString());

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[[[1u];[\"One\"];[\"1.11\"]]]");
        }

        {
            // Allow to use Optional values for NOT NULL columns
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .OptionalUint64(Nothing())
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Inclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), EStatus::SUCCESS, streamPart.GetIssues().ToString());

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[[[0u];[\"Zero\"];[\"0.11\"]];[[1u];[\"One\"];[\"1.11\"]]]");
        }

        {
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .Uint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Exclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[]");
        }

        {
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .Uint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .ReturnNotNullAsOptional(false)
                .From(TKeyBound::Exclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[]");
        }
    }

    // Same but use reverce order of column in tuple
    Y_UNIT_TEST(TestReadTableNotNullBorder2) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
            CREATE TABLE `Root/Test` (
                aaa String,
                zzz Uint64 NOT NULL,
                PRIMARY KEY (zzz)
            );
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (zzz, aaa) VALUES (1u, "One");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        {
            auto selectResult = session.ExecuteDataQuery(R"(
                SELECT zzz, aaa FROM `Root/Test`;
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(selectResult.GetStatus(), EStatus::SUCCESS);
            auto text = FormatResultSetYson(selectResult.GetResultSet(0));
            UNIT_ASSERT_VALUES_EQUAL("[[1u;[\"One\"]]]", text);
        }

        {
            TValueBuilder valueFrom;
            valueFrom.BeginTuple()
                .AddElement()
                    .Uint64(1)
                .EndTuple();

            auto settings = TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Inclusive(valueFrom.Build()));

            auto it = session.ReadTable("Root/Test", settings).ExtractValueSync();

            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SUCCESS);

            auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());
            UNIT_ASSERT_VALUES_EQUAL(str, "[[[\"One\"];[1u]]]");
        }
    }

    enum class EReadTableMultiShardMode {
        Normal,
        UseSnapshot,
    };

    void TestReadTableMultiShard(EReadTableMultiShardMode mode, bool wholeTable) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto tableBuilder = client.GetTableBuilder();
        tableBuilder
            .AddNullableColumn("Key", EPrimitiveType::Uint32)
            .AddNullableColumn("Fk", EPrimitiveType::Uint64)
            .AddNullableColumn("Value", EPrimitiveType::String);
        tableBuilder.SetPrimaryKeyColumns(TVector<TString>{"Key", "Fk"});

        TCreateTableSettings createTableSettings =
            TCreateTableSettings()
                .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(16));

        auto result = session.CreateTable("Root/Test", tableBuilder.Build(), createTableSettings).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Fk, Value) VALUES
            (1u, 1u, "One"),
            (1000000000u, 2u, "Two"),
            (4294967295u, 4u, "Last");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        TValueBuilder valueFrom;
        valueFrom.BeginTuple()
            .AddElement()
                .OptionalUint32(1)
            .EndTuple();

        TValueBuilder valueTo;
        valueTo.BeginTuple()
            .AddElement()
                .OptionalUint32(1000000000u)
            .AddElement()
                .OptionalUint64(2000000000u)
            .EndTuple();

        TReadTableSettings readTableSettings =
            wholeTable ? TReadTableSettings().Ordered() :
            TReadTableSettings()
                .Ordered()
                .From(TKeyBound::Inclusive(valueFrom.Build()))
                .To(TKeyBound::Inclusive(valueTo.Build()));

        switch (mode) {
            case EReadTableMultiShardMode::Normal:
                break;
            case EReadTableMultiShardMode::UseSnapshot:
                readTableSettings.UseSnapshot(true);
                break;
        }

        auto it = session.ReadTable("Root/Test", readTableSettings).ExtractValueSync();

        struct TRows {
            ui32 Key;
            ui64 Fk;
            TString Value;
        };
        TVector<TRows> expected;
        expected.push_back({1u, 1u, "One"});
        expected.push_back({1000000000u, 2u, "Two"});
        if (wholeTable) {
            expected.push_back({4294967295u, 4u, "Last"});
        }
        int row = 0;
        while (true) {
            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();

            if (streamPart.EOS()) {
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(streamPart.IsSuccess(), true);

            auto rsParser = TResultSetParser(streamPart.ExtractPart());

            while (rsParser.TryNextRow()) {
                const TRows& exp = expected[row++];

                rsParser.ColumnParser(0).OpenOptional();
                auto key = rsParser.ColumnParser(0).GetUint32();
                UNIT_ASSERT_VALUES_EQUAL(key, exp.Key);

                rsParser.ColumnParser(1).OpenOptional();
                auto key2 = rsParser.ColumnParser(1).GetUint64();
                UNIT_ASSERT_VALUES_EQUAL(key2, exp.Fk);

                rsParser.ColumnParser(2).OpenOptional();
                auto val = rsParser.ColumnParser(2).GetString();
                UNIT_ASSERT_VALUES_EQUAL(val, exp.Value);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(row, wholeTable ? 3 : 2);

        // Attempt to call ReadNext on finished iterator causes ContractViolation
        UNIT_ASSERT_EXCEPTION(it.ReadNext().GetValueSync().EOS(), NYdb::TContractViolation);
    }

    Y_UNIT_TEST(TestReadTableMultiShard) {
        TestReadTableMultiShard(EReadTableMultiShardMode::Normal, false);
    }

    Y_UNIT_TEST(TestReadTableMultiShardUseSnapshot) {
        TestReadTableMultiShard(EReadTableMultiShardMode::UseSnapshot, false);
    }

    Y_UNIT_TEST(TestReadTableMultiShardWholeTable) {
        TestReadTableMultiShard(EReadTableMultiShardMode::Normal, true);
    }

    Y_UNIT_TEST(TestReadTableMultiShardWholeTableUseSnapshot) {
        TestReadTableMultiShard(EReadTableMultiShardMode::UseSnapshot, true);
    }

    void TestReadTableMultiShardWithDescribe(bool rowLimit) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto tableBuilder = client.GetTableBuilder();
        tableBuilder
            .AddNullableColumn("Key", EPrimitiveType::Uint32)
            .AddNullableColumn("Key2", EPrimitiveType::Uint32)
            .AddNullableColumn("Value", EPrimitiveType::String);
        tableBuilder.SetPrimaryKeyColumns(TVector<TString>{"Key", "Key2"});

        TCreateTableSettings createTableSettings =
            TCreateTableSettings()
                .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(10));

        auto result = session.CreateTable("Root/Test", tableBuilder.Build(), createTableSettings).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(R"___(
            UPSERT INTO `Root/Test` (Key, Key2, Value) VALUES
            (1u,          2u,          "A"),
            (429496730u,  20000u,      "B"),
            (858993459u,  20000u,      "C"),
            (1288490188u, 20000u,      "D"),
            (3865470565u, 200000000u,  "E"),
            (3865470565u, 200000001u,  "F");
        )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        TDescribeTableSettings describeTableSettings =
            TDescribeTableSettings()
                .WithKeyShardBoundary(true);

        TDescribeTableResult describeResult = session.DescribeTable("Root/Test", describeTableSettings)
            .GetValueSync();
        UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);

        TVector<TString> expected;
        expected.push_back(R"___([[1u];[2u];["A"]])___");
        expected.push_back(R"___([[429496730u];[20000u];["B"]])___");
        expected.push_back(R"___([[858993459u];[20000u];["C"]])___");
        expected.push_back(R"___([[1288490188u];[20000u];["D"]])___");
        expected.push_back(R"___([[3865470565u];[200000000u];["E"]])___");
        expected.push_back(R"___([[3865470565u];[200000001u];["F"]])___");

        int row = 0;
        for (const auto& range : describeResult.GetTableDescription().GetKeyRanges()) {
            TReadTableSettings readTableSettings;
            if (auto from = range.From()) {
                readTableSettings.From(from.GetRef());
            }
            if (auto to = range.To()) {
                readTableSettings.To(to.GetRef());
            }
            if (rowLimit) {
                readTableSettings.RowLimit(1);
            }

            auto it = session.ReadTable("Root/Test", readTableSettings).ExtractValueSync();

            while (true) {
                TReadTableResultPart streamPart = it.ReadNext().GetValueSync();

                if (streamPart.EOS()) {
                    break;
                }

                UNIT_ASSERT_VALUES_EQUAL(streamPart.IsSuccess(), true);

                int readRows = 0;
                auto rsParser = TResultSetParser(streamPart.ExtractPart());
                while (rsParser.TryNextRow()) {
                    auto columns = rsParser.ColumnsCount();
                    const auto& expRow = expected[row++];
                    ++readRows;
                    TString tmp = "[";
                    for (size_t c = 0; c < columns; c++) {
                        auto colYson = FormatValueYson(rsParser.GetValue(c));
                        tmp += colYson;
                        if (c != columns - 1)
                            tmp += ";";
                    }
                    tmp += "]";
                    UNIT_ASSERT_VALUES_EQUAL(tmp, expRow);
                }
                if (rowLimit) {
                    UNIT_ASSERT(readRows <= 1);
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(row, rowLimit ? 5 : expected.size());
    }

    Y_UNIT_TEST(TestReadTableMultiShardWithDescribe) {
        TestReadTableMultiShardWithDescribe(false);
    }

    Y_UNIT_TEST(TestReadTableMultiShardWithDescribeAndRowLimit) {
        TestReadTableMultiShardWithDescribe(true);
    }


    void TestReadTable(TSession& session, int rowsTotalCount, int batchLimitBytes, int batchLimitRows) {
        int row = 0;
        TReadTableSettings readTableSettings;
        readTableSettings.Ordered(true);
        readTableSettings.BatchLimitBytes(batchLimitBytes);
        readTableSettings.BatchLimitRows(batchLimitRows);

        auto it = session.ReadTable("Root/Test", readTableSettings).ExtractValueSync();

        TStringStream out;
        while (true) {
            TReadTableResultPart streamPart = it.ReadNext().GetValueSync();

            if (streamPart.EOS()) {
                break;
            }

            UNIT_ASSERT_VALUES_EQUAL(streamPart.IsSuccess(), true);

            auto rsParser = TResultSetParser(streamPart.ExtractPart());
            i64 batchRows = 0;
            out << "---- batch start ----" << Endl;
            while (rsParser.TryNextRow()) {
                auto columns = rsParser.ColumnsCount();
                ++row;
                ++batchRows;
                TString tmp = "[";
                for (size_t c = 0; c < columns; c++) {
                    auto colYson = FormatValueYson(rsParser.GetValue(c));
                    tmp += colYson;
                    if (c != columns - 1)
                        tmp += ";";
                }
                tmp += "]";
                out << tmp << Endl;
            }
            out << "---- batch end ----" << Endl;
            UNIT_ASSERT(!batchLimitRows || batchRows <= batchLimitRows);
        }
        Cerr << out.Str();
        UNIT_ASSERT_VALUES_EQUAL(row, rowsTotalCount);
    }

    Y_UNIT_TEST(TestReadTableBatchLimits) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto tableBuilder = client.GetTableBuilder();
        tableBuilder
            .AddNullableColumn("Key", EPrimitiveType::Uint32)
            .AddNullableColumn("Key2", EPrimitiveType::Uint32)
            .AddNullableColumn("Value", EPrimitiveType::String);
        tableBuilder.SetPrimaryKeyColumns(TVector<TString>{"Key", "Key2"});

        TCreateTableSettings createTableSettings =
            TCreateTableSettings()
                .PartitioningPolicy(TPartitioningPolicy().UniformPartitions(10));

        auto result = session.CreateTable("Root/Test", tableBuilder.Build(), createTableSettings).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        int rowsTotalCount = 100;
        TStringStream query;
        query << R"_(  UPSERT INTO `Root/Test` (Key, Key2, Value) VALUES )_";
        for (int i = 0; i < rowsTotalCount; ++i) {
            query << Sprintf(R"_( (%d, %d, "A")%s)_", i, 2 * i, i + 1 < rowsTotalCount ? "," : ";");
        }
        result = session.ExecuteDataQuery(query.Str(),
            TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        // all limits disabled
        TestReadTable(session, rowsTotalCount, 0, 0);
        for (int i = 1; i <= rowsTotalCount; i *= 2) {
            // test BatchLimitRows
            TestReadTable(session, rowsTotalCount, 0, i);
        }
        // test BatchLimitBytes == 1 returns not more than one row
        TestReadTable(session, rowsTotalCount, 1, 1);
    }

    Y_UNIT_TEST(TestReadWrongTable) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto it = session.ReadTable("Root/NoTable").ExtractValueSync();

        // SUCCESS because stream is connected
        // TODO: probably we need to change protocol to make one preventive read
        // to get real status
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);
        TReadTableResultPart streamPart = it.ReadNext().GetValueSync();

        Cerr << streamPart.GetIssues().ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(streamPart.IsSuccess(), false);
        UNIT_ASSERT_VALUES_EQUAL(streamPart.GetStatus(), EStatus::SCHEME_ERROR);

        UNIT_ASSERT_VALUES_EQUAL(it.ReadNext().GetValueSync().EOS(), true);
    }

    Y_UNIT_TEST(TestReadTableSnapshot) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_TRACE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::READ_TABLE_API, NLog::PRI_TRACE);
        ui16 grpc = server.GetPort();

        TString location = TStringBuilder() << "localhost:" << grpc;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        NYdb::NTable::TTableClient client(connection);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
            CREATE TABLE `/Root/EmptyTable` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto it = session.ReadTable("/Root/EmptyTable").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), EStatus::SUCCESS);

        // We expect at least one part that also specifies a snapshot
        TReadTableResultPart streamPart = it.ReadNext().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(streamPart.IsSuccess(), true);
        UNIT_ASSERT_VALUES_EQUAL(bool(streamPart.GetSnapshot()), true);
        UNIT_ASSERT_GT(streamPart.GetSnapshot()->GetStep(), 0u);
        UNIT_ASSERT_GT(streamPart.GetSnapshot()->GetTxId(), 0u);

        TResultSetParser parser(streamPart.GetPart());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnsCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnIndex("Key"), 0);
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnIndex("Value"), 1);
        UNIT_ASSERT(!parser.TryNextRow());

        TReadTableResultPart lastPart = it.ReadNext().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(lastPart.IsSuccess(), false);
        UNIT_ASSERT_VALUES_EQUAL(lastPart.EOS(), true);
    }

    Y_UNIT_TEST(RetryOperationTemplate) {
        TKikimrWithGrpcAndRootSchema server;
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);

        auto createFuture = client.RetryOperation([](TSession session) {
            return session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___");
        });

        auto upsertFuture = createFuture.Apply([client](const TAsyncStatus& asyncStatus) mutable {
            auto status = asyncStatus.GetValue();
            if (!status.IsSuccess()) {
                return NThreading::MakeFuture(status);
            }

            return client.RetryOperation<TDataQueryResult>([](TSession session) {
                return session.ExecuteDataQuery(R"___(
                    UPSERT INTO `Root/Test` (Key, Value) VALUES (1u, "One");
                )___", TTxControl::BeginTx().CommitTx());
            }, TRetryOperationSettings().MaxRetries(0));
        });

        auto upsertStatus = upsertFuture.GetValueSync();
        UNIT_ASSERT(upsertStatus.IsSuccess());

        TMaybe<TResultSet> selectResult;
        auto selectStatus = client.RetryOperationSync([&selectResult] (TSession session) {
            auto result = session.ExecuteDataQuery(R"___(
                SELECT * FROM `Root/Test`;
            )___", TTxControl::BeginTx().CommitTx()).GetValueSync();

            if (result.IsSuccess()) {
                selectResult = result.GetResultSet(0);
            }

            return result;
        }, TRetryOperationSettings().MaxRetries(0));

        UNIT_ASSERT(selectStatus.IsSuccess());

        TResultSetParser parser(*selectResult);
        UNIT_ASSERT(parser.TryNextRow());
        driver.Stop(true);
    }

    void CheckRetryResult(const TStatus& status, const TVector<TResultSet>& resultSets, bool expectSuccess)
    {
        if (expectSuccess) {
            UNIT_ASSERT_C(status.IsSuccess(), status);
            UNIT_ASSERT_VALUES_EQUAL(resultSets.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSets[0].ColumnsCount(), 3);
        } else {
            UNIT_ASSERT_C(!status.IsSuccess(), status);
        }
    }

    void TestRetryOperationAsync(NYdb::NTable::TTableClient& client,
        const TVector<EStatus>& retriableStatuses, bool expectSuccess,
        TRetryOperationSettings settings = TRetryOperationSettings())
    {
        size_t retryNumber = 0;
        TVector<TResultSet> resultSets;
        auto operation = [&retryNumber, &resultSets, &retriableStatuses] (TSession session) -> TAsyncStatus {
            // iterate over all providen statuses and return TStatus to emulate error
            if (retryNumber < retriableStatuses.size()) {
                TStatus status(retriableStatuses[retryNumber++], {});
                return NThreading::MakeFuture<TStatus>(status);
            }
            auto queryStatus = session.ExecuteDataQuery(
                "SELECT 1, 2, 3", TTxControl::BeginTx().CommitTx()).GetValueSync();
            resultSets = queryStatus.GetResultSets();
            return NThreading::MakeFuture<TStatus>(queryStatus);
        };
        auto operationWithoutSession = [&operation] (TTableClient client) -> TAsyncStatus {
            auto session = client.CreateSession().GetValueSync().GetSession();
            return operation(session);
        };

        const auto retrySettings = settings.MaxRetries(retriableStatuses.size()).Verbose(true);
        auto result = client.RetryOperation(operation, retrySettings);
        CheckRetryResult(result.GetValueSync(), resultSets, expectSuccess);

        retryNumber = 0;
        auto resultWithoutSession = client.RetryOperation(operationWithoutSession, retrySettings);
        CheckRetryResult(resultWithoutSession.GetValueSync(), resultSets, expectSuccess);
    }

    void TestRetryOperationSync(NYdb::NTable::TTableClient& client,
        const TVector<EStatus>& retriableStatuses, bool expectSuccess,
        TRetryOperationSettings settings = TRetryOperationSettings())
    {
        size_t retryNumber = 0;
        TVector<TResultSet> resultSets;
        auto operation = [&retryNumber, &resultSets, &retriableStatuses] (TSession session) -> TStatus {
            // iterate over all providen statuses and return TStatus to emulate error
            if (retryNumber < retriableStatuses.size()) {
                TStatus status(retriableStatuses[retryNumber++], {});
                return status;
            }
            auto queryStatus = session.ExecuteDataQuery(
                "SELECT 1, 2, 3", TTxControl::BeginTx().CommitTx()).GetValueSync();
            resultSets = queryStatus.GetResultSets();
            return TStatus(queryStatus);
        };
        auto operationWithoutSession = [&operation] (TTableClient client) -> TStatus {
            auto session = client.CreateSession().GetValueSync().GetSession();
            return operation(session);
        };

        const auto retrySettings = settings.MaxRetries(retriableStatuses.size()).Verbose(true);
        auto result = client.RetryOperationSync(operation, retrySettings);
        CheckRetryResult(result, resultSets, expectSuccess);

        retryNumber = 0;
        auto resultWithoutSession = client.RetryOperationSync(operationWithoutSession, retrySettings);
        CheckRetryResult(resultWithoutSession, resultSets, expectSuccess);
    }

    TVector<EStatus> GetRetriableAlwaysStatuses() {
        return {EStatus::OVERLOADED, EStatus::CLIENT_RESOURCE_EXHAUSTED, EStatus::UNAVAILABLE,
            EStatus::BAD_SESSION, EStatus::SESSION_BUSY};
    }

    TVector<EStatus> GetRetriableOnOptionStatuses() {
        return {EStatus::NOT_FOUND, EStatus::UNDETERMINED, EStatus::TRANSPORT_UNAVAILABLE};
    }

    Y_UNIT_TEST(RetryOperationAsync) {
        TKikimrWithGrpcAndRootSchema server;
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);

        TestRetryOperationAsync(client, GetRetriableAlwaysStatuses(), true);
        TestRetryOperationAsync(client, {EStatus::NOT_FOUND}, true, TRetryOperationSettings().RetryNotFound(true));
        TestRetryOperationAsync(client, {EStatus::UNDETERMINED}, true, TRetryOperationSettings().Idempotent(true));
        TestRetryOperationAsync(client, {EStatus::TRANSPORT_UNAVAILABLE}, true, TRetryOperationSettings().Idempotent(true));
        TestRetryOperationAsync(client, {EStatus::NOT_FOUND}, false, TRetryOperationSettings().RetryNotFound(false));
        TestRetryOperationAsync(client, {EStatus::UNDETERMINED}, false, TRetryOperationSettings().Idempotent(false));
        TestRetryOperationAsync(client, {EStatus::TRANSPORT_UNAVAILABLE}, false, TRetryOperationSettings().Idempotent(false));

        driver.Stop(true);
    }

    Y_UNIT_TEST(RetryOperationSync) {
        TKikimrWithGrpcAndRootSchema server;
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);

        TestRetryOperationSync(client, GetRetriableAlwaysStatuses(), true);
        TestRetryOperationSync(client, {EStatus::NOT_FOUND}, true, TRetryOperationSettings().RetryNotFound(true));
        TestRetryOperationSync(client, {EStatus::UNDETERMINED}, true, TRetryOperationSettings().Idempotent(true));
        TestRetryOperationSync(client, {EStatus::TRANSPORT_UNAVAILABLE}, true, TRetryOperationSettings().Idempotent(true));
        TestRetryOperationSync(client, {EStatus::NOT_FOUND}, false, TRetryOperationSettings().RetryNotFound(false));
        TestRetryOperationSync(client, {EStatus::UNDETERMINED}, false, TRetryOperationSettings().Idempotent(false));
        TestRetryOperationSync(client, {EStatus::TRANSPORT_UNAVAILABLE}, false, TRetryOperationSettings().Idempotent(false));

        driver.Stop(true);
    }

    Y_UNIT_TEST(RetryOperationLimitedDuration) {
        TKikimrWithGrpcAndRootSchema server;
        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);

        const size_t MaxRetries = 3; // OVERLOADED error has slow backoff policy, 2 retries takes about 2 sec
        const double MaxDurationSec = 0.5; // Less than 2 sec, should be enough to fail on retry with duration limit
        const auto retrySettings = TRetryOperationSettings().MaxRetries(MaxRetries).Verbose(true);
        const auto retrySettingsLimited = TRetryOperationSettings(retrySettings).MaxTimeout(TDuration::Seconds(MaxDurationSec));
        size_t retryNumber = 0;

        // Asynchronous version
        auto operation = [&retryNumber] (TSession /*session*/) -> TAsyncStatus {
            if (retryNumber++ < MaxRetries) {
                return NThreading::MakeFuture(TStatus(EStatus::OVERLOADED, {}));
            }
            return NThreading::MakeFuture(TStatus(EStatus::SUCCESS, {}));
        };

        retryNumber = 0;
        UNIT_ASSERT(client.RetryOperation(operation, retrySettings).GetValueSync().IsSuccess());

        retryNumber = 0;
        UNIT_ASSERT(!client.RetryOperation(operation, retrySettingsLimited).GetValueSync().IsSuccess());

        // Synchronous version
        auto operationSync = [&retryNumber] (TSession /*session*/) -> TStatus {
            if (retryNumber++ < MaxRetries) {
                return TStatus(EStatus::OVERLOADED, {});
            }
            return TStatus(EStatus::SUCCESS, {});
        };

        retryNumber = 0;
        UNIT_ASSERT(client.RetryOperationSync(operationSync, retrySettings).IsSuccess());

        retryNumber = 0;
        UNIT_ASSERT(!client.RetryOperationSync(operationSync, retrySettingsLimited).IsSuccess());

        driver.Stop(true);
    }

    Y_UNIT_TEST(QueryLimits) {
        NKikimrConfig::TAppConfig appConfig;
        auto& tableServiceConfig = *appConfig.MutableTableServiceConfig();
        tableServiceConfig.SetQueryLimitBytes(200);
        tableServiceConfig.SetParametersLimitBytes(100);

        TKikimrWithGrpcAndRootSchema server(appConfig);

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);
        auto session = client.CreateSession().ExtractValueSync().GetSession();

        auto result = session.ExecuteSchemeQuery(R"___(
            CREATE TABLE `Root/Test` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )___").ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(Sprintf(R"___(
            UPSERT INTO `Root/Test` (Key, Value) VALUES (1u, "%s");
        )___", TString(100, '*').c_str()), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        result = session.ExecuteDataQuery(Sprintf(R"___(
            UPSERT INTO `Root/Test` (Key, Value) VALUES (1u, "%s");
        )___", TString(200, '*').c_str()), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);

        auto params = client.GetParamsBuilder()
            .AddParam("$value").String(TString(50, '*'))
                .Build()
            .Build();

        result = session.ExecuteDataQuery(R"___(
            DECLARE $value AS String;
            UPSERT INTO `Root/Test` (Key, Value) VALUES (1u, $value);
        )___", TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        params = client.GetParamsBuilder()
            .AddParam("$value").String(TString(100, '*'))
                .Build()
            .Build();

        result = session.ExecuteDataQuery(R"___(
            DECLARE $value AS String;
            UPSERT INTO `Root/Test` (Key, Value) VALUES (1u, $value);
        )___", TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(DeleteTableWithDeletedIndex) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(
            TDriverConfig()
                .SetEndpoint(
                    TStringBuilder() << "localhost:" << server.GetPort())
        );

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();

        {

            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .AddNullableColumn("uid", EPrimitiveType::Uint64)
                .SetPrimaryKeyColumn("key")
                .AddSecondaryIndex("uid", "uid");

            auto desc = builder.Build();

            auto result = session.CreateTable("Root/Test",
                std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropIndexes({"uid"});

            auto result = session.AlterTable("/Root/Test", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = session.DropTable("Root/Test").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterTableAddIndex) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(
            TDriverConfig()
                .SetEndpoint(
                    TStringBuilder() << "localhost:" << server.GetPort())
        );

        {
            NYdb::NOperation::TOperationClient operationClient(driver);
            auto result = operationClient.List<NYdb::NTable::TBuildIndexOperation>().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0); // No operations in progress
        }

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            result = session.ExecuteDataQuery(R"___(
                UPSERT INTO `Root/Test` (Key, Value) VALUES (1u, "One");
            )___", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("", {"Value"})});

            auto result = session.AlterTable("/Root/Test", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("SomeName", TVector<TString>())});

            auto result = session.AlterTable("/Root/Test", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("NewIndex", {"Value"})});

            auto result = session.AlterTable("", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
        }

        {

            NYdb::NTable::TClientSettings clientSettings;
            clientSettings.AuthToken("badguy@builtin");
            NYdb::NTable::TTableClient clientbad(driver, clientSettings);
            auto getSessionResult = clientbad.CreateSession().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
            auto session = getSessionResult.GetSession();
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("NewIndex", {"Value"})});

            auto result = session.AlterTable("/Root/Test", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNAUTHORIZED, result.GetIssues().ToString());

        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("NewIndex", {"Value"})});

            auto result = session.AlterTable("/Root/Test", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("NewIndex", {"Value"})});

            auto result = session.AlterTable("/Root/WrongPath", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
        }

        {
            NYdb::NOperation::TOperationClient operationClient(driver);
            auto result = operationClient.List<NYdb::NTable::TBuildIndexOperation>().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 1);
            auto op = result.GetList()[0];
            UNIT_ASSERT_VALUES_EQUAL(op.Ready(), true);
            UNIT_ASSERT_VALUES_EQUAL(op.Status().GetStatus(), EStatus::SUCCESS);
            auto meta = op.Metadata();
            UNIT_ASSERT_VALUES_EQUAL(meta.State, NYdb::NTable::EBuildIndexState::Done);
            UNIT_ASSERT_DOUBLES_EQUAL(meta.Progress, 100, 0.001);

            UNIT_ASSERT_VALUES_EQUAL(meta.Path, "/Root/Test");
            UNIT_ASSERT_VALUES_EQUAL(meta.Desctiption.GetRef().GetIndexName(), "NewIndex");
            UNIT_ASSERT_VALUES_EQUAL(meta.Desctiption.GetRef().GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(meta.Desctiption.GetRef().GetIndexColumns()[0], "Value");


            auto result2 = operationClient.Get<NYdb::NTable::TBuildIndexOperation>(result.GetList()[0].Id()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result2.Status().GetStatus(), EStatus::SUCCESS, result2.Status().GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result2.Metadata().State, NYdb::NTable::EBuildIndexState::Done);
            UNIT_ASSERT_DOUBLES_EQUAL(result2.Metadata().Progress, 100, 0.001);

            UNIT_ASSERT_VALUES_EQUAL(result2.Metadata().Path, "/Root/Test");
            UNIT_ASSERT_VALUES_EQUAL(result2.Metadata().Desctiption.GetRef().GetIndexName(), "NewIndex");
            UNIT_ASSERT_VALUES_EQUAL(result2.Metadata().Desctiption.GetRef().GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result2.Metadata().Desctiption.GetRef().GetIndexColumns()[0], "Value");

            {
                // Cancel already finished operation do nothing
                auto result3 = operationClient.Cancel(result.GetList()[0].Id()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result3.GetStatus(), EStatus::PRECONDITION_FAILED, result3.GetIssues().ToString());
            }

            {
                auto result3 = operationClient.Forget(result.GetList()[0].Id()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result3.GetStatus(), EStatus::SUCCESS, result3.GetIssues().ToString());
            }

            {
                auto result3 = operationClient.Get<NYdb::NTable::TBuildIndexOperation>(result.GetList()[0].Id()).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result3.Status().GetStatus(), EStatus::PRECONDITION_FAILED, result3.Status().GetIssues().ToString());
            }
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropIndexes({"NewIndex"});

            auto result = session.AlterTable("/Root/Test", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            TDescribeTableResult describeResult = session.DescribeTable("Root/Test")
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetIndexDescriptions().size(), 0);
        }
    }

    Y_UNIT_TEST(AlterTableAddIndexAsyncOp) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(
            TDriverConfig()
                .SetEndpoint(
                    TStringBuilder() << "localhost:" << server.GetPort())
        );

        {
            NYdb::NOperation::TOperationClient operationClient(driver);
            auto result = operationClient.List<NYdb::NTable::TBuildIndexOperation>().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0); // No operations in progress
        }

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            result = session.ExecuteDataQuery(R"___(
                UPSERT INTO `Root/Test` (Key, Value) VALUES (1u, "One");
            )___", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("NewIndex", {"Value"})});

            // Check that forget_after is not supported
            {
                auto settingsWithForgetAfter = settings;
                settingsWithForgetAfter.ForgetAfter(TDuration::Hours(1));
                auto result = session.AlterTableLong("/Root/Test", settingsWithForgetAfter).ExtractValueSync();
                UNIT_ASSERT(!result.Status().IsSuccess());
                UNIT_ASSERT_C(result.Ready(), result.Status().GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL_C(result.Status().GetStatus(), NYdb::EStatus::UNSUPPORTED, result.Status().GetIssues().ToString());
            }

            auto result = session.AlterTableLong("/Root/Test", settings).ExtractValueSync();

            // Build index is async operation
            UNIT_ASSERT_C(!result.Ready(), result.Status().GetIssues().ToString());

            NYdb::NOperation::TOperationClient operationClient(driver);

            for (;;) {
                auto getResult = operationClient.Get<NYdb::NTable::TBuildIndexOperation>(result.Id()).GetValueSync();
                if (getResult.Ready()) {
                    UNIT_ASSERT_VALUES_EQUAL_C(getResult.Status().GetStatus(), EStatus::SUCCESS, getResult.Status().GetIssues().ToString());
                    break;
                } else {
                    Sleep(TDuration::MilliSeconds(100));
                }
            }
        }

        // Add column in to table with index
        {
            auto type = TTypeBuilder().BeginOptional().Primitive(EPrimitiveType::Uint64).EndOptional().Build();
            auto alter = TAlterTableSettings().AppendAddColumns(TColumn("NewColumn", type));

            auto result = session.AlterTableLong("/Root/Test", alter).ExtractValueSync();

            // Build index is async operation
            UNIT_ASSERT(result.Ready());

            UNIT_ASSERT_VALUES_EQUAL_C(result.Status().GetStatus(), EStatus::SUCCESS, result.Status().GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterTableAddIndexWithDataColumn) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(
            TDriverConfig()
                .SetEndpoint(
                    TStringBuilder() << "localhost:" << server.GetPort())
        );

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();

        {
            auto result = session.ExecuteSchemeQuery(R"___(
                CREATE TABLE `Root/Test` (
                    Key Uint64,
                    Fk Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )___").ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            result = session.ExecuteDataQuery(R"___(
                UPSERT INTO `Root/Test` (Key, Fk, Value) VALUES (1u, 111u, "One");
            )___", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendAddIndexes({TIndexDescription("NewIndex", {"Fk"}, {"Value"})});

            auto result = session.AlterTable("/Root/Test", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto res = session.DescribeTable("Root/Test").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 3);
            const auto& indexDesc = res.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.size(), 1);
            const auto& index = indexDesc[0];
            UNIT_ASSERT_VALUES_EQUAL(index.GetIndexName(), "NewIndex");
            UNIT_ASSERT_VALUES_EQUAL(index.GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(index.GetIndexColumns()[0], "Fk");
            UNIT_ASSERT_VALUES_EQUAL(index.GetDataColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(index.GetDataColumns()[0], "Value");
        }

        auto it = session.ReadTable("/Root/Test/NewIndex/indexImplTable").ExtractValueSync();
        TReadTableResultPart streamPart = it.ReadNext().GetValueSync();
        auto str = NYdb::FormatResultSetYson(streamPart.ExtractPart());

        UNIT_ASSERT_VALUES_EQUAL(str, "[[[111u];[1u];[\"One\"]]]");
    }



    Y_UNIT_TEST(QueryStats) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));
        NYdb::NTable::TTableClient client(driver);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        const ui32 SHARD_COUNT = 4;

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto tableSettings = NYdb::NTable::TCreateTableSettings().PartitioningPolicy(
                NYdb::NTable::TPartitioningPolicy().UniformPartitions(SHARD_COUNT));

            auto result = session.CreateTable("/Root/Foo", tableBuilder.Build(), tableSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        for (bool returnStats : {false, true}) {
            NYdb::NTable::TExecDataQuerySettings execSettings;
            if (returnStats) {
                execSettings.CollectQueryStats(ECollectQueryStatsMode::Profile);
            }
            {
                auto query = "UPSERT INTO `/Root/Foo` (Key, Value) VALUES (0, 'aa');";
                auto result = session.ExecuteDataQuery(
                            query,
                            TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

                if (!returnStats) {
                    UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), false);
                } else {
                    // Cerr << "\nQUERY: " << query << "\nSTATS:\n" << result.GetStats()->ToString() << Endl;
                    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                    if (stats.query_phases().size() == 1) {
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Foo");
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).updates().rows(), 1);
                        UNIT_ASSERT(stats.query_phases(0).table_access(0).updates().bytes() > 1);
                        UNIT_ASSERT(stats.query_phases(0).cpu_time_us() > 0);
                        UNIT_ASSERT(stats.total_duration_us() > 0);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/Foo");
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 1);
                        UNIT_ASSERT(stats.query_phases(1).table_access(0).updates().bytes() > 1);
                        UNIT_ASSERT(stats.query_phases(1).cpu_time_us() > 0);
                        UNIT_ASSERT(stats.total_duration_us() > 0);
                    }
                }
            }

            {
                auto query = "UPSERT INTO `/Root/Foo` (Key, Value) VALUES (1, Utf8('bb')), (0xffffffff, Utf8('cc'));";
                auto result = session.ExecuteDataQuery(
                            query,
                            TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

                if (!returnStats) {
                    UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), false);
                } else {
                    // Cerr << "\nQUERY: " << query << "\nSTATS:\n" << result.GetStats()->ToString() << Endl;
                    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access().size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).name(), "/Root/Foo");
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(1).table_access(0).updates().rows(), 2);
                    UNIT_ASSERT(stats.query_phases(1).table_access(0).updates().bytes() > 1);
                    UNIT_ASSERT(stats.query_phases(1).cpu_time_us() > 0);
                    UNIT_ASSERT(stats.total_duration_us() > 0);
                }
            }

            {
                auto query = "SELECT * FROM `/Root/Foo`;";
                auto result = session.ExecuteDataQuery(
                            query,
                            TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

                if (!returnStats) {
                    UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), false);
                } else {
                    // Cerr << "\nQUERY: " << query << "\nSTATS:\n" << result.GetStats()->ToString() << Endl;
                    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Foo");
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 3);
                    UNIT_ASSERT(stats.query_phases(0).table_access(0).reads().bytes() > 3);
                    UNIT_ASSERT(stats.query_phases(0).cpu_time_us() > 0);
                    UNIT_ASSERT(stats.total_duration_us() > 0);
                }
            }

            {
                auto query = "SELECT * FROM `/Root/Foo` WHERE Key == 1;";
                auto result = session.ExecuteDataQuery(
                            query,
                            TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

                if (!returnStats) {
                    UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), false);
                } else {
                    // Cerr << "\nQUERY: " << query << "\nSTATS:\n" << result.GetStats()->ToString() << Endl;
                    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Foo");
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1);
                    UNIT_ASSERT(stats.query_phases(0).table_access(0).reads().bytes() > 1);
                    UNIT_ASSERT(stats.query_phases(0).cpu_time_us() > 0);
                    UNIT_ASSERT(stats.total_duration_us() > 0);
                }
            }

            {
                auto query = "DELETE FROM `/Root/Foo` WHERE Key > 0;";
                auto result = session.ExecuteDataQuery(
                            query,
                            TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();

                if (!returnStats) {
                    UNIT_ASSERT_VALUES_EQUAL(result.GetStats().Defined(), false);
                } else {
                    // Cerr << "\nQUERY: " << query << "\nSTATS:\n" << result.GetStats()->ToString() << Endl;
                    auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

                    int idx = 0;
                    if (stats.query_phases().size() == 2) {
                        idx = 0;
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
                        UNIT_ASSERT(stats.query_phases(0).table_access().empty());
                        idx = 1;
                    }

                    // 1st phase: find matching rows
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(idx).table_access().size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(idx).table_access(0).name(), "/Root/Foo");
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(idx).table_access(0).reads().rows(), 2);
                    UNIT_ASSERT(stats.query_phases(idx).cpu_time_us() > 0);
                    // 2nd phase: delete found rows
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(idx + 1).table_access().size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(idx + 1).table_access(0).name(), "/Root/Foo");
                    UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(idx + 1).table_access(0).deletes().rows(), 2);
                    UNIT_ASSERT(stats.query_phases(idx + 1).cpu_time_us() > 0);
                    UNIT_ASSERT(stats.total_duration_us() > 0);
                }
            }
        }

        sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(CopyTables) {
        TKikimrWithGrpcAndRootSchemaNoSystemViews server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable("/Root/Table-1", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables({{"/Root/Table-1", "/Root/Table-2"}}).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-1", "/Root/Table-3"}
                                     , {"/Root/Table-2", "/Root/Table-4"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-1", "/Root/Table-5"}
                                     , {"/Root/Table-2", "/Root/Table-6"}
                                     , {"/Root/Table-3", "/Root/Table-7"}
                                     , {"/Root/Table-4", "/Root/Table-8"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables(
                                     { }).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-1", "/Root/Table-1"}
                                     , {"/Root/Table-2", "/Root/Table-9"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-1", "/Root/dir_no_exist/Table-1"}
                                     , {"/Root/Table-2", "/Root/Table-9"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-1", "/Root/Table-2"}
                                     , {"/Root/Table-2", "/Root/Table-9"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-1", "/Root/Table-9"}
                                     , {"/Root/Table-1", "/Root/Table-10"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetStatus());
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-1", "/Root/Table-3"}
                                     , {"/Root/Table-2", "/Root/Table-4"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus()); // do not fail on exist
        }

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumn("Key");
            tableBuilder.AddSecondaryIndex("user-index", "Value");

            auto result = session.CreateTable("/Root/Indexed-Table-1", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables(
                                     {NYdb::NTable::TCopyItem("/Root/Indexed-Table-1", "/Root/Indexed-Table-2")})
                              .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables(
                                     {NYdb::NTable::TCopyItem("/Root/Indexed-Table-1", "/Root/Omited-Indexes-Table-3").SetOmitIndexes()})
                              .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables(
                                     {NYdb::NTable::TCopyItem("/Root/Indexed-Table-1", "/Root/Omited-Indexes-Table-4").SetOmitIndexes(),
                                      NYdb::NTable::TCopyItem("/Root/Indexed-Table-2", "/Root/Omited-Indexes-Table-5").SetOmitIndexes(),
                                      NYdb::NTable::TCopyItem("/Root/Omited-Indexes-Table-3", "/Root/Omited-Indexes-Table-6").SetOmitIndexes()
                                      })
                              .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables(
                                     {NYdb::NTable::TCopyItem("/Root/Indexed-Table-1", "/Root/Indexed-Table-7"),
                                      NYdb::NTable::TCopyItem("/Root/Indexed-Table-2", "/Root/Omited-Indexes-Table-8").SetOmitIndexes()
                                     })
                              .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }


        {
            auto asyncDescDir = NYdb::NScheme::TSchemeClient(connection).ListDirectory("/Root");
            asyncDescDir.Wait();
            const auto& val = asyncDescDir.GetValue();
            auto entry = val.GetEntry();
            UNIT_ASSERT_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::Directory);

            auto children = val.GetChildren();
            UNIT_ASSERT_EQUAL(children.size(), 16);
            for (const auto& child: children) {
                UNIT_ASSERT_EQUAL(child.Type, NYdb::NScheme::ESchemeEntryType::Table);

                auto result = session.DropTable(TStringBuilder() << "Root" << "/" <<  child.Name).ExtractValueSync();
                UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            }
        }
    }

    Y_UNIT_TEST(RenameTables) {
        TKikimrWithGrpcAndRootSchemaNoSystemViews server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable("/Root/Table-1", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.RenameTables({{"/Root/Table-1", "/Root/Table-2"}}).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.CopyTables({{"/Root/Table-2", "/Root/Table-1"}}).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/Table-2"} }
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/Table-3"}
                                     , {"/Root/Table-2", "/Root/Table-4"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.RenameTables(
                                     { }).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetStatus());
        }

        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/Table-1"}
                                     , {"/Root/Table-2", "/Root/Table-9"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/dir_no_exist/Table-1"}
                                     , {"/Root/Table-2", "/Root/Table-9"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/Table-2"}
                                     , {"/Root/Table-2", "/Root/Table-9"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }

        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/Table-9"}
                                     , {"/Root/Table-1", "/Root/Table-10"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus());
        }


        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/Table-3"}
                                     , {"/Root/Table-2", "/Root/Table-4"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus()); // do not fail on exist
        }

        {
            auto result = session.CopyTables(
                                     { {"/Root/Table-3", "/Root/Table-1"}
                                     , {"/Root/Table-4", "/Root/Table-2"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.RenameTables(
                                     { {"/Root/Table-1", "/Root/Table-3"}
                                     , {"/Root/Table-2", "/Root/Table-4"}}
                                     ).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetStatus()); // do not fail on exist
        }

        {
            auto result = session.RenameTables(
                                     {NYdb::NTable::TRenameItem("/Root/Table-4", "/Root/Table-1").SetReplaceDestination()})
                              .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.RenameTables(
                                     {NYdb::NTable::TRenameItem("/Root/Table-2", "/Root/Table-1").SetReplaceDestination(),
                                     {"/Root/Table-3", "/Root/Table-2"}})
                              .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto asyncDescDir = NYdb::NScheme::TSchemeClient(connection).ListDirectory("/Root");
            asyncDescDir.Wait();
            const auto& val = asyncDescDir.GetValue();
            auto entry = val.GetEntry();
            UNIT_ASSERT_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::Directory);

            auto children = val.GetChildren();
            UNIT_ASSERT_EQUAL_C(children.size(), 2, children.size());
            for (const auto& child: children) {
                UNIT_ASSERT_EQUAL(child.Type, NYdb::NScheme::ESchemeEntryType::Table);

                auto result = session.DropTable(TStringBuilder() << "Root" << "/" <<  child.Name).ExtractValueSync();
                UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
                UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetStatus());
            }
        }
    }


    namespace {

        TStoragePools CreatePoolsForTenant(TClient& client, const TDomainsInfo::TDomain::TStoragePoolKinds& pool_types, const TString& tenant)
        {
            TStoragePools result;
            for (auto& poolType: pool_types) {
                auto& poolKind = poolType.first;
                result.emplace_back(client.CreateStoragePool(poolKind, tenant), poolKind);
            }
            return result;
        }

        NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclarationSetting(const TString& name)
        {
            NKikimrSubDomains::TSubDomainSettings subdomain;
            subdomain.SetName(name);
            return subdomain;
        }

        NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSetting(const TString& name, const TStoragePools& pools = {})
        {
            NKikimrSubDomains::TSubDomainSettings subdomain;
            subdomain.SetName(name);
            subdomain.SetCoordinators(1);
            subdomain.SetMediators(1);
            subdomain.SetPlanResolution(10);
            subdomain.SetTimeCastBucketsPerMediator(2);
            for (auto& pool: pools) {
                *subdomain.AddStoragePools() = pool;
            }
            return subdomain;
        }

        enum class EDefaultTableProfile {
            Enabled,
            Disabled,
        };

        void InitSubDomain(
                TKikimrWithGrpcAndRootSchema& server,
                EDefaultTableProfile defaultTableProfile = EDefaultTableProfile::Enabled)
        {
            TClient client(*server.ServerSettings);

            {
                TString tenant_name = "ydb_ut_tenant";
                TString tenant = Sprintf("/Root/%s", tenant_name.c_str());

                TStoragePools tenant_pools = CreatePoolsForTenant(client, server.ServerSettings->StoragePoolTypes, tenant_name);

                UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                        client.CreateSubdomain("/Root", GetSubDomainDeclarationSetting(tenant_name)));
                UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_INPROGRESS,
                                        client.AlterSubdomain("/Root", GetSubDomainDefaultSetting(tenant_name, tenant_pools), TDuration::MilliSeconds(500)));

                server.Tenants_->Run(tenant);
            }

            if (defaultTableProfile == EDefaultTableProfile::Enabled) {
                TAutoPtr<NMsgBusProxy::TBusConsoleRequest> request(new NMsgBusProxy::TBusConsoleRequest());
                auto &item = *request->Record.MutableConfigureRequest()->AddActions()
                    ->MutableAddConfigItem()->MutableConfigItem();
                item.SetKind((ui32)NKikimrConsole::TConfigItem::TableProfilesConfigItem);
                auto &profiles = *item.MutableConfig()->MutableTableProfilesConfig();
                {
                    auto& policy = *profiles.AddStoragePolicies();
                    policy.SetName("default");
                    auto& family = *policy.AddColumnFamilies();
                    family.SetId(0);
                    family.MutableStorageConfig()->MutableSysLog()->SetPreferredPoolKind("ssd");
                    family.MutableStorageConfig()->MutableLog()->SetPreferredPoolKind("ssd");
                    family.MutableStorageConfig()->MutableData()->SetPreferredPoolKind("ssd");
                }
                {
                    auto& profile = *profiles.AddTableProfiles();
                    profile.SetName("default");
                    profile.SetStoragePolicy("default");
                }
                TAutoPtr<NBus::TBusMessage> reply;
                NBus::EMessageStatus msgStatus = client.SyncCall(request, reply);
                UNIT_ASSERT_VALUES_EQUAL(msgStatus, NBus::MESSAGE_OK);
                auto resp = dynamic_cast<NMsgBusProxy::TBusConsoleResponse*>(reply.Get())->Record;
                UNIT_ASSERT_VALUES_EQUAL(resp.GetStatus().GetCode(), Ydb::StatusIds::SUCCESS);
            }
        }

        class TPrintableIssues {
        public:
            TPrintableIssues(const NYql::TIssues& issues)
                : Issues(issues)
            { }

            friend IOutputStream& operator<<(IOutputStream& out, const TPrintableIssues& v) {
                v.Issues.PrintTo(out);
                return out;
            }

        private:
            const NYql::TIssues& Issues;
        };

    }

    Y_UNIT_TEST(SimpleColumnFamilies) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        InitSubDomain(server);

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8, "alt");
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable("/Root/ydb_ut_tenant/Table-1", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-1").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "alt");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetName(), "alt");
        }

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable("/Root/ydb_ut_tenant/Table-2", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-2").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
        }

        {
            auto alterSettings = TAlterTableSettings()
                .AlterColumnFamily("Value", "alt");

            auto result = session.AlterTable("/Root/ydb_ut_tenant/Table-2", alterSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-2").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "alt");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetName(), "alt");
        }

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8, "alt");
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto tableSettings = TCreateTableSettings()
                .StoragePolicy(TStoragePolicy()
                    .AppendColumnFamilies(TColumnFamilyPolicy()
                        .Name("alt")
                        .Compressed(true)));

            auto result = session.CreateTable("/Root/ydb_ut_tenant/Table-3", tableBuilder.Build(), tableSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-3").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "alt");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetName(), "alt");
        }

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable("/Root/ydb_ut_tenant/Table-4", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-4").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
        }

        {
            auto alterSettings = TAlterTableSettings()
                .AlterColumnFamily("Value", "alt")
                .BeginAddColumnFamily("alt")
                    .SetCompression(EColumnFamilyCompression::None)
                .EndAddColumnFamily();

            auto result = session.AlterTable("/Root/ydb_ut_tenant/Table-4", alterSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-4").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "alt");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetName(), "alt");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetCompression(), EColumnFamilyCompression::None);
        }

        {
            auto alterSettings = TAlterTableSettings()
                .BeginAlterColumnFamily("alt")
                    .SetCompression(EColumnFamilyCompression::LZ4)
                .EndAlterColumnFamily();

            auto result = session.AlterTable("/Root/ydb_ut_tenant/Table-4", alterSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-4").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "alt");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetName(), "alt");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetCompression(), EColumnFamilyCompression::LZ4);
        }

        for (int tableIdx = 1; tableIdx <= 4; ++tableIdx) {
            TString query = Sprintf(R"___(
                DECLARE $Key AS Uint32;
                DECLARE $Value AS Utf8;
                UPSERT INTO `Root/ydb_ut_tenant/Table-%d` (Key, Value) VALUES
                    ($Key, $Value);
            )___", tableIdx);

            for (ui32 key = 0; key < 1000; ++key) {
                auto paramsBuilder = client.GetParamsBuilder();
                auto params = paramsBuilder
                    .AddParam("$Key")
                        .Uint32(key)
                        .Build()
                    .AddParam("$Value")
                        .Utf8("test")
                        .Build()
                    .Build();
                auto result = session
                    .ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW())
                        .CommitTx(), std::move(params))
                    .ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                    "Table " << tableIdx << " Key " << key
                    << " Status: " << result.GetStatus()
                    << " Issues: " << TPrintableIssues(result.GetIssues()));
            }
        }
    }

    Y_UNIT_TEST(ColumnFamiliesWithStorageAndIndex) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        InitSubDomain(server);

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8, "alt");
            tableBuilder.SetPrimaryKeyColumn("Key");
            tableBuilder.AddSecondaryIndex("MyIndex", "Value");

            auto tableSettings = TCreateTableSettings()
                .StoragePolicy(TStoragePolicy()
                    .AppendColumnFamilies(TColumnFamilyPolicy()
                        .Name("alt")
                        .Data("hdd")
                        .Compressed(true)));

            auto result = session.CreateTable(
                    "/Root/ydb_ut_tenant/Table-1",
                    tableBuilder.Build(),
                    tableSettings)
                .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }
    }

    Y_UNIT_TEST(ColumnFamiliesDescriptionWithStorageAndIndex) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        InitSubDomain(server);

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8, "alt")
                .BeginColumnFamily("alt")
                    .SetData("hdd")
                    .SetCompression(EColumnFamilyCompression::LZ4)
                .EndColumnFamily();
            tableBuilder.SetPrimaryKeyColumn("Key");
            tableBuilder.AddSecondaryIndex("MyIndex", "Value");

            auto result = session.CreateTable(
                    "/Root/ydb_ut_tenant/Table-1",
                    tableBuilder.Build())
                .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-1").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "alt");
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetName(), "alt");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetData(), "hdd");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetCompression(), EColumnFamilyCompression::LZ4);
        }
    }

    Y_UNIT_TEST(ColumnFamiliesExternalBlobsWithoutDefaultProfile) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_NOTICE);
        server.Server_->GetRuntime()->GetAppData().FeatureFlags.SetEnablePublicApiExternalBlobs(true);
        InitSubDomain(server, EDefaultTableProfile::Disabled);

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8, "alt")
                .BeginStorageSettings()
                    .SetTabletCommitLog0("ssd")
                    .SetTabletCommitLog1("ssd")
                    .SetExternal("hdd")
                    .SetStoreExternalBlobs(true)
                .EndStorageSettings()
                .BeginColumnFamily("default")
                    .SetData("ssd")
                .EndColumnFamily()
                .BeginColumnFamily("alt")
                    .SetData("hdd")
                    .SetCompression(EColumnFamilyCompression::LZ4)
                .EndColumnFamily();
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable(
                    "/Root/ydb_ut_tenant/Table-1",
                    tableBuilder.Build())
                .ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS,
                "Status: " << result.GetStatus() << " Issues: " << TPrintableIssues(result.GetIssues()));
        }

        {
            auto res = session.DescribeTable("Root/ydb_ut_tenant/Table-1").ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(res.GetStatus(), EStatus::SUCCESS,
                "Status: " << res.GetStatus() << " Issues: " << TPrintableIssues(res.GetIssues()));
            auto columns = res.GetTableDescription().GetTableColumns();
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Name, "Key");
            UNIT_ASSERT_VALUES_EQUAL(columns[0].Family, "");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Name, "Value");
            UNIT_ASSERT_VALUES_EQUAL(columns[1].Family, "alt");
            const auto& settings = res.GetTableDescription().GetStorageSettings();
            UNIT_ASSERT_VALUES_EQUAL(settings.GetExternal(), "hdd");
            UNIT_ASSERT_VALUES_EQUAL(settings.GetStoreExternalBlobs(), true);
            const auto& families = res.GetTableDescription().GetColumnFamilies();
            UNIT_ASSERT_EQUAL(families.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetName(), "default");
            UNIT_ASSERT_VALUES_EQUAL(families[0].GetData(), "ssd");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetName(), "alt");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetData(), "hdd");
            UNIT_ASSERT_VALUES_EQUAL(families[1].GetCompression(), EColumnFamilyCompression::LZ4);
        }
    }

    Y_UNIT_TEST(TestDescribeTableWithShardStats) {
        TKikimrWithGrpcAndRootSchema server;

        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(sessionResult.GetStatus(), EStatus::SUCCESS);
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder();
            tableBuilder
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .AddNullableColumn("Value", EPrimitiveType::Utf8);
            tableBuilder.SetPrimaryKeyColumn("Key");

            auto tableSettings = NYdb::NTable::TCreateTableSettings().PartitioningPolicy(
                NYdb::NTable::TPartitioningPolicy().UniformPartitions(2));

            auto result = session.CreateTable("/Root/Foo", tableBuilder.Build(), tableSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            auto result = session.ExecuteDataQuery(R"___(
                UPSERT INTO `Root/Foo` (Key, Value)
                    VALUES(1, "bar");
                )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }

        {
            TDescribeTableSettings describeTableSettings =
                TDescribeTableSettings()
                    .WithTableStatistics(true);

            auto res = session.DescribeTable("Root/Foo", describeTableSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionsCount(), 2);
            //only table statistics
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionStats().size(), 0);
        }

        {
            TDescribeTableSettings describeTableSettings =
                TDescribeTableSettings()
                    .WithTableStatistics(true)
                    .WithPartitionStatistics(true);

            auto res = session.DescribeTable("Root/Foo", describeTableSettings).ExtractValueSync();
            UNIT_ASSERT_EQUAL(res.IsTransportError(), false);
            UNIT_ASSERT_EQUAL(res.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(res.GetTableDescription().GetPartitionStats().size(), 2);
        }
    }

    Y_UNIT_TEST(TestExplicitPartitioning) {
        TKikimrWithGrpcAndRootSchema server;

        auto connection = TDriver(
            TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        TTableClient client(connection);

        auto sessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        {
            auto tableBuilder = client.GetTableBuilder()
                .AddNullableColumn("Value", EPrimitiveType::Utf8)
                .AddNullableColumn("SubKey", EPrimitiveType::Utf8)
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .SetPrimaryKeyColumn("Key");

            TExplicitPartitions partitions;
            partitions.AppendSplitPoints(TValueBuilder().BeginTuple().AddElement().OptionalUint32(10).EndTuple().Build());
            partitions.AppendSplitPoints(TValueBuilder().BeginTuple().AddElement().OptionalUint32(20).EndTuple().Build());

            auto tableSettings = TCreateTableSettings().PartitioningPolicy(
                TPartitioningPolicy().ExplicitPartitions(partitions).AutoPartitioning(
                        EAutoPartitioningPolicy::AutoSplitMerge));

            auto result = session.CreateTable("/Root/Foo", tableBuilder.Build(), tableSettings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            for(ui32 idx = 0; idx < 25; ++idx) {
                // trying to wait for split merge to complete (if enabled...???)
                EnsureTablePartitions(client, "/Root/Foo", 3);
                Sleep(TDuration::Seconds(1));
            }

        }

        {
            auto result = session.ExecuteDataQuery(R"___(
                UPSERT INTO `Root/Foo` (Key, Value) VALUES
                    (1, "one"),
                    (2, "two"),
                    (12, "twelve"),
                    (15, "fifteen"),
                    (17, "seventeen"),
                    (100500, "too much")
            )___", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto res = session.ExecuteDataQuery("select count(*) from `Root/Foo`", TTxControl::BeginTx().CommitTx())
                .GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            TResultSetParser parser{res.GetResultSet(0)};
            while (parser.TryNextRow()) {
                UNIT_ASSERT_EQUAL(6, TValueParser{parser.GetValue(0)}.GetUint64());
            }
        }

        EnsureTablePartitions(client, "/Root/Foo", 3);
    }

    Y_UNIT_TEST(CreateAndAltertTableWithCompactionPolicy) {
        TKikimrWithGrpcAndRootSchema server;
        server.Server_->SetupDefaultProfiles();

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .SetCompactionPolicy("compaction2");

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TClient client(*server.ServerSettings);
            auto describeResult = client.Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable()
                .GetPartitionConfig().GetCompactionPolicy().GetGeneration().size(), 2);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .SetCompactionPolicy("default");

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TClient client(*server.ServerSettings);
            auto describeResult = client.Ls(tableName);
            UNIT_ASSERT_VALUES_EQUAL(describeResult->Record.GetPathDescription().GetTable()
                .GetPartitionConfig().GetCompactionPolicy().GetGeneration().size(), 3);
        }
    }

    Y_UNIT_TEST(CreateTableWithUniformPartitions) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .SetUniformPartitions(4);

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        EnsureTablePartitions(client, tableName, 4);
    }

    Y_UNIT_TEST(CreateTableWithUniformPartitionsAndAutoPartitioning) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .SetUniformPartitions(4)
                .BeginPartitioningSettings()
                    .SetPartitioningBySize(true)
                .EndPartitioningSettings();

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), true);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 2048);
        }
    }

    Y_UNIT_TEST(CreateTableWithPartitionAtKeys) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            TExplicitPartitions partitions;
            partitions.AppendSplitPoints(TValueBuilder().BeginTuple().AddElement().OptionalUint32(10).EndTuple().Build());
            partitions.AppendSplitPoints(TValueBuilder().BeginTuple().AddElement().OptionalUint32(20).EndTuple().Build());

            auto builder = TTableBuilder()
                .AddNullableColumn("Value", EPrimitiveType::Utf8)
                .AddNullableColumn("SubKey", EPrimitiveType::Utf8)
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .SetPrimaryKeyColumn("Key")
                .SetPartitionAtKeys(partitions);

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        EnsureTablePartitions(client, tableName, 3);
    }

    Y_UNIT_TEST(CreateTableWithPartitionAtKeysAndAutoPartitioning) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            TExplicitPartitions partitions;
            partitions.AppendSplitPoints(TValueBuilder().BeginTuple().AddElement().OptionalUint32(10).EndTuple().Build());
            partitions.AppendSplitPoints(TValueBuilder().BeginTuple().AddElement().OptionalUint32(20).EndTuple().Build());

            auto builder = TTableBuilder()
                .AddNullableColumn("Value", EPrimitiveType::Utf8)
                .AddNullableColumn("SubKey", EPrimitiveType::Utf8)
                .AddNullableColumn("Key", EPrimitiveType::Uint32)
                .SetPrimaryKeyColumn("Key")
                .SetPartitionAtKeys(partitions)
                .BeginPartitioningSettings()
                    .SetPartitioningBySize(true)
                .EndPartitioningSettings();

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), true);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 3);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 2048);
        }
    }

    Y_UNIT_TEST(CreateAndAltertTableWithPartitioningBySize) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .BeginPartitioningSettings()
                    .SetPartitioningBySize(true)
                    .SetPartitionSizeMb(100)
                    .SetMinPartitionsCount(2)
                    .SetMaxPartitionsCount(50)
                .EndPartitioningSettings();

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), true);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 100);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMaxPartitionsCount(), 50);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .BeginAlterPartitioningSettings()
                    .SetPartitionSizeMb(50)
                    .SetMinPartitionsCount(4)
                    .SetMaxPartitionsCount(100)
                .EndAlterPartitioningSettings();

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), true);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 50);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 4);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMaxPartitionsCount(), 100);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .BeginAlterPartitioningSettings()
                    .SetPartitioningBySize(false)
                .EndAlterPartitioningSettings();

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), false);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .BeginAlterPartitioningSettings()
                    .SetPartitioningByLoad(true)
                .EndAlterPartitioningSettings();

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), true);
        }
    }

    Y_UNIT_TEST(CreateAndAltertTableWithPartitioningByLoad) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .BeginPartitioningSettings()
                    .SetPartitioningByLoad(true)
                .EndPartitioningSettings();

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), false);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), true);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 1);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .BeginAlterPartitioningSettings()
                    .SetPartitioningBySize(true)
                .EndAlterPartitioningSettings();

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), true);
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), true);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 2048);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .BeginAlterPartitioningSettings()
                    .SetPartitioningByLoad(false)
                .EndAlterPartitioningSettings();

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
        }
    }

    Y_UNIT_TEST(CheckDefaultTableSettings1) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key");

            auto desc = builder.Build();
            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), false);
        }
    }

    Y_UNIT_TEST(CheckDefaultTableSettings2) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .BeginPartitioningSettings()
                    .SetPartitioningBySize(true)
                .EndPartitioningSettings();

            auto desc = builder.Build();
            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), false);
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), true);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitionSizeMb(), 2048);
        }
    }

    Y_UNIT_TEST(CheckDefaultTableSettings3) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .BeginPartitioningSettings()
                    .SetPartitioningByLoad(true)
                .EndPartitioningSettings();

            auto desc = builder.Build();
            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            const auto& partSettings = describeResult.GetTableDescription().GetPartitioningSettings();
            UNIT_ASSERT(partSettings.GetPartitioningByLoad().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningByLoad().GetRef(), true);
            UNIT_ASSERT(partSettings.GetPartitioningBySize().Defined());
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetPartitioningBySize().GetRef(), false);
            UNIT_ASSERT_VALUES_EQUAL(partSettings.GetMinPartitionsCount(), 1);
        }
    }

    Y_UNIT_TEST(CreateAndAltertTableWithKeyBloomFilter) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .SetKeyBloomFilter(true);

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT(describeResult.GetTableDescription().GetKeyBloomFilter().Defined());
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetKeyBloomFilter().GetRef(), true);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .SetKeyBloomFilter(false);

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT(describeResult.GetTableDescription().GetKeyBloomFilter().Defined());
            UNIT_ASSERT_VALUES_EQUAL(describeResult.GetTableDescription().GetKeyBloomFilter().GetRef(), false);
        }
    }

    Y_UNIT_TEST(CreateAndAltertTableWithReadReplicasSettings) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        const TString tableName = "Root/Test";

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .SetReadReplicasSettings(TReadReplicasSettings::EMode::AnyAz, 2);

            auto desc = builder.Build();

            auto result = session.CreateTable(tableName, std::move(desc)).GetValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT(describeResult.GetTableDescription().GetReadReplicasSettings().Defined());
            UNIT_ASSERT(describeResult.GetTableDescription().GetReadReplicasSettings()->GetMode()
                == TReadReplicasSettings::EMode::AnyAz);
            UNIT_ASSERT_VALUES_EQUAL(
                describeResult.GetTableDescription().GetReadReplicasSettings()->GetReadReplicasCount(), 2);
        }
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .SetReadReplicasSettings(TReadReplicasSettings::EMode::PerAz, 1);

            auto result = session.AlterTable(tableName, settings).ExtractValueSync();

            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TDescribeTableResult describeResult = session.DescribeTable(tableName)
                .GetValueSync();
            UNIT_ASSERT_EQUAL(describeResult.GetStatus(), EStatus::SUCCESS);
            UNIT_ASSERT(describeResult.GetTableDescription().GetReadReplicasSettings().Defined());
            UNIT_ASSERT(describeResult.GetTableDescription().GetReadReplicasSettings()->GetMode()
                == TReadReplicasSettings::EMode::PerAz);
            UNIT_ASSERT_VALUES_EQUAL(
                describeResult.GetTableDescription().GetReadReplicasSettings()->GetReadReplicasCount(), 1);
        }
    }

    Y_UNIT_TEST(CreateTableWithMESettings) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS, getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();
        auto builder = TTableBuilder()
            .AddNullableColumn("key", EPrimitiveType::Uint64)
            .AddNullableColumn("value", EPrimitiveType::Utf8)
            .SetPrimaryKeyColumn("key")
            .SetReadReplicasSettings(TReadReplicasSettings::EMode::AnyAz, 1);

        auto desc = builder.Build();

        TCreateTableSettings createTableSettings =
            TCreateTableSettings()
            .ReplicationPolicy(TReplicationPolicy().ReplicasCount(1).CreatePerAvailabilityZone(false));
        auto result = session.CreateTable("Root/Test", std::move(desc), createTableSettings).GetValueSync();
        UNIT_ASSERT_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        UNIT_ASSERT_STRING_CONTAINS_C(
            result.GetIssues().ToString(),
            "Warning: Table profile and ReadReplicasSettings are set. They are mutually exclusive. Use either one of them.",
            "Unexpected error message");
    }

    Y_UNIT_TEST(TableKeyRangesSinglePartition) {
        TKikimrWithGrpcAndRootSchema server;

        NYdb::TDriver driver(TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()));

        NYdb::NTable::TTableClient client(driver);
        auto getSessionResult = client.CreateSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(getSessionResult.GetStatus(), EStatus::SUCCESS,
            getSessionResult.GetIssues().ToString());
        auto session = getSessionResult.GetSession();

        {
            auto builder = TTableBuilder()
                .AddNullableColumn("key", EPrimitiveType::Uint64)
                .AddNullableColumn("value", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key");

            auto desc = builder.Build();
            auto result = session.CreateTable("Root/Test", std::move(desc)).GetValueSync();
            UNIT_ASSERT_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto describeResult = session.DescribeTable("Root/Test").ExtractValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            auto tableDesc = describeResult.GetTableDescription();
            UNIT_ASSERT_VALUES_EQUAL(tableDesc.GetKeyRanges().size(), 0);
        }

        {
            auto describeTableSettings = TDescribeTableSettings()
                .WithKeyShardBoundary(true);
            auto describeResult = session.DescribeTable("Root/Test", describeTableSettings).ExtractValueSync();
            UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

            auto tableDesc = describeResult.GetTableDescription();
            auto& keyRanges = tableDesc.GetKeyRanges();
            UNIT_ASSERT_VALUES_EQUAL(keyRanges.size(), 1);
            UNIT_ASSERT(!keyRanges[0].From());
            UNIT_ASSERT(!keyRanges[0].To());
        }
    }
}
