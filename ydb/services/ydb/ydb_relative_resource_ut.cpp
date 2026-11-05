#include "ydb_relative_database_ut.h"

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <chrono>
#include <google/protobuf/util/message_differencer.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NYdb;

namespace {

const TString TenantPath = "/Root/mydb";

template <class TResult>
void AssertSuccess(const TResult& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void AssertPayload(const TResultSet& resultSet, const TString& expected) {
    TResultSetParser parser(resultSet);
    UNIT_ASSERT(parser.TryNextRow());
    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Payload").GetOptionalUtf8().value(), expected);
    UNIT_ASSERT(!parser.TryNextRow());
}

void AssertReadTable(NYdb::NTable::TSession& session, const TString& path, const TString& expected) {
    auto iterator = session.ReadTable(path).GetValueSync();
    AssertSuccess(iterator);
    ui32 rows = 0;
    while (true) {
        auto part = iterator.ReadNext().GetValueSync();
        if (part.EOS()) {
            break;
        }
        AssertSuccess(part);
        auto resultSet = part.ExtractPart();
        if (resultSet.RowsCount()) {
            AssertPayload(resultSet, expected);
        }
        rows += resultSet.RowsCount();
    }
    UNIT_ASSERT_VALUES_EQUAL(rows, 1);
}

} // namespace

Y_UNIT_TEST_SUITE(YdbRelativeResourcePaths) {

Y_UNIT_TEST_TWIN(FlagControlsPaths, enableRelativePaths) {
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableRelativePaths(enableRelativePaths);
    TKikimrWithGrpcAndRootSchema server(config);
    const TString endpoint = TStringBuilder() << "localhost:" << server.GetPort();
    TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root"));
    NYdb::NScheme::TSchemeClient schemeClient(driver);
    AssertSuccess(schemeClient.MakeDirectory("/Root/path").GetValueSync());

    auto stub = Ydb::Scheme::V1::SchemeService::NewStub(
        grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials()));
    for (const TStringBuf path : {TStringBuf("/Root/path"), TStringBuf("Root/path")}) {
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
        context.AddMetadata("x-ydb-database", "/Root");
        Ydb::Scheme::DescribePathRequest request;
        request.set_path(TString(path));
        Ydb::Scheme::DescribePathResponse response;
        const auto status = stub->DescribePath(&context, request, &response);
        UNIT_ASSERT_C(status.ok(), status.error_message());
        const auto expected = enableRelativePaths && !path.StartsWith('/')
            ? Ydb::StatusIds::SCHEME_ERROR : Ydb::StatusIds::SUCCESS;
        UNIT_ASSERT_VALUES_EQUAL_C(response.operation().status(), expected, response.DebugString());
    }
}

Y_UNIT_TEST_TWIN(RateLimiterCoordinationPathsRespectFlag, enableRelativePaths) {
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableRelativePaths(enableRelativePaths);
    TKikimrWithGrpcAndRootSchema server(config);
    const TString endpoint = TStringBuilder() << "localhost:" << server.GetPort();
    TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root"));
    NCoordination::TClient coordinationClient(driver);
    AssertSuccess(coordinationClient.CreateNode("/Root/limiter").GetValueSync());
    NRateLimiter::TRateLimiterClient rateLimiterClient(driver);

    for (const std::string path : {"/Root/limiter", "limiter"}) {
        const auto created = rateLimiterClient.CreateResource(path, "quota",
            NRateLimiter::TCreateResourceSettings().MaxUnitsPerSecond(1000)).GetValueSync();
        if (!enableRelativePaths && path == "limiter") {
            UNIT_ASSERT_VALUES_EQUAL(created.GetStatus(), EStatus::BAD_REQUEST);
            continue;
        }
        AssertSuccess(created);
        AssertSuccess(rateLimiterClient.AlterResource(path, "quota",
            NRateLimiter::TAlterResourceSettings().MaxUnitsPerSecond(2000)).GetValueSync());
        const auto described = rateLimiterClient.DescribeResource(path, "quota").GetValueSync();
        AssertSuccess(described);
        UNIT_ASSERT_VALUES_EQUAL(described.GetResourcePath(), "quota");
        const auto listed = rateLimiterClient.ListResources(path, "", NRateLimiter::TListResourcesSettings().Recursive(true)).GetValueSync();
        AssertSuccess(listed);
        UNIT_ASSERT_VALUES_EQUAL(listed.GetResourcePaths().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(listed.GetResourcePaths().front(), "quota");
        AssertSuccess(rateLimiterClient.AcquireResource(path, "quota",
            NRateLimiter::TAcquireResourceSettings().Amount(1)
                .OperationTimeout(TDuration::Seconds(30)).CancelAfter(TDuration::Seconds(20))).GetValueSync());
        AssertSuccess(rateLimiterClient.DropResource(path, "quota").GetValueSync());
    }

}

Y_UNIT_TEST(RelativeDatabaseWorksForDiscoveryAndSubsequentRequests) {
    TRelativeDatabaseFixture fixture(TenantPath);
    auto driver = fixture.CreateDriver("mydb");
    NYdb::NScheme::TSchemeClient schemeClient(driver);
    AssertSuccess(schemeClient.MakeDirectory("relative_dir").GetValueSync());
    AssertSuccess(schemeClient.DescribePath("relative_dir").GetValueSync());
    AssertSuccess(schemeClient.ListDirectory("relative_dir").GetValueSync());

    NYdb::NTable::TTableClient tableClient(driver);
    const auto sessionResult = tableClient.CreateSession().GetValueSync();
    AssertSuccess(sessionResult);
    auto session = sessionResult.GetSession();

    AssertSuccess(session.CreateTable("relative_dir/relative_path_test", tableClient.GetTableBuilder()
        .AddNullableColumn("Id", EPrimitiveType::Uint64)
        .AddNullableColumn("Payload", EPrimitiveType::Utf8)
        .SetPrimaryKeyColumn("Id").Build()).GetValueSync());

    AssertSuccess(session.DescribeTable("relative_dir/relative_path_test").GetValueSync());

    TValueBuilder rows;
    rows.BeginList().AddListItem().BeginStruct()
        .AddMember("Id").Uint64(1).AddMember("Payload").Utf8("value")
        .EndStruct().EndList();
    AssertSuccess(tableClient.BulkUpsert(
        "relative_dir/relative_path_test",
        rows.Build()).GetValueSync());

    AssertSuccess(tableClient.BulkUpsert(
        "relative_dir/relative_path_test",
        NYdb::NTable::EDataFormat::CSV,
        "2,2\n").GetValueSync());

    AssertSuccess(session.AlterTable(
        "relative_dir/relative_path_test",
        NYdb::NTable::TAlterTableSettings().AppendAddColumns(
            NYdb::TColumn("Extra", TTypeBuilder().BeginOptional()
                .Primitive(EPrimitiveType::Uint64).EndOptional().Build()))).GetValueSync());

    TValueBuilder keys;
    keys.BeginList()
        .AddListItem().BeginStruct().AddMember("Id").Uint64(1).EndStruct()
        .AddListItem().BeginStruct().AddMember("Id").Uint64(2).EndStruct()
        .EndList();
    auto readRowsResult = tableClient.ReadRows(
        "relative_dir/relative_path_test",
        keys.Build()).GetValueSync();
    AssertSuccess(readRowsResult);
    UNIT_ASSERT_VALUES_EQUAL(readRowsResult.GetResultSet().RowsCount(), 2u);

    AssertSuccess(session.CopyTable(
        "relative_dir/relative_path_test",
        "relative_dir/copied_once").GetValueSync());
    AssertSuccess(session.CopyTables({{
        "relative_dir/relative_path_test",
        "relative_dir/copied_twice"}}).GetValueSync());
    AssertSuccess(session.RenameTables({{
        "relative_dir/copied_twice",
        "relative_dir/renamed"}}).GetValueSync());

    AssertSuccess(session.DescribeSystemView(".sys/partition_stats").GetValueSync());

    NCoordination::TClient coordinationClient(driver);
    AssertSuccess(coordinationClient.CreateNode(
        "relative_dir/coordination",
        NCoordination::TCreateNodeSettings()
            .SelfCheckPeriod(TDuration::MilliSeconds(1234))).GetValueSync());
    AssertSuccess(coordinationClient.AlterNode(
        "relative_dir/coordination",
        NCoordination::TAlterNodeSettings()
            .SessionGracePeriod(TDuration::MilliSeconds(5678))).GetValueSync());
    const auto describeNodeResult = coordinationClient.DescribeNode(
        "relative_dir/coordination").GetValueSync();
    AssertSuccess(describeNodeResult);
    UNIT_ASSERT_VALUES_EQUAL(
        describeNodeResult.GetResult().GetSessionGracePeriod().value(),
        TDuration::MilliSeconds(5678));

    auto startSessionResult = coordinationClient.StartSession(
        "relative_dir/coordination").GetValueSync();
    AssertSuccess(startSessionResult);
    auto coordinationSession = startSessionResult.ExtractResult();
    AssertSuccess(coordinationSession.Close().GetValueSync());
    AssertSuccess(coordinationClient.DropNode("relative_dir/coordination").GetValueSync());

    AssertSuccess(session.DropTable("relative_dir/relative_path_test").GetValueSync());
    AssertSuccess(session.DropTable("relative_dir/copied_once").GetValueSync());
    AssertSuccess(session.DropTable("relative_dir/renamed").GetValueSync());
    AssertSuccess(schemeClient.ModifyPermissions(
        "relative_dir",
        NYdb::NScheme::TModifyPermissionsSettings().AddInterruptInheritance(true)).GetValueSync());
    AssertSuccess(schemeClient.RemoveDirectory("relative_dir").GetValueSync());
}

Y_UNIT_TEST(NestedDatabaseResourcePaths) {
    for (const TString database : {"/Root/mydb", "/Root/team/mydb", "/Root/Root/mydb"}) {
        TRelativeDatabaseFixture fixture(database);
        auto driver = fixture.CreateDriver(database);
        NYdb::NScheme::TSchemeClient scheme(driver);
        NYdb::NTable::TTableClient table(driver);
        const auto sessionResult = table.CreateSession().GetValueSync();
        AssertSuccess(sessionResult);
        auto session = sessionResult.GetSession();
        AssertSuccess(scheme.MakeDirectory("Root").GetValueSync());
        for (const auto& [directory, payload] : TVector<std::pair<TString, TString>>{
            {"", "target"}, {"mydb/", "database-name"}, {"Root2/", "root-boundary"}, {"Root/mydb/", "repeated-root"},
        }) {
            if (directory) {
                AssertSuccess(scheme.MakeDirectory(directory).GetValueSync());
            }
            const TString path = directory + "Config";
            AssertSuccess(session.CreateTable(path, table.GetTableBuilder()
                .AddNullableColumn("Id", EPrimitiveType::Uint64)
                .AddNullableColumn("Payload", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("Id").Build()).GetValueSync());
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct()
                .AddMember("Id").Uint64(1).AddMember("Payload").Utf8(payload)
                .EndStruct().EndList();
            AssertSuccess(table.BulkUpsert(path, rows.Build()).GetValueSync());
        }

        for (const auto& spelling : TVector<TString>{database, database.substr(TStringBuf("/Root/").size())}) {
            auto spellingDriver = fixture.CreateDriver(spelling);
            NYdb::NScheme::TSchemeClient spellingScheme(spellingDriver);
            AssertSuccess(spellingScheme.ListDirectory("mydb").GetValueSync());
            AssertSuccess(spellingScheme.ListDirectory("Root").GetValueSync());
            NYdb::NTable::TTableClient spellingTable(spellingDriver);
            for (ui32 iteration = 0; iteration < 3; ++iteration) {
                const auto tableSessionResult = spellingTable.CreateSession().GetValueSync();
                AssertSuccess(tableSessionResult);
                auto tableSession = tableSessionResult.GetSession();
                for (const auto& path : TVector<TString>{database + "/Config", "Config"}) {
                    AssertReadTable(tableSession, path, "target");
                }
                AssertReadTable(tableSession, "mydb/Config", "database-name");
                AssertReadTable(tableSession, "Root2/Config", "root-boundary");
                AssertReadTable(tableSession, "Root/mydb/Config", "repeated-root");
                const auto data = tableSession.ExecuteDataQuery("SELECT Payload FROM Config;",
                    NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
                AssertSuccess(data);
                AssertPayload(data.GetResultSet(0), "target");
                AssertSuccess(tableSession.Close().GetValueSync());

                NYdb::NQuery::TQueryClient query(spellingDriver);
                const auto querySessionResult = query.GetSession().GetValueSync();
                AssertSuccess(querySessionResult);
                auto querySession = querySessionResult.GetSession();
                const auto result = querySession.ExecuteQuery("SELECT Payload FROM Config;",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
                AssertSuccess(result);
                AssertPayload(result.GetResultSet(0), "target");
            }
        }
    }
}

Y_UNIT_TEST(DescribeExternalObjectsWithRelativePaths) {
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableRelativePaths(true);
    config.MutableFeatureFlags()->SetEnableExternalDataSources(true);
    TKikimrWithGrpcAndRootSchema server(config);
    TDriver driver(TDriverConfig()
        .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort()).SetDatabase("/Root"));
    NYdb::NTable::TTableClient client(driver);
    const auto sessionResult = client.CreateSession().GetValueSync();
    AssertSuccess(sessionResult);
    auto session = sessionResult.GetSession();
    AssertSuccess(session.ExecuteSchemeQuery(R"(
        CREATE EXTERNAL DATA SOURCE `/Root/externalDataSource` WITH (
            SOURCE_TYPE = "ObjectStorage",
            LOCATION = "localhost:1",
            AUTH_METHOD = "NONE"
        );
        CREATE EXTERNAL TABLE `/Root/externalTable` (key Uint64) WITH (
            DATA_SOURCE = "/Root/externalDataSource",
            LOCATION = "folder",
            FORMAT = "csv_with_names"
        );
    )").GetValueSync());

    auto compareDescriptions = [](const auto& relative, const auto& absolute) {
        google::protobuf::util::MessageDifferencer differencer;
        differencer.IgnoreField(relative.GetDescriptor()->FindFieldByName("self"));
        UNIT_ASSERT_C(differencer.Compare(relative, absolute), relative.DebugString());
    };
    const auto relativeSource = session.DescribeExternalDataSource("externalDataSource").GetValueSync();
    const auto absoluteSource = session.DescribeExternalDataSource("/Root/externalDataSource").GetValueSync();
    AssertSuccess(relativeSource);
    AssertSuccess(absoluteSource);
    compareDescriptions(TProtoAccessor::GetProto(relativeSource.GetExternalDataSourceDescription()),
        TProtoAccessor::GetProto(absoluteSource.GetExternalDataSourceDescription()));

    const auto relativeTable = session.DescribeExternalTable("externalTable").GetValueSync();
    const auto absoluteTable = session.DescribeExternalTable("/Root/externalTable").GetValueSync();
    AssertSuccess(relativeTable);
    AssertSuccess(absoluteTable);
    const auto description = TProtoAccessor::GetProto(relativeTable.GetExternalTableDescription());
    compareDescriptions(description, TProtoAccessor::GetProto(absoluteTable.GetExternalTableDescription()));
    UNIT_ASSERT_VALUES_EQUAL(description.data_source_path(), "/Root/externalDataSource");
    UNIT_ASSERT_VALUES_EQUAL(description.location(), "folder");
}

} // YdbRelativeResourcePaths
