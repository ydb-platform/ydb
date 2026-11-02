#include "../ydb_common_ut.h"

#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <chrono>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NYdb;

namespace {

const TString TenantPath = "/Root/mydb";
const TString TenantPoolKind = "mydb";

template <class TResult>
void AssertSuccess(const TResult& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

struct TResourceFixture {
    TKikimrWithGrpcAndRootSchema Server;
    ui16 Port;
    TString Endpoint;

    explicit TResourceFixture(const TString& database)
        : Server({}, {}, {}, false, nullptr, [&](auto& settings) {
            settings.FeatureFlags.SetEnableRelativePaths(true);
            settings.StoragePoolTypes.clear();
            settings.AddStoragePool(TenantPoolKind, database + ':' + TenantPoolKind);
        })
    {
        if (const auto parent = database.substr(0, database.rfind('/')); parent != "/Root") {
            TDriver rootDriver(TDriverConfig()
                .SetEndpoint(TStringBuilder() << "localhost:" << Server.GetPort()).SetDatabase("/Root")
                .SetDiscoveryMode(EDiscoveryMode::Sync));
            AssertSuccess(NScheme::TSchemeClient(rootDriver).MakeDirectory(parent).GetValueSync());
        }
        Ydb::Cms::CreateDatabaseRequest request;
        request.set_path(database);
        auto* storage = request.mutable_resources()->add_storage_units();
        storage->set_unit_kind(TenantPoolKind);
        storage->set_count(1);
        Server.Tenants_->CreateTenant(std::move(request));
        Port = Server.GetPortManager().GetPort();
        Server.GetServer().EnableGRpc(Port, Server.Tenants_->List(database).front(), database);
        Endpoint = TStringBuilder() << "localhost:" << Port;
    }

    TDriver CreateDriver(const TString& database) const {
        return TDriver(TDriverConfig().SetEndpoint(Endpoint).SetDatabase(database).SetDiscoveryMode(EDiscoveryMode::Sync));
    }
};

ui32 ReadTable(Ydb::Table::V1::TableService::Stub& stub, const TString& database,
    const TString& sessionId, const TString& path, const TString& expectedPayload = {})
{
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    context.AddMetadata("x-ydb-database", database);
    Ydb::Table::ReadTableRequest request;
    request.set_session_id(sessionId);
    request.set_path(path);
    auto reader = stub.StreamReadTable(&context, request);
    Ydb::Table::ReadTableResponse response;
    ui32 rows = 0;
    while (reader->Read(&response)) {
        UNIT_ASSERT_C(response.status() == Ydb::StatusIds::SUCCESS, response.DebugString());
        if (expectedPayload) {
            TResultSetParser parser{TResultSet(response.result().result_set())};
            while (parser.TryNextRow()) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Payload").GetOptionalUtf8().value(), expectedPayload);
            }
        }
        rows += response.result().result_set().rows_size();
    }
    const auto status = reader->Finish();
    UNIT_ASSERT_C(status.ok(), status.error_message());
    return rows;
}

void AssertPayload(const TResultSet& resultSet, const TString& expected) {
    TResultSetParser parser(resultSet);
    UNIT_ASSERT(parser.TryNextRow());
    UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Payload").GetOptionalUtf8().value(), expected);
    UNIT_ASSERT(!parser.TryNextRow());
}

void AssertReadTable(NYdb::NTable::TSession& session, const TString& path, const TString& expected = {}, ui32 expectedRows = 1) {
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
        if (expected && resultSet.RowsCount()) {
            AssertPayload(resultSet, expected);
        }
        rows += resultSet.RowsCount();
    }
    UNIT_ASSERT_VALUES_EQUAL(rows, expectedRows);
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
    TResourceFixture fixture(TenantPath);
    const ui16 tenantGrpcPort = fixture.Port;
    auto channel = grpc::CreateChannel(fixture.Endpoint, grpc::InsecureChannelCredentials());
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

    auto tableStub = Ydb::Table::V1::TableService::NewStub(channel);
    for (const auto& database : TVector<TString>{"/Root/mydb", "mydb"}) {
        TDriver spellingDriver(TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << tenantGrpcPort)
            .SetDatabase(database).SetDiscoveryMode(EDiscoveryMode::Sync));
        NYdb::NTable::TTableClient spellingClient(spellingDriver);
        const auto spellingSessionResult = spellingClient.CreateSession().GetValueSync();
        AssertSuccess(spellingSessionResult);
        auto spellingSession = spellingSessionResult.GetSession();
        for (const auto& path : TVector<TString>{
            "/Root/mydb/relative_dir/relative_path_test",
            "relative_dir/relative_path_test",
        }) {
            AssertReadTable(spellingSession, path, {}, 2);
            UNIT_ASSERT_VALUES_EQUAL(ReadTable(*tableStub, database, TString(spellingSession.GetId()), path), 2);
        }
    }

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

    NExport::TExportClient exportClient(driver);
    for (const auto& path : TVector<TString>{"Root/mydb/missing", "/Root/mydb/missing", "missing"}) {
        NExport::TExportToS3Settings exportSettings;
        exportSettings
            .Endpoint("localhost:1")
            .Bucket("bucket")
            .AccessKey("access-key")
            .SecretKey("secret-key")
            .AppendItem({.Src = path, .Dst = "missing"});
        const auto exportResult = exportClient.ExportToS3(exportSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(exportResult.Status().GetStatus(), EStatus::SCHEME_ERROR,
            path << ": " << exportResult.Status().GetIssues().ToString());
    }
    for (const auto& distinctPath : TVector<TString>{"/Root/mydb/missing", "missing"}) {
        NExport::TExportToS3Settings exportSettings;
        exportSettings
            .Endpoint("localhost:1")
            .Bucket("bucket")
            .AccessKey("access-key")
            .SecretKey("secret-key")
            .AppendItem({.Src = "Root/mydb/missing", .Dst = "one"})
            .AppendItem({.Src = distinctPath, .Dst = "two"});
        const auto exportResult = exportClient.ExportToS3(exportSettings).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(exportResult.Status().GetStatus(), EStatus::SCHEME_ERROR,
            exportResult.Status().GetIssues().ToString());
    }

    AssertSuccess(session.DropTable("relative_dir/relative_path_test").GetValueSync());
    AssertSuccess(session.DropTable("relative_dir/copied_once").GetValueSync());
    AssertSuccess(session.DropTable("relative_dir/renamed").GetValueSync());
    AssertSuccess(schemeClient.ModifyPermissions(
        "relative_dir",
        NYdb::NScheme::TModifyPermissionsSettings().AddInterruptInheritance(true)).GetValueSync());
    AssertSuccess(schemeClient.RemoveDirectory("relative_dir").GetValueSync());
}

Y_UNIT_TEST_TWIN(NestedDatabaseResourcePaths, RepeatedRoot) {
    const TString parent = RepeatedRoot ? "Root" : "team";
    const TString database = "/Root/" + parent + "/mydb";
    TResourceFixture fixture(database);
    auto driver = fixture.CreateDriver(database);
    NYdb::NScheme::TSchemeClient scheme(driver);
    NYdb::NTable::TTableClient table(driver);
    const auto sessionResult = table.CreateSession().GetValueSync();
    AssertSuccess(sessionResult);
    auto session = sessionResult.GetSession();
    for (const auto& [directory, payload] : TVector<std::pair<TString, TString>>{
        {"", "target"}, {"mydb/", "database-name"}, {"Root2/", "root-boundary"},
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

    auto channel = grpc::CreateChannel(fixture.Endpoint, grpc::InsecureChannelCredentials());
    auto rawTable = Ydb::Table::V1::TableService::NewStub(channel);
    TVector<TString> databaseSpellings{database, parent + "/mydb"};
    for (const auto& spelling : databaseSpellings) {
        auto spellingDriver = fixture.CreateDriver(spelling);
        NYdb::NScheme::TSchemeClient spellingScheme(spellingDriver);
        AssertSuccess(spellingScheme.ListDirectory("mydb").GetValueSync());
        NYdb::NTable::TTableClient spellingTable(spellingDriver);
        for (ui32 iteration = 0; iteration < 3; ++iteration) {
            const auto tableSessionResult = spellingTable.CreateSession().GetValueSync();
            AssertSuccess(tableSessionResult);
            auto tableSession = tableSessionResult.GetSession();
            for (const auto& path : TVector<TString>{database + "/Config", "Config"}) {
                AssertReadTable(tableSession, path, "target");
                UNIT_ASSERT_VALUES_EQUAL(ReadTable(*rawTable, spelling, TString(tableSession.GetId()), path, "target"), 1);
            }
            AssertReadTable(tableSession, "mydb/Config", "database-name");
            AssertReadTable(tableSession, "Root2/Config", "root-boundary");
            UNIT_ASSERT_VALUES_EQUAL(ReadTable(*rawTable, spelling, TString(tableSession.GetId()),
                "mydb/Config", "database-name"), 1);
            UNIT_ASSERT_VALUES_EQUAL(ReadTable(*rawTable, spelling, TString(tableSession.GetId()),
                "Root2/Config", "root-boundary"), 1);
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

} // YdbRelativeResourcePaths
