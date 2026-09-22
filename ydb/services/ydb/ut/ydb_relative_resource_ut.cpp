#include "../ydb_common_ut.h"

#include <ydb/core/base/counters.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NYdb;

namespace {

const TString TenantPath = "/Root/mydb";
const TString TenantPoolKind = "mydb";

struct TDiscoveryResult {
    grpc::Status Status;
    Ydb::Discovery::ListEndpointsResponse Response;
};

TDiscoveryResult ListEndpoints(
        Ydb::Discovery::V1::DiscoveryService::Stub& stub,
        TStringBuf metadataDatabase,
        TStringBuf bodyDatabase)
{
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    context.AddMetadata("x-ydb-database", TString(metadataDatabase));

    Ydb::Discovery::ListEndpointsRequest request;
    request.set_database(TString(bodyDatabase));

    TDiscoveryResult result;
    result.Status = stub.ListEndpoints(&context, request, &result.Response);
    return result;
}

void AssertSuccess(const TDiscoveryResult& result) {
    UNIT_ASSERT_C(result.Status.ok(), result.Status.error_message());
    UNIT_ASSERT_VALUES_EQUAL_C(
        result.Response.operation().status(),
        Ydb::StatusIds::SUCCESS,
        result.Response.operation().DebugString());
}

template <class TResult>
void AssertSuccess(const TResult& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

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
            rows += resultSet.RowsCount();
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(rows, 1);
}

} // namespace

Y_UNIT_TEST_SUITE(YdbRelativeResourcePaths) {

Y_UNIT_TEST_TWIN(FlagControlsPathsButNotMonitoring, enableRelativePaths) {
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
    auto methodCounters = GetServiceCounters(server.GetServer().GetRuntime()->GetAppData().Counters, "ydb")
        ->GetSubgroup("api_service", "scheme")->GetSubgroup("method", "DescribePath");
    UNIT_ASSERT_VALUES_EQUAL(methodCounters
        ->GetNamedCounter("name", "api.grpc.request.relative_resource_count", true)->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(methodCounters
        ->GetNamedCounter("name", "api.grpc.request.relative_database_count", true)->Val(), 0);
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

    auto methodCounters = GetServiceCounters(server.GetServer().GetRuntime()->GetAppData().Counters, "ydb")
        ->GetSubgroup("api_service", "rate_limiter")->GetSubgroup("method", "CreateResource");
    // The relative request is counted even when the feature is off and it is rejected.
    UNIT_ASSERT_VALUES_EQUAL(methodCounters
        ->GetNamedCounter("name", "api.grpc.request.relative_resource_count", true)->Val(), 1);
    UNIT_ASSERT_VALUES_EQUAL(methodCounters
        ->GetNamedCounter("name", "api.grpc.request.relative_database_count", true)->Val(), 0);
}

Y_UNIT_TEST(RelativeDatabaseWorksForDiscoveryAndSubsequentRequests) {
    TKikimrWithGrpcAndRootSchema server({}, {}, {}, false, nullptr, [](auto& settings) {
        settings.StoragePoolTypes.clear();
        settings.AddStoragePool(TenantPoolKind, TStringBuilder() << TenantPath << ':' << TenantPoolKind);
    });

    Ydb::Cms::CreateDatabaseRequest createRequest;
    createRequest.set_path(TenantPath);
    auto* storage = createRequest.mutable_resources()->add_storage_units();
    storage->set_unit_kind(TenantPoolKind);
    storage->set_count(1);
    server.Tenants_->CreateTenant(std::move(createRequest));

    const ui16 tenantGrpcPort = server.GetPortManager().GetPort();
    server.GetServer().EnableGRpc(
        tenantGrpcPort,
        server.Tenants_->List(TenantPath).front(),
        TString(TenantPath));

    auto channel = grpc::CreateChannel(
        TStringBuilder() << "localhost:" << tenantGrpcPort,
        grpc::InsecureChannelCredentials());
    auto stub = Ydb::Discovery::V1::DiscoveryService::NewStub(channel);

    AssertSuccess(ListEndpoints(*stub, "/Root/mydb", "/Root/mydb"));
    AssertSuccess(ListEndpoints(*stub, "mydb", "mydb"));
    AssertSuccess(ListEndpoints(*stub, "/Root/mydb", "mydb"));
    AssertSuccess(ListEndpoints(*stub, "", "/Root/mydb"));

    TDriver driver(TDriverConfig()
        .SetEndpoint(TStringBuilder() << "localhost:" << tenantGrpcPort)
        .SetDatabase("mydb")
        .SetDiscoveryMode(EDiscoveryMode::Sync));
    // Resource paths below are relative after discovery unless explicitly testing absolute compatibility.
    NYdb::NScheme::TSchemeClient schemeClient(driver);
    AssertSuccess(schemeClient.MakeDirectory("relative_dir").GetValueSync());
    AssertSuccess(schemeClient.DescribePath("relative_dir").GetValueSync());
    AssertSuccess(schemeClient.ListDirectory("relative_dir").GetValueSync());

    NYdb::NTable::TTableClient tableClient(driver);
    const auto sessionResult = tableClient.CreateSession().GetValueSync();
    AssertSuccess(sessionResult);
    auto session = sessionResult.GetSession();

    auto tableBuilder = tableClient.GetTableBuilder();
    tableBuilder
        .AddNullableColumn("Id", EPrimitiveType::Uint64)
        .AddNullableColumn("Payload", EPrimitiveType::Utf8)
        .SetPrimaryKeyColumn("Id");
    AssertSuccess(session.CreateTable(
        "relative_dir/relative_path_test",
        tableBuilder.Build()).GetValueSync());

    AssertSuccess(session.DescribeTable("relative_dir/relative_path_test").GetValueSync());

    TValueBuilder rows;
    rows.BeginList();
    rows.AddListItem()
        .BeginStruct()
            .AddMember("Id").Uint64(1)
            .AddMember("Payload").Utf8("value")
        .EndStruct();
    rows.EndList();
    AssertSuccess(tableClient.BulkUpsert(
        "relative_dir/relative_path_test",
        rows.Build()).GetValueSync());

    AssertSuccess(tableClient.BulkUpsert(
        "relative_dir/relative_path_test",
        NYdb::NTable::EDataFormat::CSV,
        "2,2\n").GetValueSync());

    auto extraType = TTypeBuilder()
        .BeginOptional()
            .Primitive(EPrimitiveType::Uint64)
        .EndOptional()
        .Build();
    AssertSuccess(session.AlterTable(
        "relative_dir/relative_path_test",
        NYdb::NTable::TAlterTableSettings().AppendAddColumns(
            NYdb::TColumn("Extra", extraType))).GetValueSync());

    TValueBuilder keys;
    keys.BeginList();
    keys.AddListItem()
        .BeginStruct()
            .AddMember("Id").Uint64(1)
        .EndStruct();
    keys.AddListItem()
        .BeginStruct()
            .AddMember("Id").Uint64(2)
        .EndStruct();
    keys.EndList();
    auto readRowsResult = tableClient.ReadRows(
        "relative_dir/relative_path_test",
        keys.Build()).GetValueSync();
    AssertSuccess(readRowsResult);
    TResultSetParser readRowsParser(readRowsResult.GetResultSet());
    ui32 readRowsCount = 0;
    while (readRowsParser.TryNextRow()) {
        ++readRowsCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(readRowsCount, 2u);

    const auto assertReadTable = [&](NYdb::NTable::TSession& readSession, TStringBuf path) {
        auto iterator = readSession.ReadTable(TString(path)).GetValueSync();
        AssertSuccess(iterator);

        ui32 rowCount = 0;
        while (true) {
            auto part = iterator.ReadNext().GetValueSync();
            if (part.EOS()) {
                break;
            }
            AssertSuccess(part);

            TResultSetParser readParser(part.ExtractPart());
            while (readParser.TryNextRow()) {
                ++rowCount;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(rowCount, 2u);
    };

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
            assertReadTable(spellingSession, path);
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

    AssertSuccess(schemeClient.MakeDirectory("mydb").GetValueSync());
    NYdb::NTopic::TTopicClient topicClient(driver);
    // The database name is not a cluster-root prefix and must remain part of the resource path.
    const TString topicPath = "mydb/topic";
    NYdb::NTopic::TCreateTopicSettings topicSettings;
    topicSettings.BeginConfigurePartitioningSettings().MinActivePartitions(1).EndConfigurePartitioningSettings();
    AssertSuccess(topicClient.CreateTopic(topicPath, topicSettings).GetValueSync());
    auto topicStub = Ydb::Topic::V1::TopicService::NewStub(channel);
    auto persQueueStub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
    ui32 writeIndex = 0;
    for (const auto& database : TVector<TString>{"/Root/mydb", "mydb"}) {
        TDriver spellingDriver(TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << tenantGrpcPort)
            .SetDatabase(database).SetDiscoveryMode(EDiscoveryMode::Sync));
        NYdb::NTopic::TTopicClient spellingTopic(spellingDriver);
        for (const auto& path : TVector<TString>{"/Root/mydb/mydb/topic", topicPath}) {
            const auto describe = [&](auto& stub, auto& request, auto& response, auto method) {
                grpc::ClientContext context;
                context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
                context.AddMetadata("x-ydb-database", database);
                request.mutable_operation_params()->set_operation_mode(Ydb::Operations::OperationParams::SYNC);
                request.set_path(path);
                const auto status = (stub.*method)(&context, request, &response);
                UNIT_ASSERT_C(status.ok(), database << ": " << path << ": " << status.error_message());
                UNIT_ASSERT_C(response.operation().status() == Ydb::StatusIds::SUCCESS,
                    database << ": " << path << ": " << response.DebugString());
            };
            Ydb::Topic::DescribeTopicRequest topicRequest;
            Ydb::Topic::DescribeTopicResponse topicResponse;
            describe(*topicStub, topicRequest, topicResponse, &Ydb::Topic::V1::TopicService::Stub::DescribeTopic);
            Ydb::Topic::DescribeTopicResult topicResult;
            UNIT_ASSERT(topicResponse.operation().result().UnpackTo(&topicResult));
            UNIT_ASSERT_VALUES_EQUAL(topicResult.partitions_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(topicResult.partitions(0).partition_id(), 0);

            Ydb::Topic::DescribePartitionRequest partitionRequest;
            partitionRequest.set_partition_id(0);
            partitionRequest.set_include_location(true);
            Ydb::Topic::DescribePartitionResponse partitionResponse;
            describe(*topicStub, partitionRequest, partitionResponse, &Ydb::Topic::V1::TopicService::Stub::DescribePartition);
            Ydb::Topic::DescribePartitionResult partitionResult;
            UNIT_ASSERT(partitionResponse.operation().result().UnpackTo(&partitionResult));
            UNIT_ASSERT_VALUES_EQUAL(partitionResult.partition().partition_id(), 0);

            Ydb::PersQueue::V1::DescribeTopicRequest legacyRequest;
            Ydb::PersQueue::V1::DescribeTopicResponse legacyResponse;
            describe(*persQueueStub, legacyRequest, legacyResponse, &Ydb::PersQueue::V1::PersQueueService::Stub::DescribeTopic);
            Ydb::PersQueue::V1::DescribeTopicResult legacyResult;
            UNIT_ASSERT(legacyResponse.operation().result().UnpackTo(&legacyResult));
            UNIT_ASSERT_VALUES_EQUAL(legacyResult.settings().partitions_count(), 1);

            AssertSuccess(spellingTopic.DescribeTopic(path).GetValueSync());
            auto writeSession = spellingTopic.CreateSimpleBlockingWriteSession(
                NYdb::NTopic::TWriteSessionSettings()
                    .Path(path)
                    .ProducerId("relative-database-test-" + ToString(writeIndex++))
                    .PartitionId(0)
                    .DirectWriteToPartition(true)
                    .Codec(NYdb::NTopic::ECodec::RAW));
            UNIT_ASSERT_C(writeSession->Write(NYdb::NTopic::TWriteMessage("message"), nullptr, TDuration::Seconds(30)),
                database << ": " << path);
            UNIT_ASSERT_C(writeSession->Close(TDuration::Seconds(30)), database << ": " << path);
        }
    }
    AssertSuccess(topicClient.DropTopic(topicPath).GetValueSync());
    AssertSuccess(schemeClient.RemoveDirectory("mydb").GetValueSync());

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
    TKikimrWithGrpcAndRootSchema server({}, {}, {}, false, nullptr, [&](auto& settings) {
        settings.StoragePoolTypes.clear();
        settings.AddStoragePool(TenantPoolKind, database + ':' + TenantPoolKind);
    });
    TDriver rootDriver(TDriverConfig()
        .SetEndpoint(TStringBuilder() << "localhost:" << server.GetPort())
        .SetDatabase("/Root").SetDiscoveryMode(EDiscoveryMode::Sync));
    NYdb::NScheme::TSchemeClient rootScheme(rootDriver);
    AssertSuccess(rootScheme.MakeDirectory("/Root/" + parent).GetValueSync());

    Ydb::Cms::CreateDatabaseRequest createRequest;
    createRequest.set_path(database);
    auto* storage = createRequest.mutable_resources()->add_storage_units();
    storage->set_unit_kind(TenantPoolKind);
    storage->set_count(1);
    server.Tenants_->CreateTenant(std::move(createRequest));
    const ui16 port = server.GetPortManager().GetPort();
    server.GetServer().EnableGRpc(port, server.Tenants_->List(database).front(), database);
    const TString endpoint = TStringBuilder() << "localhost:" << port;

    TDriver driver(TDriverConfig().SetEndpoint(endpoint).SetDatabase(database)
        .SetDiscoveryMode(EDiscoveryMode::Sync));
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

    auto channel = grpc::CreateChannel(endpoint, grpc::InsecureChannelCredentials());
    auto discovery = Ydb::Discovery::V1::DiscoveryService::NewStub(channel);
    auto rawTable = Ydb::Table::V1::TableService::NewStub(channel);
    TVector<TString> databaseSpellings{database, parent + "/mydb"};
    for (const auto& spelling : databaseSpellings) {
        AssertSuccess(ListEndpoints(*discovery, spelling, spelling));
        TDriver spellingDriver(TDriverConfig().SetEndpoint(endpoint).SetDatabase(spelling)
            .SetDiscoveryMode(EDiscoveryMode::Sync));
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
