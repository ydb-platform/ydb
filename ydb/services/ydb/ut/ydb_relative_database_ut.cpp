#include "../ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/coordination/coordination.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/export/export.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

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

} // namespace

Y_UNIT_TEST_SUITE(YdbRelativeDatabase) {

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
    AssertSuccess(ListEndpoints(*stub, "Root/mydb", "Root/mydb"));
    AssertSuccess(ListEndpoints(*stub, "mydb", "mydb"));
    AssertSuccess(ListEndpoints(*stub, "/Root/mydb", "mydb"));
    AssertSuccess(ListEndpoints(*stub, "", "mydb"));

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

    const auto assertReadTable = [&](TStringBuf path) {
        auto iterator = session.ReadTable(TString(path)).GetValueSync();
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

    assertReadTable("/Root/mydb/relative_dir/relative_path_test");
    assertReadTable("relative_dir/relative_path_test");

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
    NExport::TExportToS3Settings exportSettings;
    exportSettings
        .Endpoint("localhost:1")
        .Bucket("bucket")
        .AccessKey("access-key")
        .SecretKey("secret-key")
        .AppendItem({.Src = "Root/mydb/missing", .Dst = "one"})
        .AppendItem({.Src = "/Root/mydb/missing", .Dst = "two"});
    const auto exportResult = exportClient.ExportToS3(exportSettings).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL(exportResult.GetStatus(), EStatus::SCHEME_ERROR);

    AssertSuccess(schemeClient.MakeDirectory("Root").GetValueSync());
    NYdb::NTopic::TTopicClient topicClient(driver);
    // A slashless resource remains relative even when it starts with the database name.
    const TString topicPath = "Root/mydb";
    AssertSuccess(topicClient.CreateTopic(topicPath).GetValueSync());
    {
        auto writeSession = topicClient.CreateSimpleBlockingWriteSession(
            NYdb::NTopic::TWriteSessionSettings()
                .Path(topicPath)
                .ProducerId("relative-database-test")
                .PartitionId(0)
                .DirectWriteToPartition(true));
        UNIT_ASSERT(writeSession->Write("message"));
        UNIT_ASSERT(writeSession->Close());
    }
    AssertSuccess(topicClient.DropTopic(topicPath).GetValueSync());
    AssertSuccess(schemeClient.RemoveDirectory("Root").GetValueSync());

    AssertSuccess(session.DropTable("relative_dir/relative_path_test").GetValueSync());
    AssertSuccess(session.DropTable("relative_dir/copied_once").GetValueSync());
    AssertSuccess(session.DropTable("relative_dir/renamed").GetValueSync());
    AssertSuccess(schemeClient.ModifyPermissions(
        "relative_dir",
        NYdb::NScheme::TModifyPermissionsSettings().AddInterruptInheritance(true)).GetValueSync());
    AssertSuccess(schemeClient.RemoveDirectory("relative_dir").GetValueSync());
}

} // YdbRelativeDatabase
