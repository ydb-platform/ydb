#include "../ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

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

} // namespace

Y_UNIT_TEST_SUITE(YdbRelativeDatabase) {

Y_UNIT_TEST(RelativeDatabaseWorksForDiscoveryAndSubsequentRequests) {
    TKikimrWithGrpcAndRootSchema server({}, {}, {}, false, nullptr, [](auto& settings) {
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
    // Every SDK request below keeps using the relative database after synchronous discovery.
    NYdb::NTable::TTableClient tableClient(driver);
    const auto sessionResult = tableClient.CreateSession().GetValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto session = sessionResult.GetSession();

    const auto createTableResult = session.ExecuteSchemeQuery(R"(
        --!syntax_v1
        CREATE TABLE relative_path_test (
            Id Uint64,
            Payload Utf8,
            PRIMARY KEY (Id)
        );
    )").GetValueSync();
    UNIT_ASSERT_C(createTableResult.IsSuccess(), createTableResult.GetIssues().ToString());

    const auto upsertResult = session.ExecuteDataQuery(R"(
        --!syntax_v1
        UPSERT INTO relative_path_test (Id, Payload) VALUES (1u, "value");
    )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(upsertResult.IsSuccess(), upsertResult.GetIssues().ToString());

    const auto selectResult = session.ExecuteDataQuery(R"(
        --!syntax_v1
        SELECT Payload FROM relative_path_test WHERE Id = 1u;
    )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());

    TResultSetParser parser(selectResult.GetResultSet(0));
    UNIT_ASSERT(parser.TryNextRow());
    const auto payload = parser.ColumnParser("Payload").GetOptionalUtf8();
    UNIT_ASSERT(payload);
    UNIT_ASSERT_VALUES_EQUAL(*payload, "value");
    UNIT_ASSERT(!parser.TryNextRow());
}

} // YdbRelativeDatabase
