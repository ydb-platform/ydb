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
    AssertSuccess(ListEndpoints(*stub, "mydb", "mydb"));
    AssertSuccess(ListEndpoints(*stub, "", "mydb"));

    TDriver driver(TDriverConfig()
        .SetEndpoint(TStringBuilder() << "localhost:" << tenantGrpcPort)
        .SetDatabase("mydb")
        .SetDiscoveryMode(EDiscoveryMode::Sync));
    NYdb::NTable::TTableClient tableClient(driver);
    const auto sessionResult = tableClient.CreateSession().GetValueSync();
    AssertSuccess(sessionResult);
    auto session = sessionResult.GetSession();

    for (size_t i = 0; i < 2; ++i) {
        const auto result = session.ExecuteDataQuery(
            "SELECT 1;",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        AssertSuccess(result);
    }
}

} // YdbRelativeDatabase
