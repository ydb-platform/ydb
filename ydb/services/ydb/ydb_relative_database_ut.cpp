#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NYdb;

namespace {

const TString TenantPath = "/Root/Root/mydb";
const TString TenantPoolKind = "mydb";

struct TDiscoveryResult {
    grpc::Status Status;
    Ydb::Discovery::ListEndpointsResponse Response;
};

TDiscoveryResult ListEndpoints(
        Ydb::Discovery::V1::DiscoveryService::Stub& stub,
        TStringBuf metadataDatabase,
        TStringBuf bodyDatabase,
        bool includeMetadata = true,
        TStringBuf token = {})
{
    grpc::ClientContext context;
    if (includeMetadata) {
        context.AddMetadata("x-ydb-database", TString(metadataDatabase));
    }
    if (token) {
        context.AddMetadata("x-ydb-auth-ticket", TString(token));
    }

    Ydb::Discovery::ListEndpointsRequest request;
    request.set_database(TString(bodyDatabase));

    TDiscoveryResult result;
    result.Status = stub.ListEndpoints(&context, request, &result.Response);
    return result;
}

void AssertDiscoveryStatus(const TDiscoveryResult& result, Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS) {
    UNIT_ASSERT_C(result.Status.ok(), result.Status.error_message());
    UNIT_ASSERT_VALUES_EQUAL_C(
        result.Response.operation().status(),
        expected,
        result.Response.operation().DebugString());
}

template <class TResult>
void AssertSuccess(const TResult& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(YdbRelativeDatabase) {

Y_UNIT_TEST(DisabledFlagPreservesLegacyDatabaseNames) {
    NKikimrConfig::TAppConfig config;
    config.MutableFeatureFlags()->SetEnableRelativePaths(false);
    TKikimrWithGrpcAndRootSchema server(config);
    auto channel = grpc::CreateChannel(
        TStringBuilder() << "localhost:" << server.GetPort(), grpc::InsecureChannelCredentials());
    auto stub = Ydb::Discovery::V1::DiscoveryService::NewStub(channel);
    for (const TStringBuf token : {TStringBuf(), TStringBuf("root@builtin")}) {
        // Legacy slashless full path names /Root, not /Root/Root.
        AssertDiscoveryStatus(ListEndpoints(*stub, "Root", "Root", true, token));
        AssertDiscoveryStatus(ListEndpoints(*stub, "/Root", "/Root", true, token));
    }
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

    AssertDiscoveryStatus(ListEndpoints(*stub, "/Root/Root/mydb", "/Root/Root/mydb"));
    AssertDiscoveryStatus(ListEndpoints(*stub, "Root/mydb", "Root/mydb"));
    AssertDiscoveryStatus(ListEndpoints(*stub, "", "/Root/Root/mydb"));

    auto clusterChannel = grpc::CreateChannel(
        TStringBuilder() << "localhost:" << server.GetPort(),
        grpc::InsecureChannelCredentials());
    auto clusterStub = Ydb::Discovery::V1::DiscoveryService::NewStub(clusterChannel);
    for (const TStringBuf token : {TStringBuf(), TStringBuf("root@builtin")}) {
        for (const TStringBuf database : {TStringBuf("/Root/Root/mydb"), TStringBuf("Root/mydb")}) {
            AssertDiscoveryStatus(ListEndpoints(*clusterStub, database, database, true, token));
            // Authenticated requests still require a nonempty database header.
            const auto missingDatabaseStatus = token ? Ydb::StatusIds::BAD_REQUEST : Ydb::StatusIds::SUCCESS;
            AssertDiscoveryStatus(ListEndpoints(*clusterStub, "", database, true, token), missingDatabaseStatus);
            AssertDiscoveryStatus(ListEndpoints(*clusterStub, "", database, false, token), missingDatabaseStatus);
            // Discovery looks up the body database, even when the header names the root.
            AssertDiscoveryStatus(ListEndpoints(*clusterStub, "/Root", database, true, token));
        }
    }

    TDriver driver(TDriverConfig()
        .SetEndpoint(TStringBuilder() << "localhost:" << tenantGrpcPort)
        .SetDatabase("Root/mydb")
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
