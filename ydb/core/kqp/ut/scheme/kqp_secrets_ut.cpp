#include <ydb/core/base/path.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <contrib/libs/fmt/include/fmt/format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NActors;
using namespace Tests;

class TTenantsSetup {
public:
    explicit TTenantsSetup() {
        InitializeServer();
    }

    TString GetDedicatedTenantPath() const {
        return TStringBuilder() << CanonizePath(DomainName) << "/test-dedicated";
    }

    TString GetSharedTenantPath() const {
        return TStringBuilder() << CanonizePath(DomainName) << "/test-shared";
    }

    TString GetServerlessTenantPath() const {
        return TStringBuilder() << CanonizePath(DomainName) << "/test-serverless";
    }

    TTestActorRuntime* GetRuntime() const {
        return Server->GetRuntime();
    }

    TString GetEndpoint() const {
        return Endpoint;
    }

private:
    TServerSettings GetServerSettings(ui32 grpcPort) {
        const ui32 msgBusPort = PortManager.GetPort();

        return TServerSettings(msgBusPort)
            .SetGrpcPort(grpcPort)
            .SetNodeCount(1)
            .SetDomainName(DomainName)
            .SetUseRealThreads(true)
            .SetDynamicNodeCount(2)
            .AddStoragePoolType(GetDedicatedTenantPath())
            .AddStoragePoolType(GetSharedTenantPath());
    }

    void SetupResourcesTenant(Ydb::Cms::CreateDatabaseRequest& request, Ydb::Cms::StorageUnits* storage, const TString& name) {
        request.set_path(name);
        storage->set_unit_kind(name);
        storage->set_count(1);
    }

    void CreateTenants() {
        {  // Dedicated
            Ydb::Cms::CreateDatabaseRequest request;
            SetupResourcesTenant(request, request.mutable_resources()->add_storage_units(), GetDedicatedTenantPath());
            Tenants->CreateTenant(std::move(request));
        }

        {  // Shared
            Ydb::Cms::CreateDatabaseRequest request;
            SetupResourcesTenant(request, request.mutable_shared_resources()->add_storage_units(), GetSharedTenantPath());
            Tenants->CreateTenant(std::move(request));
        }

        {  // Serverless
            Ydb::Cms::CreateDatabaseRequest request;
            request.set_path(GetServerlessTenantPath());
            request.mutable_serverless_resources()->set_shared_database_path(GetSharedTenantPath());
            Tenants->CreateTenant(std::move(request));
        }

        SetupDiscovery(GetDedicatedTenantPath());
        SetupDiscovery(GetSharedTenantPath());
    }

    void SetupDiscovery(const TString& tenantPath) {
        for (auto nodeIdx : Tenants->List(tenantPath)) {
            Server->EnableGRpc(PortManager.GetPort(), nodeIdx, tenantPath);
        }
    }

    void InitializeServer() {
        const auto grpcPort = PortManager.GetPort();
        const auto serverSettings = GetServerSettings(grpcPort);

        Server = MakeIntrusive<TServer>(serverSettings);
        Server->EnableGRpc(grpcPort);

        TClient client(serverSettings);
        client.InitRootScheme();

        Endpoint = TStringBuilder() << "localhost:" << grpcPort;

        Tenants = std::make_unique<TTenants>(Server);
        CreateTenants();
    }

private:
    const TString DomainName{"Root"};
    TString Endpoint;
    TPortManager PortManager;
    TServer::TPtr Server;
    std::unique_ptr<TTenants> Tenants;
};

Y_UNIT_TEST_SUITE(KqpSecrets) {
    Y_UNIT_TEST(CreateSecretWithParamInNestedDbs) {
        auto ydb = TTenantsSetup();
        for (const auto& tenantPath : {ydb.GetDedicatedTenantPath(), ydb.GetSharedTenantPath(), ydb.GetServerlessTenantPath()}) {
            // Setup
            auto driver = std::make_unique<TDriver>(TDriverConfig()
                .SetEndpoint(ydb.GetEndpoint())
                .SetDatabase(tenantPath));
            auto queryClient = NYdb::NQuery::TQueryClient(*driver);
            const TString secretName = TString(ExtractBase(tenantPath)) + "-secret";
            const auto params = NYdb::TParamsBuilder()
                .AddParam("$value").Utf8("param-secret-value").Build()
                .Build();
            const auto query = queryClient.ExecuteQuery(
                fmt::format(
                    R"sql(
                        DECLARE $value AS Utf8;
                        CREATE SECRET `{}` WITH (value = $value);
                    )sql",
                    secretName
                ),
                NYdb::NQuery::TTxControl::NoTx(),
                params
            ).GetValueSync();

            // Checks
            UNIT_ASSERT_VALUES_EQUAL_C(query.GetStatus(), NYdb::EStatus::SUCCESS, query.GetIssues().ToOneLineString());

            auto schemeClient = NYdb::NScheme::TSchemeClient(*driver);
            const auto describeResult = schemeClient.DescribePath(CanonizePath(tenantPath + "/" + secretName)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(describeResult.GetStatus(), NYdb::EStatus::SUCCESS, describeResult.GetIssues().ToOneLineString());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
