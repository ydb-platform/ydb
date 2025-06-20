
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <contrib/libs/fmt/include/fmt/format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NActors;
using namespace NKikimrConfig;
using namespace NYdb;
using namespace Tests;

struct TFakeTicketParserActor : public TActor<TFakeTicketParserActor> {
    TFakeTicketParserActor()
        : TActor<TFakeTicketParserActor>(&TFakeTicketParserActor::StateFunc)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvTicketParser::TEvAuthorizeTicket, Handle);
    )

    void Handle(TEvTicketParser::TEvAuthorizeTicket::TPtr& ev) {
        Y_ABORT_UNLESS(ev->Get()->Ticket.EndsWith(BUILTIN_SYSTEM_DOMAIN));
        NACLib::TUserToken::TUserTokenInitFields args;
        args.UserSID = ev->Get()->Ticket;
        TIntrusivePtr<NACLib::TUserToken> userToken = MakeIntrusive<NACLib::TUserToken>(args);
        Send(ev->Sender, new TEvTicketParser::TEvAuthorizeTicketResult(ev->Get()->Ticket, userToken));
    }
};

IActor* CreateFakeTicketParser(const TTicketParserSettings&) {
    return new TFakeTicketParserActor();
}

class TTenantsSetup {
public:
    explicit TTenantsSetup()
    {
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
    TAppConfig GetAppConfig() const {
        TAppConfig appConfig;
        appConfig.MutableFeatureFlags()->SetRequireDbPrefixInSecretName(true);
        return appConfig;
    }

    TServerSettings GetServerSettings(ui32 grpcPort) {
        ui32 msgBusPort = PortManager.GetPort();
        TAppConfig appConfig = GetAppConfig();

        auto serverSettings = TServerSettings(msgBusPort)
            .SetGrpcPort(grpcPort)
            .SetNodeCount(1)
            .SetDomainName(DomainName)
            .SetAppConfig(appConfig)
            .SetFeatureFlags(appConfig.GetFeatureFlags())
            .SetUseRealThreads(true)
            .SetDynamicNodeCount(2)
            .AddStoragePoolType(GetDedicatedTenantPath())
            .AddStoragePoolType(GetSharedTenantPath());

        serverSettings.CreateTicketParser = CreateFakeTicketParser;
        return serverSettings;
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
        TVector<ui32> tenantNodesIdx = Tenants->List(tenantPath);
        for (auto nodeIdx : tenantNodesIdx) {
            Server->EnableGRpc(PortManager.GetPort(), nodeIdx, tenantPath);
        }
    }

    void InitializeServer() {
        ui32 grpcPort = PortManager.GetPort();
        TServerSettings serverSettings = GetServerSettings(grpcPort);

        Server = MakeIntrusive<TServer>(serverSettings);
        Server->EnableGRpc(grpcPort);

        TClient client(serverSettings);
        client.InitRootScheme();

        Endpoint = TStringBuilder() << "localhost:" << grpcPort;

        Tenants = std::make_unique<TTenants>(Server);
        CreateTenants();
    }

public:
    const TString DomainName{"Root"};
    TString Endpoint;
    TPortManager PortManager;
    TServer::TPtr Server;
    std::unique_ptr<TTenants> Tenants;
};

Y_UNIT_TEST_SUITE(KqpSecrets) {
    Y_UNIT_TEST(RequireDbPrefixInSecretWithInvalidName) {
        using namespace fmt::literals;
        auto ydb = TTenantsSetup();
        for (const auto& tenantPath: {ydb.GetDedicatedTenantPath(), ydb.GetSharedTenantPath(), ydb.GetServerlessTenantPath()}) {
            auto driver = std::make_unique<TDriver>(TDriverConfig()
                .SetEndpoint(ydb.GetEndpoint())
                .SetDatabase(tenantPath));

            auto queryClient = NYdb::NQuery::TQueryClient(*driver, NYdb::NQuery::TClientSettings().AuthToken("user@" BUILTIN_SYSTEM_DOMAIN));
            auto result = queryClient.ExecuteQuery("CREATE OBJECT `id` (TYPE SECRET) WITH (value=`minio`);", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::GENERIC_ERROR);
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToOneLineString(), fmt::format("Secret name id must start with database name {db}", "db"_a = ExtractBase(tenantPath)), result.GetIssues().ToOneLineString());
        }
    }

    Y_UNIT_TEST(RequireDbPrefixInSecretWithValidName) {
        using namespace fmt::literals;
        auto ydb = TTenantsSetup();
        for (const auto& tenantPath: {ydb.GetDedicatedTenantPath(), ydb.GetSharedTenantPath(), ydb.GetServerlessTenantPath()}) {
            auto driver = std::make_unique<TDriver>(TDriverConfig()
                .SetEndpoint(ydb.GetEndpoint())
                .SetDatabase(tenantPath));

            auto queryClient = NYdb::NQuery::TQueryClient(*driver, NYdb::NQuery::TClientSettings().AuthToken("user@" BUILTIN_SYSTEM_DOMAIN));
            auto result = queryClient.ExecuteQuery(fmt::format("CREATE OBJECT `{db}id` (TYPE SECRET) WITH (value=`minio`);", "db"_a = ExtractBase(tenantPath)), NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
