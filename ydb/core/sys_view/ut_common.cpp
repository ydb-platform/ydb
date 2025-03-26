#include "ut_common.h"
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>

namespace NKikimr {
namespace NSysView {

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclareSettings(const TString &name, const TStoragePools &pools) {
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    for (auto& pool: pools) {
        *subdomain.AddStoragePools() = pool;
    }
    return subdomain;
}

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSettings(const TString &name, const TStoragePools &pools) {
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(name);
    subdomain.SetCoordinators(2);
    subdomain.SetMediators(2);
    subdomain.SetPlanResolution(50);
    subdomain.SetTimeCastBucketsPerMediator(2);
    for (auto& pool: pools) {
        *subdomain.AddStoragePools() = pool;
    }
    return subdomain;
}

TTestEnv::TTestEnv(ui32 staticNodes, ui32 dynamicNodes, const TTestEnvSettings& settings) {
    auto mbusPort = PortManager.GetPort();
    auto grpcPort = PortManager.GetPort();

    TVector<NKikimrKqp::TKqpSetting> kqpSettings;

    NKikimrProto::TAuthConfig authConfig = settings.AuthConfig;
    authConfig.SetUseBuiltinDomain(true);
    Settings = new Tests::TServerSettings(mbusPort, authConfig);
    Settings->SetDomainName("Root");
    Settings->SetNodeCount(staticNodes);
    Settings->SetDynamicNodeCount(dynamicNodes);
    Settings->SetKqpSettings(kqpSettings);

    // in some tests we check data size, which depends on compaction,
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableBackgroundCompaction(false);
    featureFlags.SetEnableResourcePools(true);
    featureFlags.SetEnableFollowerStats(true);
    featureFlags.SetEnableVectorIndex(true);
    Settings->SetFeatureFlags(featureFlags);

    Settings->SetEnablePersistentQueryStats(settings.EnableSVP);
    Settings->SetEnableDbCounters(settings.EnableSVP);
    Settings->SetEnableForceFollowers(settings.EnableForceFollowers);
    Settings->SetEnableTablePgTypes(true);
    Settings->SetEnableShowCreate(true);

    NKikimrConfig::TAppConfig appConfig;
    *appConfig.MutableFeatureFlags() = Settings->FeatureFlags;
    Settings->SetAppConfig(appConfig);

    for (ui32 i : xrange(settings.StoragePools)) {
        TString poolName = Sprintf("test%d", i);
        Settings->AddStoragePool(poolName, TString("/Root:") + poolName, 2);
    }

    Settings->AppConfig->MutableHiveConfig()->AddBalancerIgnoreTabletTypes(NKikimrTabletBase::TTabletTypes::SysViewProcessor);

    Server = new Tests::TServer(*Settings);
    Server->EnableGRpc(grpcPort);

    if (settings.ShowCreateTable) {
        this->Server->SetupDefaultProfiles();
    }

    auto* runtime = Server->GetRuntime();
    for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
        runtime->GetAppData(i).UsePartitionStatsCollectorForTests = true;
    }

    Client = MakeHolder<Tests::TClient>(*Settings);

    Tenants = MakeHolder<Tests::TTenants>(Server);

    Client->InitRootScheme("Root");

    if (settings.PqTabletsN) {
        NKikimr::NPQ::FillPQConfig(Settings->PQConfig, "/Root/PQ", true);
        PqTabletIds = Server->StartPQTablets(settings.PqTabletsN);
    }

    Endpoint = "localhost:" + ToString(grpcPort);
    if (settings.ShowCreateTable) {
        DriverConfig = NYdb::TDriverConfig().SetEndpoint(Endpoint).SetDatabase("/Root");
    } else {
        DriverConfig = NYdb::TDriverConfig().SetEndpoint(Endpoint);
    }
    Driver = MakeHolder<NYdb::TDriver>(DriverConfig);

    Server->GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);
}

TTestEnv::~TTestEnv() {
    Driver->Stop(true);
}

TStoragePools TTestEnv::GetPools() const {
    TStoragePools pools;
    for (const auto& [kind, pool] : Settings->StoragePoolTypes) {
        pools.emplace_back(pool.GetName(), kind);
    }
    return pools;
}

TStoragePools TTestEnv::CreatePoolsForTenant(const TString& tenant) {
    TStoragePools result;
    for (auto& poolType: Settings->StoragePoolTypes) {
        auto& poolKind = poolType.first;
        result.emplace_back(Client->CreateStoragePool(poolKind, tenant), poolKind);
    }
    return result;
}

} // NSysView
} // NKikimr
