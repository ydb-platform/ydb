#include "ut_common.h"

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

TTestEnv::TTestEnv(ui32 staticNodes, ui32 dynamicNodes, ui32 storagePools, bool enableSVP) {
    auto mbusPort = PortManager.GetPort();
    auto grpcPort = PortManager.GetPort();

    TVector<NKikimrKqp::TKqpSetting> kqpSettings;
    NKikimrKqp::TKqpSetting setting;
    setting.SetName("_KqpAllowNewEngine");
    setting.SetValue("true");
    kqpSettings.push_back(setting);

    Settings = new Tests::TServerSettings(mbusPort);
    Settings->SetDomainName("Root");
    Settings->SetNodeCount(staticNodes);
    Settings->SetDynamicNodeCount(dynamicNodes);
    Settings->SetKqpSettings(kqpSettings);

    // in some tests we check data size, which depends on compaction,
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableBackgroundCompaction(false);
    Settings->SetFeatureFlags(featureFlags);

    if (enableSVP) {
        Settings->SetEnablePersistentQueryStats(true);
        Settings->SetEnableDbCounters(true);
    }

    for (ui32 i : xrange(storagePools)) {
        TString poolName = Sprintf("test%d", i);
        Settings->AddStoragePool(poolName, TString("/Root:") + poolName, 2);
    }

    Server = new Tests::TServer(*Settings);
    Server->EnableGRpc(grpcPort);

    auto* runtime = Server->GetRuntime();
    for (ui32 i = 0; i < runtime->GetNodeCount(); ++i) {
        runtime->GetAppData(i).UsePartitionStatsCollectorForTests = true;
    }

    Client = MakeHolder<Tests::TClient>(*Settings);

    Tenants = MakeHolder<Tests::TTenants>(Server);

    Client->InitRootScheme("Root");

    Endpoint = "localhost:" + ToString(grpcPort);
    DriverConfig = NYdb::TDriverConfig().SetEndpoint(Endpoint);
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
