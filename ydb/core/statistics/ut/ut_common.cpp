#include "ut_common.h"

namespace NKikimr {
namespace NStat {

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

TTestEnv::TTestEnv(ui32 staticNodes, ui32 dynamicNodes, ui32 storagePools) {
    auto mbusPort = PortManager.GetPort();
    auto grpcPort = PortManager.GetPort();

    Settings = new Tests::TServerSettings(mbusPort);
    Settings->SetDomainName("Root");
    Settings->SetNodeCount(staticNodes);
    Settings->SetDynamicNodeCount(dynamicNodes);

    NKikimrConfig::TFeatureFlags featureFlags;
    Settings->SetFeatureFlags(featureFlags);

    for (ui32 i : xrange(storagePools)) {
        TString poolName = Sprintf("test%d", i);
        Settings->AddStoragePool(poolName, TString("/Root:") + poolName, 2);
    }

    Server = new Tests::TServer(*Settings);
    Server->EnableGRpc(grpcPort);

    Client = MakeHolder<Tests::TClient>(*Settings);

    Tenants = MakeHolder<Tests::TTenants>(Server);

    Client->InitRootScheme("Root");

    Endpoint = "localhost:" + ToString(grpcPort);
    DriverConfig = NYdb::TDriverConfig().SetEndpoint(Endpoint);
    Driver = MakeHolder<NYdb::TDriver>(DriverConfig);

    Server->GetRuntime()->SetLogPriority(NKikimrServices::STATISTICS, NActors::NLog::PRI_DEBUG);
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

} // NStat
} // NKikimr
