#include "ut_common.h"

#include <ydb/core/base/backtrace.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/wrappers/fake_storage.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

void CreateTable(auto& session, const TString& name, ui64 partitionCount = 1) {
    auto desc = TTableBuilder()
        .AddNullableColumn("Key", EPrimitiveType::Uint64)
        .AddNullableColumn("Value", EPrimitiveType::String)
        .SetPrimaryKeyColumns({"Key"})
        .Build();

    auto settings = TCreateTableSettings();
    settings.PartitioningPolicy(TPartitioningPolicy().UniformPartitions(partitionCount));

    session.CreateTable(name, std::move(desc), std::move(settings)).GetValueSync();
}

void CreateTables(TTestEnv& env, ui64 partitionCount = 1) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    CreateTable(session, "Root/Table0", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Table0` (Key, Value) VALUES
            (0u, "Z");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());

    CreateTable(session, "Root/Tenant1/Table1", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Tenant1/Table1` (Key, Value) VALUES
            (1u, "A"),
            (2u, "B"),
            (3u, "C");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());

    CreateTable(session, "Root/Tenant2/Table2", partitionCount);
    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Tenant2/Table2` (Key, Value) VALUES
            (4u, "D"),
            (5u, "E");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());
}

} // namespace

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
    EnableYDBBacktraceFormat();

    auto mbusPort = PortManager.GetPort();
    auto grpcPort = PortManager.GetPort();

    TVector<NKikimrKqp::TKqpSetting> kqpSettings;

    NKikimrProto::TAuthConfig authConfig = settings.AuthConfig;
    authConfig.SetUseBuiltinDomain(true);
    Settings = new Tests::TServerSettings(mbusPort, authConfig);
    Settings->SetDomainName("Root");
    Settings->SetGrpcPort(grpcPort);
    Settings->SetNodeCount(staticNodes);
    Settings->SetDynamicNodeCount(dynamicNodes);
    Settings->SetKqpSettings(kqpSettings);

    // in some tests we check data size, which depends on compaction,
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableBackgroundCompaction(false);
    featureFlags.SetEnableResourcePools(true);
    featureFlags.SetEnableFollowerStats(true);
    featureFlags.SetEnableVectorIndex(true);
    featureFlags.SetEnableTieringInColumnShard(true);
    featureFlags.SetEnableExternalDataSources(true);
    featureFlags.SetEnableSparsedColumns(settings.EnableSparsedColumns);
    featureFlags.SetEnableOlapCompression(settings.EnableOlapCompression);
    featureFlags.SetEnableTableCacheModes(settings.EnableTableCacheModes);
    featureFlags.SetEnableFulltextIndex(settings.EnableFulltextIndex);
    if (settings.EnableRealSystemViewPaths) {
        featureFlags.SetEnableRealSystemViewPaths(*settings.EnableRealSystemViewPaths);
    }

    Settings->SetFeatureFlags(featureFlags);

    Settings->SetEnablePersistentQueryStats(settings.EnableSVP);
    Settings->SetEnableDbCounters(settings.EnableSVP);
    Settings->SetEnableForceFollowers(settings.EnableForceFollowers);
    Settings->SetEnableTablePgTypes(true);
    Settings->SetEnableShowCreate(true);

    NKikimrConfig::TAppConfig appConfig;
    *appConfig.MutableFeatureFlags() = Settings->FeatureFlags;
    appConfig.MutableQueryServiceConfig()->AddAvailableExternalDataSources("ObjectStorage");
    appConfig.MutableColumnShardConfig()->SetAlterObjectEnabled(settings.AlterObjectEnabled);

    auto& tableServiceConfig = *appConfig.MutableTableServiceConfig();
    tableServiceConfig = settings.TableServiceConfig;
    tableServiceConfig.SetEnableTempTablesForUser(true);

    Settings->SetAppConfig(appConfig);

    for (ui32 i : xrange(settings.StoragePools)) {
        TString poolName = Sprintf("test%d", i);
        Settings->AddStoragePool(poolName, TString("/Root:") + poolName, 2);
    }

    Settings->AppConfig->MutableHiveConfig()->AddBalancerIgnoreTabletTypes(NKikimrTabletBase::TTabletTypes::SysViewProcessor);

    if (settings.DataShardStatsReportIntervalSeconds) {
        Settings->AppConfig->MutableDataShardConfig()
            ->SetStatsReportIntervalSeconds(*settings.DataShardStatsReportIntervalSeconds);
    }

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

    Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("fakeSecret");
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

void CreateTenant(TTestEnv& env, const TString& tenantName, bool extSchemeShard, ui64 nodesCount) {
    auto subdomain = GetSubDomainDeclareSettings(tenantName);
    if (extSchemeShard) {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().CreateExtSubdomain("/Root", subdomain));
    } else {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().CreateSubdomain("/Root", subdomain));
    }

    env.GetTenants().Run("/Root/" + tenantName, nodesCount);

    auto subdomainSettings = GetSubDomainDefaultSettings(tenantName, env.GetPools());
    subdomainSettings.SetExternalSysViewProcessor(true);

    if (extSchemeShard) {
        subdomainSettings.SetExternalSchemeShard(true);
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
    } else {
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterSubdomain("/Root", subdomainSettings));
    }
}

void CreateTenants(TTestEnv& env, bool extSchemeShard) {
    CreateTenant(env, "Tenant1", extSchemeShard);
    CreateTenant(env, "Tenant2", extSchemeShard);
}

void CreateTenantsAndTables(TTestEnv& env, bool extSchemeShard, ui64 partitionCount) {
    CreateTenants(env, extSchemeShard);
    CreateTables(env, partitionCount);
}

} // NSysView
} // NKikimr
