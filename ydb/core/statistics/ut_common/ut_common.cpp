#include "ut_common.h"

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/service/service.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

// TODO remove SDK
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

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
    subdomain.SetCoordinators(1);
    subdomain.SetMediators(1);
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
    Settings->SetUseRealThreads(false);

    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableStatistics(true);
    featureFlags.SetEnableColumnStatistics(true);
    Settings->SetFeatureFlags(featureFlags);

    for (ui32 i : xrange(storagePools)) {
        TString poolName = Sprintf("test%d", i);
        Settings->AddStoragePool(poolName, TString("/Root:") + poolName, 2);
    }

    Server = new Tests::TServer(*Settings);
    Server->EnableGRpc(grpcPort);

    auto sender = Server->GetRuntime()->AllocateEdgeActor();
    Server->SetupRootStoragePools(sender);

    Client = MakeHolder<Tests::TClient>(*Settings);

    Tenants = MakeHolder<Tests::TTenants>(Server);

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

void CreateDatabase(TTestEnv& env, const TString& databaseName, size_t nodeCount) {
    auto subdomain = GetSubDomainDeclareSettings(databaseName);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().CreateExtSubdomain("/Root", subdomain));

    env.GetTenants().Run("/Root/" + databaseName, nodeCount);

    auto subdomainSettings = GetSubDomainDefaultSettings(databaseName, env.GetPools());
    subdomainSettings.SetExternalSchemeShard(true);
    subdomainSettings.SetExternalStatisticsAggregator(true);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
}

void CreateServerlessDatabase(TTestEnv& env, const TString& databaseName, TPathId resourcesDomainKey) {
    auto subdomain = GetSubDomainDeclareSettings(databaseName);
    subdomain.MutableResourcesDomainKey()->SetSchemeShard(resourcesDomainKey.OwnerId);
    subdomain.MutableResourcesDomainKey()->SetPathId(resourcesDomainKey.LocalPathId);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().CreateExtSubdomain("/Root", subdomain));

    env.GetTenants().Run("/Root/" + databaseName, 0);

    auto subdomainSettings = GetSubDomainDefaultSettings(databaseName, env.GetPools());
    subdomainSettings.SetExternalSchemeShard(true);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
}

TPathId ResolvePathId(TTestActorRuntime& runtime, const TString& path, TPathId* domainKey, ui64* saTabletId)
{
    auto sender = runtime.AllocateEdgeActor();

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    auto request = std::make_unique<TNavigate>();
    auto& entry = request->ResultSet.emplace_back();
    entry.Path = SplitPath(path);
    entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::EOp::OpPath;
    entry.ShowPrivatePath = true;
    runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.release()));

    auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
    UNIT_ASSERT(ev);
    UNIT_ASSERT(ev->Get());
    std::unique_ptr<TNavigate> response(ev->Get()->Request.Release());
    UNIT_ASSERT(response->ResultSet.size() == 1);
    auto& resultEntry = response->ResultSet[0];

    if (domainKey) {
        *domainKey = resultEntry.DomainInfo->DomainKey;
    }

    if (saTabletId && resultEntry.DomainInfo->Params.HasStatisticsAggregator()) {
        *saTabletId = resultEntry.DomainInfo->Params.GetStatisticsAggregator();
    }

    return resultEntry.TableId.PathId;
}

NKikimrScheme::TEvDescribeSchemeResult DescribeTable(TTestActorRuntime& runtime, TActorId sender, const TString &path)
{
    TAutoPtr<IEventHandle> handle;

    auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(path);
    request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);

    return *reply->MutableRecord();
}

TVector<ui64> GetTableShards(TTestActorRuntime& runtime, TActorId sender, const TString &path)
{
    TVector<ui64> shards;
    auto lsResult = DescribeTable(runtime, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    return shards;
}

TVector<ui64> GetColumnTableShards(TTestActorRuntime& runtime, TActorId sender,const TString &path)
{
    TVector<ui64> shards;
    auto lsResult = DescribeTable(runtime, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetColumnTableDescription().GetSharding().GetColumnShards())
        shards.push_back(part);

    return shards;
}

void CreateUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        )
        WITH ( UNIFORM_PARTITIONS = 4 );
    )", databaseName.c_str(), tableName.c_str())).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TStringBuilder replace;
    replace << Sprintf("REPLACE INTO `Root/%s/%s` (Key, Value) VALUES ",
        databaseName.c_str(), tableName.c_str());
    for (ui32 i = 0; i < 4; ++i) {
        if (i > 0) {
            replace << ", ";
        }
        ui64 value = 4000000000000000000ull * (i + 1);
        replace << Sprintf("(%" PRIu64 "ul, %" PRIu64 "ul)", value, value);
    }
    replace << ";";
    result = session.ExecuteDataQuery(replace, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void CreateColumnStoreTable(TTestEnv& env, const TString& databaseName, const TString& tableName,
    int shardCount)
{
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto fullTableName = Sprintf("Root/%s/%s", databaseName.c_str(), tableName.c_str());
    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint64 NOT NULL,
            Value Uint64,
            PRIMARY KEY (Key)
        )
        PARTITION BY HASH(Key)
        WITH (
            STORE = COLUMN,
            AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d
        );
    )", fullTableName.c_str(), shardCount)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < 100; ++i) {
        auto key = TValueBuilder().Uint64(i).Build();
        auto value = TValueBuilder().OptionalUint64(i).Build();
        rows.AddListItem();
        rows.BeginStruct();
        rows.AddMember("Key", key);
        rows.AddMember("Value", value);
        rows.EndStruct();
    }
    rows.EndList();

    result = client.BulkUpsert(fullTableName, rows.Build()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void DropTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    auto result = session.ExecuteSchemeQuery(Sprintf(R"(
        DROP TABLE `Root/%s/%s`;
    )", databaseName.c_str(), tableName.c_str())).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

std::shared_ptr<TCountMinSketch> ExtractCountMin(TTestActorRuntime& runtime, TPathId pathId) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(1));

    NStat::TRequest req;
    req.PathId = pathId;
    req.ColumnTag = 1;

    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = NStat::EStatType::COUNT_MIN_SKETCH;
    evGet->StatRequests.push_back(req);

    auto sender = runtime.AllocateEdgeActor(1);
    runtime.Send(statServiceId, sender, evGet.release(), 1, true);
    auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

    UNIT_ASSERT(evResult);
    UNIT_ASSERT(evResult->Get());
    UNIT_ASSERT(evResult->Get()->StatResponses.size() == 1);

    auto rsp = evResult->Get()->StatResponses[0];
    auto stat = rsp.CountMinSketch;
    UNIT_ASSERT(rsp.Success);
    UNIT_ASSERT(stat.CountMin);

    return stat.CountMin;
}

void ValidateCountMin(TTestActorRuntime& runtime, TPathId pathId) {
    auto countMin = ExtractCountMin(runtime, pathId);

    for (ui32 i = 0; i < 4; ++i) {
        ui64 value = 4000000000000000000ull * (i + 1);
        auto probe = countMin->Probe((const char *)&value, sizeof(ui64));
        UNIT_ASSERT_VALUES_EQUAL(probe, 1);
    }
}

void ValidateCountMinAbsense(TTestActorRuntime& runtime, TPathId pathId) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(1));

    NStat::TRequest req;
    req.PathId = pathId;
    req.ColumnTag = 1;

    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = NStat::EStatType::COUNT_MIN_SKETCH;
    evGet->StatRequests.push_back(req);

    auto sender = runtime.AllocateEdgeActor(1);
    runtime.Send(statServiceId, sender, evGet.release(), 1, true);
    auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

    UNIT_ASSERT(evResult);
    UNIT_ASSERT(evResult->Get());
    UNIT_ASSERT(evResult->Get()->StatResponses.size() == 1);

    auto rsp = evResult->Get()->StatResponses[0];
    UNIT_ASSERT(!rsp.Success);
}

void AnalyzeTable(TTestActorRuntime& runtime, const TPathId& pathId, ui64 tabletId) {
    auto ev = std::make_unique<TEvStatistics::TEvAnalyzeTable>();
    auto& record = ev->Record;
    PathIdFromPathId(pathId, record.MutableTable()->MutablePathId());
    record.AddTypes(NKikimrStat::EColumnStatisticType::TYPE_COUNT_MIN_SKETCH);

    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(tabletId, sender, ev.release());
    runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeTableResponse>(sender);
}


} // NStat
} // NKikimr
