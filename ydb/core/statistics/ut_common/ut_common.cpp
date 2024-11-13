#include "ut_common.h"

#include <ydb/core/statistics/service/service.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/core/testlib/actors/wait_events.h>
#include <ydb/core/testlib/tenant_helpers.h>

#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace NKikimr {
namespace NStat {

TTestEnv::TTestEnv(ui32 staticNodes, ui32 dynamicNodes, bool useRealThreads)
    : CSController(NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>())
{
    auto mbusPort = PortManager.GetPort();
    auto grpcPort = PortManager.GetPort();

    Settings = new Tests::TServerSettings(mbusPort);
    Settings->SetDomainName("Root");
    Settings->SetNodeCount(staticNodes);
    Settings->SetDynamicNodeCount(dynamicNodes);
    Settings->SetUseRealThreads(useRealThreads);
    Settings->AddStoragePoolType("hdd1");
    Settings->AddStoragePoolType("hdd2");

    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableStatistics(true);
    featureFlags.SetEnableColumnStatistics(true);
    Settings->SetFeatureFlags(featureFlags);

    Server = new Tests::TServer(*Settings);
    Server->EnableGRpc(grpcPort);

    auto sender = Server->GetRuntime()->AllocateEdgeActor();
    Server->SetupRootStoragePools(sender);

    Client = MakeHolder<Tests::TClient>(*Settings);

    Tenants = MakeHolder<Tests::TTenants>(Server);

    Endpoint = "localhost:" + ToString(grpcPort);
    DriverConfig = NYdb::TDriverConfig().SetEndpoint(Endpoint);
    Driver = MakeHolder<NYdb::TDriver>(DriverConfig);

    CSController->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
    CSController->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
    CSController->SetOverrideReduceMemoryIntervalLimit(1LLU << 30);

    Server->GetRuntime()->SetLogPriority(NKikimrServices::STATISTICS, NActors::NLog::PRI_DEBUG);
}

TTestEnv::~TTestEnv() {
    Driver->Stop(true);
}

TString CreateDatabase(TTestEnv& env, const TString& databaseName,
    size_t nodeCount, bool isShared, const TString& poolName)
{
    auto& runtime = *env.GetServer().GetRuntime();
    auto fullDbName = Sprintf("/Root/%s", databaseName.c_str());

    using TEvCreateDatabaseRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
        Ydb::Cms::CreateDatabaseRequest,
        Ydb::Cms::CreateDatabaseResponse>;

    Ydb::Cms::CreateDatabaseRequest request;
    request.set_path(fullDbName);
    if (isShared) {
        auto* resources = request.mutable_shared_resources();
        auto* storage = resources->add_storage_units();
        storage->set_unit_kind(poolName);
        storage->set_count(1);
    } else {
        auto* resources = request.mutable_resources();
        auto* storage = resources->add_storage_units();
        storage->set_unit_kind(poolName);
        storage->set_count(1);
    }

    auto future = NRpcService::DoLocalRpc<TEvCreateDatabaseRequest>(
        std::move(request), "", "", runtime.GetActorSystem(0));
    auto response = runtime.WaitFuture(std::move(future));
    UNIT_ASSERT(response.operation().ready());
    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

    env.GetTenants().Run(fullDbName, nodeCount);

    if (!env.GetServer().GetSettings().UseRealThreads) {
        runtime.SimulateSleep(TDuration::Seconds(1));
    }

    return fullDbName;
}

TString CreateServerlessDatabase(TTestEnv& env, const TString& databaseName, const TString& sharedName, size_t nodeCount) {
    auto& runtime = *env.GetServer().GetRuntime();
    auto fullDbName = Sprintf("/Root/%s", databaseName.c_str());

    using TEvCreateDatabaseRequest = NKikimr::NGRpcService::TGrpcRequestOperationCall<
        Ydb::Cms::CreateDatabaseRequest,
        Ydb::Cms::CreateDatabaseResponse>;

    Ydb::Cms::CreateDatabaseRequest request;
    request.set_path(fullDbName);
    request.mutable_serverless_resources()->set_shared_database_path(sharedName);

    auto future = NRpcService::DoLocalRpc<TEvCreateDatabaseRequest>(
        std::move(request), "", "", runtime.GetActorSystem(0));
    auto response = runtime.WaitFuture(std::move(future));
    UNIT_ASSERT(response.operation().ready());
    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

    env.GetTenants().Run(fullDbName, nodeCount);

    if (!env.GetServer().GetSettings().UseRealThreads) {
        runtime.SimulateSleep(TDuration::Seconds(1));
    }

    return fullDbName;
}

TPathId ResolvePathId(TTestActorRuntime& runtime, const TString& path, TPathId* domainKey, ui64* saTabletId) {
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

    if (saTabletId) {
        if (resultEntry.DomainInfo->Params.HasStatisticsAggregator()) {
            *saTabletId = resultEntry.DomainInfo->Params.GetStatisticsAggregator();
        } else {
            auto resourcesDomainKey = resultEntry.DomainInfo->ResourcesDomainKey;
            auto request = std::make_unique<TNavigate>();
            auto& entry = request->ResultSet.emplace_back();
            entry.TableId = TTableId(resourcesDomainKey.OwnerId, resourcesDomainKey.LocalPathId);
            entry.RequestType = TNavigate::TEntry::ERequestType::ByTableId;
            entry.Operation = TNavigate::EOp::OpPath;
            entry.RedirectRequired = false;
            runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.release()));

            auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get());
            std::unique_ptr<TNavigate> response(ev->Get()->Request.Release());
            UNIT_ASSERT(response->ResultSet.size() == 1);
            auto& secondResultEntry = response->ResultSet[0];

            if (secondResultEntry.DomainInfo->Params.HasStatisticsAggregator()) {
                *saTabletId = secondResultEntry.DomainInfo->Params.GetStatisticsAggregator();
            }
        }
    }

    return resultEntry.TableId.PathId;
}

NKikimrScheme::TEvDescribeSchemeResult DescribeTable(TTestActorRuntime& runtime, TActorId sender, const TString& path) {
    TAutoPtr<IEventHandle> handle;

    auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
    request->Record.MutableDescribePath()->SetPath(path);
    request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
    runtime.Send(new IEventHandle(MakeTxProxyID(), sender, request.Release()));
    auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);

    return *reply->MutableRecord();
}

TVector<ui64> GetTableShards(TTestActorRuntime& runtime, TActorId sender, const TString& path) {
    TVector<ui64> shards;
    auto lsResult = DescribeTable(runtime, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetTablePartitions())
        shards.push_back(part.GetDatashardId());

    return shards;
}

TVector<ui64> GetColumnTableShards(TTestActorRuntime& runtime, TActorId sender, const TString& path) {
    TVector<ui64> shards;
    auto lsResult = DescribeTable(runtime, sender, path);
    for (auto &part : lsResult.GetPathDescription().GetColumnTableDescription().GetSharding().GetColumnShards())
        shards.push_back(part);

    return shards;
}

Ydb::StatusIds::StatusCode ExecuteYqlScript(TTestEnv& env, const TString& script, bool mustSucceed) {
    auto& runtime = *env.GetServer().GetRuntime();

    using TEvExecuteYqlRequest = NGRpcService::TGrpcRequestOperationCall<
        Ydb::Scripting::ExecuteYqlRequest,
        Ydb::Scripting::ExecuteYqlResponse>;

    Ydb::Scripting::ExecuteYqlRequest request;
    request.set_script(script);

    auto future = NRpcService::DoLocalRpc<TEvExecuteYqlRequest>(
        std::move(request), "", "", runtime.GetActorSystem(0));
    auto response = runtime.WaitFuture(std::move(future));

    UNIT_ASSERT(response.operation().ready());
    if (mustSucceed) {
        UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
    }
    return response.operation().status();
}

void CreateUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value Uint64,
            PRIMARY KEY (Key)
        )
        WITH ( UNIFORM_PARTITIONS = 4 );
    )", databaseName.c_str(), tableName.c_str()));

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
    ExecuteYqlScript(env, replace);
}

void CreateColumnStoreTable(TTestEnv& env, const TString& databaseName, const TString& tableName,
    int shardCount)
{
    auto fullTableName = Sprintf("Root/%s/%s", databaseName.c_str(), tableName.c_str());
    auto& runtime = *env.GetServer().GetRuntime();

    ExecuteYqlScript(env, Sprintf(R"(
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
    )", fullTableName.c_str(), shardCount));
    runtime.SimulateSleep(TDuration::Seconds(1));

    ExecuteYqlScript(env, Sprintf(R"(
        ALTER OBJECT `%s` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_key, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Key']}`);
    )", fullTableName.c_str()));
    runtime.SimulateSleep(TDuration::Seconds(1));

    ExecuteYqlScript(env, Sprintf(R"(
        ALTER OBJECT `%s` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_value, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Value']}`);
    )", fullTableName.c_str()));
    runtime.SimulateSleep(TDuration::Seconds(1));

    using TEvBulkUpsertRequest = NGRpcService::TGrpcRequestOperationCall<
        Ydb::Table::BulkUpsertRequest,
        Ydb::Table::BulkUpsertResponse>;

    Ydb::Table::BulkUpsertRequest request;
    request.set_table(fullTableName);
    auto* rows = request.mutable_rows();

    auto* reqRowType = rows->mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type();
    auto* reqKeyType = reqRowType->add_members();
    reqKeyType->set_name("Key");
    reqKeyType->mutable_type()->set_type_id(Ydb::Type::UINT64);
    auto* reqValueType = reqRowType->add_members();
    reqValueType->set_name("Value");
    reqValueType->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UINT64);

    auto* reqRows = rows->mutable_value();
    for (size_t i = 0; i < ColumnTableRowsNumber; ++i) {
        auto* row = reqRows->add_items();
        row->add_items()->set_uint64_value(i);
        row->add_items()->set_uint64_value(i);
    }

    auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(
        std::move(request), "", "", runtime.GetActorSystem(0));
    auto response = runtime.WaitFuture(std::move(future));

    UNIT_ASSERT(response.operation().ready());
    UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

    env.GetController()->WaitActualization(TDuration::Seconds(1));
}

std::vector<TTableInfo> GatherColumnTablesInfo(TTestEnv& env, const TString& fullDbName, ui8 tableCount) {
    auto& runtime = *env.GetServer().GetRuntime();
    auto sender = runtime.AllocateEdgeActor();

    std::vector<TTableInfo> ret;
    for (ui8 tableId = 1; tableId <= tableCount; tableId++) {
        TTableInfo tableInfo;
        tableInfo.Path = Sprintf("%s/Table%u", fullDbName.c_str(), tableId);
        tableInfo.ShardIds = GetColumnTableShards(runtime, sender, tableInfo.Path);
        tableInfo.PathId = ResolvePathId(runtime, tableInfo.Path, &tableInfo.DomainKey, &tableInfo.SaTabletId);
        ret.emplace_back(tableInfo);
    }
    return ret;
}

TDatabaseInfo CreateDatabaseColumnTables(TTestEnv& env, ui8 tableCount, ui8 shardCount) {
    auto fullDbName = CreateDatabase(env, "Database");

    for (ui8 tableId = 1; tableId <= tableCount; tableId++) {
        CreateColumnStoreTable(env, "Database", Sprintf("Table%u", tableId), shardCount);
    }

    return {
        .FullDatabaseName = fullDbName,
        .Tables = GatherColumnTablesInfo(env, fullDbName, tableCount)
    };
}

TDatabaseInfo CreateServerlessDatabaseColumnTables(TTestEnv& env, ui8 tableCount, ui8 shardCount) {
    auto fullServerlessDbName = CreateDatabase(env, "Shared", 1, true);
    auto fullDbName = CreateServerlessDatabase(env, "Database", "/Root/Shared");

    for (ui8 tableId = 1; tableId <= tableCount; tableId++) {
        CreateColumnStoreTable(env, "Database", Sprintf("Table%u", tableId), shardCount);
    }

    return {
        .FullDatabaseName = fullServerlessDbName,
        .Tables = GatherColumnTablesInfo(env, fullDbName, tableCount)
    };
}

void DropTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    ExecuteYqlScript(env, Sprintf(R"(
        DROP TABLE `Root/%s/%s`;
    )", databaseName.c_str(), tableName.c_str()));
}

std::shared_ptr<TCountMinSketch> ExtractCountMin(TTestActorRuntime& runtime, const TPathId& pathId, ui64 columnTag) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(1));

    NStat::TRequest req;
    req.PathId = pathId;
    req.ColumnTag = columnTag;

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

void ValidateCountMinColumnshard(TTestActorRuntime& runtime, const TPathId& pathId, ui64 expectedProbe) {
    auto countMin = ExtractCountMin(runtime, pathId);

    ui32 value = 1;
    auto actualProbe = countMin->Probe((const char *)&value, sizeof(value));
    UNIT_ASSERT_VALUES_EQUAL(actualProbe, expectedProbe);
}

void ValidateCountMinDatashard(TTestActorRuntime& runtime, TPathId pathId) {
    auto countMin = ExtractCountMin(runtime, pathId);

    for (ui32 i = 0; i < 4; ++i) {
        ui64 value = 4000000000000000000ull * (i + 1);
        auto probe = countMin->Probe((const char *)&value, sizeof(ui64));
        UNIT_ASSERT_VALUES_EQUAL(probe, 1);
    }
}

void ValidateCountMinDatashardAbsense(TTestActorRuntime& runtime, TPathId pathId) {
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

TAnalyzedTable::TAnalyzedTable(const TPathId& pathId)
    : PathId(pathId)
{}

TAnalyzedTable::TAnalyzedTable(const TPathId& pathId, const std::vector<ui32>& columnTags)
    : PathId(pathId)
    , ColumnTags(columnTags)
{}

void TAnalyzedTable::ToProto(NKikimrStat::TTable& tableProto) const {
    PathIdFromPathId(PathId, tableProto.MutablePathId());
    tableProto.MutableColumnTags()->Add(ColumnTags.begin(), ColumnTags.end());
}

std::unique_ptr<TEvStatistics::TEvAnalyze> MakeAnalyzeRequest(const std::vector<TAnalyzedTable>& tables, const TString operationId) {
    auto ev = std::make_unique<TEvStatistics::TEvAnalyze>();
    NKikimrStat::TEvAnalyze& record = ev->Record;
    record.SetOperationId(operationId);
    record.AddTypes(NKikimrStat::EColumnStatisticType::TYPE_COUNT_MIN_SKETCH);
    for (const TAnalyzedTable& table : tables)
        table.ToProto(*record.AddTables());
    return ev;
}

void Analyze(TTestActorRuntime& runtime, ui64 saTabletId, const std::vector<TAnalyzedTable>& tables, const TString operationId) {
    auto ev = MakeAnalyzeRequest(tables, operationId);

    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(saTabletId, sender, ev.release());
    auto evResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

    const auto& record = evResponse->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), operationId);
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
}

void AnalyzeTable(TTestActorRuntime& runtime, ui64 shardTabletId, const TAnalyzedTable& table) {
    auto ev = std::make_unique<TEvStatistics::TEvAnalyzeTable>();
    auto& record = ev->Record;
    table.ToProto(*record.MutableTable());
    record.AddTypes(NKikimrStat::EColumnStatisticType::TYPE_COUNT_MIN_SKETCH);

    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(shardTabletId, sender, ev.release());
    runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeTableResponse>(sender);
}

void AnalyzeStatus(TTestActorRuntime& runtime, TActorId sender, ui64 saTabletId, const TString operationId, const NKikimrStat::TEvAnalyzeStatusResponse::EStatus expectedStatus) {
    auto analyzeStatusRequest = std::make_unique<TEvStatistics::TEvAnalyzeStatus>();
    analyzeStatusRequest->Record.SetOperationId(operationId);
    runtime.SendToPipe(saTabletId, sender, analyzeStatusRequest.release());

    auto analyzeStatusResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeStatusResponse>(sender);
    UNIT_ASSERT(analyzeStatusResponse);
    UNIT_ASSERT_VALUES_EQUAL(analyzeStatusResponse->Get()->Record.GetOperationId(), operationId);
    UNIT_ASSERT_VALUES_EQUAL(analyzeStatusResponse->Get()->Record.GetStatus(), expectedStatus);
}

void WaitForSavedStatistics(TTestActorRuntime& runtime, const TPathId& pathId) {
    TWaitForFirstEvent<TEvStatistics::TEvSaveStatisticsQueryResponse> waiter(runtime, [pathId](const auto& ev){
        return ev->Get()->PathId == pathId;
    });

    waiter.Wait();
}



} // NStat
} // NKikimr
