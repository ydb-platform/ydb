#include "ut_common.h"

#include <ydb/core/statistics/service/service.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/testlib/tenant_helpers.h>

#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>

#include <yql/essentials/public/udf/udf_data_type.h>

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace NKikimr {
namespace NStat {

TTestEnv::TTestEnv(ui32 staticNodes, ui32 dynamicNodes, bool useRealThreads,
    std::function<void(Tests::TServerSettings&)> modifySettings)
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
    Settings->SetColumnShardAlterObjectEnabled(true);
    auto* stats = Settings->AppConfig->MutableStatisticsConfig();
    stats->SetBaseStatsSendInitialDelaySeconds(3);
    stats->SetBaseStatsSendIntervalSecondsDedicated(1);
    stats->SetBaseStatsSendIntervalSecondsServerless(1);
    stats->SetBaseStatsPropagateIntervalSecondsDedicated(1);
    stats->SetBaseStatsPropagateIntervalSecondsServerless(1);

    // Speed up datashard partition stats reporting (default 10s) so that
    // schemeshard gets full stats faster, especially after reboots.
    Settings->AppConfig->MutableDataShardConfig()->SetStatsReportIntervalSeconds(1);

    // Speed up columnshard periodic stats reporting (default 60s) so that
    // schemeshard gets full stats faster, especially after reboots.
    auto* columnShardStats = Settings->AppConfig->MutableColumnShardConfig()->MutableStatistics();
    columnShardStats->SetReportBaseStatisticsPeriodMs(1000);
    columnShardStats->SetReportExecutorStatisticsPeriodMs(1000);

    // With LLVM enabled, scan queries calculating column statistics are very slow for some reason
    // (10s of seconds), so we disable it.
    Settings->AppConfig->MutableTableServiceConfig()->SetEnableKqpScanQueryUseLlvm(false);

    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableStatistics(true);
    featureFlags.SetEnableColumnStatistics(true);
    featureFlags.SetEnableAnalyzeLongRunningOperation(true);
    Settings->SetFeatureFlags(featureFlags);

    modifySettings(*Settings);

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

    Server->GetRuntime()->SetLogPriority(NKikimrServices::STATISTICS, NActors::NLog::PRI_DEBUG);
}

TTestEnv::~TTestEnv() {
    Driver->Stop(true);

    if (ThreadPoolStarted) {
        ThreadPool.Stop();
    }

    Server->ShutdownGRpc();
}

namespace {

void WaitForDatabaseRunning(TTestEnv& env, const TString& path);
void WaitForPath(TTestEnv& env, const TString& path, NSchemeCache::TSchemeCacheNavigate::EOp operation);

} // anonymous namespace

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
    WaitForDatabaseRunning(env, fullDbName);
    WaitForPath(env, fullDbName, NSchemeCache::TSchemeCacheNavigate::EOp::OpList);

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
    WaitForDatabaseRunning(env, fullDbName);
    WaitForPath(env, fullDbName, NSchemeCache::TSchemeCacheNavigate::EOp::OpList);

    return fullDbName;
}

namespace {

void WaitForDatabaseRunning(TTestEnv& env, const TString& path) {
    auto& runtime = *env.GetServer().GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();
    Ydb::Cms::GetDatabaseStatusResult lastResult;

    for (ui32 attempt = 0; attempt < 300; ++attempt) {
        auto request = std::make_unique<NConsole::TEvConsole::TEvGetTenantStatusRequest>();
        request->Record.MutableRequest()->set_path(path);
        runtime.SendToPipe(MakeConsoleID(), sender, request.release(), 0, GetPipeConfigWithRetries());

        auto response = runtime.GrabEdgeEvent<NConsole::TEvConsole::TEvGetTenantStatusResponse>(
            sender, TDuration::Seconds(30));
        UNIT_ASSERT_C(response, "Timed out waiting for database status: " << path);
        response->Get()->Record.GetResponse().operation().result().UnpackTo(&lastResult);
        if (lastResult.state() == Ydb::Cms::GetDatabaseStatusResult::RUNNING) {
            return;
        }

        if (env.GetServer().GetSettings().UseRealThreads) {
            Sleep(TDuration::MilliSeconds(100));
        } else {
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
    }

    UNIT_FAIL("Database " << path << " is not running, last status: " << lastResult.DebugString());
}

void WaitForPath(TTestEnv& env, const TString& path, NSchemeCache::TSchemeCacheNavigate::EOp operation) {
    auto& runtime = *env.GetServer().GetRuntime();
    const auto sender = runtime.AllocateEdgeActor();

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvRequest = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TEvResponse = TEvTxProxySchemeCache::TEvNavigateKeySetResult;

    TNavigate::EStatus lastStatus = TNavigate::EStatus::Unknown;
    for (ui32 attempt = 0; attempt < 300; ++attempt) {
        auto request = std::make_unique<TNavigate>();
        auto& entry = request->ResultSet.emplace_back();
        entry.Path = SplitPath(path);
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        entry.Operation = operation;
        entry.ShowPrivatePath = true;
        runtime.Send(MakeSchemeCacheID(), sender, new TEvRequest(request.release()));

        auto ev = runtime.GrabEdgeEventRethrow<TEvResponse>(sender);
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get());
        std::unique_ptr<TNavigate> response(ev->Get()->Request.Release());
        UNIT_ASSERT_VALUES_EQUAL(response->ResultSet.size(), 1);
        lastStatus = response->ResultSet.front().Status;
        if (lastStatus == TNavigate::EStatus::Ok) {
            return;
        }

        if (env.GetServer().GetSettings().UseRealThreads) {
            Sleep(TDuration::MilliSeconds(100));
        } else {
            runtime.SimulateSleep(TDuration::MilliSeconds(100));
        }
    }

    UNIT_FAIL("Path " << path << " is not available, last navigation status: " << static_cast<ui32>(lastStatus));
}

} // anonymous namespace

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

static TString GetIssuesString(const Ydb::Operations::Operation& operation) {
    NYql::TIssues issues;
    NYql::IssuesFromMessage(operation.issues(), issues);
    return issues.ToString();
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
        UNIT_ASSERT_VALUES_EQUAL_C(
            response.operation().status(), Ydb::StatusIds::SUCCESS,
            GetIssuesString(response.operation()));
    }
    return response.operation().status();
}

const std::vector<TColumnDesc>& SimpleColumnList() {
    static const std::vector<TColumnDesc> ret {
        {
            .Name = "Value",
            .TypeId = NScheme::NTypeIds::String,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                row.add_items()->set_bytes_value(ToString(key % 10));
            },
        },
    };

    return ret;
}

const std::vector<TColumnDesc>& MultiColumnValueColumns() {
    static const std::vector<TColumnDesc> ret {
        {
            .Name = "Value1",
            .TypeId = NScheme::NTypeIds::String,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                row.add_items()->set_bytes_value(ToString(key % 10));
            },
        },
        {
            .Name = "Value2",
            .TypeId = NScheme::NTypeIds::String,
            .AddValue = [](ui64 key, Ydb::Value& row) {
                row.add_items()->set_bytes_value(ToString(key % 20));
            },
        },
    };

    return ret;
}

void CreateUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        )
        WITH ( UNIFORM_PARTITIONS = 4 );
    )", databaseName.c_str(), tableName.c_str()));
}

void PrepareUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    CreateUniformTable(env, databaseName, tableName);

    TStringBuilder replace;
    replace << Sprintf("REPLACE INTO `Root/%s/%s` (Key, Value) VALUES ",
        databaseName.c_str(), tableName.c_str());
    for (ui32 i = 0; i < 4; ++i) {
        if (i > 0) {
            replace << ", ";
        }
        ui64 value = 4000000000000000000ull * (i + 1);
        replace << Sprintf("(%" PRIu64 "ul, \"%" PRIu64 "\")", value, value);
    }
    replace << ";";
    ExecuteYqlScript(env, replace);
}

namespace {

// Builds a TTableInfo from a path, resolving shard IDs and path ID.
// Used by all table-creation helpers to avoid duplicating this boilerplate.
TTableInfo MakeTableInfo(TTestActorRuntime& runtime, const TString& databaseName,
    const TString& tableName, bool columnShard) {
    TTableInfo tableInfo;
    tableInfo.Path = Sprintf("/Root/%s/%s", databaseName.c_str(), tableName.c_str());
    if (columnShard) {
        tableInfo.ShardIds = GetColumnTableShards(runtime, runtime.AllocateEdgeActor(), tableInfo.Path);
    } else {
        tableInfo.ShardIds = GetTableShards(runtime, runtime.AllocateEdgeActor(), tableInfo.Path);
    }
    tableInfo.PathId = ResolvePathId(runtime, tableInfo.Path, &tableInfo.DomainKey, &tableInfo.SaTabletId);
    return tableInfo;
}

} // anonymous namespace

TTableInfo CreateColumnTable(TTestEnv& env, const TString& databaseName, const TString& tableName,
    int shardCount, const std::vector<TColumnDesc>& valueColumns)
{
    auto fullTableName = Sprintf("Root/%s/%s", databaseName.c_str(), tableName.c_str());
    auto& runtime = *env.GetServer().GetRuntime();

    TStringBuilder createTable;
    createTable << "CREATE TABLE `" << fullTableName <<"` (Key Uint64 NOT NULL";
    for (const auto& col : valueColumns) {
        createTable << ", " << col.Name << " " << NScheme::TypeName(col.TypeId);
    }
    createTable << ", PRIMARY KEY (Key)) "
        << "PARTITION BY HASH(Key) "
        << "WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = " << shardCount << ");";

    ExecuteYqlScript(env, createTable);
    WaitForPath(env, "/" + fullTableName, NSchemeCache::TSchemeCacheNavigate::EOp::OpPath);

    return MakeTableInfo(runtime, databaseName, tableName, true);
}

void InsertDataIntoTable(
        TTestEnv& env, const TString& databaseName, const TString& tableName,
        size_t rowCount, const std::vector<TColumnDesc>& valueColumns) {
    auto fullTableName = Sprintf("Root/%s/%s", databaseName.c_str(), tableName.c_str());
    auto& runtime = *env.GetServer().GetRuntime();

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
    for (const auto& col : valueColumns) {
        auto* reqColType = reqRowType->add_members();
        reqColType->set_name(col.Name);
        reqColType->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(
            static_cast<Ydb::Type_PrimitiveTypeId>(col.TypeId));
    }

    auto* reqRows = rows->mutable_value();
    for (ui64 key = 0; key < rowCount; ++key) {
        auto* row = reqRows->add_items();
        row->add_items()->set_uint64_value(key);
        for (const auto& col : valueColumns) {
            col.AddValue(key, *row);
        }
    }

    auto future = NRpcService::DoLocalRpc<TEvBulkUpsertRequest>(
        std::move(request), "", "", runtime.GetActorSystem(0));
    auto response = runtime.WaitFuture(std::move(future));

    UNIT_ASSERT(response.operation().ready());
    UNIT_ASSERT_VALUES_EQUAL_C(
        response.operation().status(), Ydb::StatusIds::SUCCESS,
        GetIssuesString(response.operation()));
    env.GetController()->WaitActualization(TDuration::Seconds(1));
}

TTableInfo PrepareColumnTable(TTestEnv& env, const TString& databaseName, const TString& tableName,
    int shardCount)
{
    auto info = CreateColumnTable(env, databaseName, tableName, shardCount);
    InsertDataIntoTable(env, databaseName, tableName, ColumnTableRowsNumber);
    return info;
}

TTableInfo PrepareColumnTableWithIndexes(TTestEnv& env, const TString& databaseName, const TString& tableName,
    int shardCount)
{
    auto info = CreateColumnTable(env, databaseName, tableName, shardCount);

    auto fullTableName = Sprintf("Root/%s/%s", databaseName.c_str(), tableName.c_str());
    auto& runtime = *env.GetServer().GetRuntime();

    ExecuteYqlScript(env, Sprintf(R"(
        ALTER OBJECT `%s` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_key, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Key']}`);
    )", fullTableName.c_str()));
    runtime.SimulateSleep(TDuration::MilliSeconds(200));

    ExecuteYqlScript(env, Sprintf(R"(
        ALTER OBJECT `%s` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS,
                    `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`,
                    `COMPACTION_PLANNER.FEATURES`=`{"accumulator_portion_size_limit":0}`);
    )", fullTableName.c_str()));
    runtime.SimulateSleep(TDuration::MilliSeconds(200));

    ExecuteYqlScript(env, Sprintf(R"(
        ALTER OBJECT `%s` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_value, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Value']}`);
    )", fullTableName.c_str()));
    runtime.SimulateSleep(TDuration::MilliSeconds(200));

    InsertDataIntoTable(env, databaseName, tableName, ColumnTableRowsNumber);

    env.GetController()->WaitActualization(TDuration::Seconds(1));

    return info;
}

TTableInfo PrepareMultiColumnColumnTable(
        TTestEnv& env, const TString& databaseName, const TString& tableName, int shardCount) {
    auto fullTableName = Sprintf("Root/%s/%s", databaseName.c_str(), tableName.c_str());
    auto& runtime = *env.GetServer().GetRuntime();

    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `%s` (
            Key Uint64 NOT NULL,
            Value1 String,
            Value2 String,
            PRIMARY KEY (Key),
            STATISTICS multi_stat ON (Value1, Value2) WITH (COUNT_MIN_SKETCH)
        )
        PARTITION BY HASH(Key)
        WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = %d);
    )", fullTableName.c_str(), shardCount));
    runtime.SimulateSleep(TDuration::Seconds(1));

    InsertDataIntoTable(env, databaseName, tableName, ColumnTableRowsNumber, MultiColumnValueColumns());

    return MakeTableInfo(runtime, databaseName, tableName, true);
}

TTableInfo PrepareMultiColumnUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    auto& runtime = *env.GetServer().GetRuntime();

    ExecuteYqlScript(env, Sprintf(R"(
        CREATE TABLE `Root/%s/%s` (
            Key Uint64,
            Value1 String,
            Value2 String,
            PRIMARY KEY (Key),
            STATISTICS multi_stat ON (Value1, Value2) WITH (COUNT_MIN_SKETCH)
        )
        WITH ( UNIFORM_PARTITIONS = 4 );
    )", databaseName.c_str(), tableName.c_str()));

    InsertDataIntoTable(env, databaseName, tableName, ColumnTableRowsNumber, MultiColumnValueColumns());

    return MakeTableInfo(runtime, databaseName, tableName, false);
}

TTableInfo PrepareUniformTableWithData(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    CreateUniformTable(env, databaseName, tableName);
    InsertDataIntoTable(env, databaseName, tableName, ColumnTableRowsNumber);

    auto& runtime = *env.GetServer().GetRuntime();
    return MakeTableInfo(runtime, databaseName, tableName, false);
}

TTableInfo PrepareTable(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard) {
    if (columnShard) {
        return PrepareColumnTable(env, databaseName, tableName, 1);
    }
    return PrepareUniformTableWithData(env, databaseName, tableName);
}

TTableInfo PrepareTableWithIndexes(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard) {
    if (columnShard) {
        return PrepareColumnTableWithIndexes(env, databaseName, tableName, 4);
    }
    return PrepareUniformTableWithData(env, databaseName, tableName);
}

TTableInfo PrepareMultiColumnTable(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard) {
    if (columnShard) {
        return PrepareMultiColumnColumnTable(env, databaseName, tableName);
    }
    return PrepareMultiColumnUniformTable(env, databaseName, tableName);
}

TTableInfo CreateEmptyTable(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard) {
    if (columnShard) {
        return CreateColumnTable(env, databaseName, tableName, 4);
    }
    CreateUniformTable(env, databaseName, tableName);
    auto& runtime = *env.GetServer().GetRuntime();
    return MakeTableInfo(runtime, databaseName, tableName, false);
}

void ValidateStatistics(TTestActorRuntime& runtime, const TPathId& pathId, ui64 N) {
    // TAnalyzeActor builds count-min sketches based on column cardinality, not
    // index declarations, so both ColumnShard and DataShard produce the same
    // statistics for the same data. Key column (tag 1): high cardinality
    // (N distinct values) -> ndv >= 0.8 * n -> no CMS. Value column (tag 2):
    // low cardinality (10 distinct values, Value = key % 10) -> CMS with probes.
    std::vector<TCountMinSketchProbes> expected = {
        {.Tag = 1, .Probes = std::nullopt},
        {.Tag = 2, .Probes = {{{"1", N / 10}, {"2", N / 10}, {"10", 0}}}},
    };
    CheckCountMinSketch(runtime, pathId, expected);
}

void DropTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    ExecuteYqlScript(env, Sprintf(R"(
        DROP TABLE `Root/%s/%s`;
    )", databaseName.c_str(), tableName.c_str()));
}

std::vector<TResponse> GetStatistics(
        TTestActorRuntime& runtime, const TPathId& pathId, EStatType statType,
        const std::vector<std::optional<ui32>>& columnTags, ui32 nodeIdx) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(nodeIdx));

    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = statType;
    for (auto tag : columnTags) {
        TRequest req{ .PathId = pathId };
        if (tag) {
            req.ColumnTags = *tag;
        }
        evGet->StatRequests.push_back(std::move(req));
    }

    auto sender = runtime.AllocateEdgeActor(nodeIdx);
    runtime.Send(statServiceId, sender, evGet.release(), nodeIdx, true);
    auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

    UNIT_ASSERT(evResult);
    UNIT_ASSERT(evResult->Get());
    return std::move(evResult->Get()->StatResponses);
}


void CheckCountMinSketch(
        TTestActorRuntime& runtime, const TPathId& pathId,
        const std::vector<TCountMinSketchProbes>& expected) {
    std::vector<std::optional<ui32>> columnTags;
    for (auto item : expected) {
        columnTags.push_back(item.Tag);
    }
    auto responses = GetStatistics(runtime, pathId, EStatType::COUNT_MIN_SKETCH, columnTags);
    UNIT_ASSERT_VALUES_EQUAL(responses.size(), expected.size());

    for (size_t i = 0; i < responses.size(); ++i) {
        const auto& stat = responses[i];
        const auto& probes = expected[i].Probes;
        if (probes) {
            UNIT_ASSERT(stat.Success);

            auto countMin = stat.CountMinSketch.CountMin.get();
            UNIT_ASSERT(countMin != nullptr);

            for (const auto& item : *probes) {
                auto probe = countMin->Probe(item.Value.data(), item.Value.size());
                UNIT_ASSERT_VALUES_EQUAL(item.Expected, probe);
            }
        } else {
            UNIT_ASSERT(!stat.Success);
        }
    }
}

static TString ExecuteYqlScriptFetchBytes(TTestEnv& env, const TString& script) {
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
    UNIT_ASSERT_VALUES_EQUAL_C(
        response.operation().status(), Ydb::StatusIds::SUCCESS,
        GetIssuesString(response.operation()));

    Ydb::Scripting::ExecuteYqlResult result;
    UNIT_ASSERT(response.operation().result().UnpackTo(&result));
    UNIT_ASSERT_VALUES_EQUAL(result.result_sets_size(), 1);
    const auto& resultSet = result.result_sets(0);
    UNIT_ASSERT_VALUES_EQUAL(resultSet.rows_size(), 1);
    return resultSet.rows(0).items(0).bytes_value();
}

static void CheckMultiColumnCountMinSketch(
        TTestActorRuntime& runtime, const TPathId& pathId,
        const std::vector<ui32>& columnTags,
        const std::optional<std::vector<TCountMinSketchProbes::TProbe>>& probes) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(1));

    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = EStatType::COUNT_MIN_SKETCH;
    evGet->StatRequests.push_back(TRequest{ .PathId = pathId, .ColumnTags = columnTags });

    auto sender = runtime.AllocateEdgeActor(1);
    runtime.Send(statServiceId, sender, evGet.release(), 1, true);
    auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

    UNIT_ASSERT(evResult);
    UNIT_ASSERT(evResult->Get());
    auto& responses = evResult->Get()->StatResponses;
    UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
    const auto& stat = responses[0];

    if (probes) {
        UNIT_ASSERT(stat.Success);

        auto countMin = stat.CountMinSketch.CountMin.get();
        UNIT_ASSERT(countMin != nullptr);

        for (const auto& item : *probes) {
            auto probe = countMin->Probe(item.Value.data(), item.Value.size());
            UNIT_ASSERT_VALUES_EQUAL(item.Expected, probe);
        }
    } else {
        UNIT_ASSERT(!stat.Success);
    }
}

void CheckMultiColumnStatisticsProbes(
        TTestEnv& env, TTestActorRuntime& runtime, const TPathId& pathId,
        const std::vector<ui32>& columnTags) {
    auto present = ExecuteYqlScriptFetchBytes(env,
        "SELECT StablePickle(AsTuple(Just(\"0\"), Just(\"0\"))) AS p;");
    auto absent = ExecuteYqlScriptFetchBytes(env,
        "SELECT StablePickle(AsTuple(Just(\"0\"), Just(\"1\"))) AS p;");

    // The cost-based optimizer will build probe keys in C++ via StablePickleTuple() (it cannot run a
    // YQL script), so cross-check that the helper reproduces the exact StablePickle bytes here.
    const auto str = [](TStringBuf value) {
        return TPickleColumnValue{.Type = NScheme::NTypeIds::String, .Value = TString(value)};
    };
    UNIT_ASSERT_VALUES_EQUAL(present, StablePickleTuple({str("0"), str("0")}));
    UNIT_ASSERT_VALUES_EQUAL(absent, StablePickleTuple({str("0"), str("1")}));

    CheckMultiColumnCountMinSketch(runtime, pathId, columnTags,
        { { { present, ColumnTableRowsNumber / 20 }, { absent, 0 } } });
}

namespace {

// Single-quoted YQL string literal with backslash/quote escaping.
TString YqlQuote(TStringBuf value) {
    TStringBuilder out;
    out << '\'';
    for (char c : value) {
        if (c == '\\' || c == '\'') {
            out << '\\';
        }
        out << c;
    }
    out << '\'';
    return out;
}

TString RenderYqlLiteral(const TPickleColumnValue& col) {
    Y_ENSURE(col.Value, "CheckStablePickleTupleMatchesYql expects present values");
    const TString quoted = YqlQuote(*col.Value);
    TString literal;
    if (col.Type == NScheme::NTypeIds::Decimal) {
        literal = TStringBuilder() << "Decimal(" << quoted << ","
            << ui32(col.DecimalPrecision) << "," << ui32(col.DecimalScale) << ")";
    } else {
        // The YQL type-constructor callable name equals the data type's name (e.g. "Int32", "Uuid").
        const TStringBuf ctorName = NUdf::GetDataTypeInfo(NUdf::GetDataSlot(col.Type)).Name;
        literal = TStringBuilder() << ctorName << "(" << quoted << ")";
    }
    return col.Nullable ? (TStringBuilder() << "Just(" << literal << ")") : literal;
}

} // anonymous namespace

void CheckStablePickleTupleMatchesYql(TTestEnv& env, const std::vector<TPickleColumnValue>& columns) {
    TStringBuilder script;
    script << "SELECT StablePickle(AsTuple(";
    for (size_t i = 0; i < columns.size(); ++i) {
        if (i > 0) {
            script << ", ";
        }
        script << RenderYqlLiteral(columns[i]);
    }
    script << ")) AS p;";

    const TString expected = ExecuteYqlScriptFetchBytes(env, script);
    const TString actual = StablePickleTuple(columns);
    UNIT_ASSERT_VALUES_EQUAL_C(expected, actual, "StablePickle mismatch for: " << script);
}

TAnalyzedTable::TAnalyzedTable(const TPathId& pathId)
    : PathId(pathId)
{}

TAnalyzedTable::TAnalyzedTable(const TPathId& pathId, const std::vector<ui32>& columnTags)
    : PathId(pathId)
    , ColumnTags(columnTags)
{}

void TAnalyzedTable::ToProto(NKikimrStat::TTable& tableProto) const {
    PathId.ToProto(tableProto.MutablePathId());
    tableProto.MutableColumnTags()->Add(ColumnTags.begin(), ColumnTags.end());
}

std::unique_ptr<TEvStatistics::TEvAnalyze> MakeAnalyzeRequest(
        const std::vector<TAnalyzedTable>& tables,
        const TString operationId, TString databaseName) {
    auto ev = std::make_unique<TEvStatistics::TEvAnalyze>();
    NKikimrStat::TEvAnalyze& record = ev->Record;
    record.SetOperationId(operationId);
    record.SetDatabase(std::move(databaseName));
    record.AddTypes(NKikimrStat::EColumnStatisticType::TYPE_COUNT_MIN_SKETCH);
    for (const TAnalyzedTable& table : tables)
        table.ToProto(*record.AddTables());
    return ev;
}

NKikimrStat::TEvAnalyzeResponse Analyze(
        TTestActorRuntime& runtime, ui64 saTabletId, const std::vector<TAnalyzedTable>& tables,
        const TString operationId, TString databaseName,
        NKikimrStat::TEvAnalyzeResponse::EStatus expectedStatus) {
    auto ev = MakeAnalyzeRequest(tables, operationId, databaseName);

    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(saTabletId, sender, ev.release());
    auto evResponse = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);

    const auto& record = evResponse->Get()->Record;
    UNIT_ASSERT_VALUES_EQUAL(record.GetOperationId(), operationId);
    UNIT_ASSERT_VALUES_EQUAL(record.GetStatus(), expectedStatus);
    return record;
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

i64 GetBackgroundAnalyzeCompletedCount(TTestActorRuntime& runtime) {
    auto counters = runtime.GetAppData(1).Counters;
    auto completedCounter = GetServiceCounters(counters, "statistics")
        ->GetSubgroup("subsystem", "background_analyze")
        ->GetSubgroup("status", "completed")
        ->FindCounter("BackgroundAnalyze");
    return completedCounter ? completedCounter->Val() : 0;
}

void WaitForBackgroundAnalyzeCompleted(TTestActorRuntime& runtime, i64 expectedCount) {
    while (GetBackgroundAnalyzeCompletedCount(runtime) < expectedCount) {
        runtime.SimulateSleep(TDuration::MilliSeconds(100));
    }
}

// Waits for the background-analyze completed counter to stop incrementing
// for at least stableSecs seconds, ensuring all race-condition-triggered
// traversals have finished. Returns the final counter value.
i64 WaitForBackgroundAnalyzeToStabilize(TTestActorRuntime& runtime, size_t timeoutSec, size_t stableSecs) {
    auto prev = GetBackgroundAnalyzeCompletedCount(runtime);
    size_t stable = 0;
    for (size_t i = 0; i < timeoutSec; ++i) {
        runtime.SimulateSleep(TDuration::Seconds(1));
        auto curr = GetBackgroundAnalyzeCompletedCount(runtime);
        if (curr == prev) {
            ++stable;
            if (stable >= stableSecs) {
                return curr;
            }
        } else {
            stable = 0;
            prev = curr;
        }
    }
    return prev;
}

// Ensures the primary background collection has fully completed for the given
// table. We use the BackgroundAnalyze completed counter, which is monotonically
// increasing and never misses a traversal. WaitForBackgroundAnalyzeCompleted
// waits for at least expectedCount traversals to finish;
// WaitForBackgroundAnalyzeToStabilize then waits for the counter to stop
// incrementing, ensuring all race-condition-triggered spurious traversals
// have also completed.
//
// The columnShard parameter is accepted for API symmetry with
// ValidateStatistics but does not change the waiting logic.
i64 WaitForPrimaryCollection(
    TTestActorRuntime& runtime, const TPathId& /*pathId*/,
    ui64 /*expectedRowCount*/, i64 expectedCount, bool /*columnShard*/) {
    WaitForBackgroundAnalyzeCompleted(runtime, expectedCount);
    return WaitForBackgroundAnalyzeToStabilize(runtime);
}

void WaitForSchemeShardStatsUpdate(
    TTestActorRuntime& runtime, ui64 ssTabletId, bool requireFull)
{
    bool statsUpdateSent = false;
    auto sendObserver = runtime.AddObserver<TEvStatistics::TEvSchemeShardStats>([&](auto& ev) {
        if (ev->Get()->Record.GetSchemeShardId() != ssTabletId) {
            return;
        }
        if (!requireFull) {
            statsUpdateSent = true;
            return;
        }
        NKikimrStat::TSchemeShardStats statRecord;
        if (statRecord.ParseFromString(ev->Get()->Record.GetStats())
                && statRecord.GetAreAllStatsFull())
        {
            statsUpdateSent = true;
        }
    });
    runtime.WaitFor(
        requireFull ? "full TEvSchemeShardStats from SchemeShard"
                    : "TEvSchemeShardStats from SchemeShard",
        [&]{ return statsUpdateSent; });
}

ui64 GetRowCount(TTestActorRuntime& runtime, ui32 nodeIndex, TPathId pathId) {
    auto statServiceId = NStat::MakeStatServiceID(runtime.GetNodeId(nodeIndex));
    NStat::TRequest req;
    req.PathId = pathId;

    auto evGet = std::make_unique<TEvStatistics::TEvGetStatistics>();
    evGet->StatType = NStat::EStatType::SIMPLE;
    evGet->StatRequests.push_back(req);

    auto sender = runtime.AllocateEdgeActor(nodeIndex);
    runtime.Send(statServiceId, sender, evGet.release(), nodeIndex, true);
    auto evResult = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvGetStatisticsResult>(sender);

    UNIT_ASSERT(evResult);
    UNIT_ASSERT(evResult->Get());
    UNIT_ASSERT(evResult->Get()->StatResponses.size() == 1);

    auto rsp = evResult->Get()->StatResponses[0];
    auto stat = rsp.Simple;

    return stat.RowCount;
}

void ValidateRowCount(TTestActorRuntime& runtime, ui32 nodeIndex, TPathId pathId, size_t expectedRowCount) {
    ui64 rowCount = 0;
    while (rowCount == 0) {
        rowCount = GetRowCount(runtime, nodeIndex, pathId);

        if (rowCount != 0) {
            UNIT_ASSERT_VALUES_EQUAL(rowCount, expectedRowCount);
            break;
        }

        runtime.SimulateSleep(TDuration::Seconds(1));
    }
}

void WaitForRowCount(
        TTestActorRuntime& runtime, ui32 nodeIndex,
        TPathId pathId, size_t expectedRowCount, size_t timeoutSec) {
    ui64 lastRowCount = 0;
    for (size_t i = 0; i <= timeoutSec; ++i) {
        lastRowCount = GetRowCount(runtime, nodeIndex, pathId);
        if (i % 5 == 0) {
            Cerr << "row count: " << lastRowCount << " (expected: " << expectedRowCount << ")\n";
        }
        if (lastRowCount == expectedRowCount) {
            return;
        }
        runtime.SimulateSleep(TDuration::Seconds(1));
    }
    UNIT_ASSERT_C(false, "timed out, last row count: " << lastRowCount);
}

NKikimrAnalyzeOp::TEvListResponse TestListAnalyzeOps(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, ui64 pageSize, const TString& pageToken,
    Ydb::StatusIds::StatusCode expectedStatus)
{
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(saTabletId, sender,
        new TEvStatistics::TEvAnalyzeOpListRequest(dbName, pageSize, pageToken));
    auto ev = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeOpListResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Record.GetStatus(), expectedStatus, ev->Get()->Record.ShortDebugString());
    return ev->Get()->Record;
}

NKikimrAnalyzeOp::TEvGetResponse TestGetAnalyzeOp(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, const TString& binaryOpId,
    Ydb::StatusIds::StatusCode expectedStatus)
{
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(saTabletId, sender,
        new TEvStatistics::TEvAnalyzeOpGetRequest(dbName, binaryOpId));
    auto ev = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeOpGetResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Record.GetStatus(), expectedStatus, ev->Get()->Record.ShortDebugString());
    return ev->Get()->Record;
}

NKikimrAnalyzeOp::TEvCancelResponse TestCancelAnalyzeOp(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, const TString& binaryOpId,
    Ydb::StatusIds::StatusCode expectedStatus)
{
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(saTabletId, sender,
        new TEvStatistics::TEvAnalyzeOpCancelRequest(dbName, binaryOpId));
    auto ev = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeOpCancelResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Record.GetStatus(), expectedStatus, ev->Get()->Record.ShortDebugString());
    return ev->Get()->Record;
}

NKikimrAnalyzeOp::TEvForgetResponse TestForgetAnalyzeOp(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, const TString& binaryOpId,
    Ydb::StatusIds::StatusCode expectedStatus)
{
    auto sender = runtime.AllocateEdgeActor();
    runtime.SendToPipe(saTabletId, sender,
        new TEvStatistics::TEvAnalyzeOpForgetRequest(dbName, binaryOpId));
    auto ev = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeOpForgetResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL_C(ev->Get()->Record.GetStatus(), expectedStatus, ev->Get()->Record.ShortDebugString());
    return ev->Get()->Record;
}

} // NStat
} // NKikimr
