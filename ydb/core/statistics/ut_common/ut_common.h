#pragma once

#include <library/cpp/threading/future/async.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/protos/analyze_operation.pb.h>

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimrStat {
    class TTable;
}

namespace NKikimr {
namespace NStat {

static constexpr ui32 ColumnTableRowsNumber = 1000;

class TTestEnv {
public:
    TTestEnv(ui32 staticNodes = 1, ui32 dynamicNodes = 1, bool useRealThreads = false,
        std::function<void(Tests::TServerSettings&)> modifySettings = [](Tests::TServerSettings&) {});
    ~TTestEnv();

    Tests::TServer& GetServer() const {
        return *Server;
    }

    Tests::TClient& GetClient() const {
        return *Client;
    }

    Tests::TTenants& GetTenants() const {
        return *Tenants;
    }

    NYdb::TDriver& GetDriver() const {
        return *Driver;
    }

    const TString& GetEndpoint() const {
        return Endpoint;
    }

    const Tests::TServerSettings::TPtr GetSettings() const {
        return Settings;
    }

    auto& GetController() {
        return CSController;
    }

    template<typename TFunc>
    auto RunInThreadPool(TFunc&& func) {
        if (!ThreadPoolStarted) {
            ThreadPool.Start();
            ThreadPoolStarted = true;
        }

        auto future = NThreading::Async(std::forward<TFunc>(func), ThreadPool);
        return Server->GetRuntime()->WaitFuture(std::move(future));
    }

private:
    TPortManager PortManager;

    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;
    THolder<Tests::TTenants> Tenants;
    TAdaptiveThreadPool ThreadPool;
    bool ThreadPoolStarted = false;

    TString Endpoint;
    NYdb::TDriverConfig DriverConfig;
    THolder<NYdb::TDriver> Driver;
    NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> CSController;
};

Ydb::StatusIds::StatusCode ExecuteYqlScript(TTestEnv& env, const TString& script, bool mustSucceed = true);

TString CreateDatabase(TTestEnv& env, const TString& databaseName,
    size_t nodeCount = 1, bool isShared = false, const TString& poolName = "hdd1");

TString CreateServerlessDatabase(TTestEnv& env, const TString& databaseName, const TString& sharedName, size_t nodeCount = 0);

struct TColumnDesc {
    TString Name;
    NScheme::TTypeId TypeId;
    std::function<void(ui64, Ydb::Value&)> AddValue; // void AddValue(key, row)
};

// One value column with low-cardinality String.
const std::vector<TColumnDesc>& SimpleColumnList();

// Value1 = Key % 10, Value2 = Key % 20 (both String). For Key in [0, ColumnTableRowsNumber),
// every (Value1, Value2) pair with Value1 == Value2 % 10 occurs exactly
// ColumnTableRowsNumber / 20 times; every other pair never occurs.
const std::vector<TColumnDesc>& MultiColumnValueColumns();

struct TTableInfo {
    std::vector<ui64> ShardIds;
    ui64 SaTabletId;
    TPathId DomainKey;
    TPathId PathId;
    TString Path;
};

// Create empty column table with the requested number of shards.
TTableInfo CreateColumnTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName,
    int shardCount, const std::vector<TColumnDesc>& valueColumns = SimpleColumnList());

void InsertDataIntoTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName,
    size_t rowCount, const std::vector<TColumnDesc>& valueColumns = SimpleColumnList());

// Create a column table and insert ColumnTableRowsNumber rows.
TTableInfo PrepareColumnTable(TTestEnv& env, const TString& databaseName, const TString& tableName, int shardCount);

// Create a column table, enable count-min-sketch column indexes,
// and insert ColumnTableRowsNumber rows with some overlap to trigger compaction.
TTableInfo PrepareColumnTableWithIndexes(TTestEnv& env, const TString& databaseName, const TString& tableName, int shardCount);

// Create a column table with a two-column COUNT_MIN_SKETCH multi-column statistic
// (see MultiColumnValueColumns) and insert ColumnTableRowsNumber rows.
TTableInfo PrepareMultiColumnColumnTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName, int shardCount = 4);

// Create a datashard table with 4 uniform shards and a two-column COUNT_MIN_SKETCH
// multi-column statistic (see MultiColumnValueColumns), and insert ColumnTableRowsNumber rows.
TTableInfo PrepareMultiColumnUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName);

TPathId ResolvePathId(TTestActorRuntime& runtime, const TString& path, TPathId* domainKey = nullptr, ui64* saTabletId = nullptr);

NKikimrScheme::TEvDescribeSchemeResult DescribeTable(
    TTestActorRuntime& runtime, TActorId sender, const TString& path);
TVector<ui64> GetTableShards(TTestActorRuntime& runtime, TActorId sender, const TString &path);
TVector<ui64> GetColumnTableShards(TTestActorRuntime& runtime, TActorId sender,const TString &path);

// Create a datashard table with 4 uniform shards.
void CreateUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName);
// Create a datashard table with 4 uniform shards and insert 1 row into each shard.
void PrepareUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName);

void DropTable(TTestEnv& env, const TString& databaseName, const TString& tableName);

std::vector<TResponse> GetStatistics(
    TTestActorRuntime&, const TPathId&, EStatType,
    const std::vector<std::optional<ui32>>& columnTags, ui32 nodeIdx = 1);

std::shared_ptr<TCountMinSketch> ExtractCountMin(TTestActorRuntime& runtime, const TPathId& pathId, ui64 columnTag = 1);

struct TCountMinSketchProbes {
    struct TProbe {
        TString Value;
        ui64 Expected;
    };

    ui16 Tag;
    // If nullopt, absence of count-min sketch is expected.
    std::optional<std::vector<TProbe>> Probes;
};

void CheckCountMinSketch(
    TTestActorRuntime& runtime, const TPathId& pathId,
    const std::vector<TCountMinSketchProbes>& expected);

// Checks the multi-column count-min sketch produced from data inserted via
// MultiColumnValueColumns: the present pair ("0","0") should have count
// ColumnTableRowsNumber / 20, the absent pair ("0","1") should have count 0.
void CheckMultiColumnStatisticsProbes(
    TTestEnv& env, TTestActorRuntime& runtime, const TPathId& pathId,
    const std::vector<ui32>& columnTags);

struct TAnalyzedTable {
    TPathId PathId;
    std::vector<ui32> ColumnTags;

    TAnalyzedTable(const TPathId& pathId);
    TAnalyzedTable(const TPathId& pathId, const std::vector<ui32>& columnTags);
    void ToProto(NKikimrStat::TTable& tableProto) const;
};

std::unique_ptr<TEvStatistics::TEvAnalyze> MakeAnalyzeRequest(const std::vector<TAnalyzedTable>& tables, const TString operationId = "operationId", TString databaseName = {});

NKikimrStat::TEvAnalyzeResponse Analyze(
    TTestActorRuntime& runtime, ui64 saTabletId, const std::vector<TAnalyzedTable>& table,
    const TString operationId = "operationId", TString databaseName = {},
    NKikimrStat::TEvAnalyzeResponse::EStatus expectedStatus = NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS);
void AnalyzeShard(TTestActorRuntime& runtime, ui64 shardTabletId, const TAnalyzedTable& table);
void AnalyzeStatus(TTestActorRuntime& runtime, TActorId sender, ui64 saTabletId, const TString operationId, const NKikimrStat::TEvAnalyzeStatusResponse::EStatus expectedStatus);

void WaitForSavedStatistics(TTestActorRuntime& runtime, const TPathId& pathId);

ui64 GetRowCount(TTestActorRuntime& runtime, ui32 nodeIndex, TPathId pathId);
void ValidateRowCount(TTestActorRuntime& runtime, ui32 nodeIndex, TPathId pathId, size_t expectedRowCount);
void WaitForRowCount(
    TTestActorRuntime& runtime, ui32 nodeIndex,
    TPathId pathId, size_t expectedRowCount, size_t timeoutSec = 130);

NKikimrAnalyzeOp::TEvListResponse TestListAnalyzeOps(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, ui64 pageSize = 100, const TString& pageToken = {},
    Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS);

NKikimrAnalyzeOp::TEvGetResponse TestGetAnalyzeOp(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, const TString& binaryOpId,
    Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS);

NKikimrAnalyzeOp::TEvCancelResponse TestCancelAnalyzeOp(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, const TString& binaryOpId,
    Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS);

NKikimrAnalyzeOp::TEvForgetResponse TestForgetAnalyzeOp(
    TTestActorRuntime& runtime, ui64 saTabletId,
    const TString& dbName, const TString& binaryOpId,
    Ydb::StatusIds::StatusCode expectedStatus = Ydb::StatusIds::SUCCESS);

} // namespace NStat
} // namespace NKikimr
