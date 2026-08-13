#pragma once

#include <library/cpp/threading/future/async.h>
#include <ydb/core/statistics/common/stable_pickle.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/protos/analyze_operation.pb.h>

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/library/actors/testlib/test_runtime.h>
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

// Create a datashard table with 4 uniform shards and insert ColumnTableRowsNumber rows
// (Value = key % 10, matching PrepareColumnTable's data pattern).
TTableInfo PrepareUniformTableWithData(TTestEnv& env, const TString& databaseName, const TString& tableName);

// Table-type-parameterized dispatchers.
// Each dispatcher selects ONLY the table type at creation time. The data inserted
// and the statistics produced are identical for both types, so twinned test bodies
// call these without if(ColumnShard) branching.

// Create a table of the requested type and insert ColumnTableRowsNumber rows.
TTableInfo PrepareTable(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

// Create a table of the requested type suitable for background traversal tests
// (column tables get CMS indexes; datashard tables get a uniform table with data).
TTableInfo PrepareTableWithIndexes(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

// Create a table of the requested type with a two-column COUNT_MIN_SKETCH
// multi-column statistic and insert ColumnTableRowsNumber rows.
TTableInfo PrepareMultiColumnTable(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

// Create an empty table of the requested type (no rows inserted).
TTableInfo CreateEmptyTable(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

// Type-independent assertion: checks the saved count-min sketch has the expected
// element count / probe values for the data inserted by PrepareTable/PrepareTableWithIndexes.
// TAnalyzeActor builds CMS based on column cardinality, not index declarations,
// so both ColumnShard and DataShard produce the same statistics for the same data.
// Key column (tag 1) has high cardinality -> no CMS (nullopt);
// Value column (tag 2) has low cardinality (10 distinct values) -> CMS with probes.
void ValidateStatistics(TTestActorRuntime& runtime, const TPathId& pathId, ui64 N = ColumnTableRowsNumber);

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

// Asserts that the C++ StablePickleTuple() helper reproduces, byte-for-byte, what YQL's StablePickle(AsTuple(Just(Ctor("value"))...)) does
void CheckStablePickleTupleMatchesYql(TTestEnv& env, const std::vector<TPickleColumnValue>& columns);

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
void AnalyzeStatus(TTestActorRuntime& runtime, TActorId sender, ui64 saTabletId, const TString operationId, const NKikimrStat::TEvAnalyzeStatusResponse::EStatus expectedStatus);

// RAII observer that counts TEvSaveStatisticsQueryResponse events for a
// specific table. The observer is automatically removed on destruction.
class TSaveStatisticsObserver {
public:
    TSaveStatisticsObserver(TTestActorRuntime& runtime, const TPathId& pathId)
        : PathId(pathId)
        , SaveCount(0)
        , Observer(runtime.AddObserver<TEvStatistics::TEvSaveStatisticsQueryResponse>(
            [this](auto& ev) {
                if (ev->Get()->PathId == PathId) {
                    ++SaveCount;
                }
            }))
    {}

    ~TSaveStatisticsObserver() {
        Observer.Remove();
    }

    size_t GetSaveCount() const {
        return SaveCount;
    }

private:
    TPathId PathId;
    size_t SaveCount;
    TTestActorRuntime::TEventObserverHolder Observer;
};

// Returns the current value of the BackgroundAnalyze completed counter.
i64 GetBackgroundAnalyzeCompletedCount(TTestActorRuntime& runtime);

// Polls the BackgroundAnalyze completed counter until it reaches expectedCount.
// This ensures FinishTraversal has completed (all save batches processed and
// LastAnalyzeRowUpdates has been set), avoiding race conditions where a
// second traversal is triggered because TEvSchemeShardStats hasn't been
// processed yet when FinishTraversal runs.
void WaitForBackgroundAnalyzeCompleted(TTestActorRuntime& runtime, i64 expectedCount = 1);

// Waits for the background-analyze completed counter to stop incrementing
// for at least stableSecs seconds, ensuring all race-condition-triggered
// traversals have finished. Returns the final counter value.
i64 WaitForBackgroundAnalyzeToStabilize(TTestActorRuntime& runtime, size_t timeoutSec = 10, size_t stableSecs = 3);

// Ensures the primary background collection has fully completed for the given
// table. Polls the BackgroundAnalyze completed counter until it reaches
// expectedCount, then waits for it to stabilize. Returns the final counter
// value. The columnShard parameter is for API symmetry with ValidateStatistics.
i64 WaitForPrimaryCollection(
    TTestActorRuntime& runtime, const TPathId& pathId,
    ui64 expectedRowCount = ColumnTableRowsNumber, i64 expectedCount = 1,
    bool columnShard = true);

void WaitForSchemeShardStatsUpdate(
    TTestActorRuntime& runtime, ui64 ssTabletId, bool requireFull = false);

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
