#pragma once

#include <library/cpp/threading/future/async.h>
#include <ydb/core/statistics/common/stable_pickle.h>
#include <ydb/core/statistics/events.h>
#include <ydb/core/protos/analyze_operation.pb.h>

#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

#include <cmath>

namespace NKikimrStat {
    class TTable;
}

namespace NKikimr {
namespace NStat {

static constexpr ui32 ColumnTableRowsNumber = 1000;

// Rank-error bound for approximate EQ_HEIGHT: 2 * n / (oversampleFactor * ceil(cbrt(n))).
inline ui64 EqHeightDesignRankErrorBound(ui64 n = ColumnTableRowsNumber, ui32 oversampleFactor = 8) {
    const ui32 b = static_cast<ui32>(std::ceil(std::cbrt(static_cast<double>(n))));
    return 2 * n / (static_cast<ui64>(oversampleFactor) * b);
}

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

// DataShard table split at PARTITION_AT_KEYS (4 shards), two-column COUNT_MIN_SKETCH.
TTableInfo PrepareMultiColumnUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName);

// Create a column table with a two-column EQ_HEIGHT_HISTOGRAM multi-column statistic
// (see MultiColumnValueColumns) and insert ColumnTableRowsNumber rows.
TTableInfo PrepareMultiColumnEqHeightColumnTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName, int shardCount = 4);

// DataShard table split at PARTITION_AT_KEYS (4 shards), two-column EQ_HEIGHT_HISTOGRAM.
TTableInfo PrepareMultiColumnEqHeightUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName);

// Create a table of the requested type with a two-column EQ_HEIGHT_HISTOGRAM
// multi-column statistic and insert ColumnTableRowsNumber rows.
TTableInfo PrepareMultiColumnEqHeightTable(TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

// STATISTICS without WITH on (Value1, Value2).
TTableInfo PrepareMultiColumnAllTypesTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

// STATISTICS without WITH on the primary key.
TTableInfo PrepareDeclaredPkAllTypesTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

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

// Multi-column variant: requests a single statistic under a multi-column
// TColumnTags(vector<ui32>), which serializes to a comma-joined key. Use this
// for multi-column statistics (e.g. EQ_HEIGHT_HISTOGRAM) where the single-column
// GetStatistics above would construct a TColumnTags(ui32) that only collides
// with the multi-column key by coincidence of SerializeColumnTags.
std::vector<TResponse> GetStatisticsMultiColumn(
    TTestActorRuntime&, const TPathId&, EStatType,
    const std::vector<ui32>& columnTags, ui32 nodeIdx = 1);

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

// A probe for EQ_HEIGHT_HISTOGRAM: `Key` is a pre-built presort key (use
// MakeUint64PresortKey for a single Uint64 column), and `Expected` is the exact
// value EstimateLessOrEqual must return.
struct TEqHeightHistogramProbe {
    TString Key;
    ui64 Expected;
};

// Build a presort key for a single Uint64 column value using TPresortEncoder.
// The encoding is memcomparable: memcmp on the result equals value order.
// isOptional must match what PresortKey sees in the ANALYZE query: YQL wraps
// nullable column references in Optional (DataShard PK columns in these tests),
// while ColumnShard NOT NULL PK columns are encoded as non-optional.
inline TString MakeUint64PresortKey(ui64 value, bool isOptional = true) {
    NMiniKQL::TPresortEncoder enc;
    enc.AddType(NYql::NUdf::EDataSlot::Uint64, isOptional, /*isDesc=*/false);
    enc.Start();
    auto pod = NYql::NUdf::TUnboxedValuePod(value);
    enc.Encode(isOptional ? pod.MakeOptional() : pod);
    TStringBuf buf = enc.Finish();
    return TString(buf.data(), buf.size());
}

// Optional NULL for a single Uint64 column. Not valid for NOT NULL columns.
inline TString MakeNullPresortKey() {
    NMiniKQL::TPresortEncoder enc;
    enc.AddType(NYql::NUdf::EDataSlot::Uint64, /*isOptional=*/true, /*isDesc=*/false);
    enc.Start();
    enc.Encode(NYql::NUdf::TUnboxedValuePod()); // empty == NULL
    TStringBuf buf = enc.Finish();
    return TString(buf.data(), buf.size());
}

// Optional-null tuple of `n` String columns. Do not use MakeNullPresortKey() here.
inline TString MakeNullStringTuplePresortKey(size_t n) {
    UNIT_ASSERT_C(n > 0, "MakeNullStringTuplePresortKey: empty tuple");
    NMiniKQL::TPresortEncoder enc;
    for (size_t i = 0; i < n; ++i) {
        enc.AddType(NYql::NUdf::EDataSlot::String, /*isOptional=*/true, /*isDesc=*/false);
    }
    enc.Start();
    for (size_t i = 0; i < n; ++i) {
        enc.Encode(NYql::NUdf::TUnboxedValuePod());
    }
    TStringBuf buf = enc.Finish();
    return TString(buf.data(), buf.size());
}

// Presort key for a String tuple. isOptional follows column nullability
// (same as MakeUint64PresortKey). Strings must fit in Embedded (<= 14 bytes).
inline TString MakeStringTuplePresortKey(const std::vector<TStringBuf>& values, bool isOptional = true) {
    NMiniKQL::TPresortEncoder enc;
    for (size_t i = 0; i < values.size(); ++i) {
        enc.AddType(NYql::NUdf::EDataSlot::String, isOptional, /*isDesc=*/false);
    }
    enc.Start();
    for (auto value : values) {
        // TUnboxedValuePod::Embedded stores strings up to 14 bytes inline.
        // Longer strings require a heap-backed TStringValue, which needs a
        // TScopedAlloc; guard against silent corruption if a future test
        // uses a longer probe string.
        UNIT_ASSERT_C(value.size() <= 14,
            "MakeStringTuplePresortKey: string of " << value.size()
            << " bytes exceeds the 14-byte Embedded limit");
        auto pod = NYql::NUdf::TUnboxedValuePod::Embedded(
            NYql::NUdf::TStringRef(value.data(), value.size()));
        enc.Encode(isOptional ? pod.MakeOptional() : pod);
    }
    TStringBuf buf = enc.Finish();
    return TString(buf.data(), buf.size());
}

inline TString MakeUint64StringPresortKey(ui64 key, TStringBuf value, bool isOptional = true) {
    NMiniKQL::TPresortEncoder enc;
    enc.AddType(NYql::NUdf::EDataSlot::Uint64, isOptional, /*isDesc=*/false);
    enc.AddType(NYql::NUdf::EDataSlot::String, isOptional, /*isDesc=*/false);
    enc.Start();
    auto keyPod = NYql::NUdf::TUnboxedValuePod(key);
    enc.Encode(isOptional ? keyPod.MakeOptional() : keyPod);
    UNIT_ASSERT_C(value.size() <= 14,
        "MakeUint64StringPresortKey: string of " << value.size()
        << " bytes exceeds the 14-byte Embedded limit");
    auto valuePod = NYql::NUdf::TUnboxedValuePod::Embedded(
        NYql::NUdf::TStringRef(value.data(), value.size()));
    enc.Encode(isOptional ? valuePod.MakeOptional() : valuePod);
    TStringBuf buf = enc.Finish();
    return TString(buf.data(), buf.size());
}

// Fetch EQ_HEIGHT via the stat service. Optional args add assertions
// (count, min buckets, probes, IsExact, true-rank vs sourceKeys).
void CheckEqHeightHistogram(
    TTestActorRuntime& runtime, const TPathId& pathId,
    const std::vector<ui32>& columnTags,
    std::optional<ui64> expectedTotalCount = std::nullopt,
    std::optional<size_t> expectedMinBuckets = std::nullopt,
    std::optional<std::vector<TEqHeightHistogramProbe>> probes = std::nullopt,
    std::optional<bool> requireExact = std::nullopt,
    std::optional<std::vector<TString>> sourceKeys = std::nullopt,
    std::optional<ui64> maxTrueRankError = std::nullopt);

// Count rows in `.metadata/statistics_v2` for one (path, type, column_tags) key.
ui64 CountStatisticsV2Rows(
    TTestEnv& env, const TString& databaseName, const TPathId& pathId,
    EStatType statType, const TString& columnTags);

// Table with a declared EQ_HEIGHT_HISTOGRAM on the primary key column (Key).
TTableInfo PrepareDeclaredPkEqHeightTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

// Table with PRIMARY KEY (Key, Value1) and ColumnTableRowsNumber rows.
TTableInfo PrepareCompositePkTable(
    TTestEnv& env, const TString& databaseName, const TString& tableName, bool columnShard);

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
