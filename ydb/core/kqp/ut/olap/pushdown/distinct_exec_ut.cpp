#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/helpers/test_case.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <optional>
#include <utility>

#include <util/string/split.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NKqp {

namespace {

static TIntrusivePtr<NMonitoring::TDynamicCounters> ResolveScanCounterGroup(TIntrusivePtr<NMonitoring::TDynamicCounters> root, const TString& path) {
    TVector<TString> parts;
    Split(path, "/", parts);
    if (parts.empty()) {
        return nullptr;
    }
    const TString service = parts[0];
    auto current = GetServiceCounters(root, service);
    if (!current) {
        return nullptr;
    }
    for (size_t i = 1; i + 1 < parts.size(); i += 2) {
        const TString& key = parts[i];
        const TString& value = parts[i + 1];
        current = current->FindSubgroup(key, value);
        if (!current) {
            return nullptr;
        }
    }
    return current;
}

static i64 ReadDistinctLimitSyncPointInvocations(TKikimrRunner& kikimr) {
    auto* runtime = kikimr.GetTestServer().GetRuntime();
    UNIT_ASSERT(runtime != nullptr);
    auto root = runtime->GetAppData().Counters;
    UNIT_ASSERT(root != nullptr);
    auto group = ResolveScanCounterGroup(root, "tablets/subsystem/columnshard/module_id/Scan");
    UNIT_ASSERT_C(group != nullptr, "Scan counter subgroup not found");
    auto counter = group->FindCounter("Deriviative/DistinctLimit/SyncPoint/Invocations");
    UNIT_ASSERT_C(counter != nullptr, "DistinctLimit sync point counter not found");
    return counter->Val();
}

static i64 ReadDictionaryOnlyOptimizations(TKikimrRunner& kikimr) {
    auto* runtime = kikimr.GetTestServer().GetRuntime();
    UNIT_ASSERT(runtime != nullptr);
    auto root = runtime->GetAppData().Counters;
    UNIT_ASSERT(root != nullptr);
    auto group = ResolveScanCounterGroup(root, "tablets/subsystem/columnshard/module_id/Scan");
    UNIT_ASSERT_C(group != nullptr, "Scan counter subgroup not found");
    auto counter = group->FindCounter("Deriviative/Dictionary/OnlyOptimization/Count");
    UNIT_ASSERT_C(counter != nullptr, "Dictionary OnlyOptimization counter not found");
    return counter->Val();
}

// TPredicateFilter in the reader fetching plan (row-level PK mask, non-trivial NotAppliedFilter).
static i64 ReadPredicateFilterInvocations(TKikimrRunner& kikimr) {
    auto* runtime = kikimr.GetTestServer().GetRuntime();
    UNIT_ASSERT(runtime != nullptr);
    auto root = runtime->GetAppData().Counters;
    UNIT_ASSERT(root != nullptr);
    auto group = ResolveScanCounterGroup(root, "tablets/subsystem/columnshard/module_id/Scan");
    UNIT_ASSERT_C(group != nullptr, "Scan counter subgroup not found");
    auto counter = group->FindCounter("Deriviative/PredicateFilter/Invocations");
    UNIT_ASSERT_C(counter != nullptr, "PredicateFilter counter not found");
    return counter->Val();
}

class TLocalHelperModuloTsSharding: public TLocalHelper {
public:
    using TLocalHelper::TLocalHelper;

    std::vector<TString> GetShardingColumns() const override {
        return {"timestamp"};
    }
};

std::shared_ptr<arrow::RecordBatch> BuildBatchForRows(
    const std::vector<i64>& timestamps,
    const std::vector<TString>& resourceIds,
    const TString& uidPrefix,
    const std::vector<std::optional<i32>>* levels = nullptr)
{
    Y_ABORT_UNLESS(timestamps.size() == resourceIds.size());
    if (levels) {
        Y_ABORT_UNLESS(timestamps.size() == levels->size());
    }
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO), false),
        arrow::field("resource_id", arrow::utf8()),
        arrow::field("uid", arrow::utf8(), false),
        arrow::field("level", arrow::int32()),
        arrow::field("message", arrow::utf8()),
        arrow::field("new_column1", arrow::uint64()),
    });

    arrow::TimestampBuilder tsBuilder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder resourceBuilder;
    arrow::StringBuilder uidBuilder;
    arrow::Int32Builder levelBuilder;
    arrow::StringBuilder msgBuilder;
    arrow::UInt64Builder ncolBuilder;

    for (ui64 i = 0; i < timestamps.size(); ++i) {
        const auto ts = timestamps[i];
        Y_ABORT_UNLESS(tsBuilder.Append(ts).ok());
        const TString& rid = resourceIds[i];
        Y_ABORT_UNLESS(resourceBuilder.Append(rid.data(), rid.size()).ok());
        const TString uid = TStringBuilder() << uidPrefix << "_" << i;
        Y_ABORT_UNLESS(uidBuilder.Append(uid.data(), uid.size()).ok());
        if (levels) {
            if ((*levels)[i].has_value()) {
                Y_ABORT_UNLESS(levelBuilder.Append(*(*levels)[i]).ok());
            } else {
                Y_ABORT_UNLESS(levelBuilder.AppendNull().ok());
            }
        } else {
            Y_ABORT_UNLESS(levelBuilder.Append((i32)(i % 5)).ok());
        }
        Y_ABORT_UNLESS(msgBuilder.Append("m").ok());
        Y_ABORT_UNLESS(ncolBuilder.Append(i).ok());
    }

    std::shared_ptr<arrow::TimestampArray> a1;
    std::shared_ptr<arrow::StringArray> a2;
    std::shared_ptr<arrow::StringArray> a3;
    std::shared_ptr<arrow::Int32Array> a4;
    std::shared_ptr<arrow::StringArray> a5;
    std::shared_ptr<arrow::UInt64Array> a6;

    Y_ABORT_UNLESS(tsBuilder.Finish(&a1).ok());
    Y_ABORT_UNLESS(resourceBuilder.Finish(&a2).ok());
    Y_ABORT_UNLESS(uidBuilder.Finish(&a3).ok());
    Y_ABORT_UNLESS(levelBuilder.Finish(&a4).ok());
    Y_ABORT_UNLESS(msgBuilder.Finish(&a5).ok());
    Y_ABORT_UNLESS(ncolBuilder.Finish(&a6).ok());

    return arrow::RecordBatch::Make(schema, timestamps.size(), {a1, a2, a3, a4, a5, a6});
}

std::vector<TString> MakeUniqueJsonPayloads(const TString& prefix, const ui32 count) {
    std::vector<TString> payloads;
    payloads.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        payloads.emplace_back(TStringBuilder() << R"({"a.b.c":")" << prefix << "_" << i << R"("})");
    }
    return payloads;
}

// {"a.b.c": "<prefix>_<i % distinctCount>", "other": "grp_<i % otherGroups>"}
std::vector<TString> MakeJsonPayloadsWithOtherKey(const TString& prefix, const ui32 count, const ui32 distinctCount, const ui32 otherGroups) {
    Y_ABORT_UNLESS(distinctCount > 0 && otherGroups > 0);
    std::vector<TString> payloads;
    payloads.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        payloads.emplace_back(TStringBuilder() << R"({"a.b.c":")" << prefix << "_" << (i % distinctCount) << R"(","other":"grp_)"
                                               << (i % otherGroups) << R"("})");
    }
    return payloads;
}

// Mix of missing JSON path, JSON null, and string values for JSON_VALUE NULL DISTINCT tests.
std::vector<TString> MakeJsonPayloadsWithMissingPathAndJsonNull(const ui32 count, const ui32 valueDistinct) {
    Y_ABORT_UNLESS(valueDistinct > 0);
    std::vector<TString> payloads;
    payloads.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        if (i % 4 == 0) {
            payloads.emplace_back(R"({"other":"missing"})");
        } else if (i % 4 == 1) {
            payloads.emplace_back(R"({"a.b.c":null,"other":"jnull"})");
        } else {
            payloads.emplace_back(TStringBuilder() << R"({"a.b.c":"v_)" << (i % valueDistinct) << R"(","other":"val"})");
        }
    }
    return payloads;
}

// Missing JSON path only (no JSON-null variant in the dictionary): FinishDictionaryOnly must add a NULL row.
std::vector<TString> MakeJsonPayloadsWithMissingPathOnly(const ui32 count, const ui32 valueDistinct) {
    Y_ABORT_UNLESS(valueDistinct > 0);
    std::vector<TString> payloads;
    payloads.reserve(count);
    for (ui32 i = 0; i < count; ++i) {
        if (i % 3 == 0) {
            payloads.emplace_back(R"({"other":"missing"})");
        } else {
            payloads.emplace_back(TStringBuilder() << R"({"a.b.c":"v_)" << (i % valueDistinct) << R"(","other":"val"})");
        }
    }
    return payloads;
}

std::shared_ptr<arrow::RecordBatch> BuildBatchForRowsWithJsonPayload(
    const std::vector<i64>& timestamps,
    const std::vector<TString>& resourceIds,
    const std::vector<TString>& jsonPayloads,
    const TString& uidPrefix)
{
    Y_ABORT_UNLESS(timestamps.size() == resourceIds.size());
    Y_ABORT_UNLESS(timestamps.size() == jsonPayloads.size());
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO), false),
        arrow::field("resource_id", arrow::utf8()),
        arrow::field("uid", arrow::utf8(), false),
        arrow::field("level", arrow::int32()),
        arrow::field("message", arrow::utf8()),
        arrow::field("json_payload", arrow::utf8()),
        arrow::field("new_column1", arrow::uint64()),
    });

    arrow::TimestampBuilder tsBuilder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
    arrow::StringBuilder resourceBuilder;
    arrow::StringBuilder uidBuilder;
    arrow::Int32Builder levelBuilder;
    arrow::StringBuilder msgBuilder;
    arrow::StringBuilder jsonBuilder;
    arrow::UInt64Builder ncolBuilder;

    for (ui64 i = 0; i < timestamps.size(); ++i) {
        const auto ts = timestamps[i];
        Y_ABORT_UNLESS(tsBuilder.Append(ts).ok());
        const TString& rid = resourceIds[i];
        Y_ABORT_UNLESS(resourceBuilder.Append(rid.data(), rid.size()).ok());
        const TString uid = TStringBuilder() << uidPrefix << "_" << i;
        Y_ABORT_UNLESS(uidBuilder.Append(uid.data(), uid.size()).ok());
        Y_ABORT_UNLESS(levelBuilder.Append((i32)(i % 5)).ok());
        Y_ABORT_UNLESS(msgBuilder.Append("m").ok());
        const TString& jsonPayload = jsonPayloads[i];
        Y_ABORT_UNLESS(jsonBuilder.Append(jsonPayload.data(), jsonPayload.size()).ok());
        Y_ABORT_UNLESS(ncolBuilder.Append(i).ok());
    }

    std::shared_ptr<arrow::TimestampArray> a1;
    std::shared_ptr<arrow::StringArray> a2;
    std::shared_ptr<arrow::StringArray> a3;
    std::shared_ptr<arrow::Int32Array> a4;
    std::shared_ptr<arrow::StringArray> a5;
    std::shared_ptr<arrow::StringArray> a6;
    std::shared_ptr<arrow::UInt64Array> a7;

    Y_ABORT_UNLESS(tsBuilder.Finish(&a1).ok());
    Y_ABORT_UNLESS(resourceBuilder.Finish(&a2).ok());
    Y_ABORT_UNLESS(uidBuilder.Finish(&a3).ok());
    Y_ABORT_UNLESS(levelBuilder.Finish(&a4).ok());
    Y_ABORT_UNLESS(msgBuilder.Finish(&a5).ok());
    Y_ABORT_UNLESS(jsonBuilder.Finish(&a6).ok());
    Y_ABORT_UNLESS(ncolBuilder.Finish(&a7).ok());

    return arrow::RecordBatch::Make(schema, timestamps.size(), {a1, a2, a3, a4, a5, a6, a7});
}

std::shared_ptr<arrow::RecordBatch> BuildBatchForTimestamps(const std::vector<i64>& timestamps, const TString& uidPrefix) {
    std::vector<TString> rids;
    rids.reserve(timestamps.size());
    for (auto ts : timestamps) {
        rids.emplace_back(ToString(ts));
    }
    return BuildBatchForRows(timestamps, rids, uidPrefix);
}

std::vector<i64> PickTimestampsForShard(const ui32 shardIdx, const ui32 shardsCount, const ui32 count, i64 start = 1) {
    std::vector<i64> result;
    result.reserve(count);
    for (i64 ts = start; (ui32)result.size() < count; ++ts) {
        const ui64 h = XXH64(&ts, sizeof(ts), 0);
        if (h % shardsCount == shardIdx) {
            result.emplace_back(ts);
        }
    }
    return result;
}

std::vector<TString> MakeRepeatedResourceIds(const TString& prefix, const ui32 distinctCount, const ui32 totalCount) {
    Y_ABORT_UNLESS(distinctCount > 0);
    std::vector<TString> rids;
    rids.reserve(totalCount);
    for (ui32 i = 0; i < totalCount; ++i) {
        rids.emplace_back(TStringBuilder() << prefix << "_" << (i % distinctCount));
    }
    return rids;
}

TString BuildDistinctScanQueryText(
    const TString& tablePath,
    bool withForceDistinct,
    const TString& forceDistinctColumn,
    const TString& selectList,
    const TString& whereClause,
    const TString& orderByClause,
    const std::optional<ui64> sqlLimit,
    bool withForceDistinctLimitPragma = true,
    const std::optional<ui64> forceDistinctLimitValue = std::nullopt)
{
    TStringBuilder q;
    q << R"(
        --!syntax_v1
        PRAGMA Kikimr.OptEnableOlapPushdown = "true";
    )";
    if (withForceDistinct) {
        q << R"(
        PRAGMA Kikimr.OptForceOlapPushdownDistinct = ")" << forceDistinctColumn << R"(";
    )";
        if (withForceDistinctLimitPragma && (forceDistinctLimitValue.has_value() || sqlLimit.has_value())) {
            // Pragma limit defaults to sqlLimit when forceDistinctLimitValue is omitted (typical E2E case).
            const ui64 pragmaLimit = forceDistinctLimitValue.has_value() ? *forceDistinctLimitValue : *sqlLimit;
            q << R"(
        PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = ")" << pragmaLimit << R"(";
    )";
        }
    }

    q << "\n\n        SELECT DISTINCT " << selectList << " FROM `" << tablePath << "`";
    if (!whereClause.empty()) {
        q << "\n" << whereClause;
    }
    if (!orderByClause.empty()) {
        q << "\n" << orderByClause;
    }
    if (sqlLimit.has_value()) {
        q << "\nLIMIT " << *sqlLimit;
    }
    return q;
}

TCollectedStreamResult RunDistinctScanQuery(
    NYdb::NTable::TTableClient& tableClient,
    const TString& tablePath,
    bool withForceDistinct,
    const TString& forceDistinctColumn,
    const TString& selectList,
    const TString& whereClause,
    const TString& orderByClause,
    const std::optional<ui64> sqlLimit,
    bool withForceDistinctLimitPragma = true,
    const std::optional<ui64> forceDistinctLimitValue = std::nullopt)
{
    const TString q = BuildDistinctScanQueryText(
        tablePath, withForceDistinct, forceDistinctColumn, selectList, whereClause, orderByClause,
        sqlLimit, withForceDistinctLimitPragma, forceDistinctLimitValue);
    auto it = tableClient.StreamExecuteScanQuery(q).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    return CollectStreamResult(it);
}

TCollectedStreamResult RunJsonValueDistinctScanQuery(
    NYdb::NTable::TTableClient& tableClient,
    const TString& tablePath,
    bool withForceDistinct,
    const TString& whereClause,
    const std::optional<ui64> sqlLimit,
    bool withForceDistinctLimitPragma = true,
    const std::optional<ui64> forceDistinctLimitValue = std::nullopt)
{
    TStringBuilder q;
    q << R"(
        --!syntax_v1
        PRAGMA Kikimr.OptEnableOlapPushdown = "true";
    )";
    if (withForceDistinct) {
        q << R"(
        PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
        PRAGMA Kikimr.OptForceOlapPushdownDistinct = "jsonDoc";
    )";
        if (withForceDistinctLimitPragma && (forceDistinctLimitValue.has_value() || sqlLimit.has_value())) {
            const ui64 pragmaLimit = forceDistinctLimitValue.has_value() ? *forceDistinctLimitValue : *sqlLimit;
            q << R"(
        PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = ")" << pragmaLimit << R"(";
    )";
        }
    }

    q << "\n\n        SELECT DISTINCT JSON_VALUE(json_payload, \"$.\\\"a.b.c\\\"\") AS jsonDoc FROM `" << tablePath << "`";
    if (!whereClause.empty()) {
        q << "\n" << whereClause;
    }
    if (sqlLimit.has_value()) {
        q << "\nLIMIT " << *sqlLimit;
    }
    auto it = tableClient.StreamExecuteScanQuery(q).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    return CollectStreamResult(it);
}

TCollectedStreamResult RunDistinctQuery(
    NYdb::NTable::TTableClient& tableClient,
    const TString& tablePath,
    const bool withForceDistinct,
    ui64 sqlLimit)
{
    return RunDistinctScanQuery(
        tableClient, tablePath, withForceDistinct, "resource_id", "resource_id", {}, {},
        std::optional<ui64>(sqlLimit), true, std::nullopt);
}

enum class ECounterOnForce {
    MustGrow,
    MustStay,
};

struct TOnOffQueryRun {
    TCollectedStreamResult Off;
    TCollectedStreamResult On;
    i64 CounterBefore = 0;
    i64 CounterAfterOff = 0;
    i64 CounterAfterOn = 0;
};

template <typename TReadCounter, typename TRunQuery>
TOnOffQueryRun RunOnOffWithCounter(TReadCounter&& readCounter, TRunQuery&& runQuery) {
    TOnOffQueryRun result;
    result.CounterBefore = readCounter();
    result.Off = runQuery(false);
    result.CounterAfterOff = readCounter();
    result.On = runQuery(true);
    result.CounterAfterOn = readCounter();
    return result;
}

template <typename TRunQuery>
TOnOffQueryRun RunForcedDistinctOnOff(TKikimrRunner& kikimr, TRunQuery&& runQuery) {
    return RunOnOffWithCounter(
        [&kikimr] { return ReadDistinctLimitSyncPointInvocations(kikimr); },
        std::forward<TRunQuery>(runQuery));
}

template <typename TRunQuery>
TOnOffQueryRun RunDictionaryOnlyOnOff(TKikimrRunner& kikimr, TRunQuery&& runQuery) {
    return RunOnOffWithCounter(
        [&kikimr] { return ReadDictionaryOnlyOptimizations(kikimr); },
        std::forward<TRunQuery>(runQuery));
}

void AssertOnOffSameResult(
    const TOnOffQueryRun& run,
    const ui64 expectedRows,
    const TString& message,
    const ECounterOnForce counterOnForce = ECounterOnForce::MustGrow,
    const TStringBuf counterGrowMessage = "DistinctLimit sync point expected with force",
    const bool unordered = true)
{
    UNIT_ASSERT_VALUES_EQUAL(run.CounterAfterOff, run.CounterBefore);
    if (counterOnForce == ECounterOnForce::MustGrow) {
        UNIT_ASSERT_C(run.CounterAfterOn > run.CounterAfterOff,
            TStringBuilder() << counterGrowMessage << "; before=" << run.CounterBefore
                             << " after_off=" << run.CounterAfterOff << " after_on=" << run.CounterAfterOn);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(run.CounterAfterOn, run.CounterAfterOff);
    }
    UNIT_ASSERT_VALUES_EQUAL(run.Off.RowsCount, expectedRows);
    UNIT_ASSERT_VALUES_EQUAL(run.On.RowsCount, expectedRows);
    if (unordered) {
        CompareYsonUnordered(run.Off.ResultSetYson, run.On.ResultSetYson, message);
    } else {
        CompareYson(run.Off.ResultSetYson, run.On.ResultSetYson, message);
    }
}

void CheckDistinctLimitPastPkOrderedDuplicateRun(const TString& readerClass) {
    auto settings = TKikimrSettings().SetWithSampleTables(false);
    if (!readerClass.empty()) {
        settings.SetColumnShardReaderClassName(readerClass);
    }
    TKikimrRunner kikimr(settings);

    TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

    constexpr ui32 kRun = 50;
    constexpr ui32 kUniq = 50;
    const auto ts = PickTimestampsForShard(0, 1, kRun + kUniq, 1);
    std::vector<TString> rids;
    rids.reserve(kRun + kUniq);
    for (ui32 i = 0; i < kRun; ++i) {
        rids.emplace_back("run_a");
    }
    for (ui32 i = 0; i < kUniq; ++i) {
        rids.emplace_back(TStringBuilder() << "uniq_" << i);
    }
    TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

    auto tableClient = kikimr.GetTableClient();
    constexpr ui64 kLimit = 5;
    const TString tablePath = "/Root/olapStore/olapTable";
    auto run = RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
        return RunDistinctScanQuery(tableClient, tablePath, withForce, "resource_id", "resource_id", {}, {}, kLimit);
    });

    UNIT_ASSERT_VALUES_EQUAL(run.CounterAfterOff, run.CounterBefore);
    UNIT_ASSERT_C(run.CounterAfterOn > run.CounterAfterOff,
        TStringBuilder() << "DistinctLimit sync point expected with force; reader=" << readerClass
                         << " before=" << run.CounterBefore << " after_off=" << run.CounterAfterOff
                         << " after_on=" << run.CounterAfterOn);
    UNIT_ASSERT_VALUES_EQUAL(run.Off.RowsCount, kLimit);
    UNIT_ASSERT_VALUES_EQUAL(run.On.RowsCount, kLimit);
    // Without ORDER BY the two engines may pick different keys; force-on must not stop after the duplicate run.
    UNIT_ASSERT_C(run.On.ResultSetYson.find("run_a") != TString::npos, run.On.ResultSetYson);
}

void AssertQueryPlanContains(
    NYdb::NTable::TTableClient& tableClient,
    const TString& query,
    TStringBuf needle)
{
    auto res = StreamExplainQuery(query, tableClient);
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    const auto planRes = CollectStreamResult(res);
    UNIT_ASSERT(planRes.QueryStats.Defined());
    const TString ast = TString(planRes.QueryStats->Getquery_ast());
    UNIT_ASSERT_C(ast.find(needle) != TString::npos, ast);
}

void AssertQueryPlanNotContains(
    NYdb::NTable::TTableClient& tableClient,
    const TString& query,
    TStringBuf needle)
{
    auto res = StreamExplainQuery(query, tableClient);
    UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
    const auto planRes = CollectStreamResult(res);
    UNIT_ASSERT(planRes.QueryStats.Defined());
    const TString ast = TString(planRes.QueryStats->Getquery_ast());
    UNIT_ASSERT_C(ast.find(needle) == TString::npos, ast);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpOlapDistinctPushdownE2E) {

    Y_UNIT_TEST(JsonValueDistinct_ForcedPushdown_ReturnsSameValuesAsWithoutForce) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();
        auto qsRes = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(qsRes.IsSuccess(), qsRes.GetIssues().ToString());
        auto querySession = qsRes.GetSession();

        constexpr TStringBuf kTable = "/Root/foo_json_exec_distinct";
        auto cre = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `)" << kTable << R"(` (
                a Int64 NOT NULL,
                b Int32,
                payload JsonDocument,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(cre.IsSuccess(), cre.GetIssues().ToString());

        auto ins = querySession.ExecuteQuery(R"(
            INSERT INTO `/Root/foo_json_exec_distinct` (a, b, payload)
            VALUES (1, 1, JsonDocument('{"a.b.c" : "a1"}'));
            INSERT INTO `/Root/foo_json_exec_distinct` (a, b, payload)
            VALUES (2, 11, JsonDocument('{"a.b.c" : "a2"}'));
            INSERT INTO `/Root/foo_json_exec_distinct` (a, b, payload)
            VALUES (3, 11, JsonDocument('{"a.b.c" : "a3"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(ins.IsSuccess(), ins.GetIssues().ToString());

        const auto runDistinct = [&](bool withForcedPushdown) -> TString {
            TStringBuilder q;
            q << R"(
                --!syntax_v1
                PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            )";
            if (withForcedPushdown) {
                q << R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
                PRAGMA Kikimr.OptForceOlapPushdownDistinct = "jsonDoc";
                PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";
            )";
            }
            q << R"(
                SELECT DISTINCT JSON_VALUE(payload, "$.\"a.b.c\"") AS jsonDoc
                FROM `)" << kTable << R"(` LIMIT 10
            )";
            auto sel = querySession.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(sel.IsSuccess(), sel.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(sel.GetResultSets().size(), 1u);
            return NYdb::FormatResultSetYson(sel.GetResultSet(0));
        };

        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        const TString ysonPlain = runDistinct(false);
        const i64 syncAfterPlain = ReadDistinctLimitSyncPointInvocations(kikimr);
        const TString ysonForce = runDistinct(true);
        const i64 syncAfterForce = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterPlain, syncBefore);
        UNIT_ASSERT_C(
            syncAfterForce > syncAfterPlain,
            TStringBuilder() << "forced path must hit DistinctLimit sync point; before=" << syncBefore << " after_plain=" << syncAfterPlain
                             << " after_force=" << syncAfterForce);

        CompareYsonUnordered(ysonForce, ysonPlain,
            "JSON_VALUE DISTINCT: multiset of values must match with and without forced OLAP distinct pushdown");
    }

    Y_UNIT_TEST(JsonValueDistinct_ForcedPushdown_PragmaKeyMismatch_CompileError) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();
        auto qsRes = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(qsRes.IsSuccess(), qsRes.GetIssues().ToString());
        auto querySession = qsRes.GetSession();

        constexpr TStringBuf kTable = "/Root/foo_json_pragma_mismatch";
        auto cre = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `)" << kTable << R"(` (
                a Int64 NOT NULL,
                b Int32,
                payload JsonDocument,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(cre.IsSuccess(), cre.GetIssues().ToString());

        auto ins = querySession.ExecuteQuery(R"(
            INSERT INTO `/Root/foo_json_pragma_mismatch` (a, b, payload)
            VALUES (1, 1, JsonDocument('{"a.b.c" : "a1"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(ins.IsSuccess(), ins.GetIssues().ToString());

        auto sel = querySession.ExecuteQuery(R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "wrongAlias";

            SELECT DISTINCT JSON_VALUE(payload, "$.\"a.b.c\"") AS jsonDoc
            FROM `/Root/foo_json_pragma_mismatch` LIMIT 10
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(!sel.IsSuccess());
        const TString issues = sel.GetIssues().ToString();
        UNIT_ASSERT_C(issues.Contains("does not match") || issues.Contains("OptForceOlapPushdownDistinct"), issues);
    }

    // Force key is the DISTINCT alias of a non-pushable expression: not a stored column, no JSON_VALUE
    // that a later projection pushdown could still resolve. The optimizer must not inject KqpOlapDistinct
    // (that would fail later in type annotation). DISTINCT stays in KQP.
    Y_UNIT_TEST(ForceDistinct_UnresolvableComputedAlias_DoesNotInjectOlapDistinct) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();
        auto qsRes = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(qsRes.IsSuccess(), qsRes.GetIssues().ToString());
        auto querySession = qsRes.GetSession();

        constexpr TStringBuf kTable = "/Root/foo_unresolvable_distinct";
        auto cre = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `)" << kTable << R"(` (
                a Int64 NOT NULL,
                b Int32,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(cre.IsSuccess(), cre.GetIssues().ToString());

        auto ins = querySession.ExecuteQuery(R"(
            INSERT INTO `/Root/foo_unresolvable_distinct` (a, b)
            VALUES (1, 1), (2, 1), (3, 2);
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(ins.IsSuccess(), ins.GetIssues().ToString());

        const TString query = R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "computed";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "10";

            SELECT DISTINCT (b + 1) AS computed
            FROM `/Root/foo_unresolvable_distinct` LIMIT 10
        )";

        auto explainRes = StreamExplainQuery(query, tableClient);
        if (!explainRes.IsSuccess()) {
            const TString issues = explainRes.GetIssues().ToString();
            UNIT_ASSERT_C(
                issues.Contains("neither a stored column nor a pushed OLAP projection")
                    || issues.Contains("OptForceOlapPushdownDistinct"),
                issues);
            return;
        }
        const auto planRes = CollectStreamResult(explainRes);
        UNIT_ASSERT(planRes.QueryStats.Defined());
        const TString ast = TString(planRes.QueryStats->Getquery_ast());
        UNIT_ASSERT_C(ast.find("KqpOlapDistinct") == TString::npos, ast);

        auto it = tableClient.StreamExecuteScanQuery(query).GetValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        auto execRes = CollectStreamResult(it);
        UNIT_ASSERT_VALUES_EQUAL(execRes.RowsCount, 2u);
    }

    // JSON_VALUE ERROR ON EMPTY is not kernel-pushable. Alias projection must refuse it instead of
    // ConvertComparisonNode → BuildOlapJsonValue YQL_ENSURE abort. DISTINCT stays in KQP.
    Y_UNIT_TEST(JsonValueDistinct_ErrorOnEmpty_ForcePragma_DoesNotAbort) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();
        auto qsRes = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(qsRes.IsSuccess(), qsRes.GetIssues().ToString());
        auto querySession = qsRes.GetSession();

        constexpr TStringBuf kTable = "/Root/foo_json_returning_force";
        auto cre = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `)" << kTable << R"(` (
                a Int64 NOT NULL,
                payload JsonDocument,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(cre.IsSuccess(), cre.GetIssues().ToString());

        auto ins = querySession.ExecuteQuery(R"(
            INSERT INTO `/Root/foo_json_returning_force` (a, payload)
            VALUES (1, JsonDocument('{"a.b.c" : "a1"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(ins.IsSuccess(), ins.GetIssues().ToString());

        auto sel = querySession.ExecuteQuery(R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "jsonDoc";

            SELECT DISTINCT JSON_VALUE(payload, "$.\"a.b.c\"" ERROR ON EMPTY) AS jsonDoc
            FROM `/Root/foo_json_returning_force` LIMIT 10
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(sel.IsSuccess(), sel.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(sel.GetResultSet(0).RowsCount(), 1u);
    }

    Y_UNIT_TEST(JsonValueDistinct_ErrorOnEmpty_SourceColumnInWhere_Succeeds) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto queryClient = kikimr.GetQueryClient();
        auto qsRes = queryClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(qsRes.IsSuccess(), qsRes.GetIssues().ToString());
        auto querySession = qsRes.GetSession();

        constexpr TStringBuf kTable = "/Root/foo_json_returning_filter";
        auto cre = session.ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE TABLE `)" << kTable << R"(` (
                a Int64 NOT NULL,
                payload JsonDocument,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(cre.IsSuccess(), cre.GetIssues().ToString());

        auto ins = querySession.ExecuteQuery(R"(
            INSERT INTO `/Root/foo_json_returning_filter` (a, payload)
            VALUES (1, JsonDocument('{"a.b.c" : "a1"}'));
            INSERT INTO `/Root/foo_json_returning_filter` (a, payload)
            VALUES (2, JsonDocument('{"a.b.c" : "a1"}'));
            INSERT INTO `/Root/foo_json_returning_filter` (a, payload)
            VALUES (3, JsonDocument('{"a.b.c" : "a2"}'));
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(ins.IsSuccess(), ins.GetIssues().ToString());

        auto sel = querySession.ExecuteQuery(R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";

            SELECT DISTINCT JSON_VALUE(payload, "$.\"a.b.c\"" ERROR ON EMPTY) AS jsonDoc
            FROM `/Root/foo_json_returning_filter`
            WHERE payload IS NOT NULL
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(sel.IsSuccess(), sel.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(sel.GetResultSet(0).RowsCount(), 2u);
    }

    Y_UNIT_TEST(OneShard_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        // single columnshard & single table shard to avoid cross-shard merge effects
        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForTimestamps(PickTimestampsForShard(0, 1, 100), "u"));

        auto tableClient = kikimr.GetTableClient();

        constexpr ui64 kCap = 100;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", withForce, kCap);
            }),
            100,
            "distinct results differ with pushdown on/off");
    }

    Y_UNIT_TEST(OneShard_WithDuplicates_DistinctOnOff_SameResult_UniqueCount) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const auto ts = PickTimestampsForShard(0, 1, 100, 1);
        const auto rids = MakeRepeatedResourceIds("dup", 10, 100);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kCap = 10;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", withForce, kCap);
            }),
            10,
            "distinct results differ with pushdown on/off");
    }

    Y_UNIT_TEST(TwoShards_HalfRowsPerShard_DistinctOnOff_SameResult_AllRowsReturned) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 2, 2);

        const auto ts0 = PickTimestampsForShard(0, 2, 50, 1);
        const auto ts1 = PickTimestampsForShard(1, 2, 50, ts0.back() + 1);

        // sanity: ensure our generator really splits 50/50 by sharding function
        for (auto ts : ts0) {
            const ui64 h = XXH64(&ts, sizeof(ts), 0);
            UNIT_ASSERT_VALUES_EQUAL(h % 2, 0);
        }
        for (auto ts : ts1) {
            const ui64 h = XXH64(&ts, sizeof(ts), 0);
            UNIT_ASSERT_VALUES_EQUAL(h % 2, 1);
        }

        std::vector<i64> allTs;
        allTs.reserve(100);
        allTs.insert(allTs.end(), ts0.begin(), ts0.end());
        allTs.insert(allTs.end(), ts1.begin(), ts1.end());

        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForTimestamps(allTs, "u"));

        auto tableClient = kikimr.GetTableClient();

        constexpr ui64 kCap = 100;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", withForce, kCap);
            }),
            100,
            "distinct results differ with pushdown on/off");
    }

    Y_UNIT_TEST(TwoShards_DuplicatesAcrossShards_DistinctOnOff_SameResult_KqpMerges) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 2, 2);

        const auto ts0 = PickTimestampsForShard(0, 2, 50, 1);
        const auto ts1 = PickTimestampsForShard(1, 2, 50, ts0.back() + 1);

        std::vector<i64> allTs;
        allTs.reserve(100);
        allTs.insert(allTs.end(), ts0.begin(), ts0.end());
        allTs.insert(allTs.end(), ts1.begin(), ts1.end());

        std::vector<TString> allRids;
        allRids.reserve(100);
        // shard0 logical half
        for (ui32 i = 0; i < 50; ++i) {
            if (i < 20) {
                allRids.emplace_back(TStringBuilder() << "shared_" << i);
            } else {
                allRids.emplace_back(TStringBuilder() << "s0_" << i);
            }
        }
        // shard1 logical half
        for (ui32 i = 0; i < 50; ++i) {
            if (i < 20) {
                allRids.emplace_back(TStringBuilder() << "shared_" << i);
            } else {
                allRids.emplace_back(TStringBuilder() << "s1_" << i);
            }
        }

        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(allTs, allRids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kCap = 80;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", withForce, kCap);
            }),
            80,
            "distinct results differ with pushdown on/off");
    }

    Y_UNIT_TEST(OneShard_WithDuplicates_DistinctOnOff_LimitBelowUniques_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const auto ts = PickTimestampsForShard(0, 1, 100, 1);
        const auto rids = MakeRepeatedResourceIds("dup", 10, 100);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kLimit = 7;
        // ORDER BY + LIMIT together with OptForceOlapPushdownDistinctLimit is rejected at compile time (see PushOlapDistinct).
        auto res = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);
        UNIT_ASSERT_VALUES_EQUAL(res.RowsCount, kLimit);
    }

    // First LIMIT physical rows in PK order are the same DISTINCT key; DistinctLimit must keep scanning.
    Y_UNIT_TEST(OneShard_DuplicateRunThenUniques_DistinctLimit_OnOff_SameResult) {
        CheckDistinctLimitPastPkOrderedDuplicateRun("");
    }

    Y_UNIT_TEST(OneShard_DuplicateRunThenUniques_DistinctLimit_OnOff_SameResult_TrivialReader) {
        CheckDistinctLimitPastPkOrderedDuplicateRun("TRIVIAL");
    }

    Y_UNIT_TEST(OneShard_AllUniques_DistinctOnOff_Limit_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForTimestamps(PickTimestampsForShard(0, 1, 100), "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kLimit = 25;
        auto res = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);
        UNIT_ASSERT_VALUES_EQUAL(res.RowsCount, kLimit);
    }

    Y_UNIT_TEST(OneShard_WithDuplicates_OrderByDistinctColumn_OnOff_SameOrderedResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const auto ts = PickTimestampsForShard(0, 1, 100, 1);
        const auto rids = MakeRepeatedResourceIds("dup", 10, 100);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kCap = 10;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, "resource_id", "resource_id", {},
                    "ORDER BY resource_id", kCap);
            }),
            10,
            "ORDER BY (full distinct set): row order must match with OLAP distinct pushdown on/off",
            ECounterOnForce::MustGrow,
            "DistinctLimit sync point expected with force",
            /*unordered=*/false);
    }

    Y_UNIT_TEST(TwoShards_DuplicatesAcrossShards_DistinctOnOff_Limit_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 2, 2);

        const auto ts0 = PickTimestampsForShard(0, 2, 50, 1);
        const auto ts1 = PickTimestampsForShard(1, 2, 50, ts0.back() + 1);

        std::vector<i64> allTs;
        allTs.reserve(100);
        allTs.insert(allTs.end(), ts0.begin(), ts0.end());
        allTs.insert(allTs.end(), ts1.begin(), ts1.end());

        std::vector<TString> allRids;
        allRids.reserve(100);
        for (ui32 i = 0; i < 50; ++i) {
            allRids.emplace_back(i < 20 ? TStringBuilder() << "shared_" << i : TStringBuilder() << "s0_" << i);
        }
        for (ui32 i = 0; i < 50; ++i) {
            allRids.emplace_back(i < 20 ? TStringBuilder() << "shared_" << i : TStringBuilder() << "s1_" << i);
        }

        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(allTs, allRids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kLimit = 15;
        auto res = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);
        UNIT_ASSERT_VALUES_EQUAL(res.RowsCount, kLimit);
    }

    // Two shards, only dup_0..dup_9-style ids. ORDER BY + LIMIT with forced pushdown limit pragma is compile-rejected.
    Y_UNIT_TEST(TwoShards_OnlyDupPrefixedIds_OrderByLimit_OnOff_SameOrderedResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 2, 2);

        const auto ts0 = PickTimestampsForShard(0, 2, 50, 1);
        const auto ts1 = PickTimestampsForShard(1, 2, 50, ts0.back() + 1);

        std::vector<i64> allTs;
        allTs.reserve(100);
        allTs.insert(allTs.end(), ts0.begin(), ts0.end());
        allTs.insert(allTs.end(), ts1.begin(), ts1.end());

        const auto halfRids = MakeRepeatedResourceIds("dup", 10, 50);
        std::vector<TString> allRids;
        allRids.reserve(100);
        allRids.insert(allRids.end(), halfRids.begin(), halfRids.end());
        allRids.insert(allRids.end(), halfRids.begin(), halfRids.end());

        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(allTs, allRids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kLimit = 7;
        auto res = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {},
            "ORDER BY resource_id", kLimit);
        UNIT_ASSERT_VALUES_EQUAL(res.RowsCount, kLimit);
    }

    // PK is (timestamp, uid): filter on the first key column (prefix) — pushdown-friendly; results must match with pragma off.
    Y_UNIT_TEST(CompositePk_WhereTimestampPrefix_DistinctLevel_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelper(kikimr).CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        constexpr ui64 tsBegin = 1'000'000;
        constexpr size_t rowCount = 30;
        TLocalHelper(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", 0, tsBegin, rowCount);

        const TString where = TStringBuilder() << R"(WHERE `timestamp` >= DateTime::FromMicroseconds()" << tsBegin
            << R"() AND `timestamp` < DateTime::FromMicroseconds()" << (tsBegin + rowCount) << ")";

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kDistinctCap = 5;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, "level", "`level`", where, {}, kDistinctCap);
            }),
            5,
            "distinct+pk-prefix where results differ with pushdown on/off");

        constexpr ui64 kLimit = 3;
        auto resL = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "level", "`level`", where, "ORDER BY `level`", kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resL.RowsCount, kLimit);
    }

    // WHERE narrows scanned rows while SQL LIMIT stays high: ColumnShard distinct sync must use robust limit
    // (min of filter-derived cap and requested limit), same final result as plain DISTINCT.
    Y_UNIT_TEST(OneShard_WhereSubset_HighSqlLimit_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 50, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 50, 50);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        const TString where = TStringBuilder() << "WHERE `timestamp` >= DateTime::FromMicroseconds(" << ts.front()
            << ") AND `timestamp` <= DateTime::FromMicroseconds(" << ts[9] << ")";

        constexpr ui64 kHighLimit = 1000;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctScanQuery(
                    tableClient, "/Root/olapStore/olapTable", withForce, "resource_id", "resource_id", where, {}, kHighLimit);
            }),
            10u,
            "WHERE subset + high SQL LIMIT: distinct pushdown must match plain (robust limit path)");
    }

    // WHERE covers the whole portion (FullUsage): no TPredicateFilter, row mask is allow-all at sync point.
    Y_UNIT_TEST(OneShard_WhereFullPortion_LowDistinctLimit_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 50, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 50, 50);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        const TString where = TStringBuilder() << "WHERE `timestamp` >= DateTime::FromMicroseconds(" << ts.front()
            << ") AND `timestamp` <= DateTime::FromMicroseconds(" << ts.back() << ")";

        constexpr ui64 kLimit = 50;
        const i64 predicateBefore = ReadPredicateFilterInvocations(kikimr);
        auto run = RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
            return RunDistinctScanQuery(
                tableClient, "/Root/olapStore/olapTable", withForce, "resource_id", "resource_id", where, {}, kLimit);
        });
        UNIT_ASSERT_VALUES_EQUAL(ReadPredicateFilterInvocations(kikimr), predicateBefore);
        AssertOnOffSameResult(run, kLimit,
            "WHERE full portion: all distinct values, no row-level PK filter at reader");
    }

    // WHERE cuts the portion (PartialUsage): TPredicateFilter builds row-level NotAppliedFilter; Seen must
    // count only in-window rows (regression for early stop before iterator/filter fix).
    Y_UNIT_TEST(OneShard_WherePartialPortion_LowDistinctLimit_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 50, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 50, 50);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        const TString where = TStringBuilder() << "WHERE `timestamp` >= DateTime::FromMicroseconds(" << ts[40]
            << ") AND `timestamp` <= DateTime::FromMicroseconds(" << ts[49] << ")";

        constexpr ui64 kLimit = 10;
        const i64 predicateBefore = ReadPredicateFilterInvocations(kikimr);
        auto run = RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
            return RunDistinctScanQuery(
                tableClient, "/Root/olapStore/olapTable", withForce, "resource_id", "resource_id", where, {}, kLimit);
        });
        const i64 predicateAfter = ReadPredicateFilterInvocations(kikimr);
        UNIT_ASSERT_C(predicateAfter > predicateBefore,
            TStringBuilder() << "Partial PK range must run TPredicateFilter; before=" << predicateBefore
                             << " after=" << predicateAfter);
        AssertOnOffSameResult(run, kLimit,
            "WHERE partial portion + low distinct limit: pushdown must match plain DISTINCT");
    }

    Y_UNIT_TEST(OneShard_WherePartialPortion_LowDistinctLimit_JsonValue_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 50, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 50, 50);
        const auto jsonPayloads = MakeUniqueJsonPayloads("jv", 50);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        auto tableClient = kikimr.GetTableClient();
        const TString where = TStringBuilder() << "WHERE `timestamp` >= DateTime::FromMicroseconds(" << ts[40]
            << ") AND `timestamp` <= DateTime::FromMicroseconds(" << ts[49] << ")";

        constexpr ui64 kLimit = 10;
        const i64 predicateBefore = ReadPredicateFilterInvocations(kikimr);
        auto run = RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
            return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, where, kLimit);
        });
        const i64 predicateAfter = ReadPredicateFilterInvocations(kikimr);
        UNIT_ASSERT_C(predicateAfter > predicateBefore,
            TStringBuilder() << "Partial PK range must run TPredicateFilter; before=" << predicateBefore
                             << " after=" << predicateAfter);
        AssertOnOffSameResult(run, kLimit,
            "JSON_VALUE DISTINCT + WHERE partial portion + low distinct limit: pushdown must match plain");
    }

    // Non-PK filter is pushed as an SSA Filter next to the DISTINCT marker. The marker must not be AND-merged
    // with the real filter by TGraph::Collapse (regression: "not appropriate scalar type for bool interpretation").
    Y_UNIT_TEST(OneShard_NonPkFilter_SimpleDistinct_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 50, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 10, 50);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        // level = i % 5, resource_id = rid_(i % 10) => level = 2 keeps rid_2 and rid_7 only.
        constexpr TStringBuf kWhere = "WHERE level = 2";
        constexpr ui64 kLimit = 100;

        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctScanQuery(
                    tableClient, "/Root/olapStore/olapTable", withForce, "resource_id", "resource_id", TString(kWhere), {}, kLimit);
            }),
            2u,
            "non-PK filter + DISTINCT: pushdown must match plain");
    }

    // JSON_VALUE alias DISTINCT + pushed non-PK filter + PK range.
    Y_UNIT_TEST(OneShard_NonPkFilter_JsonValueDistinct_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 50, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 50, 50);
        // jsonDoc = jv_(i % 10), level = i % 5 => level = 3 keeps jv_3 and jv_8.
        const auto jsonPayloads = MakeJsonPayloadsWithOtherKey("jv", 50, 10, 2);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        auto tableClient = kikimr.GetTableClient();
        const TString where = TStringBuilder() << "WHERE level = 3 AND `timestamp` >= DateTime::FromMicroseconds(" << ts[0]
            << ") AND `timestamp` <= DateTime::FromMicroseconds(" << ts[49] << ")";
        constexpr ui64 kLimit = 100;

        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, where, kLimit);
            }),
            2u,
            "JSON_VALUE DISTINCT + non-PK filter: pushdown must match plain");
    }

    // JSON_VALUE alias DISTINCT + JSON_VALUE filter on another path of the same JSON column.
    // The projection must be named by the SELECT alias so the DISTINCT key resolves although two JSON_VALUE nodes exist.
    Y_UNIT_TEST(OneShard_JsonValueDistinct_JsonFilterSameColumn_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 60, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 60, 60);
        // jsonDoc = jv_(i % 10), other = grp_(i % 4) => other = grp_1 keeps i in {1,5,9,...}: jv_1, jv_5, jv_9, jv_3, jv_7.
        const auto jsonPayloads = MakeJsonPayloadsWithOtherKey("jv", 60, 10, 4);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        auto tableClient = kikimr.GetTableClient();
        const TString where = R"(WHERE JSON_VALUE(json_payload, "$.other") = "grp_1")";
        constexpr ui64 kLimit = 100;

        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, where, kLimit);
            }),
            5u,
            "JSON_VALUE DISTINCT + JSON_VALUE filter on the same column: pushdown must match plain");
    }

    // Subselect projecting a strict subset of columns and filtering on a non-PK one (the shape that produces
    // ExtractMembers over the read in the logical plan): alias DISTINCT with pushdown must match plain evaluation.
    Y_UNIT_TEST(OneShard_JsonValueDistinct_SubselectSubsetColumns_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 50, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 50, 50);
        // jsonDoc = jv_(i % 10), level = i % 5 => level = 3 keeps jv_3 and jv_8.
        const auto jsonPayloads = MakeJsonPayloadsWithOtherKey("jv", 50, 10, 2);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        auto tableClient = kikimr.GetTableClient();
        auto run = [&](const bool withForce) {
            TStringBuilder q;
            q << R"(
                --!syntax_v1
                PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            )";
            if (withForce) {
                q << R"(
                PRAGMA Kikimr.OptEnableOlapPushdownProjections = "true";
                PRAGMA Kikimr.OptForceOlapPushdownDistinct = "jsonDoc";
                PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = "100";
                )";
            }
            q << R"(
                SELECT DISTINCT JSON_VALUE(json_payload, "$.\"a.b.c\"") AS jsonDoc
                FROM (SELECT level, json_payload FROM `/Root/olapStore/olapTable` WHERE level = 3)
                LIMIT 100
            )";
            auto it = tableClient.StreamExecuteScanQuery(q).GetValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            return CollectStreamResult(it);
        };

        AssertOnOffSameResult(RunForcedDistinctOnOff(kikimr, run), 2u,
            "JSON_VALUE DISTINCT over a subselect with a column subset: pushdown must match plain");
    }

    // JSON column stored as SUB_COLUMNS with dictionary encoded sub-columns: forced DISTINCT over JSON_VALUE fetches only
    // the sub-column dictionary (dictionary-only counter grows) and returns the same values as the plain evaluation;
    // a filter on another path of the same column keeps the row read and still matches.
    Y_UNIT_TEST(OneShard_JsonValueDistinct_DictionarySubColumn_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        auto tableClient = kikimr.GetTableClient();
        {
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto res = session.ExecuteSchemeQuery(R"(
                ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=json_payload,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_UNIQUE_FRACTION`=`1`);
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const auto ts = PickTimestampsForShard(0, 1, 60, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 60, 60);
        // jsonDoc = jv_(i % 10), other = grp_(i % 4).
        const auto jsonPayloads = MakeJsonPayloadsWithOtherKey("jv", 60, 10, 4);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        constexpr ui64 kLimit = 100;

        AssertOnOffSameResult(
            RunDictionaryOnlyOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, "", kLimit);
            }),
            10u,
            "JSON_VALUE DISTINCT over a dictionary sub-column: pushdown must match plain",
            ECounterOnForce::MustGrow,
            "dictionary-only fetch expected for DISTINCT over a dictionary sub-column");

        // Filter on another path of the same JSON column: rows are needed, dictionary-only must stay off.
        const TString where = R"(WHERE JSON_VALUE(json_payload, "$.other") = "grp_1")";
        AssertOnOffSameResult(
            RunDictionaryOnlyOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, where, kLimit);
            }),
            5u,
            "JSON_VALUE DISTINCT over a dictionary sub-column + JSON filter: pushdown must match plain",
            ECounterOnForce::MustStay);
    }

    // Same as DictionarySubColumn, but the JSON column is stored with dense dictionary encoding.
    Y_UNIT_TEST(OneShard_JsonValueDistinct_DenseDictionarySubColumn_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        auto tableClient = kikimr.GetTableClient();
        {
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto res = session.ExecuteSchemeQuery(R"(
                ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=json_payload,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0`,
                    `DICTIONARY_UNIQUE_FRACTION`=`1`, `DENSE_ENCODING_VERSION`=`1`);
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const auto ts = PickTimestampsForShard(0, 1, 60, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 60, 60);
        const auto jsonPayloads = MakeJsonPayloadsWithOtherKey("jv", 60, 10, 4);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        constexpr ui64 kLimit = 100;

        AssertOnOffSameResult(
            RunDictionaryOnlyOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, "", kLimit);
            }),
            10u,
            "JSON_VALUE DISTINCT over a dense dictionary sub-column: pushdown must match plain",
            ECounterOnForce::MustGrow,
            "dictionary-only fetch expected for DISTINCT over a dense dictionary sub-column");

        const TString where = R"(WHERE JSON_VALUE(json_payload, "$.other") = "grp_1")";
        AssertOnOffSameResult(
            RunDictionaryOnlyOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, where, kLimit);
            }),
            5u,
            "JSON_VALUE DISTINCT over a dense dictionary sub-column + JSON filter: pushdown must match plain",
            ECounterOnForce::MustStay);
    }

    // JSON_VALUE of an absent path and of JSON null both become SQL NULL; DISTINCT must keep a single NULL.
    Y_UNIT_TEST(OneShard_JsonValueDistinct_MissingPathAndJsonNull_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const auto ts = PickTimestampsForShard(0, 1, 40, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 40, 40);
        const auto jsonPayloads = MakeJsonPayloadsWithMissingPathAndJsonNull(40, 2);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kLimit = 100;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, "", kLimit);
            }),
            3u,
            "JSON_VALUE DISTINCT with missing path and JSON null: force on/off must match (single NULL)");
    }

    // Dictionary-only SUB_COLUMNS: some rows have no value for the JSON key, so FinishDictionaryOnly adds a NULL row.
    Y_UNIT_TEST(OneShard_JsonValueDistinct_DictionarySubColumn_MissingPath_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        auto tableClient = kikimr.GetTableClient();
        {
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto res = session.ExecuteSchemeQuery(R"(
                ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=json_payload,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0`, `DICTIONARY_UNIQUE_FRACTION`=`1`);
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const auto ts = PickTimestampsForShard(0, 1, 60, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 60, 60);
        const auto jsonPayloads = MakeJsonPayloadsWithMissingPathOnly(60, 2);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        constexpr ui64 kLimit = 100;
        AssertOnOffSameResult(
            RunDictionaryOnlyOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, "", kLimit);
            }),
            3u,
            "JSON_VALUE DISTINCT over a dictionary sub-column with missing path: NULL must appear once",
            ECounterOnForce::MustGrow,
            "dictionary-only fetch expected for DISTINCT over a sparse dictionary sub-column");
    }

    // Same missing-path NULL as DictionarySubColumn, with dense dictionary encoding.
    Y_UNIT_TEST(OneShard_JsonValueDistinct_DenseDictionarySubColumn_MissingPath_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding helper(kikimr);
        helper.SetWithJsonDocument(true);
        helper.SetShardingMethod("HASH_FUNCTION_MODULO_N");
        helper.CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        auto tableClient = kikimr.GetTableClient();
        {
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto res = session.ExecuteSchemeQuery(R"(
                ALTER OBJECT `/Root/olapStore` (TYPE TABLESTORE) SET (ACTION=ALTER_COLUMN, NAME=json_payload,
                    `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0`,
                    `DICTIONARY_UNIQUE_FRACTION`=`1`, `DENSE_ENCODING_VERSION`=`1`);
            )").GetValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        const auto ts = PickTimestampsForShard(0, 1, 60, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 60, 60);
        const auto jsonPayloads = MakeJsonPayloadsWithMissingPathOnly(60, 2);
        helper.SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRowsWithJsonPayload(ts, rids, jsonPayloads, "u"));

        constexpr ui64 kLimit = 100;
        AssertOnOffSameResult(
            RunDictionaryOnlyOnOff(kikimr, [&](bool withForce) {
                return RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", withForce, "", kLimit);
            }),
            3u,
            "JSON_VALUE DISTINCT over a dense dictionary sub-column with missing path: NULL must appear once",
            ECounterOnForce::MustGrow,
            "dictionary-only fetch expected for DISTINCT over a dense sparse sub-column");
    }

    // No matching rows: SYNC_DISTINCT_LIMIT must forward empty stages without breaking the scan pipeline.
    Y_UNIT_TEST(OneShard_WhereFalse_ForcedDistinct_EmptySameAsPlain) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        const auto ts = PickTimestampsForShard(0, 1, 20, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 5, 20);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr TStringBuf kWhereFalse = "WHERE 1 = 0";
        constexpr ui64 kLimit = 10;

        auto resOff = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", TString(kWhereFalse), {}, kLimit);
        auto resOn = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", TString(kWhereFalse), {}, kLimit);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 0u);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 0u);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson, "empty result: forced vs plain");
    }

    // Nullable DISTINCT key: NULL must count as a single distinct value.
    Y_UNIT_TEST(OneShard_NullableLevel_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const auto ts = PickTimestampsForShard(0, 1, 30, 1);
        const auto rids = MakeRepeatedResourceIds("rid", 10, 30);
        std::vector<std::optional<i32>> levels;
        levels.reserve(30);
        for (ui32 i = 0; i < 30; ++i) {
            if (i % 3 == 0) {
                levels.emplace_back(std::nullopt);
            } else if (i % 3 == 1) {
                levels.emplace_back(1);
            } else {
                levels.emplace_back(2);
            }
        }
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u", &levels));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kCap = 10;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctScanQuery(
                    tableClient, "/Root/olapStore/olapTable", withForce, "level", "`level`", {}, {}, kCap);
            }),
            3u,
            "nullable level DISTINCT: force distinct on/off must match (NULL is one distinct value)");
    }

    // ORDER BY + LIMIT with force distinct only (no force limit pragma): ordered results must match.
    Y_UNIT_TEST(OneShard_OrderByLimit_ForceDistinctOnly_OnOff_SameOrderedResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const auto ts = PickTimestampsForShard(0, 1, 100, 1);
        const auto rids = MakeRepeatedResourceIds("dup", 10, 100);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kLimit = 7;
        const TString tablePath = "/Root/olapStore/olapTable";
        AssertQueryPlanContains(tableClient,
            BuildDistinctScanQueryText(tablePath, true, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit, false),
            "KqpOlapDistinct");
        auto resOff = RunDistinctScanQuery(tableClient, tablePath, false, "resource_id", "resource_id", {},
            "ORDER BY resource_id", kLimit, false);
        auto resOn = RunDistinctScanQuery(tableClient, tablePath, true, "resource_id", "resource_id", {},
            "ORDER BY resource_id", kLimit, false);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kLimit);
        CompareYson(resOff.ResultSetYson, resOn.ResultSetYson,
            "ORDER BY + LIMIT without force limit pragma: row order must match with force distinct on/off");
    }

    // Non-string DISTINCT key column (Timestamp).
    Y_UNIT_TEST(OneShard_DistinctTimestamp_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem(
            "/Root/olapStore/olapTable", BuildBatchForTimestamps(PickTimestampsForShard(0, 1, 50), "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kCap = 50;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctScanQuery(
                    tableClient, "/Root/olapStore/olapTable", withForce, "timestamp", "`timestamp`", {}, {}, kCap);
            }),
            kCap,
            "DISTINCT timestamp: force distinct on/off must match");
    }

    // Several ingestion batches (multiple portions); only 10 distinct resource_id values total.
    Y_UNIT_TEST(MultiInsert_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const TString tablePath = "/Root/olapStore/olapTable";
        const auto rids = MakeRepeatedResourceIds("multi", 10, 40);
        const auto ts1 = PickTimestampsForShard(0, 1, 40, 1);
        const auto ts2 = PickTimestampsForShard(0, 1, 40, ts1.back() + 1);
        const auto ts3 = PickTimestampsForShard(0, 1, 40, ts2.back() + 1);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem(tablePath, BuildBatchForRows(ts1, rids, "u1"));
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem(tablePath, BuildBatchForRows(ts2, rids, "u2"));
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem(tablePath, BuildBatchForRows(ts3, rids, "u3"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kCap = 100;
        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                return RunDistinctQuery(tableClient, tablePath, withForce, kCap);
            }),
            10u,
            "multi-insert DISTINCT (10 uniques): force distinct on/off must match");
    }

    // Aggregate pushdown wins over force-distinct (no KqpOlapDistinct in plan); both arms share the
    // same execution path. Checks result compatibility when agg + force pragmas are combined.
    Y_UNIT_TEST(SimpleDistinct_WithAggPushdownAndForcePragma_OnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);

        const auto ts = PickTimestampsForShard(0, 1, 100, 1);
        const auto rids = MakeRepeatedResourceIds("dup", 10, 100);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForRows(ts, rids, "u"));

        auto tableClient = kikimr.GetTableClient();
        // level values are idx % 5 in test batches → at most 5 distinct levels.
        constexpr ui64 kCap = 5;

        TStringBuilder qBase;
        qBase << R"(
            --!syntax_v1
            PRAGMA Kikimr.OptEnableOlapPushdown = "true";
            PRAGMA Kikimr.OptEnableOlapPushdownAggregate = "true";
        )";

        TStringBuilder qOff;
        qOff << qBase << R"(
            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` LIMIT )" << kCap;

        TStringBuilder qOn;
        qOn << qBase << R"(
            PRAGMA Kikimr.OptForceOlapPushdownDistinct = "level";
            PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = ")" << kCap << R"(";
            SELECT DISTINCT `level` FROM `/Root/olapStore/olapTable` LIMIT )" << kCap;

        AssertQueryPlanNotContains(tableClient, qOn, "KqpOlapDistinct");

        AssertOnOffSameResult(
            RunForcedDistinctOnOff(kikimr, [&](bool withForce) {
                auto it = tableClient.StreamExecuteScanQuery(withForce ? TString(qOn) : TString(qOff)).GetValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
                return CollectStreamResult(it);
            }),
            kCap,
            "DISTINCT level with agg+force pragmas: force pragma on/off must match",
            ECounterOnForce::MustStay);
    }
}

} // namespace NKikimr::NKqp
