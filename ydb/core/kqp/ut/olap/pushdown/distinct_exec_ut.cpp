#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/helpers/aggregation.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <optional>

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

    Y_UNIT_TEST(OneShard_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        // single columnshard & single table shard to avoid cross-shard merge effects
        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForTimestamps(PickTimestampsForShard(0, 1, 100), "u"));

        auto tableClient = kikimr.GetTableClient();

        constexpr ui64 kCap = 100;
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false, kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true, kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(
            syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 100);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 100);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson, "distinct results differ with pushdown on/off");
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false, kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true, kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 10);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 10);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson, "distinct results differ with pushdown on/off");
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false, kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true, kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 100);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 100);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson, "distinct results differ with pushdown on/off");
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false, kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true, kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 80);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 80);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson, "distinct results differ with pushdown on/off");
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {},
            "ORDER BY resource_id", kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", {},
            "ORDER BY resource_id", kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 10);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 10);
        CompareYson(resOff.ResultSetYson, resOn.ResultSetYson,
            "ORDER BY (full distinct set): row order must match with OLAP distinct pushdown on/off");
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "level", "`level`", where, {}, kDistinctCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, "level", "`level`", where, {}, kDistinctCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 5);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson, "distinct+pk-prefix where results differ with pushdown on/off");

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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", where, {}, kHighLimit);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", where, {}, kHighLimit);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 10u);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 10u);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", where, {}, kLimit);
        const i64 predicateAfterOff = ReadPredicateFilterInvocations(kikimr);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", where, {}, kLimit);
        const i64 predicateAfterOn = ReadPredicateFilterInvocations(kikimr);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(predicateAfterOff, predicateBefore);
        UNIT_ASSERT_VALUES_EQUAL(predicateAfterOn, predicateBefore);
        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kLimit);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", where, {}, kLimit);
        const i64 predicateAfterOff = ReadPredicateFilterInvocations(kikimr);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", where, {}, kLimit);
        const i64 predicateAfterOn = ReadPredicateFilterInvocations(kikimr);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_C(predicateAfterOn > predicateBefore,
            TStringBuilder() << "Partial PK range must run TPredicateFilter; before=" << predicateBefore
                             << " after_off=" << predicateAfterOff << " after_on=" << predicateAfterOn);
        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kLimit);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, where, kLimit);
        const i64 predicateAfterOff = ReadPredicateFilterInvocations(kikimr);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunJsonValueDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, where, kLimit);
        const i64 predicateAfterOn = ReadPredicateFilterInvocations(kikimr);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_C(predicateAfterOn > predicateBefore,
            TStringBuilder() << "Partial PK range must run TPredicateFilter; before=" << predicateBefore
                             << " after_off=" << predicateAfterOff << " after_on=" << predicateAfterOn);
        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kLimit);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
            "JSON_VALUE DISTINCT + WHERE partial portion + low distinct limit: pushdown must match plain");
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", false, "level", "`level`", {}, {}, kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", true, "level", "`level`", {}, {}, kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 3u);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 3u);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", false, "timestamp", "`timestamp`", {}, {}, kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctScanQuery(
            tableClient, "/Root/olapStore/olapTable", true, "timestamp", "`timestamp`", {}, {}, kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kCap);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kCap);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
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
        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOff = RunDistinctQuery(tableClient, tablePath, false, kCap);
        const i64 syncAfterOff = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto resOn = RunDistinctQuery(tableClient, tablePath, true, kCap);
        const i64 syncAfterOn = ReadDistinctLimitSyncPointInvocations(kikimr);

        UNIT_ASSERT_VALUES_EQUAL(syncAfterOff, syncBefore);
        UNIT_ASSERT_C(syncAfterOn > syncAfterOff,
            TStringBuilder() << "DistinctLimit sync point expected with force; before=" << syncBefore << " after_off=" << syncAfterOff
                             << " after_on=" << syncAfterOn);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 10u);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 10u);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
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

        const i64 syncBefore = ReadDistinctLimitSyncPointInvocations(kikimr);
        auto itOff = tableClient.StreamExecuteScanQuery(qOff).GetValueSync();
        UNIT_ASSERT_C(itOff.IsSuccess(), itOff.GetIssues().ToString());
        auto resOff = CollectStreamResult(itOff);

        auto itOn = tableClient.StreamExecuteScanQuery(qOn).GetValueSync();
        UNIT_ASSERT_C(itOn.IsSuccess(), itOn.GetIssues().ToString());
        auto resOn = CollectStreamResult(itOn);
        const i64 syncAfter = ReadDistinctLimitSyncPointInvocations(kikimr);
        UNIT_ASSERT_VALUES_EQUAL(syncAfter, syncBefore);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kCap);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kCap);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson,
            "DISTINCT level with agg+force pragmas: force pragma on/off must match");
    }
}

} // namespace NKikimr::NKqp
