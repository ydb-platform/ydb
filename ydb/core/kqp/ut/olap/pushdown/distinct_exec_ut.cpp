#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
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

class TLocalHelperModuloTsSharding: public TLocalHelper {
public:
    using TLocalHelper::TLocalHelper;

    std::vector<TString> GetShardingColumns() const override {
        return {"timestamp"};
    }
};

std::shared_ptr<arrow::RecordBatch> BuildBatchForRows(const std::vector<i64>& timestamps, const std::vector<TString>& resourceIds, const TString& uidPrefix) {
    Y_ABORT_UNLESS(timestamps.size() == resourceIds.size());
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

    ui64 idx = 0;
    for (ui64 i = 0; i < timestamps.size(); ++i) {
        const auto ts = timestamps[i];
        Y_ABORT_UNLESS(tsBuilder.Append(ts).ok());
        const TString& rid = resourceIds[i];
        Y_ABORT_UNLESS(resourceBuilder.Append(rid.data(), rid.size()).ok());
        const TString uid = TStringBuilder() << uidPrefix << "_" << idx;
        Y_ABORT_UNLESS(uidBuilder.Append(uid.data(), uid.size()).ok());
        Y_ABORT_UNLESS(levelBuilder.Append((i32)(idx % 5)).ok());
        Y_ABORT_UNLESS(msgBuilder.Append("m").ok());
        Y_ABORT_UNLESS(ncolBuilder.Append(idx).ok());
        ++idx;
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

TCollectedStreamResult RunDistinctScanQuery(
    NYdb::NTable::TTableClient& tableClient,
    const TString& tablePath,
    bool enablePushdown,
    const TString& forceDistinctColumn,
    const TString& selectList,
    const TString& whereClause,
    const TString& orderByClause,
    const std::optional<ui64> limit)
{
    TStringBuilder q;
    q << R"(
        --!syntax_v1
        PRAGMA Kikimr.OptEnableOlapPushdown = "true";
    )";
    if (enablePushdown) {
        q << R"(
        PRAGMA Kikimr.OptForceOlapPushdownDistinct = ")" << forceDistinctColumn << R"(";
    )";
        if (limit.has_value()) {
            q << R"(
        PRAGMA Kikimr.OptForceOlapPushdownDistinctLimit = ")" << *limit << R"(";
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
    if (limit.has_value()) {
        q << "\nLIMIT " << *limit;
    }

    auto it = tableClient.StreamExecuteScanQuery(q).GetValueSync();
    UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
    return CollectStreamResult(it);
}

TCollectedStreamResult RunDistinctQuery(
    NYdb::NTable::TTableClient& tableClient,
    const TString& tablePath,
    const bool enablePushdown,
    ui64 sqlLimit)
{
    return RunDistinctScanQuery(
        tableClient, tablePath, enablePushdown, "resource_id", "resource_id", {}, {}, std::optional<ui64>(sqlLimit));
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
}

} // namespace NKikimr::NKqp
