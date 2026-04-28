#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/helpers/local.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <optional>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NKqp {

namespace {

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

TCollectedStreamResult RunDistinctQuery(NYdb::NTable::TTableClient& tableClient, const TString& tablePath, const bool enablePushdown) {
    return RunDistinctScanQuery(tableClient, tablePath, enablePushdown, "resource_id", "resource_id", {}, {}, std::nullopt);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpOlapDistinctPushdownE2E) {

    Y_UNIT_TEST(OneShard_DistinctOnOff_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        // single columnshard & single table shard to avoid cross-shard merge effects
        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForTimestamps(PickTimestampsForShard(0, 1, 100), "u"));

        auto tableClient = kikimr.GetTableClient();

        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true);

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
        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true);

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

        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true);

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
        auto resOff = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", false);
        auto resOn = RunDistinctQuery(tableClient, "/Root/olapStore/olapTable", true);

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
        auto resOff = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);
        auto resOn = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kLimit);
    }

    Y_UNIT_TEST(OneShard_AllUniques_DistinctOnOff_Limit_SameResult) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        TLocalHelperModuloTsSharding(kikimr).SetShardingMethod("HASH_FUNCTION_MODULO_N").CreateTestOlapTable("olapTable", "olapStore", 1, 1);
        TLocalHelperModuloTsSharding(kikimr).SendDataViaActorSystem("/Root/olapStore/olapTable", BuildBatchForTimestamps(PickTimestampsForShard(0, 1, 100), "u"));

        auto tableClient = kikimr.GetTableClient();
        constexpr ui64 kLimit = 25;
        auto resOff = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);
        auto resOn = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kLimit);
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
        auto resOff = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);
        auto resOn = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, "resource_id", "resource_id", {}, "ORDER BY resource_id", kLimit);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, kLimit);
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
        auto resOff = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "level", "`level`", where, {}, std::nullopt);
        auto resOn = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, "level", "`level`", where, {}, std::nullopt);

        UNIT_ASSERT_VALUES_EQUAL(resOff.RowsCount, 5);
        UNIT_ASSERT_VALUES_EQUAL(resOn.RowsCount, 5);
        CompareYsonUnordered(resOff.ResultSetYson, resOn.ResultSetYson, "distinct+pk-prefix where results differ with pushdown on/off");

        constexpr ui64 kLimit = 3;
        auto resOffL = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", false, "level", "`level`", where, "ORDER BY `level`", kLimit);
        auto resOnL = RunDistinctScanQuery(tableClient, "/Root/olapStore/olapTable", true, "level", "`level`", where, "ORDER BY `level`", kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOffL.RowsCount, kLimit);
        UNIT_ASSERT_VALUES_EQUAL(resOnL.RowsCount, kLimit);
    }

    // NOTE: In forced pushdown mode we intentionally keep KQP-side logic minimal.
    // More complex filters (e.g. non-PK predicates) are expected to be handled correctly by KQP later.
}

} // namespace NKikimr::NKqp
