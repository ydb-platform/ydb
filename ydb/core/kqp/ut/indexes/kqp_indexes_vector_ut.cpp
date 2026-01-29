#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/kqp/runtime/kqp_vector_index_cache.h>

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/printf.h>

#include <format>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpVectorIndexes) {

    constexpr int F_NULLABLE        = 1 << 0;
    constexpr int F_COVERING        = 1 << 1;
    constexpr int F_UNDERSCORE_DATA = 1 << 2;
    constexpr int F_BIT_VECTOR      = 1 << 3;
    constexpr int F_NON_PARTITIONED = 1 << 4;
    constexpr int F_RETURNING       = 1 << 5;
    constexpr int F_SIMILARITY      = 1 << 6;
    constexpr int F_OVERLAP         = 1 << 7;

    NYdb::NTable::TDataQueryResult ExecuteDataQuery(TSession& session, const TString& query) {
        const auto txSettings = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
        return session.ExecuteDataQuery(query, txSettings,
            TExecDataQuerySettings().KeepInQueryCache(true).CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
    }

    std::vector<i64> DoPositiveQueryVectorIndex(TSession& session, TTxSettings txSettings, const TString& query, int flags = 0) {
        {
            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to explain: `" << query << "` with " << result.GetIssues().ToString());

            if (flags & F_COVERING) {
                // Check that the query doesn't use main table
                NJson::TJsonValue plan;
                NJson::ReadJsonTree(result.GetPlan(), &plan, true);
                UNIT_ASSERT(ValidatePlanNodeIds(plan));
                auto mainTableAccess = CountPlanNodesByKv(plan, "Table", "TestTable");
                UNIT_ASSERT_VALUES_EQUAL(mainTableAccess, 0);
            }
        }
        {
            auto result = session.ExecuteDataQuery(query,
                TTxControl::BeginTx(txSettings).CommitTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to execute: `" << query << "` with " << result.GetIssues().ToString());

            std::vector<i64> r;
            auto sets = result.GetResultSets();
            for (const auto& set : sets) {
                TResultSetParser parser{set};
                while (parser.TryNextRow()) {
                    auto value = parser.GetValue("pk");
                    UNIT_ASSERT_C(value.GetProto().has_int64_value(), value.GetProto().ShortUtf8DebugString());
                    r.push_back(value.GetProto().int64_value());
                }
            }
            return r;
        }
    }

    void DoPositiveQueriesVectorIndex(TSession& session, TTxSettings txSettings, const TString& mainQuery, const TString& indexQuery, int flags = 0) {
        auto toStr = [](const auto& rs) -> TString {
            TStringBuilder b;
            for (const auto& r : rs) {
                b << r << ", ";
            }
            return b;
        };
        auto mainResults = DoPositiveQueryVectorIndex(session, txSettings, mainQuery);
        absl::c_sort(mainResults);
        UNIT_ASSERT_EQUAL_C(mainResults.size(), 3, toStr(mainResults));
        UNIT_ASSERT_C(std::unique(mainResults.begin(), mainResults.end()) == mainResults.end(), toStr(mainResults));

        auto indexResults = DoPositiveQueryVectorIndex(session, txSettings, indexQuery, flags);
        absl::c_sort(indexResults);
        UNIT_ASSERT_EQUAL_C(indexResults.size(), 3, toStr(indexResults));
        UNIT_ASSERT_C(std::unique(indexResults.begin(), indexResults.end()) == indexResults.end(), toStr(indexResults));

        UNIT_ASSERT_VALUES_EQUAL(mainResults, indexResults);
    }

    void DoPositiveQueriesVectorIndexOrderBy(
        TSession& session,
        TTxSettings txSettings,
        std::string_view function,
        std::string_view direction,
        std::string_view left,
        std::string_view right,
        int flags = 0
    ) {
        constexpr std::string_view uint8Target = "$target = \"\x67\x71\x02\";";
        constexpr std::string_view bitTarget = "$target = \"\x3F\x02\x0A\";";
        const std::string_view target = flags & F_BIT_VECTOR ? bitTarget : uint8Target;
        std::string metric = std::format("Knn::{}({}, {})", function, left, right);
        // no metric in result
        {
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT * FROM `/Root/TestTable`
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "3";
                {}
                SELECT * FROM `/Root/TestTable` VIEW index
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, direction)));
            DoPositiveQueriesVectorIndex(session, txSettings, plainQuery, indexQuery, flags);
        }
        // metric in result
        {
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable`
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"({}
                pragma ydb.KMeansTreeSearchTopSize = "2";
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, metric, direction)));
            DoPositiveQueriesVectorIndex(session, txSettings, plainQuery, indexQuery, flags);
        }
        // metric as result
        {
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable`
                ORDER BY m {}
                LIMIT 3;
            )", target, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                {}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                ORDER BY m {}
                LIMIT 3;
            )", target, metric, direction)));
            DoPositiveQueriesVectorIndex(session, txSettings, plainQuery, indexQuery, flags);
        }
    }

    void DoPositiveQueriesVectorIndexOrderBy(
        TSession& session,
        TTxSettings txSettings,
        std::string_view function,
        std::string_view direction,
        int flags = 0) {
        // target is left, member is right
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, function, direction, "$target", "emb", flags);
        // target is right, member is left
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, function, direction, "emb", "$target", flags);
    }

    void DoPositiveQueriesVectorIndexOrderByCosine(
        TSession& session,
        int flags = 0,
        TTxSettings txSettings = TTxSettings::SerializableRW()) {
        // distance, default direction
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, "CosineDistance", "", flags);
        // distance, asc direction
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, "CosineDistance", "ASC", flags);
        // similarity, desc direction
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, "CosineSimilarity", "DESC", flags);
    }

    TSession DoOnlyCreateTableForVectorIndex(TTableClient& db, int flags = 0) {
        const char* dataCol = flags & F_UNDERSCORE_DATA ? "___data" : "data";
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto tableBuilder = db.GetTableBuilder();
            if (flags & F_NULLABLE) {
                tableBuilder
                    .AddNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNullableColumn("emb", EPrimitiveType::String)
                    .AddNullableColumn(dataCol, EPrimitiveType::String);
            } else {
                tableBuilder
                    .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNonNullableColumn("emb", EPrimitiveType::String)
                    .AddNonNullableColumn(dataCol, EPrimitiveType::String);
            }
            tableBuilder.SetPrimaryKeyColumns({"pk"});
            if (!(flags & F_NON_PARTITIONED)) {
                tableBuilder.BeginPartitioningSettings()
                    .SetMinPartitionsCount(3)
                .EndPartitioningSettings();
                auto partitions = TExplicitPartitions{}
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(4).EndTuple().Build())
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(6).EndTuple().Build());
                tableBuilder.SetPartitionAtKeys(partitions);
            }
            auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        return session;
    }

    TSession DoCreateTableForVectorIndex(TTableClient& db, int flags = 0) {
        const char* dataCol = flags & F_UNDERSCORE_DATA ? "___data" : "data";
        auto session = DoOnlyCreateTableForVectorIndex(db, flags);
        {
            const TString query1 = TStringBuilder()
                << "UPSERT INTO `/Root/TestTable` (pk, emb, " << dataCol << ") VALUES "
                << "(0, \"\x03\x30\x02\", \"0\"),"
                    "(1, \"\x13\x31\x02\", \"1\"),"
                    "(2, \"\x23\x32\x02\", \"2\"),"
                    "(3, \"\x53\x33\x02\", \"3\"),"
                    "(4, \"\x43\x34\x02\", \"4\"),"
                    "(5, \"\x50\x60\x02\", \"5\"),"
                    "(6, \"\x61\x11\x02\", \"6\"),"
                    "(7, \"\x12\x62\x02\", \"7\"),"
                    "(8, \"\x75\x76\x02\", \"8\"),"
                    "(9, \"\x76\x76\x02\", \"9\");";

            auto result = session.ExecuteDataQuery(Q_(query1), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        return session;
    }

    void DoCreateVectorIndex(TSession& session, int flags = 0) {
        // Add an index
        const TString createIndex(Q_(Sprintf(R"(
            ALTER TABLE `/Root/TestTable`
                ADD INDEX index1
                GLOBAL USING vector_kmeans_tree
                ON (emb)%s
                WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2%s);
            )", (flags & F_COVERING) ? ((flags & F_UNDERSCORE_DATA) ? " COVER (___data, emb)" : " COVER (data, emb)") : "",
            (flags & F_OVERLAP) ? ", overlap_clusters=2" : "")));

        auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TSession DoCreateTableAndVectorIndex(TTableClient& db, int flags = 0) {
        auto session = DoCreateTableForVectorIndex(db, flags);
        DoCreateVectorIndex(session, flags);
        return session;
    }

    TSession DoCreateTableForVectorIndexWithBitQuantization(TTableClient& db, int flags = 0) {
        auto session = DoOnlyCreateTableForVectorIndex(db, flags);
        {
            const TString query1 = TStringBuilder()
                << "UPSERT INTO `/Root/TestTable` (pk, emb, data) VALUES "
                << "(1, Untag(Knn::ToBinaryStringBit([1.f, 0.f, 0.f, 0.f, 0.f, 0.f]), \"BitVector\"), \"1\"),"
                    "(2, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 0.f, 0.f, 0.f, 0.f]), \"BitVector\"), \"2\"),"
                    "(3, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 0.f, 0.f, 0.f]), \"BitVector\"), \"3\"),"
                    "(4, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 0.f, 0.f]), \"BitVector\"), \"4\"),"
                    "(5, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 0.f]), \"BitVector\"), \"5\"),"
                    "(6, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 1.f]), \"BitVector\"), \"6\");";

            auto result = session.ExecuteDataQuery(Q_(query1), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        return session;
    }

    void DoCheckOverlap(TSession& session, const TString& indexName) {
        // Check number of rows in the posting table (should be 2x input rows)
        {
            const TString query1(Q_(Sprintf(R"(
                SELECT COUNT(*), COUNT(DISTINCT pk) FROM `/Root/TestTable/%s/indexImplPostingTable`;
            )", indexName.c_str())));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[20u;10u]]");
        }

        // Check that a select query without main table PK columns works
        // (it didn't because UniqueColumns filtering pushdown requires PK columns in the result)
        {
            const TString query1(Q1_(Sprintf(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $target = "\x67\x71\x02";
                SELECT data FROM `/Root/TestTable` VIEW %s
                ORDER BY Knn::CosineDistance(emb, $target)
                LIMIT 3;
            )", indexName.c_str())));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to execute: `" << query1 << "` with " << result.GetIssues().ToString());
        }
    }

    // Test that vector index queries work when selecting only non-PK columns
    Y_UNIT_TEST_TWIN(VectorIndexSelectWithoutPkColumns, Overlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = (Overlap ? F_OVERLAP : 0);
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        // Query selecting only 'data' column (not PK columns)
        {
            const TString query1(Q1_(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $target = "\x67\x71\x02";
                SELECT data FROM `/Root/TestTable` VIEW index1
                ORDER BY Knn::CosineDistance(emb, $target)
                LIMIT 3;
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to execute: `" << query1 << "` with " << result.GetIssues().ToString());
        }
    }

    void DoTestOrderByCosine(ui32 indexLevels, int flags) {
        // Clear cache to prevent pollution from other tests
        TVectorIndexLevelCache::Instance().Clear();

        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndex(db, flags);
        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)%s
                    WITH (%s=cosine, vector_type="uint8", vector_dimension=2, levels=%d, clusters=2%s);
                )",
                (flags & F_COVERING ? " COVER (data, emb)" : ""),
                (flags & F_SIMILARITY ? "similarity" : "distance"),
                indexLevels,
                (flags & F_OVERLAP ? ", overlap_clusters=2" : ""))));

            auto result = session.ExecuteSchemeQuery(createIndex)
                          .ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), std::vector<std::string>{"emb"});
            if (flags & F_COVERING) {
                std::vector<std::string> indexDataColumns{"data", "emb"};
                UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), indexDataColumns);
            }
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, flags & F_SIMILARITY
                ? NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity
                : NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, indexLevels);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
            UNIT_ASSERT_EQUAL(settings.OverlapClusters, (flags & F_OVERLAP) ? 2 : 0);
            UNIT_ASSERT_EQUAL(settings.OverlapRatio, 0);
        }

        if (flags & F_OVERLAP) {
            DoCheckOverlap(session, "index");
        }

        DoPositiveQueriesVectorIndexOrderByCosine(session, flags);

        {
            const TString dropIndex(Q_("ALTER TABLE `/Root/TestTable` DROP INDEX index"));
            auto result = session.ExecuteSchemeQuery(dropIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel1, Nullable, UseSimilarity) {
        DoTestOrderByCosine(1, (Nullable ? F_NULLABLE : 0) | (UseSimilarity ? F_SIMILARITY : 0));
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel1WithOverlap, Nullable, Covered) {
        DoTestOrderByCosine(1, F_OVERLAP | (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel2, Nullable, UseSimilarity) {
        DoTestOrderByCosine(2, (Nullable ? F_NULLABLE : 0) | (UseSimilarity ? F_SIMILARITY : 0));
    }

    Y_UNIT_TEST_TWIN(OrderByCosineLevel2WithCover, Nullable) {
        DoTestOrderByCosine(2, (Nullable ? F_NULLABLE : 0) | F_COVERING);
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel2WithOverlap, Nullable, Covered) {
        DoTestOrderByCosine(2, F_OVERLAP | (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel3) {
        DoTestOrderByCosine(3, 0);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel3WithOverlap) {
        DoTestOrderByCosine(3, F_OVERLAP);
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel1WithBitQuantization, Nullable, Overlap) {
        // Clear cache to prevent pollution from other tests
        TVectorIndexLevelCache::Instance().Clear();

        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_BIT_VECTOR | (Nullable ? F_NULLABLE : 0);
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndexWithBitQuantization(db, flags);
        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (similarity=cosine, vector_type="bit", vector_dimension=6, levels=1, clusters=2%s);
            )", Overlap ? ", overlap_clusters=2" : "")));

            auto result = session.ExecuteSchemeQuery(createIndex)
                          .ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), std::vector<std::string>{"emb"});
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Bit);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 6);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session, flags);

        if (flags & F_OVERLAP) {
            DoCheckOverlap(session, "index");
        }
    }

    Y_UNIT_TEST(OrderByNoUnwrap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        {
            const TString query1(Q1_(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $TargetEmbedding = String::HexDecode("677102");
                SELECT * FROM `/Root/TestTable` VIEW index1
                ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
                LIMIT 3;
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to execute: `" << query1 << "` with " << result.GetIssues().ToString());
        }

        {
            const TString query1(Q1_(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $TargetEmbedding = (SELECT emb FROM `/Root/TestTable` WHERE pk=9);
                SELECT * FROM `/Root/TestTable` VIEW index1
                ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
                LIMIT 3;
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to execute: `" << query1 << "` with " << result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(BuildIndexTimesAndUser) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);

        auto now = TInstant::Now();

        auto driver = NYdb::TDriver(NYdb::TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetDatabase("/Root")
            .SetAuthToken("root@builtin"));
        auto db = NYdb::NTable::TTableClient(driver);
        auto session = DoCreateTableForVectorIndex(db);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )"));

            auto result = session.ExecuteSchemeQuery(createIndex)
                          .ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            NYdb::NOperation::TOperationClient client(kikimr.GetDriver());
            auto list = client.List<NYdb::NTable::TBuildIndexOperation>().ExtractValueSync();
            UNIT_ASSERT_EQUAL(list.GetList().size(), 1);
            auto & op = list.GetList()[0];
            UNIT_ASSERT_EQUAL(op.Status().GetStatus(), NYdb::EStatus::SUCCESS);
            UNIT_ASSERT(op.CreateTime() >= TInstant::Seconds(now.Seconds()));
            UNIT_ASSERT(op.EndTime() >= op.CreateTime());
            UNIT_ASSERT_EQUAL(op.CreatedBy(), "root@builtin");
        }
    }

    Y_UNIT_TEST(VectorIndexNoBulkUpsert) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // BulkUpsert to the table with index should fail
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("pk").Int64(11)
                .AddMember("emb").String("\x77\x77\x02")
                .AddMember("data").String("43")
                .EndStruct();
            rows.EndList();
            auto result = db.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            auto issues = result.GetIssues().ToString();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SCHEME_ERROR, result.GetStatus());
            UNIT_ASSERT_C(issues.contains("Only async-indexed tables are supported by BulkUpsert"), issues);
        }

        const TString postingTable1_bulk = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(originalPostingTable, postingTable1_bulk);
    }

    void DoTestVectorIndexDelete(const TString& deleteQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        {
            auto result = session.ExecuteDataQuery(deleteQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[\"9\"];[\"vv\\2\"];[9]]]");
            }
        }

        // Check that PK 9 is not present in the posting table
        {
            const TString query1(Q_(R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk=9;
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[0u]]");
        }
    }

    Y_UNIT_TEST_QUAD(VectorIndexDeletePk, Covered, Overlap) {
        // DELETE WHERE from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=9;)"),
            (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexDeleteFilter, Covered) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="9";)"), (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexDeleteOn, Covered) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 9 AS `pk`;)"), (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexDeletePkReturning, Covered, Overlap) {
        // DELETE WHERE from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=9 RETURNING data, emb, pk;)"),
            F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexDeleteFilterReturning, Covered) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="9" RETURNING data, emb, pk;)"), F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexDeleteOnReturning, Covered) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 9 AS `pk` RETURNING data, emb, pk;)"), F_RETURNING | (Covered ? F_COVERING : 0));
    }

    void DoTestVectorIndexInsert(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Insert to the table with index should succeed
        {
            TString query1(Q_(R"(
                INSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (10, "\x11\x62\x02", "10"),
                (11, "\x75\x77\x02", "11")
            )"));
            query1 += (flags & F_RETURNING ? " RETURNING data, emb, pk;" : ";");

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[\"10\"];[\"\\021b\\2\"];[10]];[[\"11\"];[\"uw\\2\"];[11]]]");
            }
        }

        // Index is updated
        const TString postingTable1_ins = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that PK 7 and 10 are now in the same cluster
        // Check that PK 8 and 11 are now in the same cluster
        {
            const TString query1(Q_(R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk IN (7, 10)
                UNION ALL
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk IN (8, 11)
                ;
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                (flags & F_OVERLAP ? "[[2u];[2u]]" : "[[1u];[1u]]"));
        }
    }

    Y_UNIT_TEST_QUAD(VectorIndexInsert, Returning, Covered) {
        DoTestVectorIndexInsert((Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexInsertWithOverlap, Returning, Covered) {
        DoTestVectorIndexInsert(F_OVERLAP | (Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0));
    }

    void DoTestVectorIndexUpdateNoChange(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        TString orig = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Update to the table with index should succeed (but embedding does not change)
        {
            const TString query1(Q_(flags & F_UNDERSCORE_DATA
                ? "UPDATE `/Root/TestTable` SET `___data`=\"20\" WHERE `pk`=9;"
                : "UPDATE `/Root/TestTable` SET `data`=\"20\" WHERE `pk`=9;"
            ));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        if (flags & F_COVERING) {
            SubstGlobal(orig, "\"9\"", "\"20\"");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST_TWIN(VectorIndexUpdateNoChange, Overlap) {
        DoTestVectorIndexUpdateNoChange((Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexUpdateNoChangeCovered, Overlap) {
        DoTestVectorIndexUpdateNoChange(F_COVERING | (Overlap ? F_OVERLAP : 0));
    }

    // Similar to VectorIndexUpdateNoChange, but data column is named ___data to make it appear before __ydb_parent in struct types
    Y_UNIT_TEST_TWIN(VectorIndexUpdateColumnOrder, Overlap) {
        DoTestVectorIndexUpdateNoChange(F_COVERING | F_UNDERSCORE_DATA | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpdateNoClusterChange, Covered, Overlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        TString orig = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Update to the table with index should succeed (embedding changes, but the cluster does not)
        {
            const TString query1(Q_(R"(
                UPDATE `/Root/TestTable` SET `emb`="\x76\x75\x02" WHERE `pk`=9;
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        if (Covered) {
            SubstGlobal(orig, "\"\x76\x76\\2\"", "\"\x76\x75\\2\"");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    void DoTestVectorIndexUpdateClusterChange(const TString& updateQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, flags);

        const TString orig = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Update/upsert to the table with index should succeed (and the cluster should change)
        {
            auto result = session.ExecuteDataQuery(updateQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[\"9\"];[\"\\0031\\2\"];[9]]]");
            }
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(orig, updated);

        // Check that PK 9 and 0 are now in the same cluster
        {
            const TString query1(Q_(R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk IN (0, 9);
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                (flags & F_OVERLAP ? "[[2u]]" : "[[1u]]"));
        }
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpdatePkClusterChange, Covered, Overlap) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=9;)"),
            (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexUpdateFilterClusterChange, Covered) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="9";)"),
            (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexUpsertClusterChange, Covered) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `emb`, `data`) VALUES (9, "\x03\x31\x02", "9");)"),
            (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpdatePkClusterChangeReturning, Covered, Overlap) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=9 RETURNING `data`, `emb`, `pk`;)"),
            F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexUpdateFilterClusterChangeReturning, Covered) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="9" RETURNING `data`, `emb`, `pk`;)"),
            F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(VectorIndexUpsertClusterChangeReturning, Covered) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `emb`, `data`) VALUES (9, "\x03\x31\x02", "9") RETURNING `data`, `emb`, `pk`;)"),
            F_RETURNING | (Covered ? F_COVERING : 0));
    }

    // First index level build is processed differently when table has 1 and >1 partitions so we check both cases
    Y_UNIT_TEST_QUAD(EmptyVectorIndexUpdate, Partitioned, Overlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = (Partitioned ? 0 : F_NON_PARTITIONED) | (Overlap ? F_OVERLAP : 0);
        auto db = kikimr.GetTableClient();
        auto session = DoOnlyCreateTableForVectorIndex(db, flags);
        DoCreateVectorIndex(session, flags);

        // Check that the index has a stub cluster hierarchy but the posting table is empty
        const TString level = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplLevelTable");
        UNIT_ASSERT_VALUES_EQUAL(level, "[[[0u];[1u];[\"  \\2\"]];[[1u];[9223372036854775810u];[\"  \\2\"]]]");
        const TString posting = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_VALUES_EQUAL(posting, "[]");

        // Insert to the table with index should succeed
        {
            TString query1(Q_(R"(
                INSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (10, "\x11\x62\x02", "10");
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        // Posting table should be updated
        {
            const TString query1(Q_(R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index1/indexImplPostingTable`;
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[1u]]");
        }

        // The added vector should be found successfully
        {
            const TString query1(Q_(R"(
                SELECT pk FROM `/Root/TestTable`
                VIEW index1
                ORDER BY Knn::CosineDistance(emb, "AA\x02")
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[10]]");
        }
    }

    Y_UNIT_TEST_TWIN(CoveredVectorIndexWithFollowers, StaleRO) {
        const TString mainTableName = "/Root/TestTable";
        const TString levelTableName = "/Root/TestTable/index/indexImplLevelTable";
        const TString postingTableName = "/Root/TestTable/index/indexImplPostingTable";
        std::vector<TString> tableNames = {
            mainTableName,
            levelTableName,
            postingTableName
        };

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableAccessToIndexImplTables(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetEnableForceFollowers(true)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        //kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        //kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_COVERING;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndex(db, flags);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb) COVER (emb, data)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )"));

            auto result = session.ExecuteSchemeQuery(createIndex)
                          .ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto setFollowers = [&session](const TString& tableName) {
            const TString alterTable(Q_(Sprintf(R"(
                ALTER TABLE `%s` SET (READ_REPLICAS_SETTINGS = "PER_AZ:3");
            )", tableName.c_str())));

            auto result = session.ExecuteSchemeQuery(alterTable).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto checkFollowerDescription = [&session](const TString& tableName) {
            auto result = session.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            const auto& table = result.GetTableDescription();
            UNIT_ASSERT(table.GetReadReplicasSettings()->GetMode() == NYdb::NTable::TReadReplicasSettings::EMode::PerAz);
            UNIT_ASSERT_VALUES_EQUAL(table.GetReadReplicasSettings()->GetReadReplicasCount(), 3);
        };

        for (const TString& tableName: tableNames) {
            setFollowers(tableName);
            checkFollowerDescription(tableName);
        }

        DoPositiveQueriesVectorIndexOrderByCosine(session, flags, StaleRO ? TTxSettings::StaleRO() : TTxSettings::SerializableRW());

        if (StaleRO) {
            // from leader - should NOT read
            CheckTableReads(session, postingTableName, false, false);
            // from followers - should read
            CheckTableReads(session, postingTableName, true, true);
        } else {
            // from leader - should read
            CheckTableReads(session, postingTableName, false, true);
            // from followers - should NOT read
            CheckTableReads(session, postingTableName, true, false);
        }

        if (StaleRO) {
            CheckTableReads(session, levelTableName, false, false);
            CheckTableReads(session, levelTableName, true, true);
        } else {
            CheckTableReads(session, levelTableName, false, true);
            CheckTableReads(session, levelTableName, true, false);
        }

        // Etalon reads from main table
        CheckTableReads(session, mainTableName, false, true);
        CheckTableReads(session, mainTableName, true, false);
    }

    Y_UNIT_TEST(OrderByReject) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndex(db);

        for (const auto & check: TVector<TVector<const char*>>({
            {"distance=cosine", "Knn::CosineDistance(emb, 'abc') DESC", "Knn::CosineSimilarity(emb, ...) DESC or Knn::CosineDistance(emb, ...) ASC"},
            {"distance=cosine", "Knn::CosineSimilarity(emb, 'abc') ASC", "Knn::CosineSimilarity(emb, ...) DESC or Knn::CosineDistance(emb, ...) ASC"},
            {"similarity=cosine", "Knn::CosineDistance(emb, 'abc') DESC", "Knn::CosineSimilarity(emb, ...) DESC or Knn::CosineDistance(emb, ...) ASC"},
            {"similarity=cosine", "Knn::CosineSimilarity(emb, 'abc') ASC", "Knn::CosineSimilarity(emb, ...) DESC or Knn::CosineDistance(emb, ...) ASC"},
            {"similarity=inner_product", "Knn::InnerProductSimilarity(emb, 'abc') ASC", "Knn::InnerProductSimilarity(emb, ...) DESC"},
            {"distance=manhattan", "Knn::ManhattanDistance(emb, 'abc') DESC", "Knn::ManhattanDistance(emb, ...) ASC"},
            {"distance=euclidean", "Knn::EuclideanDistance(emb, 'abc') DESC", "Knn::EuclideanDistance(emb, ...) ASC"},
        }))
        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (%s, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )", check[0])));
            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const TString selectQuery(Q1_(Sprintf(R"(
                SELECT * FROM `/Root/TestTable`
                VIEW index ORDER BY %s
            )", check[1])));
            result = ExecuteDataQuery(session, selectQuery);
            UNIT_ASSERT_C(HasIssue(NYdb::NAdapters::ToYqlIssues(result.GetIssues()), NYql::TIssuesIds::KIKIMR_WRONG_INDEX_USAGE,
                [&](const NYql::TIssue& issue) {
                    return issue.GetMessage().Contains("sorting must contain distance: " + TString(check[2]));
                }), result.GetIssues().ToString());
            UNIT_ASSERT(!result.IsSuccess());

            session = db.CreateSession().GetValueSync().GetSession();
            const TString dropIndex(Q_("ALTER TABLE `/Root/TestTable` DROP INDEX index"));
            result = session.ExecuteSchemeQuery(dropIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(VectorResolveDuplicateEvent) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            // SetUseRealThreads(false) is required to capture events (!) but then you have to do kikimr.RunCall() for everything
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE;
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return DoCreateTableAndVectorIndex(db, flags); });

        int capturedCount = 0;
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == NYql::NDq::IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::EventType &&
                runtime->FindActorName(ev->GetRecipientRewrite()) == "NKikimr::NKqp::TKqpVectorResolveActor") {
                capturedCount++;
                if (capturedCount == 3) {
                    // Add a duplicate event (checking issue #27095)
                    auto dup = new NYql::NDq::IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived(
                        ev->Get<NYql::NDq::IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived>()->InputIndex);
                    runtime->Send(new IEventHandle(ev->Recipient, ev->Sender, dup));
                }
            }
            return false;
        };
        runtime->SetEventFilter(captureEvents);

        // Insert to the table with index should succeed
        {
            TString query1(Q_(R"(
                INSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (10, "\x11\x62\x02", "10"),
                (11, "\x77\x75\x02", "11")
            )"));

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT(result.IsSuccess());
        }
    }

    TVector<std::pair<TActorId, TActorId>> ResolveFollowers(TTestActorRuntime &runtime, ui64 tabletId, ui32 nodeIndex) {
        auto sender = runtime.AllocateEdgeActor(nodeIndex);
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), sender,
            new TEvStateStorage::TEvLookup(tabletId, 0)),
            nodeIndex, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvStateStorage::TEvInfo>(sender);
        Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK, "Failed to resolve tablet %" PRIu64, tabletId);
        return std::move(ev->Get()->Followers);
    }

    Y_UNIT_TEST_QUAD(VectorSearchPushdown, Covered, Followers) {
        // Clear cache to prevent pollution from other tests
        TVectorIndexLevelCache::Instance().Clear();

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            // SetUseRealThreads(false) is required to capture events (!) but then you have to do kikimr.RunCall() for everything
            .SetUseRealThreads(false)
            .SetEnableForceFollowers(Followers)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE | (Covered ? F_COVERING : 0);
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return DoCreateTableAndVectorIndex(db, flags); });

        if (Followers) {
            std::vector<TString> tableNames = {
                "/Root/TestTable",
                "/Root/TestTable/index1/indexImplLevelTable",
                "/Root/TestTable/index1/indexImplPostingTable"
            };
            for (const TString& tableName: tableNames) {
                const TString alterTable(Q_(Sprintf(R"(
                    ALTER TABLE `%s` SET (READ_REPLICAS_SETTINGS = "PER_AZ:3");
                )", tableName.c_str())));
                auto result = kikimr.RunCall([&] { return session.ExecuteSchemeQuery(alterTable).ExtractValueSync(); });
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }
        }

        constexpr static ui32 levelType = 1, postingType = 2, mainType = 3;
        constexpr static ui32 followerTypeFlag = 8;
        THashMap<TActorId, ui32> actorTypes;
        auto resolveActors = [&](const char* tableName, ui32 type) {
            auto shards = GetTableShards(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(), tableName);
            for (auto shardId: shards) {
                auto actorId = ResolveTablet(*runtime, shardId);
                actorTypes[actorId] = type;
                if (Followers) {
                    auto followers = ResolveFollowers(*runtime, shardId, 0);
                    for (auto& [_, followerId]: followers) {
                        actorTypes[followerId] = type | followerTypeFlag;
                    }
                }
            }
        };
        resolveActors("/Root/TestTable/index1/indexImplLevelTable", levelType);
        resolveActors("/Root/TestTable/index1/indexImplPostingTable", postingType);
        resolveActors("/Root/TestTable", mainType);

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                ui32 shardType = actorTypes[ev->GetRecipientRewrite()];
                bool isFollower = (shardType & followerTypeFlag);
                shardType = shardType & ~followerTypeFlag;
                // Check that level & posting are read from followers
                UNIT_ASSERT(isFollower == (Followers && (shardType == levelType || shardType == postingType)));
                auto & read = ev->Get<TEvDataShard::TEvRead>()->Record;
                if (shardType == levelType) {
                    // Level table reads do full scan for caching (VectorTopK cleared by cache layer)
                    UNIT_ASSERT(!read.HasVectorTopK());
                } else if (shardType == (Covered ? mainType : postingType)) {
                    // Non-covering index does topK on main table, covering does it on posting
                    UNIT_ASSERT(!read.HasVectorTopK());
                } else {
                    UNIT_ASSERT(shardType != 0);
                    UNIT_ASSERT(read.HasVectorTopK());
                    auto & topK = read.GetVectorTopK();
                    // Check that target and limit are pushed down
                    UNIT_ASSERT(topK.GetTargetVector() == "\x67\x71\x02");
                    if (shardType == (Covered ? postingType : mainType)) {
                        // Equal to LIMIT
                        UNIT_ASSERT(topK.GetLimit() == 3);
                    }
                }
            }
            return false;
        };
        runtime->SetEventFilter(captureEvents);

        {
            TString query1(Q_(R"(
                pragma ydb.KMeansTreeSearchTopSize = "2";
                $TargetEmbedding = String::HexDecode("677102");
                SELECT * FROM `/Root/TestTable`
                VIEW index1 ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
                LIMIT 3
            )"));

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query1, TTxControl::BeginTx(
                    Followers ? TTxSettings::StaleRO() : TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT(result.IsSuccess());
        }
    }

    // Helper to create Float format embedding (3D vector for better distance discrimination)
    // Float format: 4 bytes per float + 0x01 format byte
    static TString MakeFloatEmb(float v1, float v2, float v3) {
        TString result;
        result.resize(sizeof(float) * 3 + 1);
        char* p = result.Detach();
        memcpy(p, &v1, sizeof(float));
        memcpy(p + sizeof(float), &v2, sizeof(float));
        memcpy(p + 2 * sizeof(float), &v3, sizeof(float));
        p[sizeof(float) * 3] = 0x01;  // FloatVector format byte
        return result;
    }

    // Test that HNSW local index works for the posting table of a covered global vector index
    // The posting table contains the `emb` column (covered), so HNSW should be built for it
    // This test verifies that:
    // 1. VectorTopK pushdown is used for the posting table (already covered by VectorSearchPushdown)
    // 2. HNSW local index is actually used for the posting table (verified by LIMIT 7 marker)
    Y_UNIT_TEST(HnswLocalIndexForCoveredVectorIndexPostingTable) {
        // Clear caches to prevent pollution from other tests
        TVectorIndexLevelCache::Instance().Clear();

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            // SetUseRealThreads(false) is required to capture events
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        // Create table with Float embeddings (HNSW requires Float format)
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] {
            return db.CreateSession().GetValueSync().GetSession();
        });

        // Create the main table
        {
            auto tableBuilder = db.GetTableBuilder();
            tableBuilder
                .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                .AddNonNullableColumn("emb", EPrimitiveType::String)
                .AddNonNullableColumn("data", EPrimitiveType::String)
                .SetPrimaryKeyColumns({"pk"});

            auto result = kikimr.RunCall([&] {
                return session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Insert Float embeddings
        {
            auto params = db.GetParamsBuilder()
                .AddParam("$emb0").String(MakeFloatEmb(1.0f, 2.0f, 3.0f)).Build()
                .AddParam("$emb1").String(MakeFloatEmb(4.0f, 5.0f, 6.0f)).Build()
                .AddParam("$emb2").String(MakeFloatEmb(7.0f, 8.0f, 9.0f)).Build()
                .AddParam("$emb3").String(MakeFloatEmb(10.0f, 11.0f, 12.0f)).Build()
                .AddParam("$emb4").String(MakeFloatEmb(13.0f, 14.0f, 15.0f)).Build()
                .AddParam("$emb5").String(MakeFloatEmb(100.0f, 110.0f, 120.0f)).Build()
                .AddParam("$emb6").String(MakeFloatEmb(103.0f, 113.0f, 123.0f)).Build()
                .AddParam("$emb7").String(MakeFloatEmb(106.0f, 116.0f, 126.0f)).Build()
                .AddParam("$emb8").String(MakeFloatEmb(109.0f, 119.0f, 129.0f)).Build()
                .AddParam("$emb9").String(MakeFloatEmb(112.0f, 122.0f, 132.0f)).Build()
                .Build();

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(Q_(R"(
                    DECLARE $emb0 AS String;
                    DECLARE $emb1 AS String;
                    DECLARE $emb2 AS String;
                    DECLARE $emb3 AS String;
                    DECLARE $emb4 AS String;
                    DECLARE $emb5 AS String;
                    DECLARE $emb6 AS String;
                    DECLARE $emb7 AS String;
                    DECLARE $emb8 AS String;
                    DECLARE $emb9 AS String;
                    UPSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                    (0, $emb0, "0"),
                    (1, $emb1, "1"),
                    (2, $emb2, "2"),
                    (3, $emb3, "3"),
                    (4, $emb4, "4"),
                    (5, $emb5, "5"),
                    (6, $emb6, "6"),
                    (7, $emb7, "7"),
                    (8, $emb8, "8"),
                    (9, $emb9, "9");
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params)
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Create COVERED global vector index
        // Note: Using distance=cosine and vector_type="float" for HNSW compatibility
        {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteSchemeQuery(Q_(R"(
                    ALTER TABLE `/Root/TestTable`
                        ADD INDEX index1
                        GLOBAL USING vector_kmeans_tree
                        ON (emb) COVER (data, emb)
                        WITH (distance=cosine, vector_type="float", vector_dimension=3, levels=2, clusters=2);
                )")).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Restart posting table shards to trigger HNSW index building
        // HNSW indexes are built on tablet restart when the table has a vector column (emb)
        {
            auto shards = GetTableShards(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(),
                "/Root/TestTable/index1/indexImplPostingTable");
            for (auto shardId : shards) {
                RebootTablet(*runtime, shardId, runtime->AllocateEdgeActor());
            }
            runtime->SimulateSleep(TDuration::Seconds(1));
        }

        // Resolve posting table shards to verify VectorTopK pushdown and HNSW usage
        THashMap<TActorId, bool> postingShardActors;
        {
            auto shards = GetTableShards(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(),
                "/Root/TestTable/index1/indexImplPostingTable");
            for (auto shardId : shards) {
                auto actorId = ResolveTablet(*runtime, shardId);
                postingShardActors[actorId] = true;
            }
        }

        // Track that VectorTopK with LIMIT 7 is pushed down to posting table
        // LIMIT 7 is a magic marker that triggers Y_ABORT_UNLESS if HNSW is not used
        bool vectorTopKSentToPosting = false;
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            if (postingShardActors.contains(ev->GetRecipientRewrite())) {
                auto& read = ev->Get()->Record;
                if (read.HasVectorTopK()) {
                    vectorTopKSentToPosting = true;
                    // LIMIT 7 is the HNSW must-be-used marker
                    UNIT_ASSERT_VALUES_EQUAL_C(read.GetVectorTopK().GetLimit(), 7u,
                        "VectorTopK limit should be 7 (HNSW must-be-used marker)");
                }
            }
        });

        // Target embedding close to embeddings 5-9
        TString targetEmbedding = MakeFloatEmb(105.0f, 115.0f, 125.0f);

        // Execute query with LIMIT 7 (magic marker that triggers Y_ABORT_UNLESS if HNSW is not used)
        // This query uses the covered index, so it reads from posting table only (not main table)
        // The fact that this doesn't crash proves HNSW was used for the posting table
        {
            auto params = db.GetParamsBuilder()
                .AddParam("$TargetEmbedding").String(targetEmbedding).Build()
                .Build();

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(Q_(R"(
                    pragma ydb.KMeansTreeSearchTopSize = "2";
                    DECLARE $TargetEmbedding AS String;
                    SELECT pk, data FROM `/Root/TestTable`
                    VIEW index1 ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
                    LIMIT 7
                )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params)
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            // Verify results - should get 7 results, closest to target (105, 115, 125)
            auto parser = result.GetResultSetParser(0);
            std::vector<i64> pks;
            while (parser.TryNextRow()) {
                pks.push_back(parser.ColumnParser("pk").GetInt64());
            }
            // We inserted 10 rows, asked for 7
            UNIT_ASSERT_VALUES_EQUAL_C(pks.size(), 7u, "Expected 7 results");
            // First 3 should be 6, 7, 5 (or similar order based on cosine distance)
            // pk=6 has (103, 113, 123) - very close to target (105, 115, 125)
            UNIT_ASSERT_C(std::find(pks.begin(), pks.begin() + 3, 6) != pks.begin() + 3,
                "pk=6 should be in top 3 closest to target");
        }

        observer.Remove();

        // Verify that VectorTopK was actually pushed down to posting table
        UNIT_ASSERT_C(vectorTopKSentToPosting,
            "VectorTopK should have been pushed down to posting table");
    }

    Y_UNIT_TEST(VectorIndexLevelTableCache) {
        // Clear the cache before the test
        TVectorIndexLevelCache::Instance().Clear();

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            // SetUseRealThreads(false) is required to capture events
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        const int flags = F_NULLABLE;
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return DoCreateTableAndVectorIndex(db, flags); });

        // Resolve level table shards to track reads
        constexpr ui32 levelType = 1, postingType = 2, mainType = 3;
        THashMap<TActorId, ui32> actorTypes;
        auto resolveActors = [&](const char* tableName, ui32 type) {
            auto shards = GetTableShards(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(), tableName);
            for (auto shardId: shards) {
                auto actorId = ResolveTablet(*runtime, shardId);
                actorTypes[actorId] = type;
            }
        };
        resolveActors("/Root/TestTable/index1/indexImplLevelTable", levelType);
        resolveActors("/Root/TestTable/index1/indexImplPostingTable", postingType);
        resolveActors("/Root/TestTable", mainType);

        // Counter for level table reads
        std::atomic<int> levelTableReadCount{0};
        std::atomic<int> postingTableReadCount{0};

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                ui32 shardType = actorTypes[ev->GetRecipientRewrite()];
                if (shardType == levelType) {
                    levelTableReadCount++;
                } else if (shardType == postingType) {
                    postingTableReadCount++;
                }
            }
            return false;
        };
        runtime->SetEventFilter(captureEvents);

        const TString query1(Q_(R"(
            pragma ydb.KMeansTreeSearchTopSize = "2";
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            VIEW index1 ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // First query - should read from level table
        {
            levelTableReadCount = 0;
            postingTableReadCount = 0;

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            // First query should read from level table
            UNIT_ASSERT_C(levelTableReadCount.load() > 0,
                "First query should read from level table, but read count is " << levelTableReadCount.load());
            Cerr << "First query: level table reads = " << levelTableReadCount.load()
                 << ", posting table reads = " << postingTableReadCount.load() << Endl;
        }

        int firstLevelReads = levelTableReadCount.load();

        // Second query with the same target - should NOT read from level table (cached)
        {
            levelTableReadCount = 0;
            postingTableReadCount = 0;

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            Cerr << "Second query: level table reads = " << levelTableReadCount.load()
                 << ", posting table reads = " << postingTableReadCount.load() << Endl;

            // Second query should NOT read from level table (cached)
            UNIT_ASSERT_C(levelTableReadCount.load() == 0,
                "Second query should use cached level table data, but level table read count is "
                << levelTableReadCount.load() << " (first query had " << firstLevelReads << ")");
        }

        // Third query - should still use cache
        {
            levelTableReadCount = 0;
            postingTableReadCount = 0;

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            Cerr << "Third query: level table reads = " << levelTableReadCount.load()
                 << ", posting table reads = " << postingTableReadCount.load() << Endl;

            // Third query should NOT read from level table (still cached)
            UNIT_ASSERT_C(levelTableReadCount.load() == 0,
                "Third query should use cached level table data, but level table read count is "
                << levelTableReadCount.load());
        }
    }

    Y_UNIT_TEST(VectorIndexLevelTableCacheAcrossSnapshots) {
        // Test that level table cache works across different snapshots
        // Level tables are immutable after index creation, so cache should hit
        // regardless of snapshot changes

        // Clear the cache before the test
        TVectorIndexLevelCache::Instance().Clear();

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        const int flags = F_NULLABLE;
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return DoCreateTableAndVectorIndex(db, flags); });

        // Resolve level table shards to track reads
        constexpr ui32 levelType = 1;
        THashMap<TActorId, ui32> actorTypes;
        auto shards = GetTableShards(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(),
            "/Root/TestTable/index1/indexImplLevelTable");
        for (auto shardId: shards) {
            auto actorId = ResolveTablet(*runtime, shardId);
            actorTypes[actorId] = levelType;
        }

        std::atomic<int> levelTableReadCount{0};
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                if (actorTypes[ev->GetRecipientRewrite()] == levelType) {
                    levelTableReadCount++;
                }
            }
            return false;
        };
        runtime->SetEventFilter(captureEvents);

        const TString query(Q_(R"(
            pragma ydb.KMeansTreeSearchTopSize = "2";
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            VIEW index1 ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // First query - should read from level table (cache miss)
        {
            levelTableReadCount = 0;
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(levelTableReadCount.load() > 0,
                "First query should read from level table, got " << levelTableReadCount.load());
            Cerr << "Query 1 (first session tx): level reads = " << levelTableReadCount.load() << Endl;
        }

        // Second query in NEW TRANSACTION (different snapshot!) - should still hit cache
        // because level tables are immutable
        {
            levelTableReadCount = 0;
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            Cerr << "Query 2 (new tx, different snapshot): level reads = " << levelTableReadCount.load() << Endl;
            UNIT_ASSERT_C(levelTableReadCount.load() == 0,
                "Second query should use cache despite different snapshot, got " << levelTableReadCount.load());
        }

        // Third query in yet another transaction - should still use cache
        {
            levelTableReadCount = 0;
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            Cerr << "Query 3 (another new tx): level reads = " << levelTableReadCount.load() << Endl;
            UNIT_ASSERT_C(levelTableReadCount.load() == 0,
                "Third query should use cache, got " << levelTableReadCount.load());
        }

        // Create a NEW session (completely different connection) - should still hit cache
        auto session2 = kikimr.RunCall([&] {
            return db.CreateSession().GetValueSync().GetSession();
        });
        {
            levelTableReadCount = 0;
            auto result = kikimr.RunCall([&] {
                return session2.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            Cerr << "Query 4 (new session): level reads = " << levelTableReadCount.load() << Endl;
            UNIT_ASSERT_C(levelTableReadCount.load() == 0,
                "Query from new session should use cache, got " << levelTableReadCount.load());
        }
    }

}

}
}
