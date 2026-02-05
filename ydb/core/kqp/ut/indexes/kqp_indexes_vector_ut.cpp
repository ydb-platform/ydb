#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/adapters/issue/issue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/operation/operation.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <library/cpp/json/json_reader.h>

#include <format>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;

namespace {

constexpr int F_NULLABLE        = 1 << 0;
constexpr int F_COVERING        = 1 << 1;
constexpr int F_UNDERSCORE_DATA = 1 << 2;
constexpr int F_BIT_VECTOR      = 1 << 3;
constexpr int F_NON_PARTITIONED = 1 << 4;
constexpr int F_RETURNING       = 1 << 5;
constexpr int F_SIMILARITY      = 1 << 6;
constexpr int F_OVERLAP         = 1 << 7;

}

Y_UNIT_TEST_SUITE(KqpVectorIndexes) {

    template <bool UseQueryService>
    std::vector<i64> DoPositiveQueryVectorIndex(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        const std::string& query,
        int flags = 0)
    {
        {
            auto result = tableSession.ExplainDataQuery(query).ExtractValueSync();
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
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(),
                "Failed to execute: `" << query << "` with " << result.first.GetIssues().ToString());

            std::vector<i64> r;
            for (const auto& set : result.second) {
                NYdb::TResultSetParser parser{set};
                while (parser.TryNextRow()) {
                    auto value = parser.GetValue("pk");
                    UNIT_ASSERT_C(value.GetProto().has_int64_value(), value.GetProto().ShortUtf8DebugString());
                    r.push_back(value.GetProto().int64_value());
                }
            }
            return r;
        }
    }

    template <bool UseQueryService>
    void DoPositiveQueriesVectorIndex(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        const std::string& mainQuery,
        const std::string& indexQuery,
        int flags = 0)
    {
        auto toStr = [](const auto& rs) -> TString {
            TStringBuilder b;
            for (const auto& r : rs) {
                b << r << ", ";
            }
            return b;
        };
        auto mainResults = DoPositiveQueryVectorIndex<UseQueryService>(querySession, tableSession, mainQuery);
        absl::c_sort(mainResults);
        UNIT_ASSERT_EQUAL_C(mainResults.size(), 3, toStr(mainResults));
        UNIT_ASSERT_C(std::unique(mainResults.begin(), mainResults.end()) == mainResults.end(), toStr(mainResults));

        auto indexResults = DoPositiveQueryVectorIndex<UseQueryService>(querySession, tableSession, indexQuery, flags);
        absl::c_sort(indexResults);
        UNIT_ASSERT_EQUAL_C(indexResults.size(), 3, toStr(indexResults));
        UNIT_ASSERT_C(std::unique(indexResults.begin(), indexResults.end()) == indexResults.end(), toStr(indexResults));

        UNIT_ASSERT_VALUES_EQUAL(mainResults, indexResults);
    }

    template <bool UseQueryService>
    void DoPositiveQueriesVectorIndexOrderBy(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
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
            const std::string plainQuery = std::format(R"({}
                SELECT * FROM `/Root/TestTable`
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, direction);
            const std::string indexQuery = std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "3";
                {}
                SELECT * FROM `/Root/TestTable` VIEW index
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, direction);
            DoPositiveQueriesVectorIndex<UseQueryService>(querySession, tableSession, plainQuery, indexQuery, flags);
        }
        // metric in result
        {
            const std::string plainQuery = std::format(R"({}
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable`
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, metric, direction);
            const std::string indexQuery = std::format(R"({}
                pragma ydb.KMeansTreeSearchTopSize = "2";
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                ORDER BY {} {}
                LIMIT 3;
            )", target, metric, metric, direction);
            DoPositiveQueriesVectorIndex<UseQueryService>(querySession, tableSession, plainQuery, indexQuery, flags);
        }
        // metric as result
        {
            const std::string plainQuery = std::format(R"({}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable`
                ORDER BY m {}
                LIMIT 3;
            )", target, metric, direction);
            const std::string indexQuery = std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                {}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                ORDER BY m {}
                LIMIT 3;
            )", target, metric, direction);
            DoPositiveQueriesVectorIndex<UseQueryService>(querySession, tableSession, plainQuery, indexQuery, flags);
        }
    }

    template <bool UseQueryService>
    void DoPositiveQueriesVectorIndexOrderBy(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        std::string_view function,
        std::string_view direction,
        int flags = 0) {
        // target is left, member is right
        DoPositiveQueriesVectorIndexOrderBy<UseQueryService>(querySession, tableSession, function, direction, "$target", "emb", flags);
        // target is right, member is left
        DoPositiveQueriesVectorIndexOrderBy<UseQueryService>(querySession, tableSession, function, direction, "emb", "$target", flags);
    }

    template <bool UseQueryService>
    void DoPositiveQueriesVectorIndexOrderByCosine(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        int flags = 0) {
        // distance, default direction
        DoPositiveQueriesVectorIndexOrderBy<UseQueryService>(querySession, tableSession, "CosineDistance", "", flags);
        // distance, asc direction
        DoPositiveQueriesVectorIndexOrderBy<UseQueryService>(querySession, tableSession, "CosineDistance", "ASC", flags);
        // similarity, desc direction
        DoPositiveQueriesVectorIndexOrderBy<UseQueryService>(querySession, tableSession, "CosineSimilarity", "DESC", flags);
    }

    void DoOnlyCreateTableForVectorIndex(NYdb::NTable::TSession& tableSession, NYdb::NTable::TTableClient& db, int flags = 0) {
        const char* dataCol = flags & F_UNDERSCORE_DATA ? "___data" : "data";

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
                auto partitions = NYdb::NTable::TExplicitPartitions{}
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(4).EndTuple().Build())
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(6).EndTuple().Build());
                tableBuilder.SetPartitionAtKeys(partitions);
            }
            auto result = tableSession.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    template <bool UseQueryService>
    void DoTruncateTable(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession) {
        const std::string truncateTable = R"(
            TRUNCATE TABLE `/Root/TestTable`;
        )";

        auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, truncateTable);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    template <bool UseQueryService>
    bool IsTableEmpty(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, const std::string& tablePath) {
        const std::string query = std::format(R"(
            SELECT COUNT(*) FROM `{}`;
        )", tablePath);

        auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
        UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());

        auto resultSet = result.second[0];
        NYdb::TResultSetParser parser{resultSet};
        UNIT_ASSERT(parser.TryNextRow());
        auto count = parser.ColumnParser(0).GetUint64();
        return count == 0;
    }

    template <bool UseQueryService>
    void DoOnlyUpsertValuesIntoTable(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, int flags = 0) {
        const char* dataCol = flags & F_UNDERSCORE_DATA ? "___data" : "data";
        const std::string query = std::format(R"(
            UPSERT INTO `/Root/TestTable` (pk, emb, {}) VALUES
            (0, "\x03\x30\x02", "0"),
            (1, "\x13\x31\x02", "1"),
            (2, "\x23\x32\x02", "2"),
            (3, "\x53\x33\x02", "3"),
            (4, "\x43\x34\x02", "4"),
            (5, "\x50\x60\x02", "5"),
            (6, "\x61\x11\x02", "6"),
            (7, "\x12\x62\x02", "7"),
            (8, "\x75\x76\x02", "8"),
            (9, "\x76\x76\x02", "9");
        )", dataCol);

        auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
        UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
    }

    template <bool UseQueryService>
    void DoCreateTableForVectorIndex(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        NYdb::NTable::TTableClient& db,
        int flags = 0)
    {
        DoOnlyCreateTableForVectorIndex(tableSession, db, flags);
        DoOnlyUpsertValuesIntoTable<UseQueryService>(querySession, tableSession, flags);
    }

    template <bool UseQueryService>
    void DoCreateVectorIndex(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, int flags = 0) {
        // Add an index
        const char* cover = "";
        if (flags & F_COVERING) {
            cover = (flags & F_UNDERSCORE_DATA) ? " COVER (___data, emb)" : " COVER (data, emb)";
        }
        const std::string createIndex = std::format(R"(
            ALTER TABLE `/Root/TestTable`
                ADD INDEX index1
                GLOBAL USING vector_kmeans_tree
                ON (emb){}
                WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2{});
        )", cover, (flags & F_OVERLAP) ? ", overlap_clusters=2" : "");

        auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    template <bool UseQueryService>
    void DoCreateTableAndVectorIndex(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        NYdb::NTable::TTableClient& db,
        int flags = 0)
    {
        DoCreateTableForVectorIndex<UseQueryService>(querySession, tableSession, db, flags);
        DoCreateVectorIndex<UseQueryService>(querySession, tableSession, flags);
    }

    template <bool UseQueryService>
    void DoCreateTableForVectorIndexWithBitQuantization(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        NYdb::NTable::TTableClient& db,
        int flags = 0)
    {
        DoOnlyCreateTableForVectorIndex(tableSession, db, flags);
        {
            const std::string query = R"(
                UPSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (1, Untag(Knn::ToBinaryStringBit([1.f, 0.f, 0.f, 0.f, 0.f, 0.f]), "BitVector"), "1"),
                (2, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 0.f, 0.f, 0.f, 0.f]), "BitVector"), "2"),
                (3, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 0.f, 0.f, 0.f]), "BitVector"), "3"),
                (4, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 0.f, 0.f]), "BitVector"), "4"),
                (5, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 0.f]), "BitVector"), "5"),
                (6, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 1.f]), "BitVector"), "6");
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
        }
    }

    template <bool UseQueryService>
    void DoCheckOverlap(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, const std::string& indexName) {
        // Check number of rows in the posting table (should be 2x input rows)
        {
            const std::string query = std::format(R"(
                SELECT COUNT(*), COUNT(DISTINCT pk) FROM `/Root/TestTable/{}/indexImplPostingTable`;
            )", indexName);
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[20u;10u]]");
        }

        // Check that a select query without main table PK columns works
        // (it didn't because UniqueColumns filtering pushdown requires PK columns in the result)
        {
            const std::string query = std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $target = "\x67\x71\x02";
                SELECT data FROM `/Root/TestTable` VIEW {}
                ORDER BY Knn::CosineDistance(emb, $target)
                LIMIT 3;
            )", indexName);

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(),
                "Failed to execute: `" << query << "` with " << result.first.GetIssues().ToString());
        }
    }

    // Test that vector index queries work when selecting only non-PK columns
    Y_UNIT_TEST_QUAD(VectorIndexSelectWithoutPkColumns, Overlap, UseQueryService) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = (Overlap ? F_OVERLAP : 0);
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        // Query selecting only 'data' column (not PK columns)
        {
            const std::string query = R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $target = "\x67\x71\x02";
                SELECT data FROM `/Root/TestTable` VIEW index1
                ORDER BY Knn::CosineDistance(emb, $target)
                LIMIT 3;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(),
                "Failed to execute: `" << query << "` with " << result.first.GetIssues().ToString());
        }
    }

    template <bool UseQueryService>
    void DoTestOrderByCosine(ui32 indexLevels, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        {
            const char* cover = (flags & F_COVERING ? " COVER (data, emb)" : "");
            const char* metric = (flags & F_SIMILARITY ? "similarity" : "distance");
            const char* overlap = (flags & F_OVERLAP ? ", overlap_clusters=2" : "");
            const std::string createIndex = std::format(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb){}
                    WITH ({}=cosine, vector_type="uint8", vector_dimension=2, levels={}, clusters=2{});
            )", cover, metric, indexLevels, overlap);

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = tableSession.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), std::vector<std::string>{"emb"});
            if (flags & F_COVERING) {
                std::vector<std::string> indexDataColumns{"data", "emb"};
                UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), indexDataColumns);
            }
            const auto& settings = std::get<NYdb::NTable::TKMeansTreeSettings>(indexes[0].GetIndexSettings());
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
            DoCheckOverlap<UseQueryService>(querySession, tableSession, "index");
        }

        DoPositiveQueriesVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags);

        {
            const std::string dropIndex = R"(
                ALTER TABLE `/Root/TestTable` DROP INDEX index
            )";
            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, dropIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_OCT(OrderByCosineLevel1, Nullable, UseSimilarity, UseQueryService) {
        const int flags = (Nullable ? F_NULLABLE : 0) | (UseSimilarity ? F_SIMILARITY : 0);
        DoTestOrderByCosine<UseQueryService>(1, flags);
    }

    Y_UNIT_TEST_OCT(OrderByCosineLevel1WithOverlap, Nullable, Covered, UseQueryService) {
        const int flags = F_OVERLAP | (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestOrderByCosine<UseQueryService>(1, flags);
    }

    Y_UNIT_TEST_OCT(OrderByCosineLevel2, Nullable, UseSimilarity, UseQueryService) {
        const int flags = (Nullable ? F_NULLABLE : 0) | (UseSimilarity ? F_SIMILARITY : 0);
        DoTestOrderByCosine<UseQueryService>(2, flags);
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel2WithCover, Nullable, UseQueryService) {
        DoTestOrderByCosine<UseQueryService>(2, (Nullable ? F_NULLABLE : 0) | F_COVERING);
    }

    Y_UNIT_TEST_OCT(OrderByCosineLevel2WithOverlap, Nullable, Covered, UseQueryService) {
        const int flags = F_OVERLAP | (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestOrderByCosine<UseQueryService>(2, flags);
    }

    Y_UNIT_TEST_TWIN(OrderByCosineDistanceNotNullableLevel3, UseQueryService) {
        DoTestOrderByCosine<UseQueryService>(3, 0);
    }

    Y_UNIT_TEST_TWIN(OrderByCosineDistanceNotNullableLevel3WithOverlap, UseQueryService) {
        DoTestOrderByCosine<UseQueryService>(3, F_OVERLAP);
    }

    Y_UNIT_TEST(BadFormat) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();

        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            const TString createTableSql(R"(
                --!syntax_v1
                CREATE TABLE TestVector2 (id Uint64, embedding String, embedding_bit String, PRIMARY KEY (id));
            )");
            auto result = session.ExecuteSchemeQuery(createTableSql).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestVector2` ADD INDEX idx_vector_3_200 GLOBAL USING vector_kmeans_tree
                ON (embedding) WITH (distance=cosine, vector_type="uint8", vector_dimension=200, levels=3, clusters=2);
            )"));
            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const TString query1(Q_(R"(UPSERT INTO TestVector2 (id, embedding) VALUES (1, "00"), (2, "01"), (3, "10"), (4, "11"),
                (5, "00"), (6, "01"), (7, "10"), (8, "11");)"));
            auto result = session.ExecuteDataQuery(Q_(query1), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel1WithBitQuantization, Nullable, Overlap) {
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
                    WITH (similarity=cosine, vector_type="bit", vector_dimension=6, levels=1, clusters=2{});
            )", (Overlap ? ", overlap_clusters=2" : ""));

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = tableSession.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), std::vector<std::string>{"emb"});
            const auto& settings = std::get<NYdb::NTable::TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Bit);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 6);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags);

        if (flags & F_OVERLAP) {
            DoCheckOverlap<UseQueryService>(querySession, tableSession, "index");
        }
    }

    Y_UNIT_TEST_TWIN(OrderByNoUnwrap, UseQueryService) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE;
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        {
            const std::string query = R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $TargetEmbedding = String::HexDecode("677102");
                SELECT * FROM `/Root/TestTable` VIEW index1
                ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
                LIMIT 3;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(),
                "Failed to execute: `" << query << "` with " << result.first.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                $TargetEmbedding = (SELECT emb FROM `/Root/TestTable` WHERE pk=9);
                SELECT * FROM `/Root/TestTable` VIEW index1
                ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
                LIMIT 3;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(),
                "Failed to execute: `" << query << "` with " << result.first.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(BuildIndexTimesAndUser, UseQueryService) {
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
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = db.CreateSession().GetValueSync().GetSession();

        DoCreateTableForVectorIndex<UseQueryService>(querySession, tableSession, db);
        {
            const std::string createIndex = R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )";

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
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

    Y_UNIT_TEST_TWIN(VectorIndexNoBulkUpsert, UseQueryService) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE;
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        const std::string originalPostingTable = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");

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
            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).GetValueSync();
            auto issues = result.GetIssues().ToString();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SCHEME_ERROR, result.GetStatus());
            UNIT_ASSERT_C(issues.contains("Only async-indexed tables are supported by BulkUpsert"), issues);
        }

        const std::string postingTable1_bulk = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(originalPostingTable, postingTable1_bulk);
    }

    template <bool UseQueryService>
    void DoTestVectorIndexDelete(const std::string& deleteQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        {
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, deleteQuery);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[[\"9\"];[\"vv\\2\"];[9]]]");
            }
        }

        // Check that PK 9 is not present in the posting table
        {
            const std::string query = R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk=9;
            )";
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[0u]]");
        }
    }

    Y_UNIT_TEST_OCT(VectorIndexDeletePk, Covered, Overlap, UseQueryService) {
        // DELETE WHERE from the table with index should succeed
        const std::string deleteQuery = R"(DELETE FROM `/Root/TestTable` WHERE pk=9;)";
        const int flags = (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        DoTestVectorIndexDelete<UseQueryService>(deleteQuery, flags);
    }

    Y_UNIT_TEST_QUAD(VectorIndexDeleteFilter, Covered, UseQueryService) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        const std::string deleteQuery = R"(DELETE FROM `/Root/TestTable` WHERE data="9";)";
        DoTestVectorIndexDelete<UseQueryService>(deleteQuery, (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexDeleteOn, Covered, UseQueryService) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        const std::string deleteQuery = R"(DELETE FROM `/Root/TestTable` ON SELECT 9 AS `pk`;)";
        DoTestVectorIndexDelete<UseQueryService>(deleteQuery, (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_OCT(VectorIndexDeletePkReturning, Covered, Overlap, UseQueryService) {
        // DELETE WHERE from the table with index should succeed
        const std::string deleteQuery = R"(DELETE FROM `/Root/TestTable` WHERE pk=9 RETURNING data, emb, pk;)";
        const int flags = F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        DoTestVectorIndexDelete<UseQueryService>(deleteQuery, flags);
    }

    Y_UNIT_TEST_QUAD(VectorIndexDeleteFilterReturning, Covered, UseQueryService) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        const std::string deleteQuery = R"(DELETE FROM `/Root/TestTable` WHERE data="9" RETURNING data, emb, pk;)";
        DoTestVectorIndexDelete<UseQueryService>(deleteQuery, F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexDeleteOnReturning, Covered, UseQueryService) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        const std::string deleteQuery = R"(DELETE FROM `/Root/TestTable` ON SELECT 9 AS `pk` RETURNING data, emb, pk;)";
        DoTestVectorIndexDelete<UseQueryService>(deleteQuery, F_RETURNING | (Covered ? F_COVERING : 0));
    }

    template <bool UseQueryService>
    void DoTestVectorIndexInsert(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        const std::string originalPostingTable = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");

        // Insert to the table with index should succeed
        {
            std::string query = R"(
                INSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (10, "\x11\x62\x02", "10"),
                (11, "\x75\x77\x02", "11")
            )";
            query += (flags & F_RETURNING ? " RETURNING data, emb, pk;" : ";");

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[[\"10\"];[\"\\021b\\2\"];[10]];[[\"11\"];[\"uw\\2\"];[11]]]");
            }
        }

        // Index is updated
        const std::string postingTable1_ins = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that PK 7 and 10 are now in the same cluster
        // Check that PK 8 and 11 are now in the same cluster
        {
            const std::string query = R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk IN (7, 10)
                UNION ALL
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk IN (8, 11)
                ;
            )";
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]),
                (flags & F_OVERLAP ? "[[2u];[2u]]" : "[[1u];[1u]]"));
        }
    }

    Y_UNIT_TEST_OCT(VectorIndexInsert, Returning, Covered, UseQueryService) {
        const int flags = (Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0);
        DoTestVectorIndexInsert<UseQueryService>(flags);
    }

    Y_UNIT_TEST_OCT(VectorIndexInsertWithOverlap, Returning, Covered, UseQueryService) {
        const int flags = F_OVERLAP | (Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0);
        DoTestVectorIndexInsert<UseQueryService>(flags);
    }

    template <bool UseQueryService>
    void DoTestVectorIndexUpdateNoChange(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        std::string orig = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");

        // Update to the table with index should succeed (but embedding does not change)
        {
            const std::string query = (flags & F_UNDERSCORE_DATA
                ? "UPDATE `/Root/TestTable` SET `___data`=\"20\" WHERE `pk`=9;"
                : "UPDATE `/Root/TestTable` SET `data`=\"20\" WHERE `pk`=9;");

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
        }

        const std::string updated = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");
        if (flags & F_COVERING) {
            SubstGlobal(orig, "\"9\"", "\"20\"");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpdateNoChange, Overlap, UseQueryService) {
        DoTestVectorIndexUpdateNoChange<UseQueryService>((Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpdateNoChangeCovered, Overlap, UseQueryService) {
        DoTestVectorIndexUpdateNoChange<UseQueryService>(F_COVERING | (Overlap ? F_OVERLAP : 0));
    }

    // Similar to VectorIndexUpdateNoChange, but data column is named ___data to make it appear before __ydb_parent in struct types
    Y_UNIT_TEST_QUAD(VectorIndexUpdateColumnOrder, Overlap, UseQueryService) {
        DoTestVectorIndexUpdateNoChange<UseQueryService>(F_COVERING | F_UNDERSCORE_DATA | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_OCT(VectorIndexUpdateNoClusterChange, Covered, Overlap, UseQueryService) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        std::string orig = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");

        // Update to the table with index should succeed (embedding changes, but the cluster does not)
        {
            const std::string query = R"(
                UPDATE `/Root/TestTable` SET `emb`="\x76\x75\x02" WHERE `pk`=9;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
        }

        const std::string updated = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");
        if (Covered) {
            SubstGlobal(orig, "\"\x76\x76\\2\"", "\"\x76\x75\\2\"");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    template <bool UseQueryService>
    void DoTestVectorIndexUpdateClusterChange(const std::string& updateQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        flags |= F_NULLABLE;
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        const std::string orig = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");

        // Update/upsert to the table with index should succeed (and the cluster should change)
        {
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, updateQuery);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[[\"9\"];[\"\\0031\\2\"];[9]]]");
            }
        }

        const std::string updated = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(orig, updated);

        // Check that PK 9 and 0 are now in the same cluster
        {
            const std::string query = R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index1/indexImplPostingTable`
                WHERE pk IN (0, 9);
            )";
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]),
                (flags & F_OVERLAP ? "[[2u]]" : "[[1u]]"));
        }
    }

    Y_UNIT_TEST_OCT(VectorIndexUpdatePkClusterChange, Covered, Overlap, UseQueryService) {
        const std::string updateQuery = R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=9;)";
        const int flags = (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        DoTestVectorIndexUpdateClusterChange<UseQueryService>(updateQuery, flags);
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpdateFilterClusterChange, Covered, UseQueryService) {
        const std::string updateQuery = R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="9";)";
        DoTestVectorIndexUpdateClusterChange<UseQueryService>(updateQuery, (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpsertClusterChange, Covered, UseQueryService) {
        const std::string updateQuery = R"(UPSERT INTO `/Root/TestTable` (`pk`, `emb`, `data`) VALUES (9, "\x03\x31\x02", "9");)";
        DoTestVectorIndexUpdateClusterChange<UseQueryService>(updateQuery, (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_OCT(VectorIndexUpdatePkClusterChangeReturning, Covered, Overlap, UseQueryService) {
        const std::string updateQuery = R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=9 RETURNING `data`, `emb`, `pk`;)";
        const int flags = F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        DoTestVectorIndexUpdateClusterChange<UseQueryService>(updateQuery, flags);
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpdateFilterClusterChangeReturning, Covered, UseQueryService) {
        const std::string updateQuery = R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="9" RETURNING `data`, `emb`, `pk`;)";
        DoTestVectorIndexUpdateClusterChange<UseQueryService>(updateQuery, F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorIndexUpsertClusterChangeReturning, Covered, UseQueryService) {
        const std::string updateQuery = R"(UPSERT INTO `/Root/TestTable` (`pk`, `emb`, `data`) VALUES (9, "\x03\x31\x02", "9") RETURNING `data`, `emb`, `pk`;)";
        DoTestVectorIndexUpdateClusterChange<UseQueryService>(updateQuery, F_RETURNING | (Covered ? F_COVERING : 0));
    }

    // First index level build is processed differently when table has 1 and >1 partitions so we check both cases
    Y_UNIT_TEST_OCT(EmptyVectorIndexUpdate, Partitioned, Overlap, UseQueryService) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = (Partitioned ? 0 : F_NON_PARTITIONED) | (Overlap ? F_OVERLAP : 0);
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoOnlyCreateTableForVectorIndex(tableSession, tableClient, flags);
        DoCreateVectorIndex<UseQueryService>(querySession, tableSession, flags);

        // Check that the index has a stub cluster hierarchy but the posting table is empty
        const std::string level = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplLevelTable");
        UNIT_ASSERT_VALUES_EQUAL(level, "[[[0u];[1u];[\"  \\2\"]];[[1u];[9223372036854775810u];[\"  \\2\"]]]");
        const std::string posting = ReadTablePartToYson(tableSession, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_VALUES_EQUAL(posting, "[]");

        // Insert to the table with index should succeed
        {
            const std::string query = R"(
                INSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (10, "\x11\x62\x02", "10");
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
        }

        // Posting table should be updated
        {
            const std::string query = R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index1/indexImplPostingTable`;
            )";
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[1u]]");
        }

        // The added vector should be found successfully
        {
            const std::string query = R"(
                SELECT pk FROM `/Root/TestTable`
                VIEW index1
                ORDER BY Knn::CosineDistance(emb, "AA\x02")
            )";
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[10]]");
        }
    }

    Y_UNIT_TEST_QUAD(CoveredVectorIndexWithFollowers, StaleRO, UseQueryService) {
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
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        {
            const std::string createIndex = R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb) COVER (emb, data)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )";

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto setFollowers = [&](const TString& tableName) {
            const std::string query = std::format(R"(
                ALTER TABLE `{}` SET (READ_REPLICAS_SETTINGS = "PER_AZ:3");
            )", tableName.c_str());

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto checkFollowerDescription = [&tableSession](const TString& tableName) {
            auto result = tableSession.DescribeTable(tableName).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            const auto& table = result.GetTableDescription();
            UNIT_ASSERT(table.GetReadReplicasSettings()->GetMode() == NYdb::NTable::TReadReplicasSettings::EMode::PerAz);
            UNIT_ASSERT_VALUES_EQUAL(table.GetReadReplicasSettings()->GetReadReplicasCount(), 3);
        };

        for (const TString& tableName: tableNames) {
            setFollowers(tableName);
            checkFollowerDescription(tableName);
        }

        DoPositiveQueriesVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags);

        if (StaleRO) {
            // from leader - should NOT read
            CheckTableReads(tableSession, postingTableName, false, false);
            // from followers - should read
            CheckTableReads(tableSession, postingTableName, true, true);
        } else {
            // from leader - should read
            CheckTableReads(tableSession, postingTableName, false, true);
            // from followers - should NOT read
            CheckTableReads(tableSession, postingTableName, true, false);
        }

        if (StaleRO) {
            CheckTableReads(tableSession, levelTableName, false, false);
            CheckTableReads(tableSession, levelTableName, true, true);
        } else {
            CheckTableReads(tableSession, levelTableName, false, true);
            CheckTableReads(tableSession, levelTableName, true, false);
        }

        // Etalon reads from main table
        CheckTableReads(tableSession, mainTableName, false, true);
        CheckTableReads(tableSession, mainTableName, true, false);
    }

    Y_UNIT_TEST_TWIN(OrderByReject, UseQueryService) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForVectorIndex<UseQueryService>(querySession, tableSession, tableClient);

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
            const std::string createIndex = std::format(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH ({}, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )", check[0]);
            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            const std::string selectQuery = std::format(R"(
                SELECT * FROM `/Root/TestTable`
                VIEW index ORDER BY {}
            )", check[1]);
            auto dmlResult = ExecuteDML<UseQueryService>(querySession, tableSession, selectQuery);
            UNIT_ASSERT_C(HasIssue(NYdb::NAdapters::ToYqlIssues(dmlResult.first.GetIssues()), NYql::TIssuesIds::KIKIMR_WRONG_INDEX_USAGE,
                [&](const NYql::TIssue& issue) {
                    return issue.GetMessage().Contains("sorting must contain distance: " + std::string(check[2]));
                }), dmlResult.first.GetIssues().ToString());
            UNIT_ASSERT(!dmlResult.first.IsSuccess());

            tableSession = tableClient.CreateSession().GetValueSync().GetSession();
            querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
            const std::string dropIndex = R"(
                ALTER TABLE `/Root/TestTable` DROP INDEX index
            )";
            result = ExecuteDDL<UseQueryService>(querySession, tableSession, dropIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(VectorResolveDuplicateEvent, UseQueryService) {
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
        auto querySession = kikimr.RunCall([&] { return kikimr.GetQueryClient().GetSession().GetValueSync().GetSession(); });
        auto tableSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        kikimr.RunCall([&] {
            DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, db, flags);
        });

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
            const std::string query = R"(
                INSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (10, "\x11\x62\x02", "10"),
                (11, "\x77\x75\x02", "11")
            )";

            auto result = kikimr.RunCall([&] {
                return ExecuteDML<UseQueryService>(querySession, tableSession, query);
            });
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
        }
    }

    TVector<TEvStateStorage::TEvInfo::TFollowerInfo> ResolveFollowers(TTestActorRuntime & runtime, ui64 tabletId, ui32 nodeIndex) {
        auto sender = runtime.AllocateEdgeActor(nodeIndex);
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), sender,
            new TEvStateStorage::TEvLookup(tabletId, 0)),
            nodeIndex, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvStateStorage::TEvInfo>(sender);
        Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK, "Failed to resolve tablet %" PRIu64, tabletId);
        return std::move(ev->Get()->Followers);
    }

    Y_UNIT_TEST_OCT(VectorSearchPushdown, Covered, Followers, UseQueryService) {
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
        auto querySession = kikimr.RunCall([&] { return kikimr.GetQueryClient().GetSession().GetValueSync().GetSession(); });
        auto tableSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        kikimr.RunCall([&] {
            DoCreateTableAndVectorIndex<UseQueryService>(querySession, tableSession, db, flags);
        });

        if (Followers) {
            std::vector<std::string> tableNames = {
                "/Root/TestTable",
                "/Root/TestTable/index1/indexImplLevelTable",
                "/Root/TestTable/index1/indexImplPostingTable"
            };
            for (const std::string& tableName: tableNames) {
                const std::string alterTable = std::format(R"(
                    ALTER TABLE `{}` SET (READ_REPLICAS_SETTINGS = "PER_AZ:3");
                )", std::string(tableName.c_str()));
                auto result = kikimr.RunCall([&] { return ExecuteDDL<UseQueryService>(querySession, tableSession, alterTable); });
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
                    for (const auto& followerInfo: followers) {
                        actorTypes[followerInfo.FollowerTablet] = type | followerTypeFlag;
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
                if (shardType == (Covered ? mainType : postingType)) {
                    // Non-covering index does topK on main table, covering does it on posting
                    UNIT_ASSERT(!read.HasVectorTopK());
                } else {
                    UNIT_ASSERT(shardType != 0);
                    UNIT_ASSERT(read.HasVectorTopK());
                    auto & topK = read.GetVectorTopK();
                    // Check that target and limit are pushed down
                    UNIT_ASSERT(topK.GetTargetVector() == "\x67\x71\x02");
                    if (shardType == levelType) {
                        // Equal to pragma
                        UNIT_ASSERT(topK.GetLimit() == 2);
                    } else if (shardType == (Covered ? postingType : mainType)) {
                        // Equal to LIMIT
                        UNIT_ASSERT(topK.GetLimit() == 3);
                    }
                }
            }
            return false;
        };
        runtime->SetEventFilter(captureEvents);

        {
            const std::string query = R"(
                pragma ydb.KMeansTreeSearchTopSize = "2";
                $TargetEmbedding = String::HexDecode("677102");
                SELECT * FROM `/Root/TestTable`
                VIEW index1 ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
                LIMIT 3
            )";

            auto result = kikimr.RunCall([&] {
                return ExecuteDML<UseQueryService>(querySession, tableSession, query);
            });
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToOneLineString());
        }
    }

    Y_UNIT_TEST_OCT(VectorIndexTruncateTable, Covered, Overlap, UseQueryService) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableTruncateTable(true);
        auto serverSettings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        {
            const char* cover = (flags & F_COVERING ? " COVER (data, emb)" : "");
            const char* overlap = (flags & F_OVERLAP ? ", overlap_clusters=2" : "");
            const std::string createIndex = std::format(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb){}
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2{});
            )", cover, overlap);

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        DoPositiveQueriesVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession);

        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable"));
        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplLevelTable"));
        DoTruncateTable<UseQueryService>(querySession, tableSession);
        UNIT_ASSERT(IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable"));
        UNIT_ASSERT(IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplLevelTable"));

        DoOnlyUpsertValuesIntoTable<UseQueryService>(querySession, tableSession);
        DoPositiveQueriesVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession);
    }
}

}
}
