#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>
#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <yql/essentials/core/services/mounts/yql_mounts.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>

#include <library/cpp/json/json_reader.h>

#include <format>

namespace NKikimr {
namespace NKqp {

namespace {

constexpr int F_NULLABLE   = 1 << 0;
constexpr int F_COVERING   = 1 << 1;
constexpr int F_RETURNING  = 1 << 2;
constexpr int F_SIMILARITY = 1 << 3;
constexpr int F_SUFFIX_PK  = 1 << 4;
constexpr int F_WITH_INDEX = 1 << 5;
constexpr int F_OVERLAP    = 1 << 6;

}  // namespace

Y_UNIT_TEST_SUITE(KqpPrefixedVectorIndexes) {

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
            UNIT_ASSERT_C(result.first.IsSuccess(), "Failed to execute: `" << query << "` with " << result.first.GetIssues().ToString());

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
        int flags = 0,
        size_t count = 3)
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
        UNIT_ASSERT_EQUAL_C(mainResults.size(), count, toStr(mainResults));
        UNIT_ASSERT_C(std::unique(mainResults.begin(), mainResults.end()) == mainResults.end(), toStr(mainResults));

        auto indexResults = DoPositiveQueryVectorIndex<UseQueryService>(querySession, tableSession, indexQuery, flags);
        absl::c_sort(indexResults);
        UNIT_ASSERT_EQUAL_C(indexResults.size(), count, toStr(indexResults));
        UNIT_ASSERT_C(std::unique(indexResults.begin(), indexResults.end()) == indexResults.end(), toStr(indexResults));

        UNIT_ASSERT_VALUES_EQUAL(mainResults, indexResults);
    }

    template <bool UseQueryService>
    void DoPositiveQueriesPrefixedVectorIndexOrderBy(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        std::string_view function,
        std::string_view direction,
        std::string_view left,
        std::string_view right,
        int flags = 0,
        std::string_view init = "$target = \"\x67\x68\x02\";\n$user = \"user_b\";",
        size_t count = 3)
    {
        std::string metric = std::format("Knn::{}({}, {})", function, left, right);

        // no metric in result
        {
            // TODO(vitaliff): Exclude index-covered WHERE fields from KqpReadTableRanges.
            // Currently even if we SELECT only pk, emb, data WHERE user=xxx we also get `user`
            // in SELECT columns and thus it's required to add it to covered columns.
            const std::string plainQuery = std::format(R"({}
                SELECT * FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, direction);

            const std::string indexQuery = std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "3";
                {}
                SELECT * FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, direction);

            DoPositiveQueriesVectorIndex<UseQueryService>(querySession, tableSession, plainQuery, indexQuery, flags, count);
        }

        // metric in result
        {
            const std::string plainQuery = std::format(R"({}
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, metric, direction);

            const std::string indexQuery = std::format(R"({}
                pragma ydb.KMeansTreeSearchTopSize = "2";
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, metric, direction);

            DoPositiveQueriesVectorIndex<UseQueryService>(querySession, tableSession, plainQuery, indexQuery, flags, count);
        }

        // metric as result
        // TODO(mbkkt) fix this behavior too
        if constexpr (false) {
            const std::string plainQuery = std::format(R"({}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY m {}
                LIMIT 3;
            )", init, metric, direction);

            const std::string indexQuery = std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                {}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY m {}
                LIMIT 3;
            )", init, metric, direction);

            DoPositiveQueriesVectorIndex<UseQueryService>(querySession, tableSession, plainQuery, indexQuery, flags, count);
        }
    }

    template <bool UseQueryService>
    void DoPositiveQueriesPrefixedVectorIndexOrderBy(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        std::string_view function,
        std::string_view direction,
        int flags = 0,
        std::string_view init = "$target = \"\x67\x68\x02\";\n$user = \"user_b\";",
        size_t count = 3) {
        // target is left, member is right
        DoPositiveQueriesPrefixedVectorIndexOrderBy<UseQueryService>(querySession, tableSession, function, direction, "$target", "emb", flags, init, count);
        // target is right, member is left
        DoPositiveQueriesPrefixedVectorIndexOrderBy<UseQueryService>(querySession, tableSession, function, direction, "emb", "$target", flags, init, count);
    }

    template <bool UseQueryService>
    void DoPositiveQueriesPrefixedVectorIndexOrderByCosine(
        NYdb::NQuery::TSession& querySession,
        NYdb::NTable::TSession& tableSession,
        int flags = 0,
        std::string_view init = "$target = \"\x67\x68\x02\";\n$user = \"user_b\";",
        size_t count = 3)
    {
        // distance, default direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy<UseQueryService>(querySession, tableSession, "CosineDistance", "", flags, init, count);
        // distance, asc direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy<UseQueryService>(querySession, tableSession, "CosineDistance", "ASC", flags, init, count);
        // similarity, desc direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy<UseQueryService>(querySession, tableSession, "CosineSimilarity", "DESC", flags, init, count);
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
    void DoTruncateTable(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, const std::string& tablePath) {
        const std::string query = std::format(R"(
            TRUNCATE TABLE `{}`;
        )", tablePath);

        auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, query);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    void DoOnlyCreateTableForPrefixedVectorIndex(NYdb::NTable::TSession& tableSession, NYdb::NTable::TTableClient& db, int flags = 0) {
        auto tableBuilder = db.GetTableBuilder();
        if (flags & F_NULLABLE) {
            tableBuilder
                .AddNullableColumn("pk", NYdb::EPrimitiveType::Int64)
                .AddNullableColumn("user", NYdb::EPrimitiveType::String)
                .AddNullableColumn("emb", NYdb::EPrimitiveType::String)
                .AddNullableColumn("data", NYdb::EPrimitiveType::String);
        } else {
            tableBuilder
                .AddNonNullableColumn("pk", NYdb::EPrimitiveType::Int64)
                .AddNonNullableColumn("user", NYdb::EPrimitiveType::String)
                .AddNonNullableColumn("emb", NYdb::EPrimitiveType::String)
                .AddNonNullableColumn("data", NYdb::EPrimitiveType::String);
        }

        if (flags & F_SUFFIX_PK) {
            tableBuilder.SetPrimaryKeyColumns({"pk", "user"});
        } else {
            tableBuilder.SetPrimaryKeyColumns({"pk"});
        }

        tableBuilder.BeginPartitioningSettings()
            .SetMinPartitionsCount(3)
            .EndPartitioningSettings();

        if (flags & F_SUFFIX_PK) {
            auto partitions = NYdb::NTable::TExplicitPartitions{}
                .AppendSplitPoints(NYdb::TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(40).AddElement().OptionalString("").EndTuple().Build())
                .AppendSplitPoints(NYdb::TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(60).AddElement().OptionalString("").EndTuple().Build());
            tableBuilder.SetPartitionAtKeys(partitions);
        } else {
            auto partitions = NYdb::NTable::TExplicitPartitions{}
                .AppendSplitPoints(NYdb::TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(40).EndTuple().Build())
                .AppendSplitPoints(NYdb::TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(60).EndTuple().Build());
            tableBuilder.SetPartitionAtKeys(partitions);
        }

        if (flags & F_WITH_INDEX) {
            NYdb::NTable::TKMeansTreeSettings kmeans;
            kmeans.Settings.Metric = NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance;
            kmeans.Settings.VectorType = NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8;
            kmeans.Settings.VectorDimension = 2;
            kmeans.Clusters = 2;
            kmeans.Levels = 2;
            std::vector<std::string> dataColumns;
            if (flags & F_COVERING) {
                dataColumns = {"user", "emb", "data"};
            }
            tableBuilder.AddVectorKMeansTreeIndex("index", {"user", "emb"}, dataColumns, kmeans);
        }

        auto result = tableSession.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    template <bool UseQueryService>
    void InsertDataForPrefixedVectorIndex(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession) {
        const std::string query = R"(
            UPSERT INTO `/Root/TestTable` (pk, user, emb, data) VALUES)"
            "( 1, \"user_a\", \"\x03\x30\x02\", \"10\"),"
            "(11, \"user_a\", \"\x13\x31\x02\", \"11\"),"
            "(21, \"user_a\", \"\x23\x32\x02\", \"12\"),"
            "(31, \"user_a\", \"\x53\x33\x02\", \"13\"),"
            "(41, \"user_a\", \"\x43\x34\x02\", \"14\"),"
            "(51, \"user_a\", \"\x50\x60\x02\", \"15\"),"
            "(61, \"user_a\", \"\x61\x61\x02\", \"16\"),"
            "(71, \"user_a\", \"\x12\x62\x02\", \"17\"),"
            "(81, \"user_a\", \"\x75\x76\x02\", \"18\"),"
            "(91, \"user_a\", \"\x76\x76\x02\", \"19\"),"

            "( 2, \"user_b\", \"\x03\x30\x02\", \"20\"),"
            "(12, \"user_b\", \"\x13\x31\x02\", \"21\"),"
            "(22, \"user_b\", \"\x23\x32\x02\", \"22\"),"
            "(32, \"user_b\", \"\x53\x33\x02\", \"23\"),"
            "(42, \"user_b\", \"\x43\x34\x02\", \"24\"),"
            "(52, \"user_b\", \"\x50\x60\x02\", \"25\"),"
            "(62, \"user_b\", \"\x61\x61\x02\", \"26\"),"
            "(72, \"user_b\", \"\x12\x62\x02\", \"27\"),"
            "(82, \"user_b\", \"\x75\x76\x02\", \"28\"),"
            "(92, \"user_b\", \"\x76\x76\x02\", \"29\"),"

            "( 3, \"user_c\", \"\x03\x30\x02\", \"30\"),"
            "(13, \"user_c\", \"\x13\x31\x02\", \"31\"),"
            "(23, \"user_c\", \"\x23\x32\x02\", \"32\"),"
            "(33, \"user_c\", \"\x53\x33\x02\", \"33\"),"
            "(43, \"user_c\", \"\x43\x34\x02\", \"34\"),"
            "(53, \"user_c\", \"\x50\x60\x02\", \"35\"),"
            "(63, \"user_c\", \"\x61\x61\x02\", \"36\"),"
            "(73, \"user_c\", \"\x12\x62\x02\", \"37\"),"
            "(83, \"user_c\", \"\x75\x76\x02\", \"38\"),"
            "(93, \"user_c\", \"\x76\x76\x02\", \"39\");";

        auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
        UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
    }

    template <bool UseQueryService>
    void DoCreateTableForPrefixedVectorIndex(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, NYdb::NTable::TTableClient& tableClient, int flags = 0) {
        DoOnlyCreateTableForPrefixedVectorIndex(tableSession, tableClient, flags);
        InsertDataForPrefixedVectorIndex<UseQueryService>(querySession, tableSession);
    }

    template <bool UseQueryService>
    void DoCreatePrefixedVectorIndex(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, int levels, int flags = 0) {
        const char* cover = "";
        if (flags & F_COVERING) {
            cover = (flags & F_SUFFIX_PK) ? " COVER (emb, data)" : " COVER (user, emb, data)";
        }

        const std::string query = std::format(R"(
            ALTER TABLE `/Root/TestTable`
                ADD INDEX index
                GLOBAL USING vector_kmeans_tree
                ON (user, emb)
                {}
                WITH ({}=cosine, vector_type="uint8", vector_dimension=2, levels={}, clusters=2{});
        )", cover, (flags & F_SIMILARITY ? "similarity" : "distance"), levels, (flags & F_OVERLAP ? ", overlap_clusters=2" : ""));

        auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, query);
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    template <bool UseQueryService>
    void DoCheckOverlap(NYdb::NQuery::TSession& querySession, NYdb::NTable::TSession& tableSession, const std::string& indexName) {
        // Check number of rows in the posting table (should be 2x input rows)
        {
            const std::string query = std::format(R"(
                SELECT COUNT(*), COUNT(DISTINCT pk) FROM `/Root/TestTable/{}/indexImplPostingTable`;
            )", indexName);

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[60u;30u]]");
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

        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, indexLevels, flags);

        {
            auto result = tableSession.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);

            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");

            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);

            if (flags & F_COVERING) {
                std::vector<std::string> indexDataColumns;
                if (flags & F_SUFFIX_PK) {
                    indexDataColumns = {"emb", "data"};
                } else {
                    indexDataColumns = {"user", "emb", "data"};
                }
                UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), indexDataColumns);
            }

            const auto& settings = std::get<NYdb::NTable::TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, (flags & F_SIMILARITY)
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

        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags);

        {
            const std::string query = R"(
                ALTER TABLE `/Root/TestTable` DROP INDEX index
            )";

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, query);
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

    Y_UNIT_TEST_TWIN(OrderByCosineDistanceNotNullableLevel4, UseQueryService) {
        DoTestOrderByCosine<UseQueryService>(4, 0);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexOrderByCosineDistanceWithCover, Nullable, UseQueryService) {
        DoTestOrderByCosine<UseQueryService>(2, (Nullable ? F_NULLABLE : 0) | F_COVERING);
    }

    Y_UNIT_TEST_OCT(CosineDistanceWithPkSuffix, Nullable, Covered, UseQueryService) {
        const int flag = F_SUFFIX_PK | (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestOrderByCosine<UseQueryService>(2, flag);
    }

    Y_UNIT_TEST_QUAD(CosineDistanceWithPkSuffixWithOverlap, Covered, UseQueryService) {
        DoTestOrderByCosine<UseQueryService>(2, F_SUFFIX_PK | F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    template <bool UseQueryService>
    void DoTestPrefixedVectorIndexInsert(int flags) {
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

        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);

        const std::string originalPostingTable = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");

        // Insert to the table with index should succeed
        {
            std::string query = R"(
                INSERT INTO `/Root/TestTable` (pk, user, emb, data) VALUES
                (101, "user_a", "\x03\x29\x02", "101"),
                (102, "user_b", "\x03\x29\x02", "102"),
                (111, "user_a", "\x76\x75\x02", "111"),
                (112, "user_b", "\x76\x75\x02", "112")
            )";
            query += (flags & F_RETURNING ? " RETURNING data, emb, user, pk;" : ";");

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());

            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]),
                    "[[\"101\";\"\\3)\\2\";\"user_a\";101];"
                    "[\"102\";\"\\3)\\2\";\"user_b\";102];"
                    "[\"111\";\"vu\\2\";\"user_a\";111];"
                    "[\"112\";\"vu\\2\";\"user_b\";112]]");
            }
        }

        // Index is updated
        const std::string postingTable1_ins = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that PKs 1/101, 2/102, 91/111, 92/112 are now in same clusters
        {
            const std::string query = R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (1, 101)
                UNION ALL
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (2, 102)
                UNION ALL
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (91, 111)
                UNION ALL
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (92, 112)
                ;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]),
                (flags & F_OVERLAP ? "[[2u];[2u];[2u];[2u]]" : "[[1u];[1u];[1u];[1u]]"));
        }
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexInsert, Returning, Covered, UseQueryService) {
        const int flags = (Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0);
        DoTestPrefixedVectorIndexInsert<UseQueryService>(flags);
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexInsertWithOverlap, Returning, Covered, UseQueryService) {
        const int flags = F_OVERLAP | (Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0);
        DoTestPrefixedVectorIndexInsert<UseQueryService>(flags);
    }

    template <bool UseQueryService>
    void DoTestPrefixedVectorIndexInsertNewPrefix(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);

        const std::string originalPostingTable = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");

        // Insert to the table with index should succeed
        {
            const std::string query = R"(
                INSERT INTO `/Root/TestTable` (pk, user, emb, data) VALUES
                    (101, "user_a", "\x03\x29\x02", "101"),
                    (102, "user_xxx", "\x03\x29\x02", "102"),
                    (111, "user_yyy", "\x76\x75\x02", "111"),
                    (112, "user_yyy", "\x76\x75\x02", "112");
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
        }

        // Index is updated
        const std::string postingTable1_ins = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that we can now actually find new rows
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_a\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_b\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_xxx\";", 1);
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_yyy\";", 2);

        // Check that PKs 1/101, 111/112 are now in same clusters
        {
            const std::string query = R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (1, 101)
                UNION ALL
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (111, 112)
                ;
            )";
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]),
                (flags & F_OVERLAP ? "[[2u];[1u]]" : "[[1u];[1u]]"));
        }

        // Delete one of the new rows
        {
            const std::string query = R"(
                DELETE FROM `/Root/TestTable` WHERE pk=112;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
        }

        // Check that PK 112 is not present in the posting table
        {
            const std::string query = R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk = 112;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[0u]]");
        }
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexInsertNewPrefix, Nullable, Covered, UseQueryService) {
        const int flag = (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestPrefixedVectorIndexInsertNewPrefix<UseQueryService>(flag);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexInsertNewPrefixWithOverlap, Covered, UseQueryService) {
        // New prefixes are not affected by overlap - they are added directly into the Posting table
        DoTestPrefixedVectorIndexInsertNewPrefix<UseQueryService>(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    template <bool UseQueryService>
    void DoTestPrefixedVectorEmptyIndexedTableInsert(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        flags |= F_WITH_INDEX;
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoOnlyCreateTableForPrefixedVectorIndex(tableSession, tableClient, flags);

        const std::string originalPostingTable = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(originalPostingTable, "[]");

        // Insert to the table with index should succeed
        InsertDataForPrefixedVectorIndex<UseQueryService>(querySession, tableSession);

        // Index is updated
        const std::string postingTable1_ins = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that we can now actually find new rows
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_a\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_b\";");
    }

    Y_UNIT_TEST_OCT(PrefixedVectorEmptyIndexedTableInsert, Nullable, Covered, UseQueryService) {
        const int flags = (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestPrefixedVectorEmptyIndexedTableInsert<UseQueryService>(flags);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorEmptyIndexedTableInsertWithOverlap, Covered, UseQueryService) {
        // New prefixes are not affected by overlap - they are added directly into the Posting table
        DoTestPrefixedVectorEmptyIndexedTableInsert<UseQueryService>(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    // Same as PrefixedVectorEmptyIndexedTableInsert, but the index is created separately after creating the table
    template <bool UseQueryService>
    void DoTestEmptyPrefixedVectorIndexInsert(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::SEQUENCESHARD, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoOnlyCreateTableForPrefixedVectorIndex(tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);

        const std::string originalPostingTable = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(originalPostingTable, "[]");

        // Insert to the table with index should succeed
        InsertDataForPrefixedVectorIndex<UseQueryService>(querySession, tableSession);

        // Index is updated
        const std::string postingTable1_ins = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that we can now actually find new rows
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_a\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_b\";");
    }

    Y_UNIT_TEST_OCT(EmptyPrefixedVectorIndexInsert, Nullable, Covered, UseQueryService) {
        const int flag = (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestEmptyPrefixedVectorIndexInsert<UseQueryService>(flag);
    }

    Y_UNIT_TEST_QUAD(EmptyPrefixedVectorIndexInsertWithOverlap, Covered, UseQueryService) {
        // New prefixes are not affected by overlap - they are added directly into the Posting table
        DoTestEmptyPrefixedVectorIndexInsert<UseQueryService>(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    template <bool UseQueryService>
    void DoTestPrefixedVectorIndexDelete(const std::string& deleteQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);

        {
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, deleteQuery);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[\"19\";\"user_a\";\"vv\\2\";91]]");
            }
        }

        // Check that PK 91 is not present in the posting table
        {
            const std::string query = R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk=91;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[0u]]");
        }
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexDeletePk, Covered, Overlap, UseQueryService) {
        // DELETE WHERE from the table with index should succeed
        DoTestPrefixedVectorIndexDelete<UseQueryService>(R"(
            DELETE FROM `/Root/TestTable` WHERE pk=91;
        )", (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexDeleteFilter, Covered, UseQueryService) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestPrefixedVectorIndexDelete<UseQueryService>(R"(
            DELETE FROM `/Root/TestTable` WHERE data="19" AND user="user_a";
        )", (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexDeleteOn, Covered, UseQueryService) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestPrefixedVectorIndexDelete<UseQueryService>(R"(
            DELETE FROM `/Root/TestTable` ON SELECT 91 AS `pk`;
        )", (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexDeletePkReturning, Covered, Overlap, UseQueryService) {
        // DELETE WHERE from the table with index should succeed
        const std::string query = R"(
            DELETE FROM `/Root/TestTable` WHERE pk=91 RETURNING data, user, emb, pk;
        )";
        const int flag = F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        DoTestPrefixedVectorIndexDelete<UseQueryService>(query, flag);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexDeleteFilterReturning, Covered, UseQueryService) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestPrefixedVectorIndexDelete<UseQueryService>(R"(
            DELETE FROM `/Root/TestTable` WHERE data="19" AND user="user_a" RETURNING data, user, emb, pk;
        )", F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexDeleteOnReturning, Covered, UseQueryService) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestPrefixedVectorIndexDelete<UseQueryService>(R"(
            DELETE FROM `/Root/TestTable` ON SELECT 91 AS `pk` RETURNING data, user, emb, pk;
        )", F_RETURNING | (Covered ? F_COVERING : 0));
    }

    template <bool UseQueryService>
    void DoTestPrefixedVectorIndexUpdateNoChange(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);

        std::string orig = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");

        // Update to the table with index should succeed (but embedding does not change)
        {
            const std::string query = R"(
                UPDATE `/Root/TestTable` SET `data`="119" WHERE `pk`=91;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
        }

        const std::string updated = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        if (flags & F_COVERING) {
            SubstGlobal(orig, "\"19\"", "\"119\"");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexUpdateNoChange, Nullable, Covered, UseQueryService) {
        const int flags = (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestPrefixedVectorIndexUpdateNoChange<UseQueryService>(flags);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateNoChangeWithOverlap, Covered, UseQueryService) {
        DoTestPrefixedVectorIndexUpdateNoChange<UseQueryService>(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    template <bool UseQueryService>
    void DoTestPrefixedVectorIndexUpdateNoClusterChange(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);

        std::string orig = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");

        // Update to the table with index should succeed (embedding changes, but the cluster does not)
        {
            const std::string query = R"(
                UPDATE `/Root/TestTable` SET `emb`="\x76\x75\x02" WHERE `pk`=91;
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
        }

        const std::string updated = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        if (flags & F_COVERING) {
            SubstGlobal(orig, "\"\x76\x76\\2\"];[\"19", "\"\x76\x75\\2\"];[\"19");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexUpdateNoClusterChange, Nullable, Covered, UseQueryService) {
        const int flags = (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0);
        DoTestPrefixedVectorIndexUpdateNoClusterChange<UseQueryService>(flags);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateNoClusterChangeWithOverlap, Covered, UseQueryService) {
        DoTestPrefixedVectorIndexUpdateNoClusterChange<UseQueryService>(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    template <bool UseQueryService>
    void DoTestPrefixedVectorIndexUpdateClusterChange(const std::string& updateQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);
        DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);

        const std::string orig = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");

        // Update/upsert to the table with index should succeed (and the cluster should change)
        {
            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, updateQuery);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]), "[[[\"19\"];[\"\\0031\\2\"];[\"user_a\"];[91]]]");
            }
        }

        const std::string updated = ReadTablePartToYson(tableSession, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(orig, updated);

        // Check that PK 1 and 91 are now in the same cluster
        {
            const std::string query = R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (1, 91);
            )";

            auto result = ExecuteDML<UseQueryService>(querySession, tableSession, query);
            UNIT_ASSERT_C(result.first.IsSuccess(), result.first.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.second[0]),
                (flags & F_OVERLAP ? "[[2u]]" : "[[1u]]"));
        }
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexUpdatePkClusterChange, Covered, Overlap, UseQueryService) {
        const std::string query = R"(
            UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=91;
        )";
        const int flags = (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        DoTestPrefixedVectorIndexUpdateClusterChange<UseQueryService>(query, flags);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateFilterClusterChange, Covered, UseQueryService) {
        const std::string query = R"(
            UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="19" AND `user`="user_a";
        )";
        DoTestPrefixedVectorIndexUpdateClusterChange<UseQueryService>(query, Covered ? F_COVERING : 0);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpsertClusterChange, Covered, UseQueryService) {
        const std::string query = R"(
            UPSERT INTO `/Root/TestTable` (`pk`, `user`, `emb`, `data`) VALUES (91, "user_a", "\x03\x31\x02", "19");
        )";
        DoTestPrefixedVectorIndexUpdateClusterChange<UseQueryService>(query, (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexUpdatePkClusterChangeReturning, Covered, Overlap, UseQueryService) {
        const std::string query = R"(
            UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=91 RETURNING `data`, `emb`, `user`, `pk`;
        )";
        const int flags = F_NULLABLE | F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        DoTestPrefixedVectorIndexUpdateClusterChange<UseQueryService>(query, flags);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateFilterClusterChangeReturning, Covered, UseQueryService) {
        const std::string query = R"(
            UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="19" AND `user`="user_a" RETURNING `data`, `emb`, `user`, `pk`;
        )";
        DoTestPrefixedVectorIndexUpdateClusterChange<UseQueryService>(query, F_NULLABLE | F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpsertClusterChangeReturning, Covered, UseQueryService) {
        const std::string query = R"(
            UPSERT INTO `/Root/TestTable` (`pk`, `user`, `emb`, `data`) VALUES (91, "user_a", "\x03\x31\x02", "19") RETURNING `data`, `emb`, `user`, `pk`;
        )";
        DoTestPrefixedVectorIndexUpdateClusterChange<UseQueryService>(query, F_NULLABLE | F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(VectorSearchPushdown, Covered, UseQueryService) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            // SetUseRealThreads(false) is required to capture events (!) but then you have to do kikimr.RunCall() for everything
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        const int flags = (Covered ? F_COVERING : 0);
        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto tableSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto querySession = kikimr.RunCall([&] { return kikimr.GetQueryClient().GetSession().GetValueSync().GetSession(); });

        kikimr.RunCall([&] {
            DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, db, flags);
            DoCreatePrefixedVectorIndex<UseQueryService>(querySession, tableSession, 2, flags);
        });

        constexpr static int prefixType = 1, levelType = 2, postingType = 3, mainType = 4;
        THashMap<TActorId, int> actorTypes;
        auto resolveActors = [&](const char* tableName, int type) {
            auto shards = GetTableShards(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(), tableName);
            for (auto shardId: shards) {
                auto actorId = ResolveTablet(*runtime, shardId);
                actorTypes[actorId] = type;
            }
        };
        resolveActors("/Root/TestTable/index/indexImplPrefixTable", prefixType);
        resolveActors("/Root/TestTable/index/indexImplLevelTable", levelType);
        resolveActors("/Root/TestTable/index/indexImplPostingTable", postingType);
        resolveActors("/Root/TestTable", mainType);

        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                int shardType = actorTypes[ev->GetRecipientRewrite()];
                auto & read = ev->Get<TEvDataShard::TEvRead>()->Record;
                if (shardType == (Covered ? mainType : postingType)) {
                    // Non-covering index does topK on main table, covering does it on posting
                    UNIT_ASSERT(!read.HasVectorTopK());
                } else if (shardType != prefixType) {
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
                SELECT * FROM `/Root/TestTable`
                VIEW index
                WHERE user="user_a"
                ORDER BY Knn::CosineDistance(emb, "\x67\x71\x02")
                LIMIT 3
            )";

            auto result = kikimr.RunCall([&] {
                return ExecuteDML<UseQueryService>(querySession, tableSession, query);
            });
            UNIT_ASSERT(result.first.IsSuccess());
        }
    }

    template <bool UseQueryService>
    void DoPrefixedVectorIndexTruncateTable(bool covered, bool overlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableTruncateTable(true);
        auto serverSettings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto tableClient = kikimr.GetTableClient();
        auto querySession = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto tableSession = tableClient.CreateSession().GetValueSync().GetSession();

        const int flags = F_NULLABLE | (covered ? F_COVERING : 0) | (overlap ? F_OVERLAP : 0);
        DoCreateTableForPrefixedVectorIndex<UseQueryService>(querySession, tableSession, tableClient, flags);

        {
            const char* cover = "";
            if (flags & F_COVERING) {
                cover = " COVER (user, emb, data)";
            }

            const std::string createIndex = std::format(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (user, emb){}
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
                )", cover);

            auto result = ExecuteDDL<UseQueryService>(querySession, tableSession, createIndex);
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags);

        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable"));
        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplLevelTable"));
        UNIT_ASSERT(!IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplPrefixTable"));

        DoTruncateTable<UseQueryService>(querySession, tableSession, "/Root/TestTable");

        UNIT_ASSERT(IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable"));
        UNIT_ASSERT(IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplLevelTable"));
        UNIT_ASSERT(IsTableEmpty<UseQueryService>(querySession, tableSession, "/Root/TestTable/index/indexImplPrefixTable"));

        InsertDataForPrefixedVectorIndex<UseQueryService>(querySession, tableSession);
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine<UseQueryService>(querySession, tableSession, flags);
    }

    Y_UNIT_TEST_OCT(PrefixedVectorIndexTruncateTable, Covered, Overlap, UseQueryService) {
        DoPrefixedVectorIndexTruncateTable<UseQueryService>(Covered, Overlap);
    }
}

}  // namespace NKqp
}  // namespace NKikimr
