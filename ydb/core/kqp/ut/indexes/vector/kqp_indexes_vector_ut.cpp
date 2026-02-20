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

    void DoTruncateTable(TSession& session) {
        const TString truncateTable(Q_(R"(
            TRUNCATE TABLE `/Root/TestTable`;
        )"));

        auto result = session.ExecuteSchemeQuery(truncateTable).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    bool IsTableEmpty(TSession& session, const TString& tablePath) {
        TString query = Sprintf(R"(
            SELECT COUNT(*) FROM `%s`;
        )"
        , tablePath.c_str());

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto resultSet = result.GetResultSet(0);
        TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());
        auto count = parser.ColumnParser(0).GetUint64();
        return count == 0;
    }

    void DoOnlyUpsertValuesIntoTable(TSession& session, int flags = 0) {
        const char* dataCol = flags & F_UNDERSCORE_DATA ? "___data" : "data";
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

    TSession DoCreateTableForVectorIndex(TTableClient& db, int flags = 0) {
        auto session = DoOnlyCreateTableForVectorIndex(db, flags);
        DoOnlyUpsertValuesIntoTable(session, flags);
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

    Y_UNIT_TEST_QUAD(OrderByCosineOnlyVectorCovered, Nullable, Overlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndex(db, (Nullable ? F_NULLABLE : 0));
        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb) COVER (emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2%s);
                )", (Overlap ? ", overlap_clusters=2" : ""))));
            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), std::vector<std::string>{"emb"});
            UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), std::vector<std::string>{"emb"});
        }

        // Check without F_COVERING here because data column is not covered and query still accesses the main table
        DoPositiveQueriesVectorIndexOrderByCosine(session, 0);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel3) {
        DoTestOrderByCosine(3, 0);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel3WithOverlap) {
        DoTestOrderByCosine(3, F_OVERLAP);
    }

    Y_UNIT_TEST_TWIN(BadFormat, OnBuild) {
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
                CREATE TABLE TestVector2 (id Uint64, embedding String, embedding_bit String, PRIMARY KEY (id))
                WITH (PARTITION_AT_KEYS = (9));
            )");
            auto result = session.ExecuteSchemeQuery(createTableSql).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto create = [&]() {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestVector2` ADD INDEX idx_vector_3_200 GLOBAL USING vector_kmeans_tree
                ON (embedding) WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=3, clusters=2);
            )"));
            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto insert = [&]() {
            const TString query1(Q_(R"(UPSERT INTO TestVector2 (id, embedding) VALUES
                (1, ""), (2, "1"), (3, "40"), (4, "abcd"),
                (5, "00\x02"), (6, "01\x02"), (7, "10\x02"), (8, "11\x02"),
                (9, ""), (10, "1"), (11, "40"), (12, "abcd"),
                (13, "00\x02"), (14, "01\x02"), (15, "10"), (16, "11\x02");)"));
            auto result = session.ExecuteDataQuery(Q_(query1), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        if (OnBuild) {
            insert();
            create();
        } else {
            create();
            insert();
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

    TVector<TEvStateStorage::TEvInfo::TFollowerInfo> ResolveFollowers(TTestActorRuntime & runtime, ui64 tabletId, ui32 nodeIndex) {
        auto sender = runtime.AllocateEdgeActor(nodeIndex);
        runtime.Send(new IEventHandle(MakeStateStorageProxyID(), sender,
            new TEvStateStorage::TEvLookup(tabletId, 0)),
            nodeIndex, true);
        auto ev = runtime.GrabEdgeEventRethrow<TEvStateStorage::TEvInfo>(sender);
        Y_ABORT_UNLESS(ev->Get()->Status == NKikimrProto::OK, "Failed to resolve tablet %" PRIu64, tabletId);
        return std::move(ev->Get()->Followers);
    }

    Y_UNIT_TEST_QUAD(VectorSearchPushdown, Covered, Followers) {
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

    Y_UNIT_TEST_QUAD(VectorIndexTruncateTable, Covered, Overlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableTruncateTable(true);
        auto serverSettings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndex(db, flags);

        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)%s
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2%s);
                )",
                (flags & F_COVERING ? " COVER (data, emb)" : ""),
                (flags & F_OVERLAP ? ", overlap_clusters=2" : ""))));

            auto result = session.ExecuteSchemeQuery(createIndex)
                            .ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        DoPositiveQueriesVectorIndexOrderByCosine(session);

        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable"));
        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable/index/indexImplLevelTable"));
        DoTruncateTable(session);
        UNIT_ASSERT(IsTableEmpty(session, "/Root/TestTable"));
        UNIT_ASSERT(IsTableEmpty(session, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable/index/indexImplLevelTable"));

        DoOnlyUpsertValuesIntoTable(session);
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }
}

}
}
