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

#include <util/string/printf.h>

#include <format>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpPrefixedVectorIndexes) {

    constexpr int F_NULLABLE    = 1 << 0;
    constexpr int F_COVERING    = 1 << 1;
    constexpr int F_RETURNING   = 1 << 2;
    constexpr int F_SIMILARITY  = 1 << 3;
    constexpr int F_SUFFIX_PK   = 1 << 4;
    constexpr int F_WITH_INDEX  = 1 << 5;
    constexpr int F_OVERLAP     = 1 << 6;
    constexpr int F_WITH_PREFIX = 1 << 7;

    std::vector<i64> DoPositiveQueryVectorIndex(TSession& session, const TString& query, int flags = 0) {
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
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to execute: `" << query << "` with " << result.GetIssues().ToString());

            std::vector<i64> r;
            auto sets = result.GetResultSets();
            for (const auto& set : sets) {
                TResultSetParser parser{set};
                while (parser.TryNextRow()) {
                    if (flags & F_WITH_PREFIX) {
                        auto value = parser.GetValue("user");
                        UNIT_ASSERT_C(value.GetProto().has_bytes_value(), value.GetProto().ShortUtf8DebugString());
                    }
                    auto value = parser.GetValue("pk");
                    UNIT_ASSERT_C(value.GetProto().has_int64_value(), value.GetProto().ShortUtf8DebugString());
                    r.push_back(value.GetProto().int64_value());
                }
            }
            return r;
        }
    }

    void DoPositiveQueriesVectorIndex(TSession& session, const TString& mainQuery, const TString& indexQuery, int flags = 0, size_t count = 3) {
        auto toStr = [](const auto& rs) -> TString {
            TStringBuilder b;
            for (const auto& r : rs) {
                b << r << ", ";
            }
            return b;
        };
        auto mainResults = DoPositiveQueryVectorIndex(session, mainQuery);
        absl::c_sort(mainResults);
        UNIT_ASSERT_EQUAL_C(mainResults.size(), count, toStr(mainResults));
        UNIT_ASSERT_C(std::unique(mainResults.begin(), mainResults.end()) == mainResults.end(), toStr(mainResults));

        auto indexResults = DoPositiveQueryVectorIndex(session, indexQuery, flags);
        absl::c_sort(indexResults);
        UNIT_ASSERT_EQUAL_C(indexResults.size(), count, toStr(indexResults));
        UNIT_ASSERT_C(std::unique(indexResults.begin(), indexResults.end()) == indexResults.end(), toStr(indexResults));

        UNIT_ASSERT_VALUES_EQUAL(mainResults, indexResults);
    }

    void DoPositiveQueriesPrefixedVectorIndexOrderBy(
        TSession& session,
        std::string_view function,
        std::string_view direction,
        std::string_view left,
        std::string_view right,
        int flags = 0,
        std::string_view init = "$target = \"\x67\x68\x02\";\n$user = \"user_b\";",
        size_t count = 3) {
        std::string metric = std::format("Knn::{}({}, {})", function, left, right);
        // no metric in result
        {
            // We don't select user column so it's not required to be in COVER()
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT pk, emb, data FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "3";
                {}
                SELECT pk, emb, data FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, direction)));
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery, flags, count);
        }
        {
            // check the same but with user column in select to check that it's not mistakenly removed
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT * FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "3";
                {}
                SELECT * FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, direction)));
            // select * will read from the main table
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery, flags & ~F_COVERING | F_WITH_PREFIX, count);
        }
        // metric in result
        {
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT {}, pk, emb, data FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"({}
                pragma ydb.KMeansTreeSearchTopSize = "3";
                SELECT {}, pk, emb, data FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, metric, direction)));
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery, flags, count);
        }
        // metric as result
        // TODO(mbkkt) fix this behavior too
        if constexpr (false) {
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT {} AS m, pk, emb, data FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY m {}
                LIMIT 3;
            )", init, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                {}
                SELECT {} AS m, pk, emb, data FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY m {}
                LIMIT 3;
            )", init, metric, direction)));
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery, flags, count);
        }
    }

    void DoPositiveQueriesPrefixedVectorIndexOrderBy(
        TSession& session,
        std::string_view function,
        std::string_view direction,
        int flags = 0,
        std::string_view init = "$target = \"\x67\x68\x02\";\n$user = \"user_b\";",
        size_t count = 3) {
        // target is left, member is right
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, function, direction, "$target", "emb", flags, init, count);
        // target is right, member is left
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, function, direction, "emb", "$target", flags, init, count);
    }

    void DoPositiveQueriesPrefixedVectorIndexOrderByCosine(TSession& session, int flags = 0,
        std::string_view init = "$target = \"\x67\x68\x02\";\n$user = \"user_b\";", size_t count = 3) {
        // distance, default direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, "CosineDistance", "", flags, init, count);
        // distance, asc direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, "CosineDistance", "ASC", flags, init, count);
        // similarity, desc direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, "CosineSimilarity", "DESC", flags, init, count);
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

    void DoTruncateTable(TSession& session) {
        const TString truncateTable(Q_(R"(
            TRUNCATE TABLE `/Root/TestTable`;
        )"));

        auto result = session.ExecuteSchemeQuery(truncateTable).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TSession DoOnlyCreateTableForPrefixedVectorIndex(TTableClient& db, int flags = 0) {
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto tableBuilder = db.GetTableBuilder();
            if (flags & F_NULLABLE) {
                tableBuilder
                    .AddNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNullableColumn("user", EPrimitiveType::String)
                    .AddNullableColumn("emb", EPrimitiveType::String)
                    .AddNullableColumn("data", EPrimitiveType::String);
            } else {
                tableBuilder
                    .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNonNullableColumn("user", EPrimitiveType::String)
                    .AddNonNullableColumn("emb", EPrimitiveType::String)
                    .AddNonNullableColumn("data", EPrimitiveType::String);
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
                auto partitions = TExplicitPartitions{}
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(40).AddElement().OptionalString("").EndTuple().Build())
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(60).AddElement().OptionalString("").EndTuple().Build());
                tableBuilder.SetPartitionAtKeys(partitions);
            } else {
                auto partitions = TExplicitPartitions{}
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(40).EndTuple().Build())
                    .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(60).EndTuple().Build());
                tableBuilder.SetPartitionAtKeys(partitions);
            }
            if (flags & F_WITH_INDEX) {
                TKMeansTreeSettings kmeans;
                kmeans.Settings.Metric = TVectorIndexSettings::EMetric::CosineDistance;
                kmeans.Settings.VectorType = TVectorIndexSettings::EVectorType::Uint8;
                kmeans.Settings.VectorDimension = 2;
                kmeans.Clusters = 2;
                kmeans.Levels = 2;
                std::vector<std::string> dataColumns;
                if (flags & F_COVERING) {
                    dataColumns = {"emb", "data"};
                }
                tableBuilder.AddVectorKMeansTreeIndex("index", {"user", "emb"}, dataColumns, kmeans);
            }
            auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        return session;
    }

    void InsertDataForPrefixedVectorIndex(TSession& session) {
        const TString query1(Q_(R"(
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
            "(93, \"user_c\", \"\x76\x76\x02\", \"39\");"
        ));

        auto result = session.ExecuteDataQuery(
                             query1,
                             TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                      .ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    TSession DoCreateTableForPrefixedVectorIndex(TTableClient& db, int flags = 0) {
        auto session = DoOnlyCreateTableForPrefixedVectorIndex(db, flags);
        InsertDataForPrefixedVectorIndex(session);
        return session;
    }

    void DoCreatePrefixedVectorIndex(TSession & session, int levels, int flags = 0) {
        const char* cover = "";
        if (flags & F_COVERING) {
            cover = " COVER (emb, data)";
        }

        const TString createIndex(Q_(Sprintf(R"(
            ALTER TABLE `/Root/TestTable`
                ADD INDEX index
                GLOBAL USING vector_kmeans_tree
                ON (user, emb)
                %s
                WITH (%s=cosine, vector_type="uint8", vector_dimension=2, levels=%d, clusters=2%s);
            )", cover, (flags & F_SIMILARITY ? "similarity" : "distance"), levels,
            (flags & F_OVERLAP ? ", overlap_clusters=2" : ""))));

        auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[60u;30u]]");
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
        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, indexLevels, flags);
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            if (flags & F_COVERING) {
                std::vector<std::string> indexDataColumns{"emb", "data"};
                UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), indexDataColumns);
            }
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
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
            DoCheckOverlap(session, "index");
        }

        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags);

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

    Y_UNIT_TEST_QUAD(OrderByCosineLevel2WithOverlap, Nullable, Covered) {
        DoTestOrderByCosine(2, F_OVERLAP | (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel3) {
        DoTestOrderByCosine(3, 0);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel3WithOverlap) {
        DoTestOrderByCosine(3, F_OVERLAP);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel4) {
        DoTestOrderByCosine(4, 0);
    }

    Y_UNIT_TEST_TWIN(OrderByCosineDistanceWithCover, Nullable) {
        DoTestOrderByCosine(2, (Nullable ? F_NULLABLE : 0) | F_COVERING);
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
        auto session = DoCreateTableForPrefixedVectorIndex(db, (Nullable ? F_NULLABLE : 0));
        DoCreatePrefixedVectorIndex(session, 2, F_COVERING | F_SUFFIX_PK | (Overlap ? F_OVERLAP : 0));
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), (std::vector<std::string>{"user", "emb"}));
            UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), (std::vector<std::string>{"emb", "data"}));
        }

        // Check without F_COVERING here because user column is not covered and query still accesses the main table
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, 0);
    }

    Y_UNIT_TEST_QUAD(CosineDistanceWithPkSuffix, Nullable, Covered) {
        DoTestOrderByCosine(2, F_SUFFIX_PK | (Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(CosineDistanceWithPkSuffixWithOverlap, Covered) {
        DoTestOrderByCosine(2, F_SUFFIX_PK | F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST(SubPrefix) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, 0);

        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (user, pk, emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
                )"));

            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // index on (user, pk, emb) but selecting on (user order by emb)
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, 0);
    }

    void DoTestPrefixedVectorIndexInsert(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();

        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");

        // Insert to the table with index should succeed
        {
            TString query1(Q_(R"(
                INSERT INTO `/Root/TestTable` (pk, user, emb, data) VALUES
                (101, "user_a", "\x03\x29\x02", "101"),
                (102, "user_b", "\x03\x29\x02", "102"),
                (111, "user_a", "\x76\x75\x02", "111"),
                (112, "user_b", "\x76\x75\x02", "112")
            )"));
            query1 += (flags & F_RETURNING ? " RETURNING data, emb, user, pk;" : ";");

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                    "[[\"101\";\"\\3)\\2\";\"user_a\";101];"
                    "[\"102\";\"\\3)\\2\";\"user_b\";102];"
                    "[\"111\";\"vu\\2\";\"user_a\";111];"
                    "[\"112\";\"vu\\2\";\"user_b\";112]]");
            }
        }

        // Index is updated
        const TString postingTable1_ins = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that PKs 1/101, 2/102, 91/111, 92/112 are now in same clusters
        {
            const TString query1(Q_(R"(
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
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                (flags & F_OVERLAP ? "[[2u];[2u];[2u];[2u]]" : "[[1u];[1u];[1u];[1u]]"));
        }
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexInsert, Returning, Covered) {
        DoTestPrefixedVectorIndexInsert((Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexInsertWithOverlap, Returning, Covered) {
        DoTestPrefixedVectorIndexInsert(F_OVERLAP | (Returning ? F_RETURNING : 0) | (Covered ? F_COVERING : 0));
    }

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

        auto db = kikimr.GetTableClient();

        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");

        // Insert to the table with index should succeed
        {
            TString query1(Q_(R"(
                INSERT INTO `/Root/TestTable` (pk, user, emb, data) VALUES
                (101, "user_a", "\x03\x29\x02", "101"),
                (102, "user_xxx", "\x03\x29\x02", "102"),
                (111, "user_yyy", "\x76\x75\x02", "111"),
                (112, "user_yyy", "\x76\x75\x02", "112");
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        // Index is updated
        const TString postingTable1_ins = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that we can now actually find new rows
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_a\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_b\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_xxx\";", 1);
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_yyy\";", 2);

        // Check that PKs 1/101, 111/112 are now in same clusters
        {
            const TString query1(Q_(R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (1, 101)
                UNION ALL
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (111, 112)
                ;
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                (flags & F_OVERLAP ? "[[2u];[1u]]" : "[[1u];[1u]]"));
        }

        // Delete one of the new rows
        {
            auto result = session.ExecuteDataQuery("DELETE FROM `/Root/TestTable` WHERE pk=112;",
                TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        // Check that PK 112 is not present in the posting table
        {
            const TString query1(Q_(R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk=112;
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[0u]]");
        }
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexInsertNewPrefix, Nullable, Covered) {
        DoTestPrefixedVectorIndexInsertNewPrefix((Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexInsertNewPrefixWithOverlap, Covered) {
        // New prefixes are not affected by overlap - they are added directly into the Posting table
        DoTestPrefixedVectorIndexInsertNewPrefix(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

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

        auto db = kikimr.GetTableClient();

        auto session = DoOnlyCreateTableForPrefixedVectorIndex(db, flags);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(originalPostingTable, "[]");

        // Insert to the table with index should succeed
        InsertDataForPrefixedVectorIndex(session);

        // Index is updated
        const TString postingTable1_ins = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that we can now actually find new rows
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_a\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_b\";");
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorEmptyIndexedTableInsert, Nullable, Covered) {
        DoTestPrefixedVectorEmptyIndexedTableInsert((Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorEmptyIndexedTableInsertWithOverlap, Covered) {
        // New prefixes are not affected by overlap - they are added directly into the Posting table
        DoTestPrefixedVectorEmptyIndexedTableInsert(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    // Same as PrefixedVectorEmptyIndexedTableInsert, but the index is created separately after creating the table
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

        auto db = kikimr.GetTableClient();

        auto session = DoOnlyCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(originalPostingTable, "[]");

        // Insert to the table with index should succeed
        InsertDataForPrefixedVectorIndex(session);

        // Index is updated
        const TString postingTable1_ins = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable1_ins);

        // Check that we can now actually find new rows
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_a\";");
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags,
            "$target = \"\x67\x68\x02\";\n$user = \"user_b\";");
    }

    Y_UNIT_TEST_QUAD(EmptyPrefixedVectorIndexInsert, Nullable, Covered) {
        DoTestEmptyPrefixedVectorIndexInsert((Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(EmptyPrefixedVectorIndexInsertWithOverlap, Covered) {
        // New prefixes are not affected by overlap - they are added directly into the Posting table
        DoTestEmptyPrefixedVectorIndexInsert(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    void DoTestPrefixedVectorIndexDelete(const TString& deleteQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();

        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        {
            auto result = session.ExecuteDataQuery(deleteQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[\"19\";\"user_a\";\"vv\\2\";91]]");
            }
        }

        // Check that PK 91 is not present in the posting table
        {
            const TString query1(Q_(R"(
                SELECT COUNT(*) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk=91;
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[0u]]");
        }
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexDeletePk, Covered, Overlap) {
        // DELETE WHERE from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=91;)"),
            (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteFilter, Covered) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="19" AND user="user_a";)"), (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteOn, Covered) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 91 AS `pk`;)"), (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexDeletePkReturning, Covered, Overlap) {
        // DELETE WHERE from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=91 RETURNING data, user, emb, pk;)"),
            F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteFilterReturning, Covered) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="19" AND user="user_a" RETURNING data, user, emb, pk;)"), F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteOnReturning, Covered) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 91 AS `pk` RETURNING data, user, emb, pk;)"), F_RETURNING | (Covered ? F_COVERING : 0));
    }

    void DoTestPrefixedVectorIndexUpdateNoChange(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        TString orig = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");

        // Update to the table with index should succeed (but embedding does not change)
        {
            const TString query1(Q_("UPDATE `/Root/TestTable` SET `data`=\"119\" WHERE `pk`=91;"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        if (flags & F_COVERING) {
            SubstGlobal(orig, "\"19\"", "\"119\"");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateNoChange, Nullable, Covered) {
        DoTestPrefixedVectorIndexUpdateNoChange((Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdateNoChangeWithOverlap, Covered) {
        DoTestPrefixedVectorIndexUpdateNoChange(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    void DoTestPrefixedVectorIndexUpdateNoClusterChange(int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        TString orig = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");

        // Update to the table with index should succeed (embedding changes, but the cluster does not)
        {
            const TString query1(Q_(R"(
                UPDATE `/Root/TestTable` SET `emb`="\x76\x75\x02" WHERE `pk`=91;
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        if (flags & F_COVERING) {
            SubstGlobal(orig, "\"\x76\x76\\2\"];[\"19", "\"\x76\x75\\2\"];[\"19");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateNoClusterChange, Nullable, Covered) {
        DoTestPrefixedVectorIndexUpdateNoClusterChange((Nullable ? F_NULLABLE : 0) | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdateNoClusterChangeWithOverlap, Covered) {
        DoTestPrefixedVectorIndexUpdateNoClusterChange(F_OVERLAP | (Covered ? F_COVERING : 0));
    }

    void DoTestPrefixedVectorIndexUpdateClusterChange(const TString& updateQuery, int flags) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        const TString orig = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");

        // Update/upsert to the table with index should succeed (and the cluster should change)
        {
            auto result = session.ExecuteDataQuery(updateQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (flags & F_RETURNING) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[\"19\"];[\"\\0031\\2\"];[\"user_a\"];[91]]]");
            }
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_UNEQUAL(orig, updated);

        // Check that PK 1 and 91 are now in the same cluster
        {
            const TString query1(Q_(R"(
                SELECT COUNT(DISTINCT __ydb_parent) FROM `/Root/TestTable/index/indexImplPostingTable`
                WHERE pk IN (1, 91);
            )"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)),
                (flags & F_OVERLAP ? "[[2u]]" : "[[1u]]"));
        }
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdatePkClusterChange, Covered, Overlap) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=91;)"),
            F_NULLABLE | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdateFilterClusterChange, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="19" AND `user`="user_a";)"),
            F_NULLABLE | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpsertClusterChange, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `user`, `emb`, `data`) VALUES (91, "user_a", "\x03\x31\x02", "19");)"),
            F_NULLABLE | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdatePkClusterChangeReturning, Covered, Overlap) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=91 RETURNING `data`, `emb`, `user`, `pk`;)"),
            F_NULLABLE | F_RETURNING | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdateFilterClusterChangeReturning, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="19" AND `user`="user_a" RETURNING `data`, `emb`, `user`, `pk`;)"),
            F_NULLABLE | F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpsertClusterChangeReturning, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `user`, `emb`, `data`) VALUES (91, "user_a", "\x03\x31\x02", "19") RETURNING `data`, `emb`, `user`, `pk`;)"),
            F_NULLABLE | F_RETURNING | (Covered ? F_COVERING : 0));
    }

    Y_UNIT_TEST_TWIN(TwoIndexUpsert, UseUpsert) {
        NKikimrConfig::TFeatureFlags featureFlags;
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        const int flags = 0;
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
        DoCreatePrefixedVectorIndex(session, 2, flags);

        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index2
                    GLOBAL USING vector_kmeans_tree
                    ON (user, emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=3);
                )")));
            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Insert/upsert to the table should succeed
        {
            auto updateQuery = Q_(Sprintf(
                R"(%s INTO `/Root/TestTable` (`pk`, `user`, `emb`, `data`) VALUES (101, "user_a", "\x03\x31\x02", "20");)",
                UseUpsert ? "UPSERT" : "INSERT"
            ));
            auto result = session.ExecuteDataQuery(updateQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }
    }

    Y_UNIT_TEST_TWIN(VectorSearchPushdown, Covered) {
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
        auto session = kikimr.RunCall([&] {
            auto session = DoCreateTableForPrefixedVectorIndex(db, flags);
            DoCreatePrefixedVectorIndex(session, 2, flags);
            return session;
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
            TString query1(Q_(R"(
                pragma ydb.KMeansTreeSearchTopSize = "2";
                SELECT * FROM `/Root/TestTable`
                VIEW index
                WHERE user="user_a"
                ORDER BY Knn::CosineDistance(emb, "\x67\x71\x02")
                LIMIT 3
            )"));

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                    .ExtractValueSync();
            });
            UNIT_ASSERT(result.IsSuccess());
        }
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexTruncateTable, Covered, Overlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableTruncateTable(true);
        auto serverSettings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        const int flags = F_NULLABLE | (Covered ? F_COVERING : 0) | (Overlap ? F_OVERLAP : 0);
        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, flags);

        {
            const char* cover = "";
            if (flags & F_COVERING) {
                cover = " COVER (user, emb, data)";
            }

            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (user, emb)%s
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2%s);
                )", cover, "")));

            auto result = session.ExecuteSchemeQuery(createIndex)
                            .ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags);

        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable"));
        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable/index/indexImplLevelTable"));
        UNIT_ASSERT(!IsTableEmpty(session, "/Root/TestTable/index/indexImplPrefixTable"));

        DoTruncateTable(session);

        UNIT_ASSERT(IsTableEmpty(session, "/Root/TestTable"));
        UNIT_ASSERT(IsTableEmpty(session, "/Root/TestTable/index/indexImplPostingTable"));
        UNIT_ASSERT(IsTableEmpty(session, "/Root/TestTable/index/indexImplLevelTable"));
        UNIT_ASSERT(IsTableEmpty(session, "/Root/TestTable/index/indexImplPrefixTable"));

        InsertDataForPrefixedVectorIndex(session);
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, flags);
    }
}
}
}
