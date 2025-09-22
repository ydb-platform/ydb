#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>

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

    std::vector<i64> DoPositiveQueryVectorIndex(TSession& session, const TString& query, bool covered = false) {
        {
            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to explain: `" << query << "` with " << result.GetIssues().ToString());

            if (covered) {
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
                    auto value = parser.GetValue("pk");
                    UNIT_ASSERT_C(value.GetProto().has_int64_value(), value.GetProto().ShortUtf8DebugString());
                    r.push_back(value.GetProto().int64_value());
                }
            }
            return r;
        }
    }

    void DoPositiveQueriesVectorIndex(TSession& session, const TString& mainQuery, const TString& indexQuery, bool covered = false) {
        auto toStr = [](const auto& rs) -> TString {
            TStringBuilder b;
            for (const auto& r : rs) {
                b << r << ", ";
            }
            return b;
        };
        auto mainResults = DoPositiveQueryVectorIndex(session, mainQuery);
        absl::c_sort(mainResults);
        UNIT_ASSERT_EQUAL_C(mainResults.size(), 3, toStr(mainResults));
        UNIT_ASSERT_C(std::unique(mainResults.begin(), mainResults.end()) == mainResults.end(), toStr(mainResults));

        auto indexResults = DoPositiveQueryVectorIndex(session, indexQuery, covered);
        absl::c_sort(indexResults);
        UNIT_ASSERT_EQUAL_C(indexResults.size(), 3, toStr(indexResults));
        UNIT_ASSERT_C(std::unique(indexResults.begin(), indexResults.end()) == indexResults.end(), toStr(indexResults));

        UNIT_ASSERT_VALUES_EQUAL(mainResults, indexResults);
    }

    void DoPositiveQueriesPrefixedVectorIndexOrderBy(
        TSession& session,
        std::string_view function,
        std::string_view direction,
        std::string_view left,
        std::string_view right,
        bool covered = false) {
        constexpr std::string_view init = 
            "$target = \"\x67\x68\x02\";\n"
            "$user = \"user_b\";";
        std::string metric = std::format("Knn::{}({}, {})", function, left, right);
        // no metric in result
        {
            // TODO(vitaliff): Exclude index-covered WHERE fields from KqpReadTableRanges.
            // Currently even if we SELECT only pk, emb, data WHERE user=xxx we also get `user`
            // in SELECT columns and thus it's required to add it to covered columns.
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
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery, covered);
        }
        // metric in result
        {
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"({}
                pragma ydb.KMeansTreeSearchTopSize = "2";
                SELECT {}, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY {} {}
                LIMIT 3;
            )", init, metric, metric, direction)));
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery, covered);
        }
        // metric as result
        // TODO(mbkkt) fix this behavior too
        if constexpr (false) {
            const TString plainQuery(Q1_(std::format(R"({}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable`
                WHERE user = $user
                ORDER BY m {}
                LIMIT 3;
            )", init, metric, direction)));
            const TString indexQuery(Q1_(std::format(R"(
                pragma ydb.KMeansTreeSearchTopSize = "1";
                {}
                SELECT {} AS m, `/Root/TestTable`.* FROM `/Root/TestTable` VIEW index
                WHERE user = $user
                ORDER BY m {}
                LIMIT 3;
            )", init, metric, direction)));
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery, covered);
        }
    }

    void DoPositiveQueriesPrefixedVectorIndexOrderBy(
        TSession& session,
        std::string_view function,
        std::string_view direction,
        bool covered = false) {
        // target is left, member is right
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, function, direction, "$target", "emb", covered);
        // target is right, member is left
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, function, direction, "emb", "$target", covered);
    }

    void DoPositiveQueriesPrefixedVectorIndexOrderByCosine(TSession& session, bool covered = false) {
        // distance, default direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, "CosineDistance", "", covered);
        // distance, asc direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, "CosineDistance", "ASC", covered);
        // similarity, desc direction
        DoPositiveQueriesPrefixedVectorIndexOrderBy(session, "CosineSimilarity", "DESC", covered);
    }

    TSession DoCreateTableForPrefixedVectorIndex(TTableClient& db, bool nullable, bool suffixPk = false) {
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto tableBuilder = db.GetTableBuilder();
            if (nullable) {
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
            if (suffixPk) {
                tableBuilder.SetPrimaryKeyColumns({"pk", "user"});
            } else {
                tableBuilder.SetPrimaryKeyColumns({"pk"});
            }
            tableBuilder.BeginPartitioningSettings()
                .SetMinPartitionsCount(3)
                .EndPartitioningSettings();
            if (suffixPk) {
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
            auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
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
        return session;
    }

    void DoCreatePrefixedVectorIndex(TSession & session, bool useSimilarity, const TString& cover, int levels) {
        const TString createIndex(Q_(Sprintf(R"(
            ALTER TABLE `/Root/TestTable`
                ADD INDEX index
                GLOBAL USING vector_kmeans_tree
                ON (user, emb)
                %s
                WITH (%s=cosine, vector_type="uint8", vector_dimension=2, levels=%d, clusters=2);
        )", cover.c_str(), useSimilarity ? "similarity" : "distance", levels)));

        auto result = session.ExecuteSchemeQuery(createIndex)
                      .ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        // FIXME: result does not return failure/issues when index is created but fails to be filled with data
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel1, Nullable, UseSimilarity) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, Nullable);
        DoCreatePrefixedVectorIndex(session, UseSimilarity, "", 1);
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, UseSimilarity
                ? NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity
                : NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST_QUAD(OrderByCosineLevel2, Nullable, UseSimilarity) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, Nullable);
        DoCreatePrefixedVectorIndex(session, UseSimilarity, "", 2);
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, UseSimilarity
                ? NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity
                : NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel3) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, false);
        DoCreatePrefixedVectorIndex(session, false, "", 3);
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 3);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel4) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, false);
        DoCreatePrefixedVectorIndex(session, false, "", 4);
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 4);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexOrderByCosineDistanceWithCover, Nullable) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, Nullable);
        DoCreatePrefixedVectorIndex(session, false, "COVER (user, emb, data)", 2);
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            std::vector<std::string> indexDataColumns{"user", "emb", "data"};
            UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), indexDataColumns);
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, true /*covered*/);
    }

    Y_UNIT_TEST_QUAD(CosineDistanceWithPkPrefix, Nullable, Covered) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();

        auto session = DoCreateTableForPrefixedVectorIndex(db, Nullable, true);
        DoCreatePrefixedVectorIndex(session, false, Covered ? "COVER (emb, data)" : "", 2);
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            std::vector<std::string> indexKeyColumns{"user", "emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            std::vector<std::string> indexDataColumns;
            if (Covered) {
                indexDataColumns = {"emb", "data"};
            }
            UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), indexDataColumns);
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesPrefixedVectorIndexOrderByCosine(session, Covered);
    }

    void DoTestPrefixedVectorIndexInsert(bool returning, bool covered) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();

        auto session = DoCreateTableForPrefixedVectorIndex(db, false);
        DoCreatePrefixedVectorIndex(session, false, covered ? "COVER (user, emb, data)" : "", 2);

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
            query1 += (returning ? " RETURNING data, emb, user, pk;" : ";");

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (returning) {
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
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[1u];[1u];[1u];[1u]]");
        }
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexInsert, Returning, Covered) {
        DoTestPrefixedVectorIndexInsert(Returning, Covered);
    }

    void DoTestPrefixedVectorIndexDelete(const TString& deleteQuery, bool returning, bool covered) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();

        auto session = DoCreateTableForPrefixedVectorIndex(db, false);
        DoCreatePrefixedVectorIndex(session, false, covered ? "COVER (user, emb, data)" : "", 2);

        {
            auto result = session.ExecuteDataQuery(deleteQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (returning) {
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

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeletePk, Covered) {
        // DELETE WHERE from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=91;)"), false, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteFilter, Covered) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="19" AND user="user_a";)"), false, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteOn, Covered) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 91 AS `pk`;)"), false, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeletePkReturning, Covered) {
        // DELETE WHERE from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=91 RETURNING data, user, emb, pk;)"), true, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteFilterReturning, Covered) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="19" AND user="user_a" RETURNING data, user, emb, pk;)"), true, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexDeleteOnReturning, Covered) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestPrefixedVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 91 AS `pk` RETURNING data, user, emb, pk;)"), true, Covered);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateNoChange, Nullable, Covered) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, Nullable);
        DoCreatePrefixedVectorIndex(session, false, Covered ? "COVER (user, emb, data)" : "", 2);

        TString orig = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");

        // Update to the table with index should succeed (but embedding does not change)
        {
            const TString query1(Q_("UPDATE `/Root/TestTable` SET `data`=\"119\" WHERE `pk`=91;"));
            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");
        if (Covered) {
            SubstGlobal(orig, "\"19\"", "\"119\"");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST_QUAD(PrefixedVectorIndexUpdateNoClusterChange, Nullable, Covered) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, Nullable);
        DoCreatePrefixedVectorIndex(session, false, Covered ? "COVER (user, emb, data)" : "", 2);

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
        if (Covered) {
            SubstGlobal(orig, "\"\x76\x76\\2\"];[\"19", "\"\x76\x75\\2\"];[\"19");
        }
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    void DoTestPrefixedVectorIndexUpdateClusterChange(const TString& updateQuery, bool returning, bool covered) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForPrefixedVectorIndex(db, true);
        DoCreatePrefixedVectorIndex(session, false, covered ? "COVER (user, emb, data)" : "", 2);

        const TString orig = ReadTablePartToYson(session, "/Root/TestTable/index/indexImplPostingTable");

        // Update/upsert to the table with index should succeed (and the cluster should change)
        {
            auto result = session.ExecuteDataQuery(updateQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (returning) {
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
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[1u]]");
        }
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdatePkClusterChange, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=91;)"), false, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdateFilterClusterChange, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="19" AND `user`="user_a";)"), false, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpsertClusterChange, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `user`, `emb`, `data`) VALUES (91, "user_a", "\x03\x31\x02", "19");)"), false, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdatePkClusterChangeReturning, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `pk`=91 RETURNING `data`, `emb`, `user`, `pk`;)"), true, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpdateFilterClusterChangeReturning, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x02" WHERE `data`="19" AND `user`="user_a" RETURNING `data`, `emb`, `user`, `pk`;)"), true, Covered);
    }

    Y_UNIT_TEST_TWIN(PrefixedVectorIndexUpsertClusterChangeReturning, Covered) {
        DoTestPrefixedVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `user`, `emb`, `data`) VALUES (91, "user_a", "\x03\x31\x02", "19") RETURNING `data`, `emb`, `user`, `pk`;)"), true, Covered);
    }

}

}
}
