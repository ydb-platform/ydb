#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>

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

    std::vector<i64> DoPositiveQueryVectorIndex(TSession& session, const TString& query) {
        {
            auto result = session.ExplainDataQuery(query).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(),
                "Failed to explain: `" << query << "` with " << result.GetIssues().ToString());
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

    void DoPositiveQueriesVectorIndex(TSession& session, const TString& mainQuery, const TString& indexQuery) {
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

        auto indexResults = DoPositiveQueryVectorIndex(session, indexQuery);
        absl::c_sort(indexResults);
        UNIT_ASSERT_EQUAL_C(indexResults.size(), 3, toStr(indexResults));
        UNIT_ASSERT_C(std::unique(indexResults.begin(), indexResults.end()) == indexResults.end(), toStr(indexResults));

        UNIT_ASSERT_VALUES_EQUAL(mainResults, indexResults);
    }

    void DoPositiveQueriesVectorIndexOrderBy(
        TSession& session,
        std::string_view function,
        std::string_view direction,
        std::string_view left,
        std::string_view right) {
        constexpr std::string_view target = "$target = \"\x67\x71\x03\";";
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
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery);
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
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery);
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
            DoPositiveQueriesVectorIndex(session, plainQuery, indexQuery);
        }
    }

    void DoPositiveQueriesVectorIndexOrderBy(
        TSession& session,
        std::string_view function,
        std::string_view direction) {
        // target is left, member is right
        DoPositiveQueriesVectorIndexOrderBy(session, function, direction, "$target", "emb");
        // target is right, member is left
        DoPositiveQueriesVectorIndexOrderBy(session, function, direction, "emb", "$target");
    }

    void DoPositiveQueriesVectorIndexOrderByCosine(TSession& session) {
        // distance, default direction
        DoPositiveQueriesVectorIndexOrderBy(session, "CosineDistance", "");
        // distance, asc direction
        DoPositiveQueriesVectorIndexOrderBy(session, "CosineDistance", "ASC");
        // similarity, desc direction
        DoPositiveQueriesVectorIndexOrderBy(session, "CosineSimilarity", "DESC");
    }

    TSession DoCreateTableForVectorIndex(TTableClient& db, bool nullable) {
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto tableBuilder = db.GetTableBuilder();
            if (nullable) {
                tableBuilder
                    .AddNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNullableColumn("emb", EPrimitiveType::String)
                    .AddNullableColumn("data", EPrimitiveType::String);
            } else {
                tableBuilder
                    .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                    .AddNonNullableColumn("emb", EPrimitiveType::String)
                    .AddNonNullableColumn("data", EPrimitiveType::String);
            }
            tableBuilder.SetPrimaryKeyColumns({"pk"});
            tableBuilder.BeginPartitioningSettings()
                .SetMinPartitionsCount(3)
            .EndPartitioningSettings();
            auto partitions = TExplicitPartitions{}
                .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(4).EndTuple().Build())
                .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(6).EndTuple().Build());
            tableBuilder.SetPartitionAtKeys(partitions);
            auto result = session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.IsTransportError(), false);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            const TString query1(Q_(R"(
                UPSERT INTO `/Root/TestTable` (pk, emb, data) VALUES)"
                "(0, \"\x03\x30\x03\", \"0\"),"
                "(1, \"\x13\x31\x03\", \"1\"),"
                "(2, \"\x23\x32\x03\", \"2\"),"
                "(3, \"\x53\x33\x03\", \"3\"),"
                "(4, \"\x43\x34\x03\", \"4\"),"
                "(5, \"\x50\x60\x03\", \"5\"),"
                "(6, \"\x61\x11\x03\", \"6\"),"
                "(7, \"\x12\x62\x03\", \"7\"),"
                "(8, \"\x75\x76\x03\", \"8\"),"
                "(9, \"\x76\x76\x03\", \"9\");"
            ));

            auto result = session.ExecuteDataQuery(
                                 query1,
                                 TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                          .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        return session;
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel1) {
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
        auto session = DoCreateTableForVectorIndex(db, false);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )"));

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
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineSimilarityNotNullableLevel1) {
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
        auto session = DoCreateTableForVectorIndex(db, false);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )"));

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
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNullableLevel1) {
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
        auto session = DoCreateTableForVectorIndex(db, true);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )"));

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
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineSimilarityNullableLevel1) {
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
        auto session = DoCreateTableForVectorIndex(db, true);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )"));

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
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNotNullableLevel2) {
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
        auto session = DoCreateTableForVectorIndex(db, false);
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
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), std::vector<std::string>{"emb"});
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineSimilarityNotNullableLevel2) {
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
        auto session = DoCreateTableForVectorIndex(db, false);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )"));

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
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineDistanceNullableLevel2) {
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
        auto session = DoCreateTableForVectorIndex(db, true);
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
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NYdb::EStatus::SUCCESS);
            const auto& indexes = result.GetTableDescription().GetIndexDescriptions();
            UNIT_ASSERT_EQUAL(indexes.size(), 1);
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexName(), "index");
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), std::vector<std::string>{"emb"});
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(OrderByCosineSimilarityNullableLevel2) {
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
        auto session = DoCreateTableForVectorIndex(db, true);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )"));

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
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
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
        auto session = DoCreateTableForVectorIndex(db, false);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=3, clusters=2);
            )"));

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
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 3);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        // TODO: fix somehow?
        // DoPositiveQueriesVectorIndexOrderByCosine(session);
    }

    Y_UNIT_TEST(BuildIndexTimesAndUser) {
        NKikimrConfig::TAppConfig appConfig;
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);

        auto now = TInstant::Now();

        auto driver = NYdb::TDriver(NYdb::TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
            .SetDatabase("/Root")
            .SetAuthToken("root@builtin"));
        auto db = NYdb::NTable::TTableClient(driver);
        auto session = DoCreateTableForVectorIndex(db, false);
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

    Y_UNIT_TEST(VectorIndexIsNotUpdatable) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndex(db, true);

        // Add first index
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index1
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )"));

            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Upsert to the table with index should succeed
        {
            const TString query1(Q_(R"(
                UPSERT INTO `/Root/TestTable` (pk, emb, data) VALUES)"
                "(10, \"\x76\x76\x03\", \"10\");"
            ));

            auto result = session.ExecuteDataQuery(
                                 query1,
                                 TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                          .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString postingTable1 = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        
        // First index is not updated
        UNIT_ASSERT_STRINGS_EQUAL(originalPostingTable, postingTable1);

        // Add second index
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index2
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (similarity=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )"));

            auto result = session.ExecuteSchemeQuery(createIndex).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }     
        
        const TString postingTable2 = ReadTablePartToYson(session, "/Root/TestTable/index2/indexImplPostingTable");
        
        // Second index is different
        UNIT_ASSERT_STRINGS_UNEQUAL(originalPostingTable, postingTable2);
    }

}

}
}
