#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/gateway/kqp_metadata_loader.h>
#include <ydb/core/kqp/host/kqp_host_impl.h>

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

    NYdb::NTable::TDataQueryResult ExecuteDataQuery(TSession& session, const TString& query) {
        const auto txSettings = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
        return session.ExecuteDataQuery(query, txSettings,
            TExecDataQuerySettings().KeepInQueryCache(true).CollectQueryStats(ECollectQueryStatsMode::Basic)).ExtractValueSync();
    }

    std::vector<i64> DoPositiveQueryVectorIndex(TSession& session, TTxSettings txSettings, const TString& query, bool covered = false) {
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

    void DoPositiveQueriesVectorIndex(TSession& session, TTxSettings txSettings, const TString& mainQuery, const TString& indexQuery, bool covered = false) {
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

        auto indexResults = DoPositiveQueryVectorIndex(session, txSettings, indexQuery, covered);
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
        bool covered = false
    ) {
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
            DoPositiveQueriesVectorIndex(session, txSettings, plainQuery, indexQuery, covered);
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
            DoPositiveQueriesVectorIndex(session, txSettings, plainQuery, indexQuery, covered);
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
            DoPositiveQueriesVectorIndex(session, txSettings, plainQuery, indexQuery, covered);
        }
    }

    void DoPositiveQueriesVectorIndexOrderBy(
        TSession& session,
        TTxSettings txSettings,
        std::string_view function,
        std::string_view direction,
        bool covered = false) {
        // target is left, member is right
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, function, direction, "$target", "emb", covered);
        // target is right, member is left
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, function, direction, "emb", "$target", covered);
    }

    void DoPositiveQueriesVectorIndexOrderByCosine(
        TSession& session,
        TTxSettings txSettings = TTxSettings::SerializableRW(),
        bool covered = false) {
        // distance, default direction
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, "CosineDistance", "", covered);
        // distance, asc direction
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, "CosineDistance", "ASC", covered);
        // similarity, desc direction
        DoPositiveQueriesVectorIndexOrderBy(session, txSettings, "CosineSimilarity", "DESC", covered);
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

    TSession DoCreateTableAndVectorIndex(TTableClient& db, bool nullable) {
        auto session = DoCreateTableForVectorIndex(db, nullable);

        // Add an index
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

        return session;
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
        auto session = DoCreateTableForVectorIndex(db, Nullable);
        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (%s=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2);
            )", UseSimilarity ? "similarity" : "distance")));

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
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, UseSimilarity
                ? NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity
                : NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 1);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session);
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
        auto session = DoCreateTableForVectorIndex(db, Nullable);
        {
            const TString createIndex(Q_(Sprintf(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb)
                    WITH (%s=cosine, vector_type="uint8", vector_dimension=2, levels=2, clusters=2);
            )", UseSimilarity ? "similarity" : "distance")));

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
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, UseSimilarity
                ? NYdb::NTable::TVectorIndexSettings::EMetric::CosineSimilarity
                : NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
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
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
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

    Y_UNIT_TEST(VectorIndexNoBulkUpsert) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, true);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // BulkUpsert to the table with index should fail
        {
            NYdb::TValueBuilder rows;
            rows.BeginList();
            rows.AddListItem()
                .BeginStruct()
                .AddMember("pk").Int64(11)
                .AddMember("emb").String("\x77\x77\x03")
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

    void DoTestVectorIndexDelete(const TString& deleteQuery, bool returning) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, true);

        {
            auto result = session.ExecuteDataQuery(deleteQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (returning) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[\"9\"];[\"vv\\3\"];[9]]]");
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

    Y_UNIT_TEST(VectorIndexDeletePk) {
        // DELETE WHERE from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=9;)"), false);
    }

    Y_UNIT_TEST(VectorIndexDeleteFilter) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="9";)"), false);
    }

    Y_UNIT_TEST(VectorIndexDeleteOn) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 9 AS `pk`;)"), false);
    }

    Y_UNIT_TEST(VectorIndexDeletePkReturning) {
        // DELETE WHERE from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE pk=9 RETURNING data, emb, pk;)"), true);
    }

    Y_UNIT_TEST(VectorIndexDeleteFilterReturning) {
        // DELETE WHERE with non-PK filter from the table with index should succeed
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` WHERE data="9" RETURNING data, emb, pk;)"), true);
    }

    Y_UNIT_TEST(VectorIndexDeleteOnReturning) {
        // DELETE ON from the table with index should succeed too (it uses a different code path)
        DoTestVectorIndexDelete(Q_(R"(DELETE FROM `/Root/TestTable` ON SELECT 9 AS `pk` RETURNING data, emb, pk;)"), true);
    }

    void DoTestVectorIndexInsert(bool returning) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, true);

        const TString originalPostingTable = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Insert to the table with index should succeed
        {
            TString query1(Q_(R"(
                INSERT INTO `/Root/TestTable` (pk, emb, data) VALUES
                (10, "\x11\x62\x03", "10"),
                (11, "\x77\x75\x03", "11")
            )"));
            query1 += (returning ? " RETURNING data, emb, pk;" : ";");

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (returning) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[\"10\"];[\"\\021b\\3\"];[10]];[[\"11\"];[\"wu\\3\"];[11]]]");
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
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[1u];[1u]]");
        }
    }

    Y_UNIT_TEST(VectorIndexInsert) {
        DoTestVectorIndexInsert(false);
    }

    Y_UNIT_TEST(VectorIndexInsertReturning) {
        DoTestVectorIndexInsert(true);
    }

    Y_UNIT_TEST(VectorIndexUpdateNoChange) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, true);

        const TString orig = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Update to the table with index should succeed (but embedding does not change)
        {
            const TString query1(Q_(R"(
                UPDATE `/Root/TestTable` SET `data`="20" WHERE `pk`=9;
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    Y_UNIT_TEST(VectorIndexUpdateNoClusterChange) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, true);

        const TString orig = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Update to the table with index should succeed (embedding changes, but the cluster does not)
        {
            const TString query1(Q_(R"(
                UPDATE `/Root/TestTable` SET `emb`="\x76\x75\x03" WHERE `pk`=9;
            )"));

            auto result = session.ExecuteDataQuery(query1, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
        }

        const TString updated = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");
        UNIT_ASSERT_STRINGS_EQUAL(orig, updated);
    }

    void DoTestVectorIndexUpdateClusterChange(const TString& updateQuery, bool returning) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableAndVectorIndex(db, true);

        const TString orig = ReadTablePartToYson(session, "/Root/TestTable/index1/indexImplPostingTable");

        // Update/upsert to the table with index should succeed (and the cluster should change)
        {
            auto result = session.ExecuteDataQuery(updateQuery, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx())
                .ExtractValueSync();
            UNIT_ASSERT(result.IsSuccess());
            if (returning) {
                UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[[\"9\"];[\"\\0031\\3\"];[9]]]");
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
            UNIT_ASSERT_VALUES_EQUAL(NYdb::FormatResultSetYson(result.GetResultSet(0)), "[[1u]]");
        }
    }

    Y_UNIT_TEST(VectorIndexUpdatePkClusterChange) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x03" WHERE `pk`=9;)"), false);
    }

    Y_UNIT_TEST(VectorIndexUpdateFilterClusterChange) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x03" WHERE `data`="9";)"), false);
    }

    Y_UNIT_TEST(VectorIndexUpsertClusterChange) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `emb`, `data`) VALUES (9, "\x03\x31\x03", "9");)"), false);
    }

    Y_UNIT_TEST(VectorIndexUpdatePkClusterChangeReturning) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x03" WHERE `pk`=9 RETURNING `data`, `emb`, `pk`;)"), true);
    }

    Y_UNIT_TEST(VectorIndexUpdateFilterClusterChangeReturning) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPDATE `/Root/TestTable` SET `emb`="\x03\x31\x03" WHERE `data`="9" RETURNING `data`, `emb`, `pk`;)"), true);
    }

    Y_UNIT_TEST(VectorIndexUpsertClusterChangeReturning) {
        DoTestVectorIndexUpdateClusterChange(Q_(R"(UPSERT INTO `/Root/TestTable` (`pk`, `emb`, `data`) VALUES (9, "\x03\x31\x03", "9") RETURNING `data`, `emb`, `pk`;)"), true);
    }

    Y_UNIT_TEST_TWIN(SimpleVectorIndexOrderByCosineDistanceWithCover, Nullable) {
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
        auto session = DoCreateTableForVectorIndex(db, Nullable);
        {
            const TString createIndex(Q_(R"(
                ALTER TABLE `/Root/TestTable`
                    ADD INDEX index
                    GLOBAL USING vector_kmeans_tree
                    ON (emb) COVER (emb, data)
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
            std::vector<std::string> indexKeyColumns{"emb"};
            UNIT_ASSERT_EQUAL(indexes[0].GetIndexColumns(), indexKeyColumns);
            std::vector<std::string> indexDataColumns{"emb", "data"};
            UNIT_ASSERT_EQUAL(indexes[0].GetDataColumns(), indexDataColumns);
            const auto& settings = std::get<TKMeansTreeSettings>(indexes[0].GetIndexSettings());
            UNIT_ASSERT_EQUAL(settings.Settings.Metric, NYdb::NTable::TVectorIndexSettings::EMetric::CosineDistance);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorType, NYdb::NTable::TVectorIndexSettings::EVectorType::Uint8);
            UNIT_ASSERT_EQUAL(settings.Settings.VectorDimension, 2);
            UNIT_ASSERT_EQUAL(settings.Levels, 2);
            UNIT_ASSERT_EQUAL(settings.Clusters, 2);
        }
        DoPositiveQueriesVectorIndexOrderByCosine(session, TTxSettings::SerializableRW(), true /*covered*/);
    }

    Y_UNIT_TEST_TWIN(CoveredVectorIndexWithFollowers, StaleRO) {
        std::vector<TString> tableNames = {
            "/Root/TestTable/index/indexImplLevelTable",
            //"/Root/TestTable/index/indexImplPostingTable"
        };

        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableVectorIndex(true);
        featureFlags.SetEnableAccessToIndexImplTables(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetEnableForceFollowers(true)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        //kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        //kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.GetTableClient();
        auto session = DoCreateTableForVectorIndex(db, false);
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

        DoPositiveQueriesVectorIndexOrderByCosine(session, StaleRO ? TTxSettings::StaleRO() : TTxSettings::SerializableRW(), true /*covered*/);

        for (const TString& tableName: tableNames) {
            if (StaleRO) {
                // from master - should NOT read
                CheckTableReads(session, tableName, false, false);
                // from followers - should read
                CheckTableReads(session, tableName, true, true);
            } else {
                // https://github.com/ydb-platform/ydb/issues/18680
                // from master - should read
                // CheckTableReads(session, tableName, false, true);
                // from followers - should NOT read
                // CheckTableReads(session, tableName, true, false);
            }
        }
    }

    Y_UNIT_TEST(OrderByReject) {
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

}

}
}
