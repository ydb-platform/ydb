#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/testlib/helpers.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpKnn) {

    TSession CreateTableForVectorSearch(TTableClient& db, bool nullable, const TString& dataCol = "data") {
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto tableBuilder = db.GetTableBuilder();
            if (nullable) {
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

    Y_UNIT_TEST_TWIN(VectorSearchKnnPushdown, Nullable) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, Nullable, "___data"); });

        ui64 expectedLimit = 3;
        auto captureEvents = [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDataShard::TEvRead::EventType) {
                auto& read = ev->Get<TEvDataShard::TEvRead>()->Record;
                UNIT_ASSERT(read.HasVectorTopK());
                auto& topK = read.GetVectorTopK();
                UNIT_ASSERT(topK.GetTargetVector() == "\x67\x71\x02");
                UNIT_ASSERT_VALUES_EQUAL(topK.GetLimit(), expectedLimit);
            }
            return false;
        };
        runtime->SetEventFilter(captureEvents);

        auto runQuery = [&](const TString& query) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto runQueryWithParams = [&](const TString& query, TParams params) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        // Explicit columns
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT pk, emb, ___data FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Implicit columns
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Inner product similarity
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::InnerProductSimilarity(emb, $TargetEmbedding) DESC
            LIMIT 3
        )"));

        // Cosine similarity (DESC)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineSimilarity(emb, $TargetEmbedding) DESC
            LIMIT 3
        )"));

        // Manhattan distance (ASC)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::ManhattanDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Euclidean distance (ASC)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::EuclideanDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Parameters
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), db.GetParamsBuilder()
            .AddParam("$TargetEmbedding")
                .String(TString("\x67\x71\x02", 3))
                .Build()
            .Build());

        // LIMIT 1 (minimum)
        expectedLimit = 1;
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 1
        )"));

        // Larger LIMIT
        expectedLimit = 100;
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 100
        )"));

        // Verify actual results - check that top 3 PKs are correct
        // Target vector is 0x67, 0x71 (103, 113)
        // Cosine distances calculated:
        //   pk=8 (117, 118): 0.000882 - closest
        //   pk=5 (80, 96):   0.000985
        //   pk=9 (118, 118): 0.001070
        expectedLimit = 3;
        {
            TString query(Q_(R"(
                $TargetEmbedding = String::HexDecode("677102");
                SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance FROM `/Root/TestTable`
                ORDER BY distance
                LIMIT 3
            )"));

            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            // Extract PKs and distances from result
            std::vector<std::pair<i64, float>> results;
            auto parser = result.GetResultSetParser(0);
            while (parser.TryNextRow()) {
                i64 pk;
                if constexpr (Nullable) {
                    auto pkOpt = parser.ColumnParser("pk").GetOptionalInt64();
                    UNIT_ASSERT(pkOpt.has_value());
                    pk = *pkOpt;
                } else {
                    pk = parser.ColumnParser("pk").GetInt64();
                }
                auto distanceOpt = parser.ColumnParser("distance").GetOptionalFloat();
                UNIT_ASSERT(distanceOpt.has_value());
                float distance = *distanceOpt;
                results.push_back({pk, distance});
            }

            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, 8);
            UNIT_ASSERT_VALUES_EQUAL(results[1].first, 5);
            UNIT_ASSERT_VALUES_EQUAL(results[2].first, 9);

            // Check exact distance values (with small epsilon for float comparison)
            auto checkDistance = [](float actual, float expected) {
                UNIT_ASSERT_C(std::abs(actual - expected) < 0.0001f,
                    "Expected distance " << expected << ", got " << actual);
            };
            checkDistance(results[0].second, 0.000882f);
            checkDistance(results[1].second, 0.000985f);
            checkDistance(results[2].second, 0.001070f);
        }
    }

}

}
}

