#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/tx/datashard/datashard.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <ydb/library/testlib/helpers.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpKnn) {
    void CheckDistance(float actual, float expected) {
        UNIT_ASSERT_C(std::abs(actual - expected) < 0.0001f,
            "Expected distance " << expected << ", got " << actual);
    }

    template <bool Nullable>
    std::vector<std::pair<i64, float>> ExtractPksAndScores(TResultSetParser parser) {
        std::vector<std::pair<i64, float>> results;
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
        return results;
    }

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
        ui64 testTableLocalId = 0;
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto* evRead = ev->Get();
            auto& read = evRead->Record;
            auto localId = read.GetTableId().GetTableId();

            // Learn testTableLocalId from the first read with VectorTopK
            if (read.HasVectorTopK()) {
                if (testTableLocalId == 0) {
                    testTableLocalId = localId;
                }
                if (localId == testTableLocalId) {
                    auto& topK = read.GetVectorTopK();
                    UNIT_ASSERT(topK.GetTargetVector() == "\x67\x71\x02");
                    UNIT_ASSERT_VALUES_EQUAL(topK.GetLimit(), expectedLimit);
                }
            } else if (testTableLocalId != 0 && localId == testTableLocalId) {
                // VectorTopK pushdown for point lookups is not yet implemented
                if (!evRead->Keys.empty()) {
                    return;
                }
                UNIT_ASSERT_C(false, "Read from TestTable must have VectorTopK");
            }
        });

        auto runQuery = [&](const TString& query) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto runQueryWithResult = [&](const TString& query) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };

        auto runQueryWithParams = [&](const TString& query, TParams params) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        // Literal target vector, normal order (column, target)
        runQuery(Q_(R"(
            SELECT pk FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, "\x67\x71\x02")
            LIMIT 3
        )"));

        // Literal target vector, reversed order (target, column)
        runQuery(Q_(R"(
            SELECT pk FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance("\x67\x71\x02", emb)
            LIMIT 3
        )"));

        // Variable equal to function call, normal order (column, target)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT pk, emb, ___data FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"));

        // Variable equal to function call, reversed order (target, column)
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT pk, emb, ___data FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance($TargetEmbedding, emb)
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

        // LIMIT 1
        expectedLimit = 1;
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 1
        )"));

        // LIMIT 100
        expectedLimit = 100;
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 100
        )"));
        expectedLimit = 3;

        // Test two-stage query from the same table: quantized search followed by full precision reranking
        // The first query uses pushdown of VectorTopK.
        // The second query uses point lookups (WHERE pk IN ...) where pushdown of VectorTopK is not implemented.
        runQuery(Q_(R"(
            $TargetEmbedding = String::HexDecode("677102");

            $Pks = SELECT pk
            FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3;

            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance
            FROM `/Root/TestTable`
            WHERE pk IN $Pks
            ORDER BY distance
            LIMIT 3;
        )"));

        // Verify actual results - check that top 3 PKs are correct
        // Target vector is 0x67, 0x71 (103, 113)
        // Cosine distances calculated:
        //   pk=8 (117, 118): 0.000882 - closest
        //   pk=5 (80, 96):   0.000985
        //   pk=9 (118, 118): 0.001070
        {
            auto result = runQueryWithResult(Q_(R"(
                $TargetEmbedding = String::HexDecode("677102");
                SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance FROM `/Root/TestTable`
                ORDER BY distance
                LIMIT 3
            )"));

            // Extract PKs and distances from result
            auto results = ExtractPksAndScores<Nullable>(result.GetResultSetParser(0));

            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, 8);
            UNIT_ASSERT_VALUES_EQUAL(results[1].first, 5);
            UNIT_ASSERT_VALUES_EQUAL(results[2].first, 9);
            CheckDistance(results[0].second, 0.000882f);
            CheckDistance(results[1].second, 0.000985f);
            CheckDistance(results[2].second, 0.001070f);
        }

        // Test with subquery: target vector from another table
        {
            // Create a second table to store the target vector
            auto schemeResult = kikimr.RunCall([&] {
                return session.ExecuteSchemeQuery(R"(
                    CREATE TABLE `/Root/TargetVectors` (
                        id Int64 NOT NULL,
                        target_emb String,
                        PRIMARY KEY (id)
                    )
                )").ExtractValueSync();
            });
            UNIT_ASSERT_C(schemeResult.IsSuccess(), schemeResult.GetIssues().ToString());

            // Insert the target vector
            runQuery(Q_(R"(
                UPSERT INTO `/Root/TargetVectors` (id, target_emb) VALUES
                (1, "\x67\x71\x02")
            )"));

            // Run query with subquery target, normal argument order (column first)
            runQuery(Q_(R"(
                $TargetEmbedding = (SELECT target_emb FROM `/Root/TargetVectors` WHERE id = 1);
                SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance FROM `/Root/TestTable`
                ORDER BY distance
                LIMIT 3
            )"));

            // Run query with subquery target, reversed argument order (target first)
            runQuery(Q_(R"(
                $TargetEmbedding = (SELECT target_emb FROM `/Root/TargetVectors` WHERE id = 1);
                SELECT pk, Knn::CosineDistance($TargetEmbedding, emb) AS distance FROM `/Root/TestTable`
                ORDER BY distance
                LIMIT 3
            )"));
        }

        // Observer is automatically removed when it goes out of scope
        observer.Remove();
    }

    enum class EVectorType {
        Float,
        Bit,
        Uint8,
        Int8
    };

    void DoVectorKnnPushdownTest(EVectorType vectorType) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] {
            return db.CreateSession().GetValueSync().GetSession();
        });

        // Create partitioned table
        {
            auto tableBuilder = db.GetTableBuilder();
            tableBuilder
                .AddNonNullableColumn("pk", EPrimitiveType::Int64)
                .AddNonNullableColumn("emb", EPrimitiveType::String)
                .SetPrimaryKeyColumns({"pk"});
            tableBuilder.BeginPartitioningSettings()
                .SetMinPartitionsCount(3)
            .EndPartitioningSettings();
            auto partitions = TExplicitPartitions{}
                .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(3).EndTuple().Build())
                .AppendSplitPoints(TValueBuilder{}.BeginTuple().AddElement().OptionalInt64(5).EndTuple().Build());
            tableBuilder.SetPartitionAtKeys(partitions);
            auto result = kikimr.RunCall([&] {
                return session.CreateTable("/Root/TestTable", tableBuilder.Build()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        TString insertQuery;
        TString targetVector;
        TString tagName;
        i64 expectedMatchPk = 0;
        switch (vectorType) {
            case EVectorType::Float:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringFloat([1.0f, 2.0f, 3.0f]), "FloatVector")),
                    (2, Untag(Knn::ToBinaryStringFloat([4.0f, 5.0f, 6.0f]), "FloatVector")),
                    (3, Untag(Knn::ToBinaryStringFloat([7.0f, 8.0f, 9.0f]), "FloatVector")),
                    (4, Untag(Knn::ToBinaryStringFloat([10.0f, 20.0f, 30.0f]), "FloatVector")),
                    (5, Untag(Knn::ToBinaryStringFloat([13.0f, 14.0f, 15.0f]), "FloatVector")),
                    (6, Untag(Knn::ToBinaryStringFloat([100.0f, 110.0f, 120.0f]), "FloatVector"));
                )";
                targetVector = "Knn::ToBinaryStringFloat([100.0f, 110.0f, 120.0f])";
                tagName = "FloatVector";
                expectedMatchPk = 6;
                break;
            case EVectorType::Bit:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 1.f]), "BitVector")),
                    (2, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 1.f, 1.f]), "BitVector")),
                    (3, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 0.f, 1.f, 1.f, 1.f]), "BitVector")),
                    (4, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 0.f, 1.f, 1.f, 1.f, 1.f]), "BitVector")),
                    (5, Untag(Knn::ToBinaryStringBit([0.f, 0.f, 0.f, 1.f, 1.f, 1.f, 1.f, 1.f]), "BitVector")),
                    (6, Untag(Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f]), "BitVector"));
                )";
                targetVector = "Knn::ToBinaryStringBit([1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f, 1.f])";
                tagName = "BitVector";
                expectedMatchPk = 6;
                break;
            case EVectorType::Uint8:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringUint8([10ut, 20ut, 30ut]), "Uint8Vector")),
                    (2, Untag(Knn::ToBinaryStringUint8([40ut, 50ut, 60ut]), "Uint8Vector")),
                    (3, Untag(Knn::ToBinaryStringUint8([70ut, 80ut, 90ut]), "Uint8Vector")),
                    (4, Untag(Knn::ToBinaryStringUint8([100ut, 110ut, 120ut]), "Uint8Vector")),
                    (5, Untag(Knn::ToBinaryStringUint8([130ut, 140ut, 150ut]), "Uint8Vector")),
                    (6, Untag(Knn::ToBinaryStringUint8([200ut, 210ut, 220ut]), "Uint8Vector"));
                )";
                targetVector = "Knn::ToBinaryStringUint8([200ut, 210ut, 220ut])";
                tagName = "Uint8Vector";
                expectedMatchPk = 6;
                break;
            case EVectorType::Int8:
                insertQuery = R"(
                    UPSERT INTO `/Root/TestTable` (pk, emb) VALUES
                    (1, Untag(Knn::ToBinaryStringInt8([10t, 20t, 30t]), "Int8Vector")),
                    (2, Untag(Knn::ToBinaryStringInt8([40t, 50t, 60t]), "Int8Vector")),
                    (3, Untag(Knn::ToBinaryStringInt8([70t, 80t, 90t]), "Int8Vector")),
                    (4, Untag(Knn::ToBinaryStringInt8([-10t, -20t, -30t]), "Int8Vector")),
                    (5, Untag(Knn::ToBinaryStringInt8([-40t, -50t, -60t]), "Int8Vector")),
                    (6, Untag(Knn::ToBinaryStringInt8([100t, 110t, 120t]), "Int8Vector"));
                )";
                targetVector = "Knn::ToBinaryStringInt8([100t, 110t, 120t])";
                tagName = "Int8Vector";
                expectedMatchPk = 6;
                break;
            default:
                UNIT_ASSERT_C(false, "Unexpected vector type");
        }

        {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(Q_(insertQuery),
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Setup observer to verify VectorTopK pushdown
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& read = ev->Get()->Record;
            UNIT_ASSERT(read.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetLimit(), 3u);
        });

        auto runDistanceQuery = [&](const TString& distanceFunc, const TString& orderDir = "") {
            TString query = TStringBuilder()
                << "$TargetEmbedding = Untag(" << targetVector << ", \"" << tagName << "\");\n"
                << "SELECT pk, Knn::" << distanceFunc << "(emb, $TargetEmbedding) AS distance "
                << "FROM `/Root/TestTable` ORDER BY distance " << orderDir << " LIMIT 3";
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(Q_(query),
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return result;
        };

        // CosineDistance (ASC)
        {
            auto result = runDistanceQuery("CosineDistance");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 0.0f);
        }

        // CosineSimilarity (DESC)
        {
            auto result = runDistanceQuery("CosineSimilarity", "DESC");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 1.0f);
        }

        // EuclideanDistance (ASC)
        {
            auto result = runDistanceQuery("EuclideanDistance");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 0.0f);
        }

        // ManhattanDistance (ASC)
        {
            auto result = runDistanceQuery("ManhattanDistance");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
            CheckDistance(results[0].second, 0.0f);
        }

        // InnerProductSimilarity (DESC)
        {
            auto result = runDistanceQuery("InnerProductSimilarity", "DESC");
            auto results = ExtractPksAndScores<false>(result.GetResultSetParser(0));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 3u);
            UNIT_ASSERT_VALUES_EQUAL(results[0].first, expectedMatchPk);
        }

        observer.Remove();
    }

    Y_UNIT_TEST(FloatVectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Float);
    }

    Y_UNIT_TEST(BitVectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Bit);
    }

    Y_UNIT_TEST(Uint8VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Uint8);
    }

    Y_UNIT_TEST(Int8VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Int8);
    }

}

}
}

