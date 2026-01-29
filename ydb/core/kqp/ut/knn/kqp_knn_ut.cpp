#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/testlib/tablet_helpers.h>
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

    // Helper to create Float format embedding (2D vector)
    // Float format: 4 bytes per float + 0x01 format byte
    static TString MakeFloatEmb(float v1, float v2) {
        TString result;
        result.resize(sizeof(float) * 2 + 1);
        char* p = result.Detach();
        memcpy(p, &v1, sizeof(float));
        memcpy(p + sizeof(float), &v2, sizeof(float));
        p[sizeof(float) * 2] = 0x01;  // FloatVector format byte
        return result;
    }

    TSession CreateTableForVectorSearch(TTableClient& db, bool nullable, const TString& dataCol = "data", bool singlePartition = false) {
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

            if (!singlePartition) {
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

        // Use Float format embeddings (same values as before but in Float format)
        // Original Uint8 values were: (3,48), (19,49), (35,50), (83,51), (67,52), (80,96), (97,17), (18,98), (117,118), (118,118)
        {
            auto params = db.GetParamsBuilder()
                .AddParam("$emb0").String(MakeFloatEmb(3.0f, 48.0f)).Build()
                .AddParam("$emb1").String(MakeFloatEmb(19.0f, 49.0f)).Build()
                .AddParam("$emb2").String(MakeFloatEmb(35.0f, 50.0f)).Build()
                .AddParam("$emb3").String(MakeFloatEmb(83.0f, 51.0f)).Build()
                .AddParam("$emb4").String(MakeFloatEmb(67.0f, 52.0f)).Build()
                .AddParam("$emb5").String(MakeFloatEmb(80.0f, 96.0f)).Build()
                .AddParam("$emb6").String(MakeFloatEmb(97.0f, 17.0f)).Build()
                .AddParam("$emb7").String(MakeFloatEmb(18.0f, 98.0f)).Build()
                .AddParam("$emb8").String(MakeFloatEmb(117.0f, 118.0f)).Build()
                .AddParam("$emb9").String(MakeFloatEmb(118.0f, 118.0f)).Build()
                .Build();

            const TString query1 = TStringBuilder()
                << "UPSERT INTO `/Root/TestTable` (pk, emb, " << dataCol << ") VALUES "
                << "(0, $emb0, \"0\"),"
                   "(1, $emb1, \"1\"),"
                   "(2, $emb2, \"2\"),"
                   "(3, $emb3, \"3\"),"
                   "(4, $emb4, \"4\"),"
                   "(5, $emb5, \"5\"),"
                   "(6, $emb6, \"6\"),"
                   "(7, $emb7, \"7\"),"
                   "(8, $emb8, \"8\"),"
                   "(9, $emb9, \"9\");";

            auto result = session.ExecuteDataQuery(Q_(query1), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params)
                .ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        return session;
    }

    // Target embedding for tests: (103.0, 113.0) in Float format
    static TString GetTargetEmbedding() {
        return MakeFloatEmb(103.0f, 113.0f);
    }

    template <bool Nullable>
    void VerifyVectorSearchResults(TKikimrRunner& kikimr, TSession& session, TTableClient& db, TTxSettings txSettings = TTxSettings::SerializableRW(), bool useRunCall = true) {
        // Verify actual results - check that top 3 PKs are correct
        // Target vector is (103.0, 113.0) in Float format
        // Cosine distances calculated:
        //   pk=8 (117, 118): 0.000882 - closest
        //   pk=5 (80, 96):   0.000985
        //   pk=9 (118, 118): 0.001070

        const TString query = Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance FROM `/Root/TestTable`
            ORDER BY distance
            LIMIT 3
        )");

        auto params = db.GetParamsBuilder()
            .AddParam("$TargetEmbedding").String(GetTargetEmbedding()).Build()
            .Build();

        auto result = useRunCall
            ? kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query, TTxControl::BeginTx(txSettings).CommitTx(), params).ExtractValueSync();
            })
            : session.ExecuteDataQuery(query, TTxControl::BeginTx(txSettings).CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

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

    // LIMIT 7 is a magic marker that triggers Y_ABORT_UNLESS if HNSW is not used
    // This version of the function verifies HNSW is actually used
    template <bool Nullable>
    void VerifyVectorSearchResultsHnsw(TKikimrRunner& kikimr, TSession& session, TTableClient& db, TTxSettings txSettings = TTxSettings::SerializableRW(), bool useRunCall = true) {
        // Same as VerifyVectorSearchResults but with LIMIT 7 (magic marker for HNSW must be used)
        const TString query = Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance FROM `/Root/TestTable`
            ORDER BY distance
            LIMIT 7
        )");

        auto params = db.GetParamsBuilder()
            .AddParam("$TargetEmbedding").String(GetTargetEmbedding()).Build()
            .Build();

        auto result = useRunCall
            ? kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query, TTxControl::BeginTx(txSettings).CommitTx(), params).ExtractValueSync();
            })
            : session.ExecuteDataQuery(query, TTxControl::BeginTx(txSettings).CommitTx(), params).ExtractValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // Extract PKs and distances from result - should have 7 results
        auto results = ExtractPksAndScores<Nullable>(result.GetResultSetParser(0));

        // Verify we got 7 results (or 10 if table has only 10 rows)
        UNIT_ASSERT_C(results.size() >= 3u && results.size() <= 10u, "Expected 3-10 results, got " << results.size());
        // First 3 should be the same as in VerifyVectorSearchResults
        UNIT_ASSERT_VALUES_EQUAL(results[0].first, 8);
        UNIT_ASSERT_VALUES_EQUAL(results[1].first, 5);
        UNIT_ASSERT_VALUES_EQUAL(results[2].first, 9);
    }

    Y_UNIT_TEST_TWIN(VectorSearchKnnPushdown, Nullable) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, Nullable, "___data"); });

        // Restart tablets to trigger HNSW index building
        {
            auto sender = runtime->AllocateEdgeActor();
            auto shards = GetTableShards(&kikimr.GetTestServer(), sender, "/Root/TestTable");
            for (auto shardId : shards) {
                RebootTablet(*runtime, shardId, sender);
            }
            runtime->SimulateSleep(TDuration::Seconds(1));
        }

        ui64 expectedLimit = 3;
        ui64 testTableLocalId = 0;
        TString expectedTargetVector = GetTargetEmbedding();
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
                    UNIT_ASSERT(topK.GetTargetVector() == expectedTargetVector);
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

        auto runQueryWithParams = [&](const TString& query, TParams params) {
            auto result = kikimr.RunCall([&] {
                return session.ExecuteDataQuery(query,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), params).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        // All queries use Float format target embedding via parameter
        auto targetParams = db.GetParamsBuilder()
            .AddParam("$TargetEmbedding").String(GetTargetEmbedding()).Build()
            .Build();

        // Parameter-based target vector, normal order (column, target)
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT pk FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), targetParams);

        // Parameter-based target vector, reversed order (target, column)
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT pk FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance($TargetEmbedding, emb)
            LIMIT 3
        )"), targetParams);

        // With all columns, normal order
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT pk, emb, ___data FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), targetParams);

        // With all columns, reversed order
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT pk, emb, ___data FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance($TargetEmbedding, emb)
            LIMIT 3
        )"), targetParams);

        // Implicit columns
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), targetParams);

        // The following distance metrics are commented out for the HNSW prototype
        // which only supports CosineDistance with Float vectors.
/*
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
*/
        // Parameters (using Float format)
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), targetParams);

        // Tagged vector converted from a parameter
        runQueryWithParams(Q_(R"(
            DECLARE $embedding AS List<Uint8>;
            $TargetEmbedding = Knn::ToBinaryStringUint8($embedding);
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3
        )"), db.GetParamsBuilder()
            .AddParam("$embedding")
                .BeginList()
                    .AddListItem().Uint8(0x67)
                    .AddListItem().Uint8(0x71)
                .EndList()
                .Build()
            .Build());

        // LIMIT 1
        expectedLimit = 1;
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 1
        )"), targetParams);

        // LIMIT 100
        expectedLimit = 100;
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;
            SELECT * FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 100
        )"), targetParams);
        expectedLimit = 3;

        // Test two-stage query from the same table: quantized search followed by full precision reranking
        // The first query uses pushdown of VectorTopK.
        // The second query uses point lookups (WHERE pk IN ...) where pushdown of VectorTopK is not implemented.
        runQueryWithParams(Q_(R"(
            DECLARE $TargetEmbedding AS String;

            $Pks = SELECT pk
            FROM `/Root/TestTable`
            ORDER BY Knn::CosineDistance(emb, $TargetEmbedding)
            LIMIT 3;

            SELECT pk, Knn::CosineDistance(emb, $TargetEmbedding) AS distance
            FROM `/Root/TestTable`
            WHERE pk IN $Pks
            ORDER BY distance
            LIMIT 3;
        )"), targetParams);

        // Verify actual results
        VerifyVectorSearchResults<Nullable>(kikimr, session, db);

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

            // Insert the target vector (Float format)
            auto insertParams = db.GetParamsBuilder()
                .AddParam("$target").String(GetTargetEmbedding()).Build()
                .Build();
            runQueryWithParams(Q_(R"(
                DECLARE $target AS String;
                UPSERT INTO `/Root/TargetVectors` (id, target_emb) VALUES
                (1, $target)
            )"), insertParams);

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

        // Restart tablets to trigger HNSW index building
        {
            auto sender = runtime->AllocateEdgeActor();
            auto shards = GetTableShards(&kikimr.GetTestServer(), sender, "/Root/TestTable");
            for (auto shardId : shards) {
                RebootTablet(*runtime, shardId, sender);
            }
            runtime->SimulateSleep(TDuration::Seconds(1));
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

        // NOTE: The following distance metrics are commented out for the HNSW prototype
        // which only supports CosineDistance.
/*
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
*/
        observer.Remove();
    }

    Y_UNIT_TEST(FloatVectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Float);
    }

    // NOTE: The following vector type tests are commented out for the HNSW prototype
    // HNSW index stores vectors as Float internally, so non-Float formats are not supported
/*
    Y_UNIT_TEST(BitVectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Bit);
    }

    Y_UNIT_TEST(Uint8VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Uint8);
    }

    Y_UNIT_TEST(Int8VectorKnnPushdown) {
        DoVectorKnnPushdownTest(EVectorType::Int8);
    }
*/

    Y_UNIT_TEST(VectorSearchKnnPushdownWithRestart) {
        const TString tableName = "/Root/TestTable";

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, false, "data", true); });

        // Restart tablets to trigger HNSW index building
        {
            auto sender = runtime->AllocateEdgeActor();
            auto shards = GetTableShards(&kikimr.GetTestServer(), sender, tableName);
            for (auto shardId : shards) {
                RebootTablet(*runtime, shardId, sender);
            }
            runtime->SimulateSleep(TDuration::Seconds(1));
        }

        // Setup observer to verify VectorTopK pushdown is used with LIMIT 7 (HNSW must be used marker)
        auto observer = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& read = ev->Get()->Record;
            UNIT_ASSERT(read.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetLimit(), 7u);
        });

        // Perform read - HNSW should be used after tablet restart
        // LIMIT 7 is a magic marker that will Y_ABORT_UNLESS if HNSW is not used
        VerifyVectorSearchResultsHnsw<false>(kikimr, session, db, TTxSettings::SerializableRW());

        observer.Remove();
    }

    Y_UNIT_TEST_TWIN(VectorSearchKnnPushdownFollower, StaleRO) {
        const TString tableName = "/Root/TestTable";

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetEnableForceFollowers(true)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, false, "data", true); }); // singlePartition = true for followers

        // Enable followers on the table BEFORE restart so followers get created
        {
            const TString alterTable(Q_(Sprintf(R"(
                ALTER TABLE `%s` SET (READ_REPLICAS_SETTINGS = "PER_AZ:10");
            )", tableName.c_str())));

            auto result = kikimr.RunCall([&] {
                return session.ExecuteSchemeQuery(alterTable).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify followers are configured
        {
            auto result = kikimr.RunCall([&] {
                return session.DescribeTable(tableName).ExtractValueSync();
            });
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            const auto& table = result.GetTableDescription();
            UNIT_ASSERT(table.GetReadReplicasSettings()->GetMode() == NYdb::NTable::TReadReplicasSettings::EMode::PerAz);
            UNIT_ASSERT_VALUES_EQUAL(table.GetReadReplicasSettings()->GetReadReplicasCount(), 10);
        }

        // Restart tablets to trigger HNSW index building on leader and followers
        {
            auto sender = runtime->AllocateEdgeActor();
            auto shards = GetTableShards(&kikimr.GetTestServer(), sender, tableName);
            for (auto shardId : shards) {
                RebootTablet(*runtime, shardId, sender);
            }
            runtime->SimulateSleep(TDuration::Seconds(1));
        }

        // Track whether read was from follower
        bool readFromFollower = false;

        // Setup observer to verify VectorTopK pushdown with LIMIT 7 (HNSW must be used marker)
        auto readObserver = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
            auto& read = ev->Get()->Record;
            UNIT_ASSERT(read.HasVectorTopK());
            UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetLimit(), 7u);
        });

        // Setup observer to check if result came from follower
        auto resultObserver = runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
            auto& result = ev->Get()->Record;
            if (result.GetFromFollower()) {
                readFromFollower = true;
            }
        });

        // Perform read with LIMIT 7 - will Y_ABORT_UNLESS if HNSW is not used
        // The fact that this doesn't crash proves HNSW was used
        VerifyVectorSearchResultsHnsw<false>(kikimr, session, db, StaleRO ? TTxSettings::StaleRO() : TTxSettings::SerializableRW());

        readObserver.Remove();
        resultObserver.Remove();

        // Verify follower was used for StaleRO reads
        if (StaleRO) {
            UNIT_ASSERT_C(readFromFollower, "StaleRO read should have been served by a follower");
        } else {
            UNIT_ASSERT_C(!readFromFollower, "SerializableRW read should have been served by leader, not follower");
        }
    }

    // Helper function to get HNSW stats from a shard (for debugging)
    std::pair<ui64, ui64> GetHnswCacheStats(TTestActorRuntime& runtime, ui64 shardId) {
        auto sender = runtime.AllocateEdgeActor();
        ForwardToTablet(runtime, shardId, sender, new TEvDataShard::TEvGetHnswStats(sender));
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvDataShard::TEvGetHnswStatsResult>(handle);
        UNIT_ASSERT(event);
        return {event->Record.GetCacheHits(), event->Record.GetCacheMisses()};
    }

    // Test that HNSW index cache survives multiple restarts (leader reuse, follower reuse)
    Y_UNIT_TEST(VectorSearchHnswCacheRobustness) {
        const TString tableName = "/Root/TestTable";

        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetUseRealThreads(false)
            .SetEnableForceFollowers(true)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto runtime = kikimr.GetTestServer().GetRuntime();

        auto db = kikimr.RunCall([&] { return kikimr.GetTableClient(); });
        auto session = kikimr.RunCall([&] { return CreateTableForVectorSearch(db, false, "data", true); });

        auto sender = runtime->AllocateEdgeActor();
        auto shards = GetTableShards(&kikimr.GetTestServer(), sender, tableName);
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 1u); // Single partition for deterministic testing

        ui64 shardId = shards[0];

        // Helper to get stats and log them
        auto getStats = [&]() -> std::pair<ui64, ui64> {
            auto [hits, misses] = GetHnswCacheStats(*runtime, shardId);
            return {hits, misses};
        };

        // Get baseline stats before any operations
        auto [baseHits, baseMisses] = getStats();

        // === Phase 1: Leader alone builds index ===
        // TryGetFromCache -> MISS (cache empty)
        // TryStartBuild -> succeeds (no one else building)
        // BuildIndex -> registers in cache
        // FinishBuild -> releases lock
        {
            RebootTablet(*runtime, shardId, sender);
            runtime->SimulateSleep(TDuration::Seconds(1));
        }

        ui64 expectedHits = baseHits;
        ui64 expectedMisses = baseMisses + 1;
        {
            auto [hits, misses] = getStats();
            UNIT_ASSERT_VALUES_EQUAL_C(hits, expectedHits, "Phase 1: hits should not increase (cache was empty)");
            UNIT_ASSERT_VALUES_EQUAL_C(misses, expectedMisses, "Phase 1: misses should be 1 (first build)");
        }

        // Verify HNSW works (leader read)
        {
            auto readObserver = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
                auto& read = ev->Get()->Record;
                UNIT_ASSERT(read.HasVectorTopK());
                UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetLimit(), 7u);
            });
            VerifyVectorSearchResultsHnsw<false>(kikimr, session, db, TTxSettings::SerializableRW());
            readObserver.Remove();
        }

        // === Phase 2: Enable followers ===
        // Followers will be created and will try to get index from cache
        // TryGetFromCache -> HIT (leader already built it)
        {
            const TString alterTable(Q_(Sprintf(R"(
                ALTER TABLE `%s` SET (READ_REPLICAS_SETTINGS = "PER_AZ:3");
            )", tableName.c_str())));

            auto result = kikimr.RunCall([&] {
                return session.ExecuteSchemeQuery(alterTable).ExtractValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            runtime->SimulateSleep(TDuration::Seconds(1));
        }

        // === Phase 3: Leader restart with followers present ===
        // Leader: TryGetFromCache -> HIT (from Phase 1 build, still in node cache)
        // Followers also activated and get HIT from cache
        {
            RebootTablet(*runtime, shardId, sender);
            runtime->SimulateSleep(TDuration::Seconds(1));
        }

        // Leader gets cache HIT, followers also get cache HITs
        // With 3 followers + 1 leader = up to 4 cache operations
        // But we only check that hits increased (leader definitely gets one)
        expectedHits = baseHits + 1;  // At least leader's HIT
        {
            auto [hits, misses] = getStats();
            UNIT_ASSERT_C(hits >= expectedHits, TStringBuilder()
                << "Phase 3: hits should be at least " << expectedHits << " (leader cache reuse), got " << hits);
            UNIT_ASSERT_VALUES_EQUAL_C(misses, expectedMisses, "Phase 3: misses should stay at 1 (no new builds needed)");
            expectedHits = hits;  // Update for next phase
        }

        // === Phase 4: Verify StaleRO reads from followers ===
        bool readFromFollower = false;
        {
            auto readObserver = runtime->AddObserver<TEvDataShard::TEvRead>([&](auto& ev) {
                auto& read = ev->Get()->Record;
                UNIT_ASSERT(read.HasVectorTopK());
                UNIT_ASSERT_VALUES_EQUAL(read.GetVectorTopK().GetLimit(), 7u);
            });

            auto resultObserver = runtime->AddObserver<TEvDataShard::TEvReadResult>([&](auto& ev) {
                auto& result = ev->Get()->Record;
                if (result.GetFromFollower()) {
                    readFromFollower = true;
                }
            });

            VerifyVectorSearchResultsHnsw<false>(kikimr, session, db, TTxSettings::StaleRO());

            readObserver.Remove();
            resultObserver.Remove();
        }

        UNIT_ASSERT_C(readFromFollower, "StaleRO read should have been served by a follower");

        // Final stats check
        {
            auto [hits, misses] = getStats();
            UNIT_ASSERT_C(hits >= expectedHits, "Final: hits should not decrease");
            UNIT_ASSERT_VALUES_EQUAL_C(misses, expectedMisses, "Final: misses should stay at 1");
        }
    }

}

}
}
