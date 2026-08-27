#include <fmt/format.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

/*
 * With EnableCsWriteAffinity=true, the pure-expr REPLACE INTO is split into:
 *   Sink Stage (olap, N tasks, one per shard)
 *     HashShuffle (ColumnShardHashV1) ← routes rows to correct shard tasks
 *       Transform Stage (compute, 1 task, generates rows)
 *
 * With EnableCsWriteAffinity=false, the sink is inlined into the transform stage
 * (no separate Sink stage, no HashShuffle — connection is Map).
 *
 * Expected stage counts:
 *   - REPLACE/INSERT/UPDATE/DELETE: 3 stages with affinity, 2 without
 *   - CTAS: 4 stages with affinity, 3 without (extra stage for table creation)
 */
static void VerifyPlanWithAffinity(const NJson::TJsonValue& plan, TString planStr, bool enableCsWriteAffinity, ui32 expectedStagesWithAffinity = 3, ui32 expectedStagesWithoutAffinity = 2) {
    Cerr << "QQQ_:" << NJson::WriteJson(&plan, false) << Endl;
    const ui32 expectedStages = enableCsWriteAffinity ? expectedStagesWithAffinity : expectedStagesWithoutAffinity;
    const auto stages = FindPlanStages(plan);
    UNIT_ASSERT_VALUES_EQUAL_C(stages.size(), expectedStages,
        "Expected " << expectedStages << " stages (EnableCsWriteAffinity="
        << enableCsWriteAffinity << "), got " << stages.size()
        << ". Plan: " << planStr);

    if (enableCsWriteAffinity) {
        // 1. A HashShuffle connection with ColumnShardHashV1 exists (Transform→Sink link).
        const auto hashShuffleNode = FindPlanNodeByKv(plan, "Node Type", "HashShuffle");
        UNIT_ASSERT_C(hashShuffleNode.IsDefined(),
            "Expected a 'HashShuffle' connection in plan with EnableCsWriteAffinity=true. "
            "Plan: " << planStr);

        // 2. The HashShuffle node must have HashFunc=ColumnShardHashV1.
        const auto& hashShuffleMap = hashShuffleNode.GetMapSafe();
        const auto hashFuncIt = hashShuffleMap.find("HashFunc");
        UNIT_ASSERT_C(hashFuncIt != hashShuffleMap.end()
                && hashFuncIt->second.GetStringSafe() == "ColumnShardHashV1",
            "Expected 'HashShuffle' node to have HashFunc=ColumnShardHashV1. "
            "Plan: " << planStr);

        // 3. The HashShuffle node must have PlanNodeType=Connection.
        const auto planNodeTypeIt = hashShuffleMap.find("PlanNodeType");
        UNIT_ASSERT_C(planNodeTypeIt != hashShuffleMap.end()
                && planNodeTypeIt->second.GetStringSafe() == "Connection",
            "Expected 'HashShuffle' node to have PlanNodeType=Connection. "
            "Plan: " << planStr);

        // 4. A Sink stage node exists.
        const auto sinkNode = FindPlanNodeByKv(plan, "Node Type", "Sink");
        UNIT_ASSERT_C(sinkNode.IsDefined(),
            "Expected a 'Sink' stage in plan with EnableCsWriteAffinity=true. "
            "Plan: " << planStr);

        // 5. Exactly 1 HashShuffle connection.
        const ui32 hashShuffleCount = CountPlanNodesByKv(plan, "Node Type", "HashShuffle");
        UNIT_ASSERT_VALUES_EQUAL_C(hashShuffleCount, 1,
            "Expected exactly 1 HashShuffle connection. "
            "Plan: " << planStr);

        // 6. No Broadcast connection should exist.
        const auto broadcastNode = FindPlanNodeByKv(plan, "Node Type", "Broadcast");
        UNIT_ASSERT_C(!broadcastNode.IsDefined(),
            "Expected NO 'Broadcast' connection in plan with EnableCsWriteAffinity=true"
            " (should be HashShuffle with ColumnShardHashV1). Plan: " << planStr);

        // 7. Inner compute stage exists (Node Type = "Stage").
        const auto innerStageNode = FindPlanNodeByKv(plan, "Node Type", "Stage");
        UNIT_ASSERT_C(innerStageNode.IsDefined(),
            "Expected an inner 'Stage' (compute) in plan with EnableCsWriteAffinity=true. "
            "Plan: " << planStr);
    } else {
        // Without affinity, the sink is inlined — no separate Sink stage, no HashShuffle.
        const auto hashShuffleNode = FindPlanNodeByKv(plan, "Node Type", "HashShuffle");
        UNIT_ASSERT_C(!hashShuffleNode.IsDefined(),
            "Expected NO 'HashShuffle' with EnableCsWriteAffinity=false. "
            "Plan: " << planStr);
    }
}

#ifdef KQP_WRITE_TABLE_TARGET_SHARD_IDS_CHECK
const bool CHECK_MODE_ON = true;
#else
const bool CHECK_MODE_ON = false;
#endif

#define SKIP_EXPECTED_FAILURE() if (CHECK_MODE_ON && !EnableCsWriteAffinity) { return; }


Y_UNIT_TEST_SUITE(CS_WriteAffinity) {

    Y_UNIT_TEST_TWIN(Replace, EnableCsWriteAffinity) {
        SKIP_EXPECTED_FAILURE()
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString pragmaPrefix = EnableCsWriteAffinity
            ? "PRAGMA ydb.EnableCsWriteAffinity = \"true\";\n"
            : "PRAGMA ydb.EnableCsWriteAffinity = \"false\";\n";

        const int insertedRowsCount = 80;

        const TString query = pragmaPrefix +
            "$rowCount = " + ToString(insertedRowsCount) + ";" + R"(
            $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
            RETURN AsStruct($x AS Col1, $x AS Col2); });
            REPLACE INTO `/Root/Source`
            SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
            FROM AS_TABLE($data);
        )";

        {
            auto result = client.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            UNIT_ASSERT_C(result.GetStats().has_value(), "Expected query stats to be present");
            const auto planStr = result.GetStats()->GetPlan();
            UNIT_ASSERT_C(planStr.has_value(), "Expected query plan to be present");

            NJson::TJsonValue plan;
            UNIT_ASSERT_C(NJson::ReadJsonTree(TString(*planStr), &plan, true),
                "Failed to parse query plan: " << *planStr);

            VerifyPlanWithAffinity(plan, TString(*planStr), EnableCsWriteAffinity);
        }

        {
            auto result = client.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify data was written correctly
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Source` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);

            // Build expected YSON dynamically from all inserted rows (0..79).
            TString expected = "[";
            for (int i = 0; i < insertedRowsCount; ++i) {
                if (i > 0) {
                    expected += ";";
                }
                expected += TStringBuilder() << "[" << i << "u;[" << i << "]]";
            }
            expected += "]";
            CompareYson(output, expected);
        }
    }

    Y_UNIT_TEST_TWIN(Insert, EnableCsWriteAffinity) {
        SKIP_EXPECTED_FAILURE()
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString pragmaPrefix = EnableCsWriteAffinity
            ? "PRAGMA ydb.EnableCsWriteAffinity = \"true\";\n"
            : "PRAGMA ydb.EnableCsWriteAffinity = \"false\";\n";

        const int insertedRowsCount = 80;

        const TString query = pragmaPrefix +
            "$rowCount = " + ToString(insertedRowsCount) + ";" + R"(
            $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
            RETURN AsStruct($x AS Col1, $x AS Col2); });
            INSERT INTO `/Root/Source`
            SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
            FROM AS_TABLE($data);
        )";

        // Verify plan
        {
            auto result = client.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            UNIT_ASSERT_C(result.GetStats().has_value(), "Expected query stats to be present");
            const auto planStr = result.GetStats()->GetPlan();
            UNIT_ASSERT_C(planStr.has_value(), "Expected query plan to be present");

            NJson::TJsonValue plan;
            UNIT_ASSERT_C(NJson::ReadJsonTree(TString(*planStr), &plan, true),
                "Failed to parse query plan: " << *planStr);

            VerifyPlanWithAffinity(plan, TString(*planStr), EnableCsWriteAffinity);
        }

        {
            auto result = client.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify data was written correctly
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Source` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);

            TString expected = "[";
            for (int i = 0; i < insertedRowsCount; ++i) {
                if (i > 0) {
                    expected += ";";
                }
                expected += TStringBuilder() << "[" << i << "u;[" << i << "]]";
            }
            expected += "]";
            CompareYson(output, expected);
        }
    }

    Y_UNIT_TEST_TWIN(Update, EnableCsWriteAffinity) {
        SKIP_EXPECTED_FAILURE()
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Insert initial data
        {
            auto result = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source` (Col1, Col2)
                VALUES (1u, 10), (2u, 20), (3u, 30);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString pragmaPrefix = EnableCsWriteAffinity
            ? "PRAGMA ydb.EnableCsWriteAffinity = \"true\";\n"
            : "PRAGMA ydb.EnableCsWriteAffinity = \"false\";\n";

        // UPDATE: set Col2 = CAST(Col1 * 2 AS Int32) for all rows
        const TString query = pragmaPrefix +
            "UPDATE `/Root/Source` SET Col2 = CAST(Col1 * 2 AS Int32);";

        // Verify plan
        {
            auto result = client.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            UNIT_ASSERT_C(result.GetStats().has_value(), "Expected query stats to be present");
            const auto planStr = result.GetStats()->GetPlan();
            UNIT_ASSERT_C(planStr.has_value(), "Expected query plan to be present");

            NJson::TJsonValue plan;
            UNIT_ASSERT_C(NJson::ReadJsonTree(TString(*planStr), &plan, true),
                "Failed to parse query plan: " << *planStr);

            // UPDATE has 4 stages with affinity (Scan→Map→Stage→HashShuffle→Stage→Sink) vs 3 without.
            VerifyPlanWithAffinity(plan, TString(*planStr), EnableCsWriteAffinity, /* expectedStagesWithAffinity= */ 4, /* expectedStagesWithoutAffinity= */ 3);
        }

        {
            auto result = client.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify data was updated correctly
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Source` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            // Expected: (1, 2), (2, 4), (3, 6)
            CompareYson(output, "[[1u;[2]];[2u;[4]];[3u;[6]]]");
        }
    }

    Y_UNIT_TEST_TWIN(Delete, EnableCsWriteAffinity) {
        SKIP_EXPECTED_FAILURE()
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Insert initial data
        {
            auto result = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source` (Col1, Col2)
                VALUES (1u, 10), (2u, 20), (3u, 30);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString pragmaPrefix = EnableCsWriteAffinity
            ? "PRAGMA ydb.EnableCsWriteAffinity = \"true\";\n"
            : "PRAGMA ydb.EnableCsWriteAffinity = \"false\";\n";

        // DELETE: remove rows where Col1 > 1
        const TString query = pragmaPrefix +
            "DELETE FROM `/Root/Source` WHERE Col1 > 1u;";

        // Verify plan
        {
            auto result = client.ExecuteQuery(
                query,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            UNIT_ASSERT_C(result.GetStats().has_value(), "Expected query stats to be present");
            const auto planStr = result.GetStats()->GetPlan();
            UNIT_ASSERT_C(planStr.has_value(), "Expected query plan to be present");

            NJson::TJsonValue plan;
            UNIT_ASSERT_C(NJson::ReadJsonTree(TString(*planStr), &plan, true),
                "Failed to parse query plan: " << *planStr);

            // DELETE has 4 stages with affinity (Scan→Map→Stage→HashShuffle→Stage→Sink) vs 3 without.
            VerifyPlanWithAffinity(plan, TString(*planStr), EnableCsWriteAffinity, /* expectedStagesWithAffinity= */ 4, /* expectedStagesWithoutAffinity= */ 3);
        }

        {
            auto result = client.ExecuteQuery(query,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify only row with Col1=1 remains
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Source` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, "[[1u;[10]]]");
        }
    }

    Y_UNIT_TEST_TWIN(Ctas, EnableCsWriteAffinity) {
        SKIP_EXPECTED_FAILURE()
        // Verify CTAS produces identical results with EnableCsWriteAffinity=true/false
        // and checks that the query plan has different number of stages:
        // - Without pragma: single stage (transform + sink together)
        // - With pragma: two stages (transform stage + separate sink stage)
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        const TString pragmaPrefix = EnableCsWriteAffinity
            ? "PRAGMA ydb.EnableCsWriteAffinity = \"true\";\n"
            : "PRAGMA ydb.EnableCsWriteAffinity = \"false\";\n";

        const int insertedRowsCount = 80;

        {
            const TString insertQuery = pragmaPrefix +
                "$rowCount = " + ToString(insertedRowsCount) + ";" + R"(
                $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                RETURN AsStruct($x AS Col1, $x AS Col2); });
                REPLACE INTO `/Root/Source`
                SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
                FROM AS_TABLE($data);
            )";
            auto result = client.ExecuteQuery(insertQuery
                , NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString ctasQuery = pragmaPrefix +
            R"(
                CREATE TABLE `/Root/Destination` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/Source`;
            )";

        // Explain the CTAS query and inspect the physical plan.
        {
            auto result = client.ExecuteQuery(
                ctasQuery,
                NYdb::NQuery::TTxControl::NoTx(),
                NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
            ).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            UNIT_ASSERT_C(result.GetStats().has_value(), "Expected query stats to be present");
            const auto planStr = result.GetStats()->GetPlan();
            UNIT_ASSERT_C(planStr.has_value(), "Expected query plan to be present");

            NJson::TJsonValue plan;
            UNIT_ASSERT_C(NJson::ReadJsonTree(TString(*planStr), &plan, true),
                "Failed to parse query plan: " << *planStr);

            // CTAS has 4 stages with affinity (extra stage for table creation) vs 3 without.
            VerifyPlanWithAffinity(plan, TString(*planStr), EnableCsWriteAffinity, /* expectedStagesWithAffinity= */ 4, /* expectedStagesWithoutAffinity= */ 3);
        }

        // Execute CTAS query
        {
            auto result = client.ExecuteQuery(ctasQuery, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify data was written correctly
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);

            TString expected = "[";
            for (int i = 0; i < insertedRowsCount; ++i) {
                if (i > 0) {
                    expected += ";";
                }
                expected += TStringBuilder() << "[" << i << "u;[" << i << "]]";
            }
            expected += "]";
            CompareYson(output, expected);
        }
    }

} // Y_UNIT_TEST_SUITE(CS_WriteAffinity)

} // namespace NKqp
} // namespace NKikimr
