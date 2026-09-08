#include <fmt/format.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

/*
 * With EnableCsWriteAffinity=true, the CTAS (CREATE TABLE AS SELECT) is split into:
 *   Sink Stage (olap, N tasks, one per shard)
 *     HashShuffle (ColumnShardHashV1) ← routes rows to correct shard tasks
 *       Transform Stage (compute, 1 task, generates rows)
 *
 * With EnableCsWriteAffinity=false, the sink is inlined into the transform stage
 * (no separate Sink stage, no HashShuffle — connection is Map).
 *
 * Expected stage counts for CTAS:
 *   - 4 stages with affinity (extra stage for table creation)
 *   - 3 stages without affinity
 */
static void VerifyCtasPlanWithAffinity(const NJson::TJsonValue& plan, TString planStr, bool enableCsWriteAffinity, ui32 expectedStagesWithAffinity = 4, ui32 expectedStagesWithoutAffinity = 3) {
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

/*
 * Helper to verify KeyColumns in HashShuffle node match expected sharding columns.
 * When enableCsWriteAffinity is false, no HashShuffle should exist.
 */
static void VerifyHashShuffleKeyColumns(const NJson::TJsonValue& plan, const TString& planStr,
        bool enableCsWriteAffinity, const TVector<TString>& expectedKeyColumns) {
    if (!enableCsWriteAffinity) {
        // Without affinity, no HashShuffle should exist
        const auto hashShuffleNode = FindPlanNodeByKv(plan, "Node Type", "HashShuffle");
        UNIT_ASSERT_C(!hashShuffleNode.IsDefined(),
            "Expected NO 'HashShuffle' with EnableCsWriteAffinity=false. Plan: " << planStr);
        return;
    }

    // With affinity, verify HashShuffle has correct KeyColumns
    const auto hashShuffleNode = FindPlanNodeByKv(plan, "Node Type", "HashShuffle");
    UNIT_ASSERT_C(hashShuffleNode.IsDefined(),
        "Expected a 'HashShuffle' connection in plan");

    const auto& hashShuffleMap = hashShuffleNode.GetMapSafe();
    const auto keyColumnsIt = hashShuffleMap.find("KeyColumns");
    UNIT_ASSERT_C(keyColumnsIt != hashShuffleMap.end(),
        "Expected 'KeyColumns' in HashShuffle node. Plan: " << planStr);

    const auto& keyColumns = keyColumnsIt->second.GetArraySafe();
    UNIT_ASSERT_VALUES_EQUAL_C(keyColumns.size(), expectedKeyColumns.size(),
        "Expected " << expectedKeyColumns.size() << " KeyColumns, got " << keyColumns.size());

    for (size_t i = 0; i < expectedKeyColumns.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(keyColumns[i].GetStringSafe(), expectedKeyColumns[i],
            "Expected KeyColumn[" << i << "] to be '" << expectedKeyColumns[i]
            << "', got '" << keyColumns[i].GetStringSafe() << "'");
    }
}

/*
 * Helper to explain a query and return the parsed plan JSON.
 */
/*
 * Execute CTAS query: first EXPLAIN to get plan, then Execute to run the query.
 * This ensures the verified plan corresponds to the same query that produces data.
 * Returns parsed plan JSON for verification.
 */
static NJson::TJsonValue ExplainAndExecuteQuery(NYdb::NQuery::TQueryClient& client, const TString& query) {
    // First, get the execution plan
    auto explainResult = client.ExecuteQuery(
        query,
        NYdb::NQuery::TTxControl::NoTx(),
        NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain)
    ).ExtractValueSync();
    UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());

    UNIT_ASSERT_C(explainResult.GetStats().has_value(), "Expected query stats to be present");
    const auto planStr = explainResult.GetStats()->GetPlan();
    UNIT_ASSERT_C(planStr.has_value(), "Expected query plan to be present");

    NJson::TJsonValue plan;
    UNIT_ASSERT_C(NJson::ReadJsonTree(TString(*planStr), &plan, true),
        "Failed to parse query plan: " << *planStr);

    // Then, execute the query
    auto execResult = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(execResult.IsSuccess(), execResult.GetIssues().ToString());

    return plan;
}

/*
 * Unified helper to verify CTAS plan with full checks:
 * 1. Stage count verification
 * 2. HashShuffle structure verification (HashFunc, PlanNodeType, Sink, etc.)
 * 3. KeyColumns verification
 */
static void VerifyCtasPlanFull(const NJson::TJsonValue& plan, const TString& planStr,
        bool enableCsWriteAffinity,
        const TVector<TString>& expectedKeyColumns,
        ui32 expectedStagesWithAffinity = 4,
        ui32 expectedStagesWithoutAffinity = 3) {
    // 1. Verify stage count and full plan structure
    VerifyCtasPlanWithAffinity(plan, planStr, enableCsWriteAffinity,
        expectedStagesWithAffinity, expectedStagesWithoutAffinity);

    // 2. Verify KeyColumns in HashShuffle
    VerifyHashShuffleKeyColumns(plan, planStr, enableCsWriteAffinity, expectedKeyColumns);
}

/*
 * Build expected YSON for N rows where each row is (Col1=i, Col2=i).
 * Col1 is Uint64 NOT NULL, Col2 is nullable Int32.
 * Result: [[0u;[0]];[1u;[1]];...;[(n-1)u;[(n-1)]]]
 */
static TString BuildExpectedYson_Uint64_NullableInt32(int rowCount) {
    TString expected = "[";
    for (int i = 0; i < rowCount; ++i) {
        if (i > 0) expected += ";";
        expected += TStringBuilder() << "[" << i << "u;[" << i << "]]";
    }
    expected += "]";
    return expected;
}

/*
 * Build expected YSON for N rows where each row is (Col1=i, Col2=i, Col3=i).
 * Col1 Uint64 NOT NULL, Col2 Uint64 NOT NULL, Col3 nullable Int32.
 * Result: [[0u;0u;[0]];[1u;1u;[1]];...;[(n-1)u;(n-1)u;[(n-1)]]]
 */
static TString BuildExpectedYson_Uint64_Uint64_NullableInt32(int rowCount) {
    TString expected = "[";
    for (int i = 0; i < rowCount; ++i) {
        if (i > 0) expected += ";";
        expected += TStringBuilder() << "[" << i << "u;" << i << "u;[" << i << "]]";
    }
    expected += "]";
    return expected;
}

/*
 * Build expected YSON for N rows where each row is (Col1=i, Col2=i).
 * Col1 Uint64 NOT NULL, Col2 Int32 NOT NULL (from CAST+Unwrap).
 * Result: [[0u;0];[1u;1];...;[(n-1)u;(n-1)]]
 */
static TString BuildExpectedYson_Uint64_Int32(int rowCount) {
    TString expected = "[";
    for (int i = 0; i < rowCount; ++i) {
        if (i > 0) expected += ";";
        expected += TStringBuilder() << "[" << i << "u;" << i << "]";
    }
    expected += "]";
    return expected;
}

/*
 * Build expected YSON for N rows where each row is (Col1=i, Col2=i, Col3=constVal).
 * Col1 Uint64 NOT NULL, Col2 Uint64 NOT NULL, Col3 Int32 NOT NULL.
 * Result: [[0u;0u;42];[1u;1u;42];...;[(n-1)u;(n-1)u;42]]
 */
static TString BuildExpectedYson_Uint64_Uint64_Int32_Const(int rowCount, int constVal) {
    TString expected = "[";
    for (int i = 0; i < rowCount; ++i) {
        if (i > 0) expected += ";";
        expected += TStringBuilder() << "[" << i << "u;" << i << "u;" << constVal << "]";
    }
    expected += "]";
    return expected;
}

/*
 * Build PRAGMA prefix for EnableCsWriteAffinity.
 * Returns the PRAGMA line to prepend to CTAS queries.
 */
static TString BuildCsWriteAffinityPragma(bool enableCsWriteAffinity) {
    return fmt::format(R"(PRAGMA ydb.EnableCsWriteAffinity = "{}";
)", enableCsWriteAffinity ? "true" : "false");
}

/*
 * Build KQP settings with EnableCsWriteAffinity enabled or disabled.
 * Used for pure literal CTAS tests where PRAGMA doesn't propagate correctly
 * because pure literals go through the EnsureDqUnion path.
 */
static TVector<NKikimrKqp::TKqpSetting> BuildKqpSettingsWithCsWriteAffinity(bool enableCsWriteAffinity) {
    NKikimrKqp::TKqpSetting setting;
    setting.SetName("EnableCsWriteAffinity");
    setting.SetValue(enableCsWriteAffinity ? "true" : "false");
    return {setting};
}

// Number of rows inserted into source tables. Must be > shard count (8) to ensure data
// is distributed across multiple shards and write affinity is meaningful.
static const int kRowCount = 80;


Y_UNIT_TEST_SUITE(CS_WriteAffinity) {

    /*
     * CTAS with source table (80 rows), PK=PartitionBy=HASH(Col1).
     * Verifies stage count, plan structure, KeyColumns, and exact YSON data comparison.
     */
    Y_UNIT_TEST_TWIN(CtasTableSourcePkMatchesPartitionBy, EnableCsWriteAffinity) {
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

        {
            const TString insertQuery =
                "$rowCount = " + ToString(kRowCount) + ";" + R"(
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

        const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
            CREATE TABLE `/Root/Destination` (
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
            AS SELECT * FROM `/Root/Source`;
        )";

        // Execute CTAS and verify plan structure from the same execution
        {
            const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
            const TString planStr = NJson::WriteJson(&plan, false);
            VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"});
        }

        // Verify exact data
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_NullableInt32(kRowCount));
        }
    }

    /*
     * Test sharding columns (PartitionBy) are correctly passed to HashShuffle.
     * - Single sharding column: PARTITION BY HASH(Col1)
     * - Multiple sharding columns: PARTITION BY HASH(Col1, Col2)
     */
    Y_UNIT_TEST_TWIN(CtasTableSourceMultipleShardingColumns, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // Test 1: Single sharding column
        {
            // Create source table and populate with kRowCount rows
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
            {
                const TString insertQuery =
                    "$rowCount = " + ToString(kRowCount) + ";" + R"(
                    $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                        RETURN AsStruct($x AS Col1, $x AS Col2);
                    });
                    REPLACE INTO `/Root/Source`
                    SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
                    FROM AS_TABLE($data);
                )";
                auto result = client.ExecuteQuery(insertQuery,
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
                CREATE TABLE `/Root/Dest1` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/Source`;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"});
            }

            // Verify exact data
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2 FROM `/Root/Dest1` ORDER BY Col1 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_NullableInt32(kRowCount));
            }
        }

        // Test 2: Multiple sharding columns
        {
            // Create source with multiple columns and populate with kRowCount rows
            {
                auto result = client.ExecuteQuery(R"(
                    CREATE TABLE `/Root/SourceMulti` (
                        Col1 Uint64 NOT NULL,
                        Col2 Uint64 NOT NULL,
                        Col3 Int32,
                        PRIMARY KEY (Col1, Col2)
                    )
                    PARTITION BY HASH(Col1, Col2)
                    WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
                )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                const TString insertQuery =
                    "$rowCount = " + ToString(kRowCount) + ";" + R"(
                    $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                        RETURN AsStruct($x AS Col1, $x AS Col2, $x AS Col3);
                    });
                    REPLACE INTO `/Root/SourceMulti`
                    SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1,
                           Unwrap(CAST(Col2 AS Uint64)) AS Col2,
                           Unwrap(CAST(Col3 AS Int32)) AS Col3
                    FROM AS_TABLE($data);
                )";
                auto result = client.ExecuteQuery(insertQuery,
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
                CREATE TABLE `/Root/DestMulti` (
                    PRIMARY KEY (Col1, Col2)
                )
                PARTITION BY HASH(Col1, Col2)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/SourceMulti`;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1", "Col2"});
            }

            // Verify exact data
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2, Col3 FROM `/Root/DestMulti` ORDER BY Col1 ASC, Col2 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_Uint64_NullableInt32(kRowCount));
            }
        }
    }

    /*
     * Test fallback behavior when PARTITION BY is not specified.
     * Without explicit PartitionBy, sharding columns fall back to PRIMARY KEY.
     */
    Y_UNIT_TEST_TWIN(CtasTableSourceNoPartitionByUsesPrimaryKey, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // Create and populate source table
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
        {
            const TString insertQuery =
                "$rowCount = " + ToString(kRowCount) + ";" + R"(
                $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                    RETURN AsStruct($x AS Col1, $x AS Col2);
                });
                REPLACE INTO `/Root/Source`
                SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
                FROM AS_TABLE($data);
            )";
            auto result = client.ExecuteQuery(insertQuery,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // CTAS without explicit PARTITION BY — should use PRIMARY KEY as sharding columns
        {
            const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
                CREATE TABLE `/Root/DestImplicit` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/Source`;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"});
            }

            // Verify exact data
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2 FROM `/Root/DestImplicit` ORDER BY Col1 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_NullableInt32(kRowCount));
            }
        }
    }

    /*
     * Test sharding columns when PRIMARY KEY differs from sharding key (PartitionBy).
     * PK=(Col1, Col2) but PARTITION BY HASH(Col2) → KeyColumns should be ["Col2"].
     */
    Y_UNIT_TEST_TWIN(CtasTableSourcePartitionBySubsetOfPrimaryKey, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // Create and populate source table
        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    Col1 Uint64 NOT NULL,
                    Col2 Uint64 NOT NULL,
                    Col3 Int32,
                    PRIMARY KEY (Col1, Col2)
                )
                PARTITION BY HASH(Col1, Col2)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            const TString insertQuery =
                "$rowCount = " + ToString(kRowCount) + ";" + R"(
                $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                    RETURN AsStruct($x AS Col1, $x AS Col2, $x AS Col3);
                });
                REPLACE INTO `/Root/Source`
                SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1,
                       Unwrap(CAST(Col2 AS Uint64)) AS Col2,
                       Unwrap(CAST(Col3 AS Int32)) AS Col3
                FROM AS_TABLE($data);
            )";
            auto result = client.ExecuteQuery(insertQuery,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // PRIMARY KEY (Col1, Col2) but PARTITION BY HASH(Col2)
        {
            const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
                CREATE TABLE `/Root/DestDiffKey` (
                    PRIMARY KEY (Col1, Col2)
                )
                PARTITION BY HASH(Col2)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/Source`;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col2"});
            }

            // Verify exact data
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2, Col3 FROM `/Root/DestDiffKey` ORDER BY Col1 ASC, Col2 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_Uint64_NullableInt32(kRowCount));
            }
        }
    }

    /*
     * CTAS from generated data (AS_TABLE($data)) with explicit PARTITION BY HASH(Col1).
     * 100 rows, PK=PartitionBy=HASH(Col1).
     * Verifies stage count, plan structure, KeyColumns, and exact YSON data comparison.
     */
    Y_UNIT_TEST_TWIN(CtasGeneratedDataWithPartitionBy, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        // NOTE: AS_TABLE($data) doesn't go through a real table read, so PRAGMA
        // ydb.EnableCsWriteAffinity doesn't propagate. Use SetKqpSettings instead.
        settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(EnableCsWriteAffinity));
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        const TString ctasQuery = R"(
            $data = ListMap(ListFromRange(0, 100), ($x) -> {
                RETURN AsStruct($x AS Col1, $x AS Col2);
            });
            CREATE TABLE `/Root/DestGenerated` (
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
            AS SELECT
                Unwrap(CAST(Col1 AS Uint64)) AS Col1,
                Unwrap(CAST(Col2 AS Int32)) AS Col2
            FROM AS_TABLE($data);
        )";

        // Execute CTAS and verify plan structure from the same execution
        {
            const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
            const TString planStr = NJson::WriteJson(&plan, false);
            VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"},
                /* expectedStagesWithAffinity= */ 3, /* expectedStagesWithoutAffinity= */ 2);
        }

        // Verify exact data (Col2 is Int32 NOT NULL from Unwrap)
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/DestGenerated` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_Int32(100));
        }
    }

    /*
     * CTAS from pure literal expression (single row: 1u AS Col1, 42 AS Col2).
     * PK=PartitionBy=HASH(Col1).
     * Verifies stage count, plan structure, KeyColumns, and exact YSON data comparison.
     */
    Y_UNIT_TEST_TWIN(CtasPureLiteralWithPartitionBy, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        // NOTE: Pure literals go through EnsureDqUnion path. PRAGMA ydb.EnableCsWriteAffinity
        // doesn't propagate for that path, so we must use SetKqpSettings here.
        settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(EnableCsWriteAffinity));
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        const TString ctasQuery = R"(
            CREATE TABLE `/Root/DestPure` (
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
            AS SELECT 1u AS Col1, 42 AS Col2;
        )";

        // Execute CTAS and verify plan structure from the same execution
        {
            const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
            const TString planStr = NJson::WriteJson(&plan, false);
            VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"},
                /* expectedStagesWithAffinity= */ 3, /* expectedStagesWithoutAffinity= */ 2);
        }

        // Verify exact data (1u, 42)
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/DestPure` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), "[[1u;42]]");
        }
    }

    /*
     * CTAS from generated data (AS_TABLE($data)) without PARTITION BY.
     * Falls back to PRIMARY KEY for partitioning. 80 rows, PK=HASH(Col1).
     * Verifies stage count, plan structure, KeyColumns, and exact YSON data comparison.
     */
    Y_UNIT_TEST_TWIN(CtasGeneratedDataWithoutPartitionBy, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        // NOTE: AS_TABLE($data) doesn't go through a real table read, so PRAGMA
        // ydb.EnableCsWriteAffinity doesn't propagate. Use SetKqpSettings instead.
        settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(EnableCsWriteAffinity));
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        const TString ctasQuery = R"(
            $data = ListMap(ListFromRange(0, 80), ($x) -> {
                RETURN AsStruct($x AS Col1, $x AS Col2);
            });
            CREATE TABLE `/Root/DestGeneratedImplicit` (
                PRIMARY KEY (Col1)
            )
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
            AS SELECT
                Unwrap(CAST(Col1 AS Uint64)) AS Col1,
                Unwrap(CAST(Col2 AS Int32)) AS Col2
            FROM AS_TABLE($data);
        )";

        // Execute CTAS and verify plan structure from the same execution
        {
            const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
            const TString planStr = NJson::WriteJson(&plan, false);
            VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"},
                /* expectedStagesWithAffinity= */ 3, /* expectedStagesWithoutAffinity= */ 2);
        }

        // Verify exact data
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/DestGeneratedImplicit` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_Int32(80));
        }
    }

    /*
     * Test CTAS from generated data where PartitionBy differs from PRIMARY KEY.
     * PK=(Col1, Col2) but PARTITION BY HASH(Col2).
     * Note: AS_TABLE($data) goes through EnsureDqUnion path, so no HashShuffle.
     */
    Y_UNIT_TEST_TWIN(CtasGeneratedDataPartitionBySubsetOfPrimaryKey, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        // NOTE: AS_TABLE($data) doesn't go through a real table read, so PRAGMA
        // ydb.EnableCsWriteAffinity doesn't propagate. Use SetKqpSettings instead.
        settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(EnableCsWriteAffinity));
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // PRIMARY KEY (Col1, Col2) but PARTITION BY HASH(Col2), 100 rows
        {
            const TString ctasQuery = R"(
                $data = ListMap(ListFromRange(0, 100), ($x) -> {
                    RETURN AsStruct($x AS Col1, $x AS Col2, 42 AS Col3);
                });
                CREATE TABLE `/Root/DestGenDiffKey` (
                    PRIMARY KEY (Col1, Col2)
                )
                PARTITION BY HASH(Col2)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT
                    Unwrap(CAST(Col1 AS Uint64)) AS Col1,
                    Unwrap(CAST(Col2 AS Uint64)) AS Col2,
                    Unwrap(CAST(Col3 AS Int32)) AS Col3
                FROM AS_TABLE($data);
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col2"},
                    /* expectedStagesWithAffinity= */ 3, /* expectedStagesWithoutAffinity= */ 2);
            }

            // Verify exact data
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2, Col3 FROM `/Root/DestGenDiffKey` ORDER BY Col1 ASC, Col2 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_Uint64_Int32_Const(100, 42));
            }
        }
    }

    /*
     * Test CTAS with column aliasing in SELECT.
     * SELECT Col1 AS A, Col2 AS B with PARTITION BY HASH(A).
     * KeyColumns should match aliased name "A", not source name "Col1".
     */
    Y_UNIT_TEST_TWIN(CtasTableSourceSelectWithAliases, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // Create and populate source table
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
        {
            const TString insertQuery =
                "$rowCount = " + ToString(kRowCount) + ";" + R"(
                $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                    RETURN AsStruct($x AS Col1, $x AS Col2);
                });
                REPLACE INTO `/Root/Source`
                SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
                FROM AS_TABLE($data);
            )";
            auto result = client.ExecuteQuery(insertQuery,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // SELECT with aliases: Col1 AS A, Col2 AS B; PartitionBy uses aliased name 'A'
        {
            const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
                CREATE TABLE `/Root/DestAliased` (
                    PRIMARY KEY (A)
                )
                PARTITION BY HASH(A)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT Col1 AS A, Col2 AS B FROM `/Root/Source`;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"A"});
            }

            // Verify exact data (A=Col1, B=Col2)
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT A, B FROM `/Root/DestAliased` ORDER BY A ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_NullableInt32(kRowCount));
            }
        }
    }

    /*
     * Verifies that EnableCsWriteAffinity flag toggles HashShuffle on/off.
     * With affinity enabled: HashShuffle present with correct KeyColumns.
     * With affinity disabled: No HashShuffle.
     */
    Y_UNIT_TEST_TWIN(CtasTableSourceVerifyAffinityFlagTogglesHashShuffle, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // Create and populate source table
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
        {
            const TString insertQuery =
                "$rowCount = " + ToString(kRowCount) + ";" + R"(
                $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                    RETURN AsStruct($x AS Col1, $x AS Col2);
                });
                REPLACE INTO `/Root/Source`
                SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
                FROM AS_TABLE($data);
            )";
            auto result = client.ExecuteQuery(insertQuery,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
                CREATE TABLE `/Root/DestNoAffinity` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/Source`;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"});
            }

            // Verify exact data
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2 FROM `/Root/DestNoAffinity` ORDER BY Col1 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), BuildExpectedYson_Uint64_NullableInt32(kRowCount));
            }
        }
    }

    /*
     * Verifies that EnableCsWriteAffinity flag toggles HashShuffle on/off for pure literals.
     * Note: Pure literals go through EnsureDqUnion path.
     */
    Y_UNIT_TEST_TWIN(CtasPureLiteralVerifyAffinityFlagTogglesHashShuffle, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        // NOTE: Pure literals go through EnsureDqUnion path. PRAGMA ydb.EnableCsWriteAffinity
        // doesn't propagate for that path, so we must use SetKqpSettings here.
        settings.SetKqpSettings(BuildKqpSettingsWithCsWriteAffinity(EnableCsWriteAffinity));
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // Pure expr CTAS (1 row: Col1=1u, Col2=42)
        {
            const TString ctasQuery = R"(
                CREATE TABLE `/Root/DestPureNoAffinity` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT 1u AS Col1, 42 AS Col2;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"},
                    /* expectedStagesWithAffinity= */ 3, /* expectedStagesWithoutAffinity= */ 2);
            }

            // Verify exact data (1u, 42)
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2 FROM `/Root/DestPureNoAffinity` ORDER BY Col1 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                CompareYson(StreamResultToYson(it), "[[1u;42]]");
            }
        }
    }

    /*
     * CTAS with WHERE filter.
     * Verifies that filtered rows are still correctly routed to shards based on sharding key.
     * Source has 80 rows, WHERE Col1 > 40 filters to 40 rows.
     */
    Y_UNIT_TEST_TWIN(CtasTableSourceWithWhereFilter, EnableCsWriteAffinity) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        // Create and populate source table
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
        {
            const TString insertQuery =
                "$rowCount = " + ToString(kRowCount) + ";" + R"(
                $data = ListMap(ListFromRange(0, $rowCount), ($x) -> {
                    RETURN AsStruct($x AS Col1, $x AS Col2);
                });
                REPLACE INTO `/Root/Source`
                SELECT Unwrap(CAST(Col1 AS Uint64)) AS Col1, Unwrap(CAST(Col2 AS Int32)) AS Col2
                FROM AS_TABLE($data);
            )";
            auto result = client.ExecuteQuery(insertQuery,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // CTAS with WHERE filter: Col1 > 40 → 40 rows (Col1=41..80-1=79)
        {
            const TString ctasQuery = BuildCsWriteAffinityPragma(EnableCsWriteAffinity) + R"(
                CREATE TABLE `/Root/DestWhere` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/Source` WHERE Col1 > 40;
            )";

            // Execute CTAS and verify plan structure from the same execution
            {
                const auto plan = ExplainAndExecuteQuery(client, ctasQuery);
                const TString planStr = NJson::WriteJson(&plan, false);
                VerifyCtasPlanFull(plan, planStr, EnableCsWriteAffinity, {"Col1"});
            }

            // Verify exact data: rows with Col1=41..79 (39 rows)
            {
                auto it = client.StreamExecuteQuery(R"(
                    SELECT Col1, Col2 FROM `/Root/DestWhere` ORDER BY Col1 ASC;
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), NYdb::EStatus::SUCCESS, it.GetIssues().ToString());
                // Build expected: Col1=41..79, Col2=41..79 (nullable Int32 → wrapped in brackets)
                TString expected = "[";
                for (int i = 41; i < kRowCount; ++i) {
                    if (i > 41) expected += ";";
                    expected += TStringBuilder() << "[" << i << "u;[" << i << "]]";
                }
                expected += "]";
                CompareYson(StreamResultToYson(it), expected);
            }
        }
    }

} // Y_UNIT_TEST_SUITE(CS_WriteAffinity)

} // namespace NKqp
} // namespace NKikimr
