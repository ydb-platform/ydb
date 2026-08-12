#include <fmt/format.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

// Helper: configure a TKikimrRunner with CTAS / OLAP sink enabled and the
// EnableCsWriteAffinity feature flag available.  The caller may further
// customize the settings (e.g. SetNodeCount) before constructing the runner.
TKikimrSettings MakeCtasSettings() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableMoveColumnTable(true);
    auto settings = TKikimrSettings()
        .SetFeatureFlags(featureFlags)
        .SetWithSampleTables(false);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
    settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
    return settings;
}

// Helper: build a CTAS query string with the EnableCsWriteAffinity pragma
// optionally enabled.
TString BuildCtasQuery(const TString& pragmaPrefix, const TString& source,
    const TString& dest, const TString& destPk, ui32 minPartitions) {
    return TStringBuilder()
        << pragmaPrefix
        << R"(
            CREATE TABLE `)" << dest << R"(` (
                PRIMARY KEY ()" << destPk << R"()
            )
            PARTITION BY HASH()" << destPk << R"()
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = )" << minPartitions << R"()
            AS SELECT * FROM `)" << source << R"(`;
        )";
}

} // namespace

Y_UNIT_TEST_SUITE(KqpWriteAffinity) {

    // Verify that a CTAS with a large number of rows produces correct results
    // when EnableCsWriteAffinity is enabled.  This exercises the per-shard
    // write path with enough data to span multiple flushes.
    Y_UNIT_TEST(CTAS_WriteAffinity_LargeData) {
        auto settings = MakeCtasSettings();
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
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Insert 100 rows
        {
            TStringBuilder sb;
            sb << "REPLACE INTO `/Root/Source` (Col1, Col2) VALUES ";
            for (ui32 i = 1; i <= 100; ++i) {
                if (i > 1) sb << ", ";
                sb << "(" << i << "u, " << (i32)i << ")";
            }
            auto result = client.ExecuteQuery(sb, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString pragma = R"(PRAGMA ydb.EnableCsWriteAffinity = "true";
)";
        const TString ctasQuery = BuildCtasQuery(pragma, "/Root/Source", "/Root/Destination", "Col1", 2);

        {
            auto result = client.ExecuteQuery(ctasQuery, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify all 100 rows were written
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/Destination`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            // The count should be 100
            UNIT_ASSERT_C(output.Contains("100"), "Expected 100 rows, got: " << output);
        }

        // Verify a few specific rows
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination` WHERE Col1 IN (1u, 50u, 100u) ORDER BY Col1 ASC;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;[1]];[50u;[50]];[100u;[100]]])");
        }
    }

    // Verify CTAS write affinity works correctly in a multi-node cluster.
    // With multiple nodes, shards are distributed across nodes, and each
    // per-shard task should be pinned to the node hosting its shard.
    Y_UNIT_TEST(CTAS_WriteAffinity_MultiNode) {
        auto settings = MakeCtasSettings();
        settings.SetNodeCount(3);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    Col1 Uint64 NOT NULL,
                    Col2 Utf8,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 8);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source` (Col1, Col2) VALUES
                    (1u, "a"), (2u, "b"), (3u, "c"), (4u, "d"),
                    (5u, "e"), (6u, "f"), (7u, "g"), (8u, "h");
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString pragma = R"(PRAGMA ydb.EnableCsWriteAffinity = "true";
)";
        const TString ctasQuery = BuildCtasQuery(pragma, "/Root/Source", "/Root/Destination", "Col1", 4);

        {
            auto result = client.ExecuteQuery(ctasQuery, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify all rows
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination` ORDER BY Col1 ASC;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;["a"]];[2u;["b"]];[3u;["c"]];[4u;["d"]];[5u;["e"]];[6u;["f"]];[7u;["g"]];[8u;["h"]]])");
        }
    }

    // Verify CTAS write affinity works when the source is a JOIN.
    // The transform stage computes the JOIN and the sink stage writes the
    // results with per-shard affinity.
    Y_UNIT_TEST(CTAS_WriteAffinity_JoinSource) {
        auto settings = MakeCtasSettings();
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/SourceA` (
                    Id Uint64 NOT NULL,
                    Name Utf8,
                    PRIMARY KEY (Id)
                )
                PARTITION BY HASH(Id)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);

                CREATE TABLE `/Root/SourceB` (
                    Id Uint64 NOT NULL,
                    Value Int32,
                    PRIMARY KEY (Id)
                )
                PARTITION BY HASH(Id)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/SourceA` (Id, Name) VALUES
                    (1u, "alpha"), (2u, "beta"), (3u, "gamma");
                REPLACE INTO `/Root/SourceB` (Id, Value) VALUES
                    (1u, 100), (2u, 200), (3u, 300);
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString pragma = R"(PRAGMA ydb.EnableCsWriteAffinity = "true";
)";
        const TString ctasQuery = TStringBuilder()
            << pragma
            << R"(
                CREATE TABLE `/Root/Destination` (
                    PRIMARY KEY (Id)
                )
                PARTITION BY HASH(Id)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT a.Id AS Id, a.Name AS Name, b.Value AS Value
                FROM `/Root/SourceA` AS a
                INNER JOIN `/Root/SourceB` AS b ON a.Id = b.Id;
            )";

        {
            auto result = client.ExecuteQuery(ctasQuery, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Id, Name, Value FROM `/Root/Destination` ORDER BY Id ASC;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;["alpha"];[100]];[2u;["beta"];[200]];[3u;["gamma"];[300]]])");
        }
    }

    // Verify CTAS write affinity works when the source is empty.
    // The destination table should be created but contain no rows.
    Y_UNIT_TEST(CTAS_WriteAffinity_EmptySource) {
        auto settings = MakeCtasSettings();
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
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // No data inserted — source is empty

        const TString pragma = R"(PRAGMA ydb.EnableCsWriteAffinity = "true";
)";
        const TString ctasQuery = BuildCtasQuery(pragma, "/Root/Source", "/Root/Destination", "Col1", 2);

        {
            auto result = client.ExecuteQuery(ctasQuery, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify destination is empty
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/Destination`;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            UNIT_ASSERT_C(output.Contains("0"), "Expected 0 rows, got: " << output);
        }
    }

    // Verify CTAS write affinity works with a composite (multi-column) primary key.
    // The sharding hash is computed over multiple key columns.
    Y_UNIT_TEST(CTAS_WriteAffinity_CompositeKey) {
        auto settings = MakeCtasSettings();
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Source` (
                    PartKey Uint64 NOT NULL,
                    SortKey Uint32 NOT NULL,
                    Data Utf8,
                    PRIMARY KEY (PartKey, SortKey)
                )
                PARTITION BY HASH(PartKey)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source` (PartKey, SortKey, Data) VALUES
                    (1u, 10u, "row1"), (1u, 20u, "row2"),
                    (2u, 10u, "row3"), (2u, 20u, "row4"),
                    (3u, 10u, "row5"), (3u, 20u, "row6");
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString pragma = R"(PRAGMA ydb.EnableCsWriteAffinity = "true";
)";
        const TString ctasQuery = TStringBuilder()
            << pragma
            << R"(
                CREATE TABLE `/Root/Destination` (
                    PRIMARY KEY (PartKey, SortKey)
                )
                PARTITION BY HASH(PartKey)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2)
                AS SELECT * FROM `/Root/Source`;
            )";

        {
            auto result = client.ExecuteQuery(ctasQuery, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Verify all rows
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT PartKey, SortKey, Data FROM `/Root/Destination`
                ORDER BY PartKey ASC, SortKey ASC;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;10u;["row1"]];[1u;20u;["row2"]];[2u;10u;["row3"]];[2u;20u;["row4"]];[3u;10u;["row5"]];[3u;20u;["row6"]]])");
        }
    }

} // Y_UNIT_TEST_SUITE(KqpWriteAffinity)

} // namespace NKqp
} // namespace NKikimr
