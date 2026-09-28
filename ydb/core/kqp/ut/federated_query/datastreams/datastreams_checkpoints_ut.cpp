#include "common.h"

#include <ydb/core/kqp/ut/federated_query/common/common.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace fmt::literals;
using namespace NTestUtils;
using namespace NFederatedQueryTest;

Y_UNIT_TEST_SUITE(KqpStreamingQueriesCheckpoints) {

    // Verifies that DROP STREAMING QUERY removes all checkpoint data from the metadata tables,
    // including after a query text change (ALTER ... FORCE = TRUE) that starts a new execution
    // but reuses the same CheckpointId / graphId.
    //
    // Checkpoint storage uses a graphId = CheckpointId = "<executionId>-<queryPath>", where
    // CheckpointId is set once on first execution start and then reused across all restarts
    // (it is only reset if the previous execution entry is GC'd and no longer exists).
    //
    // DROP must delete the single graphId used by the query, which is derived from the
    // CurrentExecutionId (first execution) and stored in coordinators_sync, checkpoints_metadata
    // and states tables.
    Y_UNIT_TEST_F(DropStreamingQueryDeletesCheckpoints, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "dropDeletesCheckpointsInputTopic";
        constexpr char outputTopicName[] = "dropDeletesCheckpointsOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSource";
        CreatePqSource(pqSourceName);

        // Helper to count rows in a checkpoint metadata table
        auto countRows = [&](const std::string& tablePath) -> ui64 {
            const auto& result = ExecQuery(fmt::format(
                "SELECT COUNT(*) AS cnt FROM `{table}`;",
                "table"_a = tablePath
            ));
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            ui64 cnt = 0;
            CheckScriptResult(result[0], 1, 1, [&cnt](NYdb::TResultSetParser& parser) {
                cnt = parser.ColumnParser(0).GetUint64();
            });
            return cnt;
        };

        constexpr char queryName[] = "streamingQueryForCheckpointDeletion";

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        // Grant access to checkpoint metadata tables (must be done after streaming query creates the path)
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");

        // Send a message to trigger checkpoint creation
        WriteTopicMessage(inputTopicName, R"({"key": "k1", "value": "v1"})");
        ReadTopicMessages(outputTopicName, {"k1v1"});
        Sleep(CheckpointPeriod * 3);  // Wait for checkpoint to be committed

        // Wait until at least one checkpoint row appears in checkpoints_metadata
        WaitFor(TEST_OPERATION_TIMEOUT, "at least one checkpoint appears", [&]() {
            return countRows(".metadata/streaming/checkpoints/checkpoints_metadata") > 0;
        });

        // Change the query text with FORCE=TRUE — this cancels the current execution and starts a
        // new one. The CheckpointId is reused (it is only reset when the previous execution entry
        // no longer exists), so the same graphId is used after restart.
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value || "!" FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));
        // 2 execution entries (original + new), 1 running lease
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(0, 0);

        // Verify all checkpoint-related tables are empty after DROP
        UNIT_ASSERT_VALUES_EQUAL_C(
            countRows(".metadata/streaming/checkpoints/checkpoints_metadata"), 0UL,
            "Expected checkpoints_metadata to be empty after DROP STREAMING QUERY");
        UNIT_ASSERT_VALUES_EQUAL_C(
            countRows(".metadata/streaming/checkpoints/coordinators_sync"), 0UL,
            "Expected coordinators_sync to be empty after DROP STREAMING QUERY");
        UNIT_ASSERT_VALUES_EQUAL_C(
            countRows(".metadata/streaming/checkpoints/states"), 0UL,
            "Expected states to be empty after DROP STREAMING QUERY");
    }

    // Verifies that DROP STREAMING QUERY removes all checkpoint data even when the query was
    // already stopped (RUN = FALSE) before the DROP.
    //
    // This exercises the code path in TDropStreamingQueryActor where QueryExistsInTable is true
    // but the execution has already been cancelled — DeleteQueryGraphs() is called after
    // TCleanupStreamingQueryStateTableActor finishes, which in this case only needs to forget
    // the previous execution entries (there is no running execution to cancel).
    Y_UNIT_TEST_F(DropStoppedStreamingQueryDeletesCheckpoints, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "dropStoppedDeletesCheckpointsInputTopic";
        constexpr char outputTopicName[] = "dropStoppedDeletesCheckpointsOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceStopped";
        CreatePqSource(pqSourceName);

        // Helper to count rows in a checkpoint metadata table
        auto countRows = [&](const std::string& tablePath) -> ui64 {
            const auto& result = ExecQuery(fmt::format(
                "SELECT COUNT(*) AS cnt FROM `{table}`;",
                "table"_a = tablePath
            ));
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            ui64 cnt = 0;
            CheckScriptResult(result[0], 1, 1, [&cnt](NYdb::TResultSetParser& parser) {
                cnt = parser.ColumnParser(0).GetUint64();
            });
            return cnt;
        };

        constexpr char queryName[] = "stoppedStreamingQueryForCheckpointDeletion";

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT key || value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                )
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        // Grant access to checkpoint metadata tables (must be done after streaming query creates the path)
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");

        // Send a message to trigger checkpoint creation
        WriteTopicMessage(inputTopicName, R"({"key": "k2", "value": "v2"})");
        ReadTopicMessages(outputTopicName, {"k2v2"});
        Sleep(CheckpointPeriod * 3);  // Wait for checkpoint to be committed

        // Wait until at least one checkpoint row appears in checkpoints_metadata
        WaitFor(TEST_OPERATION_TIMEOUT, "at least one checkpoint appears", [&]() {
            return countRows(".metadata/streaming/checkpoints/checkpoints_metadata") > 0;
        });

        // Stop the query before dropping it
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);  // 1 execution entry, no running lease

        // Drop the already-stopped query
        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(0, 0);

        // Verify all checkpoint-related tables are empty after DROP
        UNIT_ASSERT_VALUES_EQUAL_C(
            countRows(".metadata/streaming/checkpoints/checkpoints_metadata"), 0UL,
            "Expected checkpoints_metadata to be empty after DROP of stopped STREAMING QUERY");
        UNIT_ASSERT_VALUES_EQUAL_C(
            countRows(".metadata/streaming/checkpoints/coordinators_sync"), 0UL,
            "Expected coordinators_sync to be empty after DROP of stopped STREAMING QUERY");
        UNIT_ASSERT_VALUES_EQUAL_C(
            countRows(".metadata/streaming/checkpoints/states"), 0UL,
            "Expected states to be empty after DROP of stopped STREAMING QUERY");
    }

} // Y_UNIT_TEST_SUITE(KqpStreamingQueriesCheckpoints)

} // namespace NKikimr::NKqp
