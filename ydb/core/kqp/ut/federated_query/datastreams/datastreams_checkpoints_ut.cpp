#include "common.h"

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace fmt::literals;
using namespace NTestUtils;
using namespace NFederatedQueryTest;

Y_UNIT_TEST_SUITE(KqpStreamingQueriesCheckpoints) {

    // Verifies that DROP STREAMING QUERY removes all checkpoint data from the metadata tables.
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

        // Grant access to checkpoint metadata tables upfront
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");

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

        // Send a message to trigger checkpoint creation
        WriteTopicMessage(inputTopicName, R"({"key": "k1", "value": "v1"})");
        ReadTopicMessages(outputTopicName, {"k1v1"});
        Sleep(CheckpointPeriod * 3);  // Wait for checkpoint to be committed

        // Wait until at least one checkpoint row appears in checkpoints_metadata
        WaitFor(TEST_OPERATION_TIMEOUT, "at least one checkpoint appears", [&]() {
            return countRows(".metadata/streaming/checkpoints/checkpoints_metadata") > 0;
        });

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

} // Y_UNIT_TEST_SUITE(KqpStreamingQueriesCheckpoints)

} // namespace NKikimr::NKqp
