#include "common.h"

#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NFederatedQueryTest;

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreamsQueriesRestart) {

    Y_UNIT_TEST_F(RestartQueryAfterPartitionIncrease, TStreamingTestFixture) {
        // Start a streaming query on a topic with 1 partition, stop the query,
        // increase the partition count to 20, restart WITHOUT recompilation
        // (no FORCE flag, SQL is unchanged), write data to every new partition,
        // and verify all messages appear in the output topic.
        //
        // The test body is run twice: once for non-local (external) topics and
        // once for local (internal kikimr) topics.

        // Enable local topic support before the cluster is first initialised.
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableFeatureFlags()->SetEnableUpdatingPartitionsOnStreamingQueryRestart(true);

        const auto runTest = [&](bool local) {
            const std::string suffix = local ? "_local" : "_nonlocal";
            const std::string inputTopicName  = std::string("restartAfterPartIncInputTopic")  + suffix;
            const std::string outputTopicName = std::string("restartAfterPartIncOutputTopic") + suffix;
            const std::string sourceName      = std::string("restartAfterPartIncSource")      + suffix;
            const std::string queryName       = std::string("restartAfterPartIncQuery")       + suffix;

            // Create the input topic with exactly 1 partition.
            CreateTopic(inputTopicName, NYdb::NTopic::TCreateTopicSettings()
                .PartitioningSettings(/* minActivePartitions */ 1, /* maxActivePartitions */ 1), local);
            CreateTopic(outputTopicName, std::nullopt, local);

            // For non-local topics the query references `source`.`topic`;
            // for local topics the topic name is used directly.
            std::string inputRef, outputRef;
            if (local) {
                inputRef  = fmt::format("`{}`", inputTopicName);
                outputRef = fmt::format("`{}`", outputTopicName);
            } else {
                CreatePqSource(sourceName);
                inputRef  = fmt::format("`{}`.`{}`", sourceName, inputTopicName);
                outputRef = fmt::format("`{}`.`{}`", sourceName, outputTopicName);
            }

            // Create and start a streaming query.
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    $in = SELECT value FROM {input_ref} WITH (
                        FORMAT = "json_each_row",
                        SCHEMA = (value String NOT NULL)
                    )
                    WHERE value LIKE "%data%";
                    INSERT INTO {output_ref} SELECT value FROM $in;
                END DO;)",
                "query_name"_a = queryName,
                "input_ref"_a  = inputRef,
                "output_ref"_a = outputRef
            ));

            WriteTopicMessage(inputTopicName, R"({"value": "my_data_0"})", 0, local);
            ReadTopicMessages(outputTopicName, {"my_data_0"},
                TInstant::Now() - TDuration::Seconds(100),
                /* sort */ true, local);

            // Let the query reach a stable checkpoint before stopping.
            Sleep(TDuration::Seconds(2));

            // Stop the query before altering the topic partition count.
            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
                "query_name"_a = queryName
            ));
            Sleep(TDuration::MilliSeconds(500));

            // Increase the partition count from 1 to 20 via the Topic API (not SQL,
            // to avoid PQ-gateway session-related issues during schema changes).
            {
                auto alterSettings = NYdb::NTopic::TAlterTopicSettings();
                alterSettings
                    .BeginAlterPartitioningSettings()
                        .MinActivePartitions(20)
                        .MaxActivePartitions(20)
                    .EndAlterTopicPartitioningSettings();
                const auto result = GetTopicClient(local)->AlterTopic(inputTopicName, alterSettings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            }

            // Restart WITHOUT recompilation: no FORCE flag, SQL body is unchanged.
            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
                "query_name"_a = queryName
            ));

            // Give the query time to reconnect to all 20 partitions and checkpoint.
            Sleep(TDuration::Seconds(2));

            // Write one message to each of the 20 partitions.
            // Send to new partitions (1..19) first, then old partition (0).
            constexpr ui32 messageCount = 20;
            for (ui32 i = 1; i < messageCount; ++i) {
                WriteTopicMessage(inputTopicName, fmt::format(R"({{"value": "my_data_{}"}})", i), i, local);
            }
            WriteTopicMessage(inputTopicName, R"({"value": "my_data_0"})", 0, local);

            // All 20 messages + initial message must appear in the output topic (order may vary).
            std::vector<std::string> expectedMessages = {"my_data_0"}; // initial message written before restart
            for (ui32 i = 0; i < messageCount; ++i) {
                expectedMessages.push_back(fmt::format("my_data_{}", i));
            }
            ReadTopicMessages(outputTopicName, expectedMessages,
                TInstant::Now() - TDuration::Seconds(100),
                /* sort */ true, local);

            // Cleanup.
            ExecQuery(fmt::format(R"(
                DROP STREAMING QUERY `{query_name}`;)",
                "query_name"_a = queryName
            ));
        };

        runTest(/* local */ false);
        runTest(/* local */ true);
    }

    Y_UNIT_TEST_F(PartitionPredicatePreservedAfterPartitionIncrease, TStreamingTestFixture) {
        // Goal: verify that after increasing the number of partitions in a topic, a
        // streaming query with a __ydb_partition_id predicate continues to read ONLY
        // the partitions specified in the predicate and ignores all new partitions.
        //
        // Partitions INSIDE the predicate (__ydb_partition_id < 2, i.e. 0 and 1) receive
        // valid JSON messages. Partitions OUTSIDE the predicate receive deliberately
        // invalid JSON. If the predicate ever fails to filter those partitions, the query
        // would attempt to parse the invalid JSON and fail, making the bug immediately
        // visible.
        //
        // Flow:
        //   1. Create a topic with 4 partitions, start a streaming query that reads
        //      only partitions 0-1 (via WHERE __ydb_partition_id < 2).
        //   2. Send invalid JSON to partitions 2-3 first, then valid JSON to 0-1.
        //      Only the 2 valid messages must appear in the output.
        //   3. Stop query (ALTER SET RUN = FALSE).
        //   4. Increase partitions: 4 → 20.
        //   5. Restart query (ALTER SET RUN = TRUE).
        //   6. Send invalid JSON to new partitions (4-19) first, then valid JSON to 0-1.
        //      Only 2 valid messages must arrive in the output.

        constexpr char inputTopicName[]  = "partPredicateAfterIncInputTopic";
        constexpr char outputTopicName[] = "partPredicateAfterIncOutputTopic";
        constexpr char sourceName[]      = "partPredicateAfterIncSource";
        constexpr char queryName[]       = "partPredicateAfterIncQuery";

        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(true);
        config.MutableFeatureFlags()->SetEnableUpdatingPartitionsOnStreamingQueryRestart(true);

        // Create input topic with 4 partitions
        const ui32 initialPartitionCount = 4;
        CreateTopic(inputTopicName, NYdb::NTopic::TCreateTopicSettings()
            .PartitioningSettings(initialPartitionCount, initialPartitionCount));
        CreateTopic(outputTopicName);
        CreatePqSource(sourceName);

        // ── Phase 1: start streaming query with __ydb_partition_id predicate ────
        // The query forwards messages from partitions 0 and 1 only.
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT value FROM `{source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA = (value String NOT NULL)
                )
                WHERE __ydb_partition_id < 2;
                INSERT INTO `{source}`.`{output_topic}` SELECT value FROM $in;
            END DO;)",
            "query_name"_a = queryName,
            "source"_a    = sourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        // Write to partitions outside the predicate (2, 3) first — invalid JSON.
        // If the predicate fails and these are parsed, the query would error out.
        for (ui32 i = 2; i < initialPartitionCount; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format("not_valid_json_p{}", i), i);
        }
        // Write valid JSON to partitions inside the predicate (0, 1).
        for (ui32 i = 0; i < 2; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format(R"({{"value": "before_data_p{}"}})", i), i);
        }

        // Exactly 2 messages (from partitions 0 and 1) must appear in the output.
        ReadTopicMessages(outputTopicName,
            {"before_data_p0", "before_data_p1"},
            TInstant::Now() - TDuration::Seconds(100),
            /* sort */ true);

        // Let the query reach a stable checkpoint before stopping.
        Sleep(TDuration::Seconds(2));

        // ── Phase 2: stop query, increase partitions ─────────────────────────────
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));
        Sleep(TDuration::MilliSeconds(500));

        // Increase partition count from 4 to 20 via the Topic API.
        {
            NYdb::NTopic::TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                    .MinActivePartitions(20)
                    .MaxActivePartitions(20)
                .EndAlterTopicPartitioningSettings();
            const auto alterResult = GetTopicClient()
                ->AlterTopic(inputTopicName, alterSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                alterResult.GetStatus(), NYdb::EStatus::SUCCESS,
                alterResult.GetIssues().ToOneLineString());
        }

        // ── Phase 3: restart query, verify predicate is still respected ──────────
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
            "query_name"_a = queryName
        ));

        // Give the query time to reconnect and pick up the new topology.
        Sleep(TDuration::Seconds(2));

        const TInstant afterRestart = TInstant::Now();

        // Write invalid JSON to new partitions (4–19, outside predicate) first.
        // If the predicate fails to exclude them, parsing will fail and the test breaks.
        const ui32 newPartitionCount = 20;
        for (ui32 i = 4; i < newPartitionCount; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format("not_valid_json_p{}", i), i);
        }

        // Write valid JSON to partitions 0 and 1 — these MUST reach the output.
        WriteTopicMessage(inputTopicName, R"({"value": "after_data_p0"})", 0);
        WriteTopicMessage(inputTopicName, R"({"value": "after_data_p1"})", 1);

        // Exactly 2 new valid messages must arrive; invalid JSON from partitions 4-19
        // must never be processed because they are outside the predicate.
        ReadTopicMessages(outputTopicName,
            {"after_data_p0", "after_data_p1"},
            afterRestart,
            /* sort */ true);

        // Cleanup
        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));
    }
}

} // namespace NKikimr::NKqp
