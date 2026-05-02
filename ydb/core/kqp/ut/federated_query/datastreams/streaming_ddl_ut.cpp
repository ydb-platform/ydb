#include "common.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>

#include <fmt/format.h>

#include <random>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace fmt::literals;
using namespace NYql::NConnector::NTest;
using namespace NTestUtils;

Y_UNIT_TEST_SUITE(KqpStreamingQueriesDdl) {
    Y_UNIT_TEST_F(CreateAndAlterStreamingQuery, TStreamingWithSchemaSecretsTestFixture) {
        constexpr char inputTopicName[] = "createAndAlterStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndAlterStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE SECRET test_secret WITH (value = "1234");
            CREATE TABLE test_table1 (Key Int32 NOT NULL, PRIMARY KEY (Key));
            GRANT ALL ON `/Root/test_table1` TO `test@builtin`;
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
                WHERE value REGEXP ".*v.*a.*l.*"
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        {
            const auto tableDesc = Navigate(GetRuntime(), GetRuntime().AllocateEdgeActor(), "/Root/test_table1", NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& table = tableDesc->ResultSet.at(0);
            UNIT_ASSERT_VALUES_EQUAL(table.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindTable);
            UNIT_ASSERT(table.SecurityObject->CheckAccess(NACLib::GenericFull, NACLib::TUserToken("test@builtin", {})));
        }

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            CREATE TABLE test_table2 (Key Int32 NOT NULL, PRIMARY KEY (Key));
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT value || key FROM `{pq_source}`.`{input_topic}` WITH (
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

        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key2", "value": "value2"})");
        ReadTopicMessages(outputTopicName, {"key1value1", "value2key2"});

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(2, 0);
    }

    Y_UNIT_TEST_F(CreateAndDropStreamingQuery, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndDropStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndDropStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
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

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(0, 0);
    }

    Y_UNIT_TEST_F(MaxPartitionReadSkewWithRestartAndCheckpoint, TStreamingTestFixture) {
        SetupAppConfig().MutableTableServiceConfig()->SetEnableStreamingPartitionBalancing(true);

        constexpr ui32 partitionCount = 10;
        constexpr char inputTopicName[] = "maxPartitionReadSkewRestartInputTopic";
        constexpr char outputTopicName[] = "maxPartitionReadSkewRestartOutputTopic";
        CreateTopic(inputTopicName, NTopic::TCreateTopicSettings()
            .PartitioningSettings(partitionCount, partitionCount));
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(
            R"sql(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    PRAGMA pq.MaxPartitionReadSkew = "10s";

                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT time
                    FROM `{pq_source}`.`{input_topic}`
                    WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (time String NOT NULL)
                    )
                    WHERE time LIKE "%lunch%";
                END DO;
            )sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        std::vector<std::string> firstBatch;
        for (ui32 p = 0; p < partitionCount; ++p) {
            firstBatch.push_back(fmt::format("lunch time {}", p));
            WriteTopicMessage(inputTopicName, fmt::format(R"({{"time": "lunch time {}"}})", p), p);
        }
        ReadTopicMessages(outputTopicName, firstBatch, TInstant::Now() - TDuration::Seconds(100), /* sort */ true);

        Sleep(CheckpointPeriod * 3);

        ExecQuery(fmt::format(
            R"sql(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);
            )sql",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(1, 0);
        Sleep(TDuration::MilliSeconds(500));

        std::vector<std::string> secondBatch;
        for (ui32 p = 0; p < partitionCount; ++p) {
            secondBatch.push_back(fmt::format("next lunch time {}", p));
            WriteTopicMessage(inputTopicName, fmt::format(R"({{"time": "next lunch time {}"}})", p), p);
        }

        ExecQuery(fmt::format(
            R"sql(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);
            )sql",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(2, 1);  // 2 executions (initial + restarted), 1 lease (running)
        std::vector<std::string> allExpected;
        for (const auto& s : firstBatch) {
            allExpected.push_back(s);
        }
        for (const auto& s : secondBatch) {
            allExpected.push_back(s);
        }
        ReadTopicMessages(outputTopicName, allExpected, TInstant::Now() - TDuration::Seconds(100), /* sort */ true);
    }

    Y_UNIT_TEST_F(IdleTimeoutPartitionSessionBalancer, TStreamingTestFixture) {
        {
            auto& appConfig = SetupAppConfig();
            appConfig.MutableTableServiceConfig()->SetEnableWatermarks(true);
            appConfig.MutableTableServiceConfig()->SetEnableStreamingPartitionBalancing(true);
        }

        constexpr ui32 partitionCount = 2;
        constexpr char inputTopicName[] = "idleTimeoutBalancerInputTopic";
        constexpr char outputTopicName[] = "idleTimeoutBalancerOutputTopic";
        CreateTopic(inputTopicName, NTopic::TCreateTopicSettings()
            .PartitioningSettings(partitionCount, partitionCount));
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(
            R"sql(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    PRAGMA pq.MaxPartitionReadSkew = "10s";

                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT key || value
                    FROM `{pq_source}`.`{input_topic}`
                    WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        ),
                        WATERMARK_IDLE_TIMEOUT = "PT5S"
                    );
                END DO;
            )sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        std::vector<std::string> expectedOutputs;
        for (ui32 i = 0; i < 10; ++i) {
            std::string value = fmt::format("v{}", i);
            expectedOutputs.push_back("k" + value);
            WriteTopicMessage(inputTopicName, fmt::format(R"({{"key": "k", "value": "{}"}})", value), 0);
            ReadTopicMessages(outputTopicName, expectedOutputs);
        }
    }

    Y_UNIT_TEST_F(StreamingPartitionBalancingDisabled, TStreamingTestFixture) {
        SetupAppConfig().MutableTableServiceConfig()->SetEnableStreamingPartitionBalancing(false);

        constexpr char inputTopicName[] = "streamingPartitionBalancingDisabledInputTopic";
        constexpr char outputTopicName[] = "streamingPartitionBalancingDisabledOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(
            R"sql(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    PRAGMA pq.MaxPartitionReadSkew = "10s";

                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT * FROM `{pq_source}`.`{input_topic}`;
                END DO;
            )sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), NYdb::Dev::EStatus::PRECONDITION_FAILED, "Streaming partition balancing is disabled. Please contact your system administrator to enable it");
    }

    Y_UNIT_TEST_F(MaxStreamingQueryExecutionsLimit, TStreamingTestFixture) {
        constexpr ui64 executionsLimit = 3;
        constexpr char inputTopicName[] = "maxStreamingQueryExecutionsLimitInputTopic";
        constexpr char outputTopicName[] = "maxStreamingQueryExecutionsLimitOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
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

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        std::vector<std::string> messages = {"key1value1"};
        messages.reserve(2 * executionsLimit + 1);
        for (ui64 i = 0; i < 2 * executionsLimit; ++i) {
            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    FORCE = TRUE
                ) AS
                DO BEGIN
                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT value || key FROM `{pq_source}`.`{input_topic}` WITH (
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

            const ui64 id = i + 2;
            CheckScriptExecutionsCount(std::min(id, executionsLimit + 1), 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, fmt::format(R"({{"key": "key{}", "value": "value{}"}})", id, id));

            messages.emplace_back(TStringBuilder() << "value" << id << "key" << id);
            ReadTopicMessages(outputTopicName, messages);
        }
    }

    Y_UNIT_TEST_F(CreateStreamingQueryWithDefineAction, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndAlterStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndAlterStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                DEFINE ACTION $start_query($add) AS
                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT key || value || $add FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        )
                    )
                END DEFINE;

                DO $start_query("Add1")
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1Add1"});
    }

    Y_UNIT_TEST_F(CreateStreamingQueryMatchRecognize, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createStreamingQueryMatchRecognizeInputTopic";
        constexpr char outputTopicName[] = "createStreamingQueryMatchRecognizeOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;
                PRAGMA FeatureR010="prototype";

                $matches = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        key Uint64 NOT NULL,
                        value String NOT NULL
                    )
                ) MATCH_RECOGNIZE(
                    MEASURES
                        LAST(V1.key) as v1,
                        LAST(V4.key) as v4
                    ONE ROW PER MATCH
                    PATTERN (V1 V? V4)
                    DEFINE
                        V1 as V1.value = "value1",
                        V as True,
                        V4 as V4.value = "value4"
                );

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT UNWRAP(CAST(v1 AS String) || "-" || CAST(v4 AS String)) FROM $matches;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessages(inputTopicName, {
            R"({"key": 1, "value": "value1"})",
            R"({"key": 2, "value": "value2"})",
            R"({"key": 4, "value": "value4"})",
        });
        ReadTopicMessages(outputTopicName, {"1-4"});
    }

    Y_UNIT_TEST_F(StreamingQueryReplaceAfterError, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndAlterStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndAlterStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 1, "tasks": 32 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Invalid override planner settings");

        CheckScriptExecutionsCount(1, 0);

        const auto streamingQueryDesc = Navigate(GetRuntime(), GetRuntime().AllocateEdgeActor(), JoinPath({"Root", queryName}), NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
        const auto& streamingQuery = streamingQueryDesc->ResultSet.at(0);
        UNIT_ASSERT_VALUES_EQUAL(streamingQuery.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindStreamingQuery);
        UNIT_ASSERT(streamingQuery.StreamingQueryInfo);
        UNIT_ASSERT_VALUES_EQUAL(streamingQuery.StreamingQueryInfo->Description.GetName(), queryName);

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 32 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "test message");
        ReadTopicMessage(outputTopicName, "test message");
    }

    Y_UNIT_TEST_F(StreamingQueryTextChangeWithCreateOrReplace, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createAndReplaceStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndReplaceStreamingQueryOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
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

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT value || key FROM `{pq_source}`.`{input_topic}` WITH (
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

        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "key2", "value": "value2"})");
        ReadTopicMessages(outputTopicName, {"key1value1", "value2key2"});
    }

    Y_UNIT_TEST_F(StreamingQueryCreateOrReplaceFailure, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "createOrReplaceStreamingQueryFailInputTopic";
        constexpr char outputTopicName[] = "createOrReplaceStreamingQueryFailOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "key1value1");
        ReadTopicMessages(outputTopicName, {"key1value1"});

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 10, "tasks": 1 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Invalid override planner settings");

        CheckScriptExecutionsCount(2, 0);
    }

    Y_UNIT_TEST_F(StreamingQueryWithSolomonInsert, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "streamingQuerySolomonInsertInputTopic";
        CreateTopic(inputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char solomonSinkName[] = "sinkName";
        CreateSolomonSource(solomonSinkName);

        constexpr char queryName[] = "streamingQuery";
        const TSolomonLocation soLocation = {
            .ProjectId = "cloudId1",
            .FolderId = "folderId1",
            .Service = "custom",
            .IsCloud = false,
        };
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
                SELECT
                    Unwrap(CAST(Data AS Uint64)) AS value,
                    "test-solomon-insert" AS sensor,
                    Timestamp("2025-03-12T14:40:39Z") AS ts
                FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "solomon_sink"_a = solomonSinkName,
            "solomon_project"_a = soLocation.ProjectId,
            "solomon_folder"_a = soLocation.FolderId,
            "solomon_service"_a = soLocation.Service,
            "input_topic"_a = inputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        CleanupSolomon(soLocation);
        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "1234");

        Sleep(TDuration::Seconds(2));

        std::string expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-solomon-insert"
      ]
    ],
    "ts": 1741790439,
    "value": 1234
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(soLocation), expectedMetrics);
        CleanupSolomon(soLocation);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, "4321");
        Sleep(TDuration::Seconds(2));

        expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-solomon-insert"
      ]
    ],
    "ts": 1741790439,
    "value": 4321
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(soLocation), expectedMetrics);
    }

    Y_UNIT_TEST_F(StreamingQueryWithS3Insert, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "streamingQueryS3InsertInputTopic";
        constexpr char pqSourceName[] = "sourceName";
        CreateTopic(inputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char sourceBucket[] = "test_bucket_streaming_query_s3_insert";
        constexpr char s3SinkName[] = "sinkName";
        CreateBucket(sourceBucket);
        CreateS3Source(sourceBucket, s3SinkName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{s3_sink}`.`test/` WITH (
                    FORMAT = raw
                ) SELECT
                    Data
                FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_sink"_a = s3SinkName,
            "input_topic"_a = inputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "1234");
        Sleep(TDuration::Seconds(2));

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "1234");

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, "4321");
        Sleep(TDuration::Seconds(2));

        if (const auto& s3Data = GetAllObjects(sourceBucket); !IsIn({"12344321", "43211234"}, s3Data)) {
            UNIT_FAIL("Unexpected S3 data: " << s3Data);
        }

        const auto& keys = GetObjectKeys(sourceBucket);
        UNIT_ASSERT_VALUES_EQUAL(keys.size(), 2);
        for (const auto& key : keys) {
            UNIT_ASSERT_STRING_CONTAINS(key, "test/");
            UNIT_ASSERT_C(!key.substr(5).Contains("/"), key);
        }
    }

    Y_UNIT_TEST_F(StreamingQueryWithS3Join, TStreamingTestFixture) {
        // Test that defaults are overridden for streaming queries
        auto& setting = *SetupAppConfig().MutableKQPConfig()->AddSettings();
        setting.SetName("HashJoinMode");
        setting.SetValue("grace");

        const auto pqGateway = SetupMockPqGateway();

        constexpr char sourceBucket[] = "test_streaming_query_with_s3_join";
        constexpr char objectContent[] = R"(
{"fqdn": "host1.example.com", "payload": "P1"}
{"fqdn": "host2.example.com", "payload": "P2"}
{"fqdn": "host3.example.com", "payload": "P3"})";
        CreateBucketWithObject(sourceBucket, "path/test_object.json", objectContent);

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char s3SourceName[] = "s3Source";
        CreatePqSource(pqSourceName);
        CreateS3Source(sourceBucket, s3SourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                $s3_lookup = SELECT * FROM `{s3_source}`.`path/` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        fqdn String,
                        payload String
                    )
                );

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN $s3_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_source"_a = s3SourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const std::vector<IMockPqReadSession::TMessage> sampleMessages = {
            {0, R"({"time": 0, "event": "A", "host": "host1.example.com"})"},
            {1, R"({"time": 1, "event": "B", "host": "host3.example.com"})"},
            {2, R"({"time": 2, "event": "A", "host": "host1.example.com"})"},
        };
        readSession->AddDataReceivedEvent(sampleMessages);

        const std::vector<TString> sampleResult = {"A-P1", "B-P3", "A-P1"};
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(sampleMessages);
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);
    }

    Y_UNIT_TEST_F(StreamingQueryWithYdbJoin, TStreamingTestFixture) {
        // Test that defaults are overridden for streaming queries
        auto& setting = *SetupAppConfig().MutableKQPConfig()->AddSettings();
        setting.SetName("HashJoinMode");
        setting.SetValue("grace");

        const auto connectorClient = SetupMockConnectorClient();
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "lookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock

            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                // For stream queries type annotation is executed twice, but
                // now List Split is done after type annotation optimization.
                // That is why only single call to List Split is expected.
                .ListSplitsCount = 1
            });

            const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
            const std::vector<std::string> payloadColumn = {"P1", "P2", "P3"};
            SetupMockConnectorTableData(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .NumberReadSplits = 2,
                .ResultFactory = [&]() {
                    return MakeRecordBatch(
                        MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()),
                        MakeArray<arrow::BinaryBuilder>("payload", payloadColumn, arrow::binary())
                    );
                }
            });
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                $ydb_lookup = SELECT * FROM `{ydb_source}`.`{ydb_table}`;

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN $ydb_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const std::vector<IMockPqReadSession::TMessage> sampleMessages = {
            {0, R"({"time": 0, "event": "A", "host": "host1.example.com"})"},
            {1, R"({"time": 1, "event": "B", "host": "host3.example.com"})"},
            {2, R"({"time": 2, "event": "A", "host": "host1.example.com"})"},
        };
        readSession->AddDataReceivedEvent(sampleMessages);

        const std::vector<TString> sampleResult = {"A-P1", "B-P3", "A-P1"};
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(sampleMessages);
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);
    }

    Y_UNIT_TEST_F(StreamingQueryWithDoubleYdbJoin, TStreamingTestFixture) {
        const auto connectorClient = SetupMockConnectorClient();
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "doubleYdbJoinInputTopicName";
        constexpr char outputTopicName[] = "doubleYdbJoinOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "doubleYdbJoinLookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {{"fqdn", Ydb::Type::STRING}};
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                .ListSplitsCount = 1
            });

            const std::vector<std::string> fqdnColumn = {"host1", "host2"};
            SetupMockConnectorTableData(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .NumberReadSplits = 4, // Read from ydb source is not deduplicated because spilling is disabled for streaming queries
                .ResultFactory = [&]() {
                    return MakeRecordBatch(MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()));
                }
            });
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    p.Data || "-" || la.fqdn || "-" || lb.fqdn
                FROM `{pq_source}`.`{input_topic}` AS p
                CROSS JOIN `{ydb_source}`.`{ydb_table}` AS la
                CROSS JOIN `{ydb_source}`.`{ydb_table}` AS lb
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "data1");

        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages({
            "data1-host1-host2",
            "data1-host2-host1",
            "data1-host1-host1",
            "data1-host2-host2"
        }, /* sort  */ true);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, "data2");
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages({
            "data2-host1-host2",
            "data2-host2-host1",
            "data2-host1-host1",
            "data2-host2-host2"
        }, /* sort  */ true);
    }

    Y_UNIT_TEST_TWIN_F(StreamingQueryWithStreamLookupJoin, WithFeatureFlag, TStreamingTestFixture) {
        {
            auto& setupAppConfig = SetupAppConfig();
            setupAppConfig.MutableQueryServiceConfig()->SetProgressStatsPeriodMs(0);
            if (WithFeatureFlag) {
                setupAppConfig.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(true);
            }
        }

        const auto connectorClient = SetupMockConnectorClient();
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "sljInputTopicName";
        constexpr char outputTopicName[] = "sljOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "lookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                .ListSplitsCount = WithFeatureFlag ? 7 : 0,
                .ValidateListSplitsArgs = false
            });

            if (WithFeatureFlag) {
                ui64 readSplitsCount = 0;
                const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
                SetupMockConnectorTableData(connectorClient, {
                    .TableName = ydbTable,
                    .Columns = columns,
                    .NumberReadSplits = 6,
                    .ValidateReadSplitsArgs = false,
                    .ResultFactory = [&]() {
                        readSplitsCount += 1;
                        const auto payloadColumn = readSplitsCount <= 4
                            ? std::vector<std::string>{"P1", "P2", "P3"}
                            : std::vector<std::string>{"P4", "P5", "P6"};

                        return MakeRecordBatch(
                            MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()),
                            MakeArray<arrow::BinaryBuilder>("payload", payloadColumn, arrow::binary())
                        );
                    }
                });
            }
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $ydb_lookup = SELECT * FROM `{ydb_source}`.`{ydb_table}`;

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN /*+ streamlookup(TTL 1) */ ANY $ydb_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ),
        WithFeatureFlag ? EStatus::SUCCESS : EStatus::GENERIC_ERROR,
        WithFeatureFlag ? "" : "Unsupported join strategy: streamlookup");
        if (!WithFeatureFlag) {
            return;
        }

        CheckScriptExecutionsCount(1, 1);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const std::vector<IMockPqReadSession::TMessage> sampleMessages = {
            {0, R"({"time": 0, "event": "A", "host": "host1.example.com"})"},
            {1, R"({"time": 1, "event": "B", "host": "host3.example.com"})"},
            {2, R"({"time": 2, "event": "A", "host": "host1.example.com"})"},
        };
        readSession->AddDataReceivedEvent(sampleMessages);

        const std::vector<TString> sampleResult = {"A-P1", "B-P3", "A-P1"};
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessages(sampleResult);

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(sampleMessages);
        auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        writeSession->ExpectMessages(sampleResult);

        Sleep(TDuration::Seconds(2));
        readSession->AddDataReceivedEvent(sampleMessages);
        writeSession->ExpectMessages({"A-P4", "B-P6", "A-P4"});

        CheckScriptExecutionsCount(1, 1);
        const auto results = ExecQuery(
            "SELECT ast_compressed FROM `.metadata/script_executions`;"
        );
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        CheckScriptResult(results[0], 1, 1, [](TResultSetParser& result) {
            const auto& ast = result.ColumnParser(0).GetOptionalString();
            UNIT_ASSERT(ast);
            UNIT_ASSERT_STRING_CONTAINS(*ast, "DqCnStreamLookup");
        });
    }

    Y_UNIT_TEST_F(StreamingQueryWithLocalYdbJoin, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "streamingQueryWithLocalYdbJoinInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithLocalYdbJoinOutputTopic";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char streamLookupTableName[] = "oltpStreamLookupTable";
        constexpr char oltpTableName[] = "oltpTable";
        constexpr char olapTableName[] = "olapTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{oltp_streamlookup_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `{oltp_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Value)
            );
            CREATE TABLE `{olap_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = COLUMN
            );)",
            "oltp_streamlookup_table"_a = streamLookupTableName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{oltp_streamlookup_table}`(Key, Value)
            VALUES (1, "oltp_slj1"), (2, "oltp_slj2");

            UPSERT INTO `{oltp_table}`(Key, Value)
            VALUES (1, "oltp1"), (2, "oltp2");

            INSERT INTO `{olap_table}`(Key, Value)
            VALUES (1, "olap1"), (2, "olap2");)",
            "oltp_streamlookup_table"_a = streamLookupTableName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                PRAGMA ydb.DqChannelVersion = "1";

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    Unwrap(oltp_slj.Value || "-" || oltp.Value || "-" || olap.Value)
                FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = json_each_row,
                    SCHEMA (
                        Key Int32 NOT NULL
                    )
                ) AS topic
                LEFT JOIN `{oltp_streamlookup_table}` AS oltp_slj ON topic.Key = oltp_slj.Key
                LEFT JOIN `{oltp_table}` AS oltp ON topic.Key = oltp.Key
                LEFT JOIN `{olap_table}` AS olap ON topic.Key = olap.Key
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "oltp_streamlookup_table"_a = streamLookupTableName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"Key": 1})");
        ReadTopicMessage(outputTopicName, "oltp_slj1-oltp1-olap1");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        WriteTopicMessage(inputTopicName, R"({"Key": 2})");
        const auto disposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName, "oltp_slj2-oltp2-olap2", disposition);
    }

    Y_UNIT_TEST_F(StreamingQueryWithPrecompute, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "streamingQueryWithPrecomputeInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithPrecomputeOutputTopic";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char tableName[] = "oltpTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{table_name}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );)",
            "table_name"_a = tableName
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{table_name}`
                (Key, Value)
            VALUES
                (1, "value-1");)",
            "table_name"_a = tableName
        ));

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $r = SELECT Value FROM `{table_name}`;

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    Unwrap(Data || "-" || $r)
                FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "table_name"_a = tableName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "message-1");
        ReadTopicMessage(outputTopicName, "message-1-value-1");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{table_name}`
                (Key, Value)
            VALUES
                (1, "value-2");)",
            "table_name"_a = tableName
        ));

        WriteTopicMessage(inputTopicName, "message-2");
        const auto disposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName, "message-2-value-2", disposition);
    }

    Y_UNIT_TEST_F(StreamingQueryUnderSecureScriptExecutions, TStreamingTestFixture) {
        auto& appConfig = SetupAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableSecureScriptExecutions(true);
        GetRuntime().GetAppData().FeatureFlags.SetEnableSecureScriptExecutions(true);

        constexpr char inputTopicName[] = "streamingQueryUnderSecureScriptExecutionsInputTopic";
        constexpr char outputTopicName[] = "streamingQueryUnderSecureScriptExecutionsOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessage(outputTopicName, R"({"key": "key1", "value": "value1"})");

        NOperation::TOperationClient rootClient(*GetInternalDriver(), TCommonClientSettings().AuthToken(BUILTIN_ACL_ROOT));
        {
            const auto result = rootClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetList()[0].Metadata().ExecStatus, EExecStatus::Running);
        }

        NOperation::TOperationClient testClient(*GetInternalDriver(), TCommonClientSettings().AuthToken("test@" BUILTIN_ACL_DOMAIN));
        {
            const auto result = testClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0);
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            ))",
            "query_name"_a = queryName
        ));

        {
            const auto result = rootClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(result.GetList()[0].Metadata().ExecStatus, EExecStatus::Canceled);
        }

        {
            const auto result = testClient.List<TScriptExecutionOperation>(10).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetList().size(), 0);
        }

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");

        const auto testNoAccess = [&]() {
            ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/queries`", EStatus::SCHEME_ERROR, "Cannot find table");
            ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/checkpoints/checkpoints_metadata`", EStatus::SCHEME_ERROR, "Cannot find table");
        };
        const auto testAccessAllowed = [&]() {
            const auto& resultQueries = ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/queries`");
            UNIT_ASSERT_VALUES_EQUAL(resultQueries.size(), 1);

            CheckScriptResult(resultQueries[0], 1, 1, [](TResultSetParser& parser) {
                UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser(0).GetUint64(), 1);
            });

            const auto& resultCheckpoints = ExecQuery("SELECT COUNT(*) FROM `.metadata/streaming/checkpoints/checkpoints_metadata`");
            UNIT_ASSERT_VALUES_EQUAL(resultCheckpoints.size(), 1);
        };
        const auto switchAccess = [&](bool allowed) {
            auto& runtime = GetRuntime();
            runtime.GetAppData().FeatureFlags.SetEnableSecureScriptExecutions(!allowed);

            appConfig.MutableFeatureFlags()->SetEnableSecureScriptExecutions(!allowed);

            UpdateConfig(appConfig);

            Sleep(TDuration::Seconds(1));

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = FALSE
                ))",
                "query_name"_a = queryName
            ));
        };

        testNoAccess();

        switchAccess(/* allowed */ true);
        testAccessAllowed();

        switchAccess(/* allowed */ false);
        testNoAccess();
    }

    Y_UNIT_TEST_F(OffsetsRecoveryAfterManualAndInternalRetry, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "offsetsRecoveryAfterManualAndInternalRetry,InputTopic";
        constexpr char outputTopicName[] = "offsetsRecoveryAfterManualAndInternalRetry,OutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char consumerName[] = "unknownConsumer";
        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA pq.Consumer = "{consumer_name}";
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "consumer_name"_a = consumerName
        ));

        WaitFor(TDuration::Seconds(10), "Wait fail", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Issues FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            std::string issues;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                issues = resultSet.ColumnParser("Issues").GetOptionalUtf8().value_or("");
            });

            error = TStringBuilder() << "Query issues: " << issues;
            return issues.contains("no read rule provided for consumer 'unknownConsumer' in topic");
        });

        ExecExternalQuery(fmt::format(R"(
            ALTER TOPIC `{input_topic}` ADD CONSUMER `{consumer_name}`;)",
            "input_topic"_a = inputTopicName,
            "consumer_name"_a = consumerName
        ));

        WaitFor(TDuration::Seconds(10), "Wait fail", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Status FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            std::string status;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                status = *resultSet.ColumnParser("Status").GetOptionalUtf8();
            });

            error = TStringBuilder() << "Query status: " << status;
            return status == "RUNNING";
        });

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessage(outputTopicName, R"({"key": "key1", "value": "value1"})");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, R"({"key": "key2", "value": "value2"})");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));

        ReadTopicMessage(outputTopicName, R"({"key": "key2", "value": "value2"})", disposition);
    }

    Y_UNIT_TEST_F(OffsetsAndStateRecoveryOnInternalRetry, TStreamingTestFixture) {
        QueryClientSettings = TClientSettings();

        // Join with S3 used for introducing temporary failure and force retry on specific key

        constexpr char sourceBucket[] = "test_streaming_query_recovery_on_internal_retry";
        constexpr char objectContent[] = R"(
{"fqdn": "host1.example.com", "payload": "P1"}
{"fqdn": "host2.example.com"                              })";
        constexpr char objectPath[] = "path/test_object.json";
        CreateBucketWithObject(sourceBucket, objectPath, objectContent);

        constexpr char inputTopicName[] = "internalRetryInputTopicName";
        constexpr char outputTopicName[] = "internalRetryOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char s3SourceName[] = "s3Source";
        CreatePqSource(pqSourceName);
        CreateS3Source(sourceBucket, s3SourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.HashJoinMode = "map";
                $s3_lookup = SELECT * FROM `{s3_source}`.`path/` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        fqdn String NOT NULL,
                        payload String
                    )
                );

                -- Test that offsets are recovered
                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT
                    Unwrap(l.payload) AS payload, -- Test failure here
                    p.*
                FROM $pq_source AS p
                LEFT JOIN $s3_lookup AS l
                ON (l.fqdn = p.host);

                -- Test that state also recovered
                $grouped = SELECT
                    event,
                    CAST(SOME(time) AS String) AS time,
                    SOME(payload) AS payload,
                    CAST(COUNT(*) AS String) AS count
                FROM $joined
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event;

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || time || "-" || payload || "-" || count) FROM $grouped
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_source"_a = s3SourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        // Fill HOP state for key A
        WriteTopicMessages(inputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A", "host": "host1.example.com"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A", "host": "host1.example.com"})",
        });
        ReadTopicMessage(outputTopicName, "A-2025-08-24T00:00:00.000000Z-P1-1");

        Sleep(TDuration::Seconds(2));
        auto readDisposition = TInstant::Now();

        // Write failure message for key B
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-24T00:00:00.000000Z", "event": "B", "host": "host2.example.com"})");

        // Wait script execution retry
        WaitFor(TDuration::Seconds(10), "wait retry", [&](TString& error) {
            const auto& results = ExecQuery(R"(
                SELECT MAX(lease_generation) AS generation FROM `.metadata/script_executions`;
            )");
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

            std::optional<i64> generation;
            CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& result) {
                generation = result.ColumnParser(0).GetOptionalInt64();
            });

            if (!generation || *generation < 2) {
                error = TStringBuilder() << "generation is: " << (generation ? ToString(*generation) : "null");
                return false;
            }

            return true;
        });

        // Resolve query failure
        UploadObject(sourceBucket, objectPath, R"(
{"fqdn": "host1.example.com", "payload": "P1"}
{"fqdn": "host2.example.com", "payload": "P2"             })");
        Sleep(TDuration::Seconds(2));

        // Check that offset is restored
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B", "host": "host2.example.com"})");
        ReadTopicMessage(outputTopicName, "B-2025-08-24T00:00:00.000000Z-P2-1", readDisposition);

        Sleep(TDuration::Seconds(1));
        readDisposition = TInstant::Now();

        // Check that HOP state is restored
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A", "host": "host1.example.com"})");
        ReadTopicMessages(outputTopicName, {
            "A-2025-08-25T00:00:00.000000Z-P1-1",
            "B-2025-08-25T00:00:00.000000Z-P2-1"
        }, readDisposition, /* sort */ true);
    }

    struct TTestInfo {
        std::string InputTopicName;
        std::string OutputTopicName;
        std::string PqSourceName;
        std::string QueryName;
        std::string QueryText;
    };

    TTestInfo SetupCheckpointRecoveryTest(TStreamingTestFixture& self) {
        TTestInfo info = {
            .InputTopicName = TStringBuilder() << "inputTopicName" << self.Name_,
            .OutputTopicName = TStringBuilder() << "outputTopicName" << self.Name_,
            .PqSourceName = "pqSourceName",
            .QueryName = "streamingQuery"
        };
        info.QueryText = fmt::format(R"(
            -- Test that offsets are recovered
            $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                FORMAT = "json_each_row",
                SCHEMA (
                    time String NOT NULL,
                    event String
                )
            );

            -- Test that state also recovered
            $grouped = SELECT
                event,
                CAST(SOME(time) AS String) AS time,
                CAST(COUNT(*) AS String) AS count
            FROM $pq_source
            GROUP BY
                HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                event;

            INSERT INTO `{pq_source}`.`{output_topic}`
            SELECT Unwrap(event || "-" || time || "-" || count) FROM $grouped)",
            "pq_source"_a = info.PqSourceName,
            "input_topic"_a = info.InputTopicName,
            "output_topic"_a = info.OutputTopicName
        );

        self.CreateTopic(info.InputTopicName);
        self.CreateTopic(info.OutputTopicName);
        self.CreatePqSource(info.PqSourceName);

        self.ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                {query_text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "query_text"_a = info.QueryText
        ));
        self.CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        return info;
    }

    Y_UNIT_TEST_F(OffsetsAndStateRecoveryOnManualRestart, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessages(info.OutputTopicName, {
            "A-2025-08-25T00:00:00.000000Z-1",
            "B-2025-08-25T00:00:00.000000Z-1"
        }, readDisposition, /* sort */ true);
    }

    Y_UNIT_TEST_F(OffsetsRecoveryOnQueryTextChangeBasic, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE,
                RUN = FALSE
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);
    }

    Y_UNIT_TEST_F(OffsetsRecoveryOnQueryTextChangeCreateOrReplace, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` WITH (
                RUN = FALSE
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);
    }

    Y_UNIT_TEST_F(OffsetsRecoveryOnQueryTextChangeWithFail, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE,
                RESOURCE_POOL = "unknown_pool"
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ), EStatus::NOT_FOUND, "Resource pool unknown_pool not found or you don't have access permissions");

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RESOURCE_POOL = "default"
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(3, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);
    }

    Y_UNIT_TEST_F(OffsetsAndStateRecoveryAfterQueryTextChange, TStreamingTestFixture) {
        const auto info = SetupCheckpointRecoveryTest(*this);

        WriteTopicMessages(info.InputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})",
        });
        ReadTopicMessage(info.OutputTopicName, "A-2025-08-24T00:00:00.000000Z-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                FORCE = TRUE,
                RUN = FALSE
            ) AS
            DO BEGIN
                /* some comment */
                {text}
            END DO;)",
            "query_name"_a = info.QueryName,
            "text"_a = info.QueryText
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})");
        auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(2, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "B"})");
        readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckScriptExecutionsCount(3, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-27T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessages(info.OutputTopicName, {
            "A-2025-08-26T00:00:00.000000Z-1",
            "B-2025-08-26T00:00:00.000000Z-1"
        }, readDisposition, /* sort */ true);
    }

    Y_UNIT_TEST_F(CheckpointPropagationWithStreamLookupJoinHanging, TStreamingTestFixture) {
        {
            auto& setupAppConfig = SetupAppConfig();
            setupAppConfig.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(true);
        }
        const auto connectorClient = SetupMockConnectorClient();

        constexpr char inputTopicName[] = "sljInputTopicName";
        constexpr char outputTopicName[] = "sljOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char ydbSourceName[] = "ydbSourceName";
        CreatePqSource(pqSourceName);
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "lookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            ))",
            "table"_a = ydbTable
        ));

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .DescribeCount = 2,
                .ListSplitsCount = 7,
                .ValidateListSplitsArgs = false
            });

            const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
            const std::vector<std::string> payloadColumn = std::vector<std::string>{"P1", "P2", "P3"};
            SetupMockConnectorTableData(connectorClient, {
                .TableName = ydbTable,
                .Columns = columns,
                .NumberReadSplits = 6,
                .ValidateReadSplitsArgs = false,
                .ResultFactory = [&]() {
                    return MakeRecordBatch(
                        MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()),
                        MakeArray<arrow::BinaryBuilder>("payload", payloadColumn, arrow::binary())
                    );
                }
            });
        }

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $ydb_lookup = SELECT * FROM `{ydb_source}`.`{ydb_table}`;

                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time Int32 NOT NULL,
                        event String,
                        host String
                    )
                );

                $joined = SELECT l.payload AS payload, p.* FROM $pq_source AS p
                LEFT JOIN /*+ streamlookup(TTL 1) */ ANY $ydb_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"time": 0, "event": "A", "host": "host1.example.com"})");
        ReadTopicMessage(outputTopicName, "A-P1");

        connectorClient->LockReading();
        WriteTopicMessage(inputTopicName, R"({"time": 1, "event": "B", "host": "host3.example.com"})");
        Sleep(TDuration::Seconds(2));
        const auto readDisposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        connectorClient->UnlockReading();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName, "B-P3", readDisposition);
    }

    Y_UNIT_TEST_F(CheckpointPropagationWithS3Insert, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "s3InsertCheckpointsInputTopicName";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char sourceBucket[] = "test_bucket_streaming_query_s3_insert_checkpoint_propagation";
        constexpr char s3SinkName[] = "sinkName";
        CreateBucket(sourceBucket);
        CreateS3Source(sourceBucket, s3SinkName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{s3_sink}`.`test/` WITH (
                    FORMAT = raw
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "s3_sink"_a = s3SinkName,
            "input_topic"_a = inputTopicName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "data-1");
        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "data-1");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        WriteTopicMessage(inputTopicName, "data-2");

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        Sleep(TDuration::Seconds(2));
        if (const auto& s3Data = GetAllObjects(sourceBucket); !IsIn({"data-1data-2", "data-2data-1"}, s3Data)) {
            UNIT_FAIL("Unexpected S3 data: " << s3Data);
        }
    }

    void CheckTable(TStreamingTestFixture& self, const std::string& tableName, const std::map<std::string, std::string>& rows) {
        const auto results = self.ExecQuery(fmt::format(
            "SELECT * FROM `{table}` ORDER BY Key",
            "table"_a = tableName
        ));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

        auto it = rows.begin();
        self.CheckScriptResult(results[0], 2, rows.size(), [&](TResultSetParser& parser) {
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Key").GetString(), it->first);
            UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Value").GetString(), it->second);
            ++it;
        });
    }

    Y_UNIT_TEST_F(WritingInLocalYdbTablesWithCheckpoints, TStreamingTestFixture) {
        constexpr char pqSourceName[] = "pqSource";
        CreatePqSource(pqSourceName);

        for (const bool rowTables : {true, false}) {
            const auto inputTopicName = TStringBuilder() << "writingInLocalYdbInputTopicName" << rowTables;
            CreateTopic(inputTopicName);

            const auto ydbTable = TStringBuilder() << "tableSink" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key String NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key)
                ) {settings})",
                "table"_a = ydbTable,
                "settings"_a = rowTables ? "" : "WITH (STORE = COLUMN)"
            ));

            const auto queryName = TStringBuilder() << "streamingQuery" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    UPSERT INTO `{ydb_table}`
                    SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = json_each_row,
                        SCHEMA (
                            Key String NOT NULL,
                            Value String NOT NULL
                        )
                    )
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "ydb_table"_a = ydbTable
            ));

            CheckScriptExecutionsCount(1, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value1"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1", "value1"}});

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = FALSE
                );)",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(1, 0);
            CheckTable(*this, ydbTable, {{"message1", "value1"}});

            Sleep(TDuration::Seconds(1));
            WriteTopicMessage(inputTopicName, R"({"Key": "message2", "Value": "value2"})");

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = TRUE
                );)",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(2, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value3"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1", "value3"}, {"message2", "value2"}});

            ExecQuery(fmt::format(
                "DROP STREAMING QUERY `{query_name}`",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(0, 0);
        }
    }

    Y_UNIT_TEST_F(WritingInLocalYdbTablesWithLimit, TStreamingTestFixture) {
        constexpr char pqSourceName[] = "pqSource";
        CreatePqSource(pqSourceName);

        for (const bool rowTables : {true, false}) {
            const auto inputTopicName = TStringBuilder() << "writingInLocalYdbWithLimitInputTopicName" << rowTables;
            CreateTopic(inputTopicName);

            const auto ydbTable = TStringBuilder() << "tableSink" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key String NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key)
                ) {settings})",
                "table"_a = ydbTable,
                "settings"_a = rowTables ? "" : "WITH (STORE = COLUMN)"
            ));

            const auto queryName = TStringBuilder() << "streamingQuery" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    UPSERT INTO `{ydb_table}`
                    SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = json_each_row,
                        SCHEMA (
                            Key String NOT NULL,
                            Value String NOT NULL
                        )
                    ) LIMIT 1
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "ydb_table"_a = ydbTable
            ));

            CheckScriptExecutionsCount(1, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value1"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1", "value1"}});

            Sleep(TDuration::Seconds(1));
            CheckScriptExecutionsCount(1, 0);

            ExecQuery(fmt::format(
                "DROP STREAMING QUERY `{query_name}`",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(0, 0);
        }
    }

    Y_UNIT_TEST_F(WritingInLocalYdbTablesWithProjection, TStreamingTestFixture) {
        constexpr char pqSourceName[] = "pqSource";
        CreatePqSource(pqSourceName);

        for (const bool rowTables : {true, false}) {
            const auto inputTopicName = TStringBuilder() << "writingInLocalYdbWithLimitInputTopicName" << rowTables;
            CreateTopic(inputTopicName);

            const auto ydbTable = TStringBuilder() << "tableSink" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE TABLE `{table}` (
                    Key String NOT NULL,
                    Value String NOT NULL,
                    PRIMARY KEY (Key)
                ) {settings})",
                "table"_a = ydbTable,
                "settings"_a = rowTables ? "" : "WITH (STORE = COLUMN)"
            ));

            const auto queryName = TStringBuilder() << "streamingQuery" << rowTables;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    UPSERT INTO `{ydb_table}`
                    SELECT (Key || "x") AS Key, Value FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = json_each_row,
                        SCHEMA (
                            Key String NOT NULL,
                            Value String NOT NULL
                        )
                    ) LIMIT 1
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "ydb_table"_a = ydbTable
            ));

            CheckScriptExecutionsCount(1, 1);
            Sleep(TDuration::Seconds(1));

            WriteTopicMessage(inputTopicName, R"({"Key": "message1", "Value": "value1"})");
            Sleep(TDuration::Seconds(1));
            CheckTable(*this, ydbTable, {{"message1x", "value1"}});

            Sleep(TDuration::Seconds(1));
            CheckScriptExecutionsCount(1, 0);

            ExecQuery(fmt::format(
                "DROP STREAMING QUERY `{query_name}`",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(0, 0);
        }
    }

    Y_UNIT_TEST_F(DropStreamingQueryUnderLoad, TStreamingTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableQueryServiceConfig()->SetProgressStatsPeriodMs(1);

        constexpr char inputTopicName[] = "inputTopic";
        constexpr char outputTopicName[] = "outputTopic";
        constexpr char pqSourceName[] = "pqSource";
        ExecQuery(fmt::format(R"(
            CREATE TOPIC `{input_topic}` WITH (
                min_active_partitions = 100,
                partition_count_limit = 100
            );
            CREATE TOPIC `{output_topic}` WITH (
                min_active_partitions = 100,
                partition_count_limit = 100
            );
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "NONE"
            );)",
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "pq_source"_a = pqSourceName,
            "pq_location"_a = GetKikimrRunner()->GetEndpoint(),
            "pq_database_name"_a = "/Root"
        ));

        const auto queryName = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "100";
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 100 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        auto promise = NThreading::NewPromise();
        Y_DEFER {
            promise.SetValue();
        };

        for (ui32 i = 0; i < 10; ++i) {
            GetRuntime().Register(new TTestTopicLoader(GetKikimrRunner()->GetEndpoint(), "/Root", inputTopicName, promise.GetFuture()));
        }

        Sleep(TDuration::Seconds(2));
        CheckScriptExecutionsCount(1, 1);

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(0, 0);
    }

    Y_UNIT_TEST_F(CreateStreamingQueryUnderTimeout, TStreamingWithSchemaSecretsTestFixture) {
        auto& config = *SetupAppConfig().MutableQueryServiceConfig();
        config.SetQueryTimeoutDefaultSeconds(3);
        config.SetScriptOperationTimeoutDefaultSeconds(3);

        constexpr char inputTopicName[] = "createStreamingQueryUnderTimeoutInputTopic";
        constexpr char outputTopicName[] = "createStreamingQueryUnderTimeoutOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(5));

        WriteTopicMessage(inputTopicName, "data1");
        ReadTopicMessage(outputTopicName, "data1");
        Sleep(TDuration::Seconds(5));

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        const auto& result = ExecQuery("SELECT RetryCount FROM `.sys/streaming_queries`");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("RetryCount").GetOptionalUint64(), 0);
        });
    }

    Y_UNIT_TEST_F(StreamingQueryDispositionDisabled, TStreamingWithSchemaSecretsTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableStreamingQueryDisposition(false);

        constexpr char inputTopicName[] = "createStreamingQueryDispositionDisabledInputTopic";
        constexpr char outputTopicName[] = "createStreamingQueryDispositionDisabledOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `my_query` WITH (
                STREAMING_DISPOSITION = OLDEST
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Streaming query disposition is disabled. Please contact your system administrator to enable it");
    }

    Y_UNIT_TEST_F(StreamingQueryDisposition, TStreamingWithSchemaSecretsTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableStreamingQueryDisposition(true);

        constexpr char inputTopicName[] = "createStreamingQueryDispositionInputTopic";
        constexpr char outputTopicName[] = "createStreamingQueryDispositionOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        ui64 dataIdx = 0;
        WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);
        Sleep(TDuration::Seconds(1));

        const auto readDisposition = TInstant::Now();
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);
        Sleep(TDuration::Seconds(1));

        // Test OLDEST disposition
        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                STREAMING_DISPOSITION = OLDEST
            ) AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));
        ui64 executionsCount = 0;
        CheckScriptExecutionsCount(++executionsCount, 1);

        ReadTopicMessages(outputTopicName, {"data1", "data2"});
        auto writeDisposition = TInstant::Now();

        // Test FROM_TIME disposition
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                STREAMING_DISPOSITION = (
                    FROM_TIME = "{disposition}"
                )
            );)",
            "query_name"_a = queryName,
            "disposition"_a = readDisposition.ToString()
        ));
        CheckScriptExecutionsCount(++executionsCount, 1);

        ReadTopicMessage(outputTopicName, "data2", writeDisposition);
        writeDisposition = TInstant::Now();

        // Test TIME_AGO disposition
        const auto duration = TInstant::Now() - readDisposition;
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                STREAMING_DISPOSITION = (
                    TIME_AGO = "PT{disposition}S"
                )
            );)",
            "query_name"_a = queryName,
            "disposition"_a = TStringBuilder() << duration.Seconds() << "." << duration.MicroSecondsOfSecond()
        ));
        CheckScriptExecutionsCount(++executionsCount, 1);

        ReadTopicMessage(outputTopicName, "data2", writeDisposition);
        writeDisposition = TInstant::Now();

        // Test checkpoint dispositions
        for (const std::string& disposition : {"", "FROM_CHECKPOINT", "FROM_CHECKPOINT_FORCE"}) {
            Sleep(TDuration::Seconds(1));
            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = FALSE
                );)",
                "query_name"_a = queryName
            ));
            CheckScriptExecutionsCount(std::min(executionsCount, (ui64)4), 0);

            WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    RUN = TRUE,
                    {disposition}
                );)",
                "query_name"_a = queryName,
                "disposition"_a = disposition.empty() ? TStringBuilder() : TStringBuilder() << "STREAMING_DISPOSITION = " << disposition
            ));
            CheckScriptExecutionsCount(std::min(++executionsCount, (ui64)4), 1);

            ReadTopicMessage(outputTopicName, TStringBuilder() << "data" << dataIdx, writeDisposition);
            writeDisposition = TInstant::Now();
        }

        // Test fresh disposition
        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                STREAMING_DISPOSITION = FRESH
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(std::min(++executionsCount, (ui64)4), 1);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopicName, TStringBuilder() << "data" << ++dataIdx);
        ReadTopicMessage(outputTopicName, TStringBuilder() << "data" << dataIdx, writeDisposition);
    }

    Y_UNIT_TEST_F(StreamingQueryWithMultipleWrites, TStreamingWithSchemaSecretsTestFixture) {
        constexpr char inputTopic[] = "createStreamingQueryWithMultipleWritesInputTopic";
        constexpr char outputTopic1[] = "createStreamingQueryWithMultipleWritesOutputTopic1";
        constexpr char outputTopic2[] = "createStreamingQueryWithMultipleWritesOutputTopic2";
        constexpr char pqSource[] = "sourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic1);
        CreateTopic(outputTopic2);
        CreatePqSource(pqSource);

        constexpr char sinkBucket[] = "test_bucket_streaming_query_multi_insert";
        constexpr char s3SinkName[] = "s3SinkName";
        CreateBucket(sinkBucket);
        CreateS3Source(sinkBucket, s3SinkName);

        constexpr char solomonSink[] = "solomonSinkName";
        CreateSolomonSource(solomonSink);

        constexpr char rowSinkTable[] = "rowSink";
        constexpr char columnSinkTable[] = "columnSink";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{row_table}` (
                B Utf8 NOT NULL,
                PRIMARY KEY (B)
            );
            CREATE TABLE `{column_table}` (
                C String NOT NULL,
                PRIMARY KEY (C)
            ) WITH (
                STORE = COLUMN
            );)",
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable
        ));

        constexpr char queryName[] = "streamingQuery";
        const TSolomonLocation soLocation = {
            .ProjectId = "cloudId1",
            .FolderId = "folderId1",
            .Service = "custom",
            .IsCloud = false,
        };
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $rows = SELECT * FROM `{pq_source}`.`{input_topic}`;

                INSERT INTO `{pq_source}`.`{output_topic1}` SELECT Data || "-A" AS X FROM $rows;

                INSERT INTO `{pq_source}`.`{output_topic2}` SELECT Data || "-B" AS Y FROM $rows;

                UPSERT INTO `{row_table}` SELECT Unwrap(CAST(Data || "-C" AS Utf8)) AS B FROM $rows;

                UPSERT INTO `{column_table}` SELECT Data || "-D" AS C FROM $rows;

                INSERT INTO `{s3_sink}`.`test/` WITH (
                    FORMAT = raw
                ) SELECT Data || "-E" AS D FROM $rows;

                INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
                SELECT
                    42 AS value,
                    Data || "-F" AS sensor,
                    Timestamp("2025-03-12T14:40:39Z") AS ts
                FROM $rows;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSource,
            "input_topic"_a = inputTopic,
            "output_topic1"_a = outputTopic1,
            "output_topic2"_a = outputTopic2,
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable,
            "s3_sink"_a = s3SinkName,
            "solomon_sink"_a = solomonSink,
            "solomon_project"_a = soLocation.ProjectId,
            "solomon_folder"_a = soLocation.FolderId,
            "solomon_service"_a = soLocation.Service
        ));
        CheckScriptExecutionsCount(1, 1);

        CleanupSolomon(soLocation);
        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopic, "test");
        ReadTopicMessage(outputTopic1, "test-A");
        ReadTopicMessage(outputTopic2, "test-B");

        const auto& results = ExecQuery(fmt::format(R"(
            SELECT * FROM `{row_table}`;
            SELECT * FROM `{column_table}`;)",
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable
        ));
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);

        CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("B").GetUtf8(), "test-C");
        });

        CheckScriptResult(results[1], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("C").GetString(), "test-D");
        });

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sinkBucket), "test-E");

        const std::string expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-F"
      ]
    ],
    "ts": 1741790439,
    "value": 42
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(soLocation), expectedMetrics);
    }

    Y_UNIT_TEST_F(DropStreamingQueryDuringRetries, TStreamingWithSchemaSecretsTestFixture) {
        constexpr char topic[] = "dropStreamingQueryDuringRetriesTopic";
        constexpr char pqSource[] = "pqSource";
        CreateTopic(topic);
        CreatePqSource(pqSource);

        const auto queryName = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA pq.Consumer = "test-consumer";
                INSERT INTO `{pq_source}`.`{topic}`
                SELECT * FROM `{pq_source}`.`{topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSource,
            "topic"_a = topic
        ));

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        Sleep(TDuration::Seconds(3));

        std::random_device rng;
        for (;;) {
            const auto& result = ExecQuery("SELECT RetryCount, SuspendedUntil, Issues FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            bool ok = false;
            CheckScriptResult(result[0], 3, 1, [&](TResultSetParser& resultSet) {
                Cerr << "Now " << TInstant::Now();
                if (auto suspendedUntil = resultSet.ColumnParser("SuspendedUntil").GetOptionalTimestamp()) {
                    Cerr << " SuspendedUntil " << *suspendedUntil;
                    ok = *suspendedUntil > TInstant::Now() + TDuration::MilliSeconds(500);
                    UNIT_ASSERT(*suspendedUntil);
                }
                if (auto retryCount = resultSet.ColumnParser("RetryCount").GetOptionalUint64()) {
                    Cerr << " RetryCount " << *retryCount;
                    if (*retryCount < 1) {
                        ok = false;
                    }
                } else {
                    ok = false;
                }
                if (auto issues = resultSet.ColumnParser("Issues").GetOptionalUtf8()) {
                    Cerr << " Issues " << *issues;
                }
                Cerr << Endl;
            });
            if (ok) {
                break;
            }
            Sleep(TDuration::MilliSeconds(50 + (rng() % 100))); // 100+-50ms
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));

        {
            const auto& result = ExecQuery("SELECT Status FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Status").GetOptionalUtf8(), "FAILED");
            });
        }

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(0, 0);
    }

    Y_UNIT_TEST_F(StreamingQueryDdlRetriesUnderSchemeShardRestarts, TStreamingWithSchemaSecretsTestFixture) {
        NodeCount = 5;
        LogSettings.Freeze = true;

        constexpr char inputTopicName[] = "streamingQueryDdlRetriesInputTopic";
        constexpr char outputTopicName[] = "streamingQueryDdlRetriesOutputTopic";
        constexpr char pqSourceName[] = "sourceName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);
        CreatePqSource(pqSourceName);

        GetRuntime().Register(new TTabletKiller(Tests::SchemeRoot, TDuration::MilliSeconds(500)));

        constexpr ui64 queriesCount = 1000;
        constexpr ui64 inflightLimit = 250;

        std::vector<TAsyncExecuteQueryResult> results;
        std::vector<NThreading::TFuture<void>> futures;
        for (ui64 i = 0; i < queriesCount; ++i) {
            results.emplace_back(GetQueryClient()->ExecuteQuery(fmt::format(R"(
                CREATE STREAMING QUERY `query_{i}` WITH (RUN = FALSE) AS
                DO BEGIN
                    INSERT INTO `{source}`.`{output_topic}` SELECT * FROM `{source}`.`{input_topic}`;
                END DO;

                ALTER STREAMING QUERY IF EXISTS `query_{i}` SET (RUN = FALSE);

                DROP STREAMING QUERY IF EXISTS `query_{i}`;)",
                "i"_a = i,
                "source"_a = pqSourceName,
                "output_topic"_a = outputTopicName,
                "input_topic"_a = inputTopicName
            ), TTxControl::NoTx()));

            futures.emplace_back(results.back().IgnoreResult());

            if (futures.size() >= inflightLimit) {
                NThreading::WaitAny(futures).Wait(TDuration::Seconds(10));

                // O(queriesCount * inflightLimit) but ok for test
                std::vector<NThreading::TFuture<void>> newFutures;
                newFutures.reserve(futures.size());
                for (const auto& future : futures) {
                    if (!future.HasValue()) {
                        newFutures.emplace_back(future);
                    }
                }
                futures = std::move(newFutures);
            }
        }

        for (ui64 i = 0; i < queriesCount; ++i) {
            const auto result = results[i].ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
        }
    }

    Y_UNIT_TEST_F(StreamingQueryRestartAfterShutdown, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "streamingQueryRestartAfterShutdownInputTopic";
        constexpr char outputTopicName[] = "streamingQueryRestartAfterShutdownOutputTopic";
        CreateTopic(inputTopicName, NTopic::TCreateTopicSettings().PartitioningSettings(2, 2));
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "key1value1");
        ReadTopicMessage(outputTopicName, "key1value1");

        // Finish query like shutdown
        {
            const auto& edgeActor = GetRuntime().AllocateEdgeActor();
            const auto& proxyId = MakeKqpProxyID(GetRuntime().GetFirstNodeId());

            auto listRequest = std::make_unique<TEvKqp::TEvListSessionsRequest>();
            auto& listRequestProto = listRequest->Record;
            listRequestProto.AddColumns(NSysView::Schema::QuerySessions::SessionId::ColumnId);
            listRequestProto.SetFreeSpace(std::numeric_limits<i64>::max());
            listRequestProto.SetTenantName(GetRuntime().GetAppData().TenantName);
            GetRuntime().Send(proxyId, edgeActor, listRequest.release());
            auto sessionsEv = GetRuntime().GrabEdgeEvent<TEvKqp::TEvListSessionsResponse>(edgeActor, TEST_OPERATION_TIMEOUT);
            UNIT_ASSERT(sessionsEv);

            const auto& sessionsProto = sessionsEv->Get()->Record.GetSessions();
            UNIT_ASSERT_GE(sessionsProto.size(), 1);

            for (const auto& session : sessionsProto) {
                auto closeRequest = std::make_unique<TEvKqp::TEvCloseSessionRequest>();
                closeRequest->Record.MutableRequest()->SetSessionId(session.GetSessionId());
                GetRuntime().Send(proxyId, edgeActor, closeRequest.release());
            }

            Sleep(TDuration::Seconds(2));
        }

        const auto& result = ExecQuery("SELECT RetryCount FROM `.sys/streaming_queries`");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("RetryCount").GetOptionalUint64(), 1);
        });

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, "key2value2");
        ReadTopicMessage(outputTopicName, "key2value2", disposition);
    }

    Y_UNIT_TEST_F(StreamingQueryWithTwoGroupByHops, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "streamingQueryWithTwoGroupByHopsInputTopic";
        constexpr char outputTopicName1[] = "streamingQueryWithTwoGroupByHopsOutputTopic1";
        constexpr char outputTopicName2[] = "streamingQueryWithTwoGroupByHopsOutputTopic2";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName1);
        CreateTopic(outputTopicName2);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $pq_source = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        key1 String,
                        key2 String
                    )
                );

                $grouped1 = SELECT
                    key_1,
                    CAST(SOME(time) AS String) AS time,
                    CAST(COUNT(*) AS String) AS count
                FROM $pq_source
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    (key1 || "-k1") AS key_1;

                $grouped2 = SELECT
                    key_2,
                    CAST(SOME(time) AS String) AS time,
                    CAST(COUNT(*) AS String) AS count
                FROM $pq_source
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    (key2 || "-k2") AS key_2;

                INSERT INTO `{pq_source}`.`{output_topic1}`
                SELECT Unwrap(key_1 || "-" || time || "-" || count) FROM $grouped1;

                INSERT INTO `{pq_source}`.`{output_topic2}`
                SELECT Unwrap(key_2 || "-" || time || "-" || count) FROM $grouped2;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic1"_a = outputTopicName1,
            "output_topic2"_a = outputTopicName2
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessages(inputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "key1": "A", "key2": "X"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "key1": "B", "key2": "Y"})",
        });
        ReadTopicMessage(outputTopicName1, "A-k1-2025-08-24T00:00:00.000000Z-1");
        ReadTopicMessage(outputTopicName2, "X-k2-2025-08-24T00:00:00.000000Z-1");

        const auto& result = ExecQuery("SELECT Ast FROM `.sys/streaming_queries`");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        CheckScriptResult(result[0], 1, 1, [&, check = AstChecker(1, 3)](TResultSetParser& resultSet) {
            check(*resultSet.ColumnParser("Ast").GetOptionalUtf8());
        });

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "key1": "C", "key2": "Z"})");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName1, "B-k1-2025-08-25T00:00:00.000000Z-1", disposition);
        ReadTopicMessage(outputTopicName2, "Y-k2-2025-08-25T00:00:00.000000Z-1", disposition);
    }

    Y_UNIT_TEST_F(TableMode, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;

        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);

        constexpr char topic[] = "tableMode";

        ui32 partitionCount = 4;
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);

        for (ui32 i = 0; i < partitionCount; ++i) {
            WriteTopicMessage(topic, "data", i, /* local */ true);
        }
        Sleep(TDuration::Seconds(1));

        const auto& result1 = ExecQuery(fmt::format(R"(SELECT * FROM `{topic}`)","topic"_a = topic));
        CheckScriptResult(result1[0], 1, partitionCount, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetString(), "data");
        });

        const auto& result2 = ExecQuery(fmt::format(R"(SELECT * FROM `{topic}` WITH(STREAMING="FALSE"))","topic"_a = topic));
        CheckScriptResult(result2[0], 1, partitionCount, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetString(), "data");
        });
    }

    Y_UNIT_TEST_F(UnionAllTwoTopics, TStreamingTestFixture) {
        constexpr char inputTopicName1[] = "unionAllTwoTopicsInputTopic1";
        constexpr char inputTopicName2[] = "unionAllTwoTopicsInputTopic2";
        constexpr char outputTopicName[] = "unionAllTwoTopicsOutputTopic";
        CreateTopic(inputTopicName1);
        CreateTopic(inputTopicName2);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(
            R"sql(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT key || value AS result FROM `{pq_source}`.`{input_topic1}` WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        )
                    )
                    UNION ALL
                    SELECT key || value AS result FROM `{pq_source}`.`{input_topic2}` WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        )
                    )
                END DO;
            )sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic1"_a = inputTopicName1,
            "input_topic2"_a = inputTopicName2,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName1, R"({"key": "topic1_", "value": "key1"})");
        WriteTopicMessage(inputTopicName2, R"({"key": "topic2_", "value": "key2"})");

        ReadTopicMessages(outputTopicName, {"topic1_key1", "topic2_key2"}, TInstant::Now() - TDuration::Seconds(100), /* sort */ true);

        ExecQuery(fmt::format(
            R"sql(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);
            )sql",
            "query_name"_a = queryName
        ));
    }

    Y_UNIT_TEST_F(UnionAllTopicWithItself, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "unionAllTopicWithItselfInputTopic";
        constexpr char outputTopicName[] = "unionAllTopicWithItselfOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(
            R"sql(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT key || value AS result FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        )
                    )
                    UNION ALL
                    SELECT key || value || "_dup" AS result FROM `{pq_source}`.`{input_topic}` WITH (
                        FORMAT = "json_each_row",
                        SCHEMA (
                            key String NOT NULL,
                            value String NOT NULL
                        )
                    )
                END DO;
            )sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"key": "k", "value": "v"})");

        ReadTopicMessages(outputTopicName, {"kv", "kv_dup"}, TInstant::Now() - TDuration::Seconds(100), /* sort */ true);

        ExecQuery(fmt::format(
            R"sql(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);
            )sql",
            "query_name"_a = queryName
        ));
    }
}

} // namespace NKikimr::NKqp
