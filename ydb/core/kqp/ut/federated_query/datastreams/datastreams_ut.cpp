#include "common.h"

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreams) {
    Y_UNIT_TEST_F(CreateExternalDataSource, TStreamingTestFixture) {
        CreatePqSource("sourceName");

        // DataStreams is not allowed.
        ExecSchemeQuery(fmt::format(
            R"sql(
                CREATE EXTERNAL DATA SOURCE `sourceName2` WITH (
                    SOURCE_TYPE="DataStreams",
                    LOCATION="{location}",
                    DATABASE_NAME="{database_name}",
                    AUTH_METHOD="NONE"
                );
            )sql",
            "location"_a = YDB_ENDPOINT,
            "database_name"_a = YDB_DATABASE
        ), EStatus::SCHEME_ERROR);

        // YdbTopics is not allowed.
        ExecSchemeQuery(fmt::format(
            R"sql(
                CREATE EXTERNAL DATA SOURCE `sourceName2` WITH (
                    SOURCE_TYPE="{source_type}",
                    LOCATION="{location}",
                    DATABASE_NAME="{database_name}",
                    AUTH_METHOD="NONE"
                );
            )sql",
            "source_type"_a = ToString(NYql::EDatabaseType::YdbTopics),
            "location"_a = YDB_ENDPOINT,
            "database_name"_a = YDB_DATABASE
        ), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST_F(CreateExternalDataSourceBasic, TStreamingTestFixture) {
        CreatePqSourceBasicAuth("sourceName");
    }

    Y_UNIT_TEST_F(FailedWithoutAvailableExternalDataSourcesYdb, TStreamingTestFixture) {
        SetupAppConfig().MutableQueryServiceConfig()->SetAllExternalDataSourcesAreAvailable(false);

        ExecSchemeQuery(fmt::format(
            R"sql(
                CREATE EXTERNAL DATA SOURCE `sourceName` WITH (
                    SOURCE_TYPE="Ydb",
                    LOCATION="{location}",
                    DATABASE_NAME="{database_name}",
                    AUTH_METHOD="NONE"
                );
            )sql",
            "location"_a = YDB_ENDPOINT,
            "database_name"_a = YDB_DATABASE
        ), EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST_F(CheckAvailableExternalDataSourcesYdb, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        CreatePqSource("sourceName");
    }

    Y_UNIT_TEST_F(ReadTopicFailedWithoutAvailableExternalDataSourcesYdbTopics, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        const std::string sourceName = "sourceName";
        CreatePqSource(sourceName);

        const std::string topicName = "topicName";
        CreateTopic(topicName);

        const auto scriptExecutionOperation = ExecAndWaitScript(fmt::format(R"(
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            )
            LIMIT 1;
            )",
            "source"_a=sourceName,
            "topic"_a=topicName
        ), EExecStatus::Failed);

        const auto& status = scriptExecutionOperation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::GENERIC_ERROR, status.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(status.GetIssues().ToString(), "Unsupported. Failed to load metadata for table: /Root/sourceName.[topicName] data source generic doesn't exist");
    }

    Y_UNIT_TEST_F(ReadTopicEndpointValidationWithoutAvailableExternalDataSourcesYdbTopics, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        // Execute script without existing topic
        const auto scriptExecutionOperation = ExecAndWaitScript(fmt::format(R"(
            SELECT * FROM `{source}`.`topicName` WITH (STREAMING = "TRUE")
            )",
            "source"_a=sourceName
        ), EExecStatus::Failed);

        const auto& status = scriptExecutionOperation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::GENERIC_ERROR, status.GetIssues().ToOneLineString());
        UNIT_ASSERT_STRING_CONTAINS(status.GetIssues().ToString(), "Unsupported. Failed to load metadata for table: /Root/sourceName.[topicName] data source generic doesn't exist");
    }

    Y_UNIT_TEST_F(ReadTopicEndpointValidation, TStreamingTestFixture) {
        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        // Execute script without existing topic
        const auto scriptExecutionOperation = ExecAndWaitScript(fmt::format(R"(
            SELECT * FROM `{source}`.`topicName` WITH (STREAMING = "TRUE")
            )",
            "source"_a=sourceName
        ), EExecStatus::Failed);

        const auto& status = scriptExecutionOperation.Status();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::GENERIC_ERROR, status.GetIssues().ToOneLineString());
        const auto& issues = status.GetIssues().ToString();
        UNIT_ASSERT_STRING_CONTAINS(issues, "Couldn't determine external YDB entity type");
        UNIT_ASSERT_STRING_CONTAINS(issues, "Describe path 'local/topicName' in external YDB database '/local'");
    }

    Y_UNIT_TEST_F(ReadTopic, TStreamingTestFixture) {
        auto& cfg = *SetupAppConfig().MutableQueryServiceConfig();
        cfg.AddAvailableExternalDataSources("Ydb");
        cfg.AddAvailableExternalDataSources("YdbTopics");
        cfg.SetAllExternalDataSourcesAreAvailable(false);

        const std::string sourceName = "sourceName";
        const std::string topicName = "topicName";
        ui32 partitionCount = 10;

        CreateTopic(topicName, NTopic::TCreateTopicSettings()
            .PartitioningSettings(partitionCount, partitionCount));

        CreatePqSource(sourceName);

        const auto scriptExecutionOperation = ExecScript(fmt::format(R"(
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            )
            LIMIT {partition_count};
            )",
            "source"_a=sourceName,
            "topic"_a=topicName,
            "partition_count"_a=partitionCount
        ));

        for (ui32 i = 0; i < partitionCount; ++i) {
            WriteTopicMessage(topicName, R"({"key": "key1", "value": "value1"})", i);
        }

        CheckScriptResult(scriptExecutionOperation, 2, partitionCount, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "value1");
        });
    }

    Y_UNIT_TEST_F(ReadTopicBasicNewSecrets, TStreamingWithSchemaSecretsTestFixture) {
        TestReadTopicBasic("-with-new-secret");
    }

    Y_UNIT_TEST_F(ReadTopicBasicOldSecrets, TStreamingTestFixture) {
        TestReadTopicBasic("-with-old-secret");
    }

    Y_UNIT_TEST_F(ReadTopicExplainBasic, TStreamingTestFixture) {
        const std::string sourceName = "sourceName";
        const std::string topicName = "topicName";
        CreateTopic(topicName);

        CreatePqSourceBasicAuth(sourceName);

        const auto result = GetQueryClient()->ExecuteQuery(fmt::format(
            R"(SELECT * FROM `{source}`.`{topic}` WITH (STREAMING = "TRUE"))",
            "source"_a=sourceName,
            "topic"_a=topicName
        ), TTxControl::NoTx(), TExecuteQuerySettings().ExecMode(EExecMode::Explain)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        const auto& stats = result.GetStats();
        UNIT_ASSERT(stats);

        const auto& plan = stats->GetPlan();
        UNIT_ASSERT(plan);
        UNIT_ASSERT_STRING_CONTAINS(*plan, sourceName);

        const auto& ast = stats->GetAst();
        UNIT_ASSERT(ast);
        UNIT_ASSERT_STRING_CONTAINS(*ast, sourceName);
    }

    Y_UNIT_TEST_F(InsertTopicBasic, TStreamingTestFixture) {
        SetupAppConfig().MutableQueryServiceConfig()->SetProgressStatsPeriodMs(1000);

        const std::string sourceName = "sourceName";
        const std::string inputTopicName = "inputTopicName";
        const std::string outputTopicName = "outputTopicName";
        const std::string tableName = "tableName";

        CreateTopic(outputTopicName);
        CreateTopic(inputTopicName);

        CreatePqSourceBasicAuth(sourceName);

        const auto scriptExecutionOperation = ExecScript(fmt::format(R"(
            $input = SELECT key, value FROM `{source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            );
            INSERT INTO `{source}`.`{output_topic}`
                SELECT key || value FROM $input;
            )",
            "source"_a=sourceName,
            "input_topic"_a=inputTopicName,
            "output_topic"_a=outputTopicName
        ));

        WriteTopicMessage(inputTopicName, R"({"key": "key1", "value": "value1"})");
        ReadTopicMessage(outputTopicName, "key1value1");

        WaitFor(TDuration::Seconds(5), "operation AST", [&](TString& error) {
            const auto& operation = GetScriptExecutionOperation(scriptExecutionOperation);
            const auto& metadata = operation.Metadata();
            if (const auto& ast = metadata.ExecStats.GetAst()) {
                UNIT_ASSERT_STRING_CONTAINS(*ast, sourceName);
                return true;
            }

            error = TStringBuilder() << "AST is not available, status: " << metadata.ExecStatus;
            return false;
        });

        CancelScriptExecution(scriptExecutionOperation);
    }

    Y_UNIT_TEST_F(ReadTopicWithColumnOrder, TStreamingTestFixture) {
        constexpr char topicName[] = "readTopicWithColumnOrder";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        const auto op = ExecScript(fmt::format(R"(
            PRAGMA OrderedColumns;
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    key String NOT NULL,
                    value String NOT NULL
                )
            ) LIMIT 1;

            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    value String NOT NULL,
                    key String NOT NULL
                )
            ) LIMIT 1;
            )",
            "source"_a=pqSourceName,
            "topic"_a=topicName
        ));

        WriteTopicMessage(topicName, R"({"key":"key1", "value": "value1"})");

        CheckScriptResult(op, 2, 1, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "key1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "value1");
        }, 0);

        CheckScriptResult(op, 2, 1, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "value1");
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(1).GetString(), "key1");
        }, 1);
    }

    Y_UNIT_TEST_F(ReadTopicWithDefaultSchema, TStreamingTestFixture) {
        constexpr char topicName[] = "readTopicWithDefaultSchema";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        const auto op = ExecScript(fmt::format(R"(
            PRAGMA OrderedColumns;
            SELECT * FROM `{source}`.`{topic}` WITH (STREAMING = "TRUE") LIMIT 1;
            )",
            "source"_a=pqSourceName,
            "topic"_a=topicName
        ));

        WriteTopicMessage(topicName, R"({"key":"key1", "value": "value1"})");

        CheckScriptResult(op, 1, 1, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), R"({"key":"key1", "value": "value1"})");
        });
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphBasic, TStreamingTestFixture) {
        constexpr char writeBucket[] = "test_bucket_restore_script_physical_graph";
        CreateBucket(writeBucket);

        constexpr char topicName[] = "restoreScriptTopic";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char s3SinkName[] = "s3Sink";
        CreateS3Source(writeBucket, s3SinkName);

        const auto executeQuery = [&](TScriptQuerySettings settings) {
            const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
                INSERT INTO `{s3_sink}`.`folder/` WITH (FORMAT = "json_each_row")
                SELECT * FROM `{source}`.`{topic}` WITH (
                    STREAMING = "TRUE",
                    FORMAT = "json_each_row",
                    SCHEMA = (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                ) LIMIT 1;)",
                "s3_sink"_a = s3SinkName,
                "source"_a = pqSourceName,
                "topic"_a = topicName
            ), settings);

            WriteTopicMessage(topicName, R"({"key": "key1", "value": "value1"})");
            WaitScriptExecution(operationId);

            return executionId;
        };

        const auto executionId = executeQuery({.SaveState = true});
        const std::string sampleResult = "{\"key\":\"key1\",\"value\":\"value1\"}\n";
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), sampleResult);

        ExecQuery(fmt::format(R"(
            DROP EXTERNAL DATA SOURCE `{s3_sink}`;
            DROP EXTERNAL DATA SOURCE `{pq_source}`;)",
            "s3_sink"_a = s3SinkName,
            "pq_source"_a = pqSourceName
        ));

        executeQuery({.PhysicalGraph = LoadPhysicalGraph(executionId)});
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), TStringBuilder() << sampleResult << sampleResult);
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphGroupByHop, TStreamingTestFixture) {
        constexpr char sourceTopicName[] = "restoreScriptGroupByHopTopicSource";
        constexpr char sinkTopicName[] = "restoreScriptGroupByHopTopicSink";
        CreateTopic(sourceTopicName);
        CreateTopic(sinkTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        std::vector<std::string> expectedMessages;
        const auto executeQuery = [&](TScriptQuerySettings settings) {
            const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
                $input = SELECT * FROM `{source}`.`{source_topic}` WITH (
                    STREAMING = "TRUE",
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String,
                        event String NOT NULL
                    )
                );

                INSERT INTO `{source}`.`{sink_topic}`
                SELECT
                    event
                FROM $input
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event
                LIMIT 1;)",
                "source_topic"_a = sourceTopicName,
                "sink_topic"_a = sinkTopicName,
                "source"_a = pqSourceName
            ), settings);

            WriteTopicMessages(sourceTopicName, {
                R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
                R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})"
            });

            expectedMessages.emplace_back("A");
            ReadTopicMessages(sinkTopicName, expectedMessages);

            WaitScriptExecution(operationId);

            return executionId;
        };

        const auto executionId = executeQuery({.SaveState = true});

        ExecQuery(fmt::format(R"(
            DROP EXTERNAL DATA SOURCE `{source}`;)",
            "source"_a = pqSourceName
        ));

        executeQuery({.PhysicalGraph = LoadPhysicalGraph(executionId)});
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphOnRetry, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char writeBucket[] = "test_bucket_restore_script_physical_graph_on_retry";
        CreateBucket(writeBucket);

        constexpr char topicName[] = "restoreScriptTopicOnRetry";
        CreateTopic(topicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char s3SinkName[] = "s3Sink";
        CreateS3Source(writeBucket, s3SinkName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.AtomicUploadCommit = "true";

            INSERT INTO `{s3_sink}`.`folder/` WITH (FORMAT = "json_each_row")
            SELECT * FROM `{source}`.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (
                    key String NOT NULL,
                    value String NOT NULL
                )
            ) LIMIT 1;)",
            "s3_sink"_a = s3SinkName,
            "source"_a = pqSourceName,
            "topic"_a = topicName
        ), {
            .SaveState = true,
            .RetryMapping = CreateRetryMapping({Ydb::StatusIds::BAD_REQUEST})
        });

        pqGateway->WaitReadSession(topicName)->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        WaitScriptExecution(operationId, EExecStatus::Failed, true);

        ExecQuery(fmt::format(R"(
            DROP EXTERNAL DATA SOURCE `{s3_sink}`;
            DROP EXTERNAL DATA SOURCE `{pq_source}`;)",
            "s3_sink"_a = s3SinkName,
            "pq_source"_a = pqSourceName
        ));

        pqGateway->WaitReadSession(topicName)->AddDataReceivedEvent(1, R"({"key": "key1", "value": "value1"})");
        WaitScriptExecution(operationId);

        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(writeBucket), "{\"key\":\"key1\",\"value\":\"value1\"}\n");
        UNIT_ASSERT_VALUES_EQUAL(GetUncommittedUploadsCount(writeBucket), 0);
    }

    Y_UNIT_TEST_F(RestoreScriptPhysicalGraphOnRetryWithCheckpoints, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "inputTopicName";
        CreateTopic(inputTopicName);
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(outputTopicName);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        const std::string checkpointId = CreateGuidAsString();
        const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
                $input = SELECT key, value FROM `{source}`.`{input_topic}` WITH (
                    STREAMING = "TRUE",
                    FORMAT = "json_each_row",
                    SCHEMA = (
                        key String NOT NULL,
                        value String NOT NULL
                    )
                );
                INSERT INTO `{source}`.`{output_topic}`
                    SELECT key || value FROM $input;)",
                "source"_a = sourceName,
                "input_topic"_a = inputTopicName,
                "output_topic"_a = outputTopicName
            ), {
                .SaveState = true,
                .RetryMapping = CreateRetryMapping({Ydb::StatusIds::BAD_REQUEST}),
                .CheckpointId = checkpointId
            });

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");
        WaitCheckpointUpdate(checkpointId);

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        readSession->AddDataReceivedEvent(1, R"({"key": "key1", "value": "value1"})");
        readSession->AddDataReceivedEvent(2, R"({"key": "key2", "value": "value2"})");
        readSession->AddDataReceivedEvent(3, R"({"key": "key3", "value": "value3"})");
        WaitCheckpointUpdate(checkpointId);
        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});

        WaitScriptExecution(operationId, EExecStatus::Failed, true);
        WaitCheckpointUpdate(checkpointId);

        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(4, R"({"key": "key4", "value": "value4"})");
        writeSession = pqGateway->WaitWriteSession(outputTopicName);
        writeSession->ExpectMessage("key4value4");

        CancelScriptExecution(operationId);
    }

    Y_UNIT_TEST_F(CheckpointsPropagationWithGroupByHop, TStreamingTestFixture) {
        LogSettings.Freeze = true;
        CheckpointPeriod = TDuration::Seconds(5);

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        const std::string checkpointId = CreateGuidAsString();
        const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
            INSERT INTO `{source}`.`{output_topic}`
            SELECT event FROM `{source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    time String,
                    event String NOT NULL
                )
            )
            GROUP BY HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"), event;)",
            "source"_a = sourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), {
            .SaveState = true,
            .CheckpointId = checkpointId
        });

        WriteTopicMessages(inputTopicName, {
            R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})",
            R"({"time": "2025-08-25T00:00:00.000000Z", "event": "B"})"
        });
        ReadTopicMessage(outputTopicName, "A");
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");
        WaitCheckpointUpdate(checkpointId);

        const auto& result = ExecQuery(fmt::format(R"(
            SELECT COUNT(*) AS states_count FROM (
                SELECT DISTINCT task_id FROM `.metadata/streaming/checkpoints/states`
                WHERE graph_id = "{checkpoint_id}"
            )
        )", "checkpoint_id"_a = checkpointId));
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

        CheckScriptResult(result[0], 1, 1, [](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), 2);
        });

        WaitFor(TDuration::Seconds(10), "operation stats", [&](TString& error) {
            const auto metadata = GetScriptExecutionOperation(operationId).Metadata();
            const auto& plan = metadata.ExecStats.GetPlan();
            if (plan && plan->contains("MultiHop_NewHopsCount")) {
                return true;
            }

            error = TStringBuilder() << "plan is not available, status: " << metadata.ExecStatus << ", plan: " << plan.value_or("");
            return false;
        });
    }

    Y_UNIT_TEST_F(CheckpointsOnNotDrainedChannels, TStreamingTestFixture) {
        LogSettings.Freeze = true;

        CheckpointPeriod = TDuration::Seconds(3);
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char outputTopicName[] = "outputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char sourceName[] = "sourceName";
        CreatePqSource(sourceName);

        const std::string checkpointId = CreateGuidAsString();
        const auto& [executionId, operationId] = ExecScriptNative(fmt::format(R"(
            INSERT INTO `{source}`.`{output_topic}`
            SELECT event FROM `{source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    time String,
                    event String NOT NULL
                )
            )
            GROUP BY HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"), event;)",
            "source"_a = sourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), {
            .SaveState = true,
            .CheckpointId = checkpointId
        });
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery("GRANT ALL ON `/Root/.metadata/streaming` TO `" BUILTIN_ACL_ROOT "`");

        auto writeSession = pqGateway->WaitWriteSession(outputTopicName);
        WaitCheckpointUpdate(checkpointId);
        writeSession->Lock();

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const std::string value(1_KB, 'x');
        TInstant time = TInstant::Now();
        for (ui64 i = 0; i < 100000; ++i, time += TDuration::Hours(2)) {
            readSession->AddDataReceivedEvent(i, fmt::format(R"({{"time": "{time}", "event": "{event}"}})", "time"_a = time.ToString(), "event"_a = value));
        }

        Sleep(TDuration::Seconds(6));
        writeSession->Unlock();

        for (ui64 i = 0; i < 3; ++i) {
            WaitCheckpointUpdate(checkpointId);
        }
    }

    Y_UNIT_TEST_F(S3RuntimeListingDisabledForStreamingQueries, TStreamingTestFixture) {
        constexpr char sourceBucket[] = "test_bucket_disable_runtime_listing";
        constexpr char objectPath[] = "test_bucket_object.json";
        constexpr char objectContent[] = R"({"data": "x"})";
        CreateBucketWithObject(sourceBucket, objectPath, objectContent);

        constexpr char s3SourceName[] = "s3Source";
        CreateS3Source(sourceBucket, s3SourceName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.UseRuntimeListing = "true";
            INSERT INTO `{s3_source}`.`path/` WITH (
                FORMAT = "json_each_row"
            ) SELECT * FROM `{s3_source}`.`{object_path}` WITH (
                FORMAT = "json_each_row",
                SCHEMA (
                    data String NOT NULL
                )
            )
        )", "s3_source"_a = s3SourceName, "object_path"_a = objectPath), {
            .SaveState = true
        }, /* waitRunning */ false);

        const auto& readyOp = WaitScriptExecution(operationId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Runtime listing is not supported for streaming queries, pragma value was ignored");
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "{\"data\":\"x\"}\n{\"data\": \"x\"}");
    }

    Y_UNIT_TEST_F(S3AtomicUploadCommitDisabledForStreamingQueries, TStreamingTestFixture) {
        constexpr char sourceBucket[] = "test_bucket_disable_atomic_upload_commit";
        constexpr char objectPath[] = "test_bucket_object.json";
        constexpr char objectContent[] = R"({"data": "x"})";
        CreateBucketWithObject(sourceBucket, objectPath, objectContent);

        constexpr char s3SourceName[] = "s3Source";
        CreateS3Source(sourceBucket, s3SourceName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.AtomicUploadCommit = "true";
            INSERT INTO `{s3_source}`.`path/` WITH (
                FORMAT = "json_each_row"
            ) SELECT * FROM `{s3_source}`.`{object_path}` WITH (
                FORMAT = "json_each_row",
                SCHEMA (
                    data String NOT NULL
                )
            )
        )", "s3_source"_a = s3SourceName, "object_path"_a = objectPath), {
            .SaveState = true
        }, /* waitRunning */ false);

        const auto& readyOp = WaitScriptExecution(operationId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Atomic upload commit is not supported for streaming queries, pragma value was ignored");
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "{\"data\":\"x\"}\n{\"data\": \"x\"}");
    }

    Y_UNIT_TEST_F(S3PartitioningKeysFlushTimeout, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();
        constexpr char sourceBucket[] = "test_bucket_partitioning_keys_flush";
        constexpr char s3SourceName[] = "s3Source";
        CreateBucket(sourceBucket);
        CreateS3Source(sourceBucket, s3SourceName);

        constexpr char inputTopicName[] = "inputTopicName";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreatePqSource(pqSourceName);

        const auto& [_, operationId] = ExecScriptNative(fmt::format(R"(
            PRAGMA s3.OutputKeyFlushTimeout = "1s";
            PRAGMA ydb.DisableCheckpoints = "TRUE";
            PRAGMA ydb.MaxTasksPerStage = "1";

            INSERT INTO `{s3_source}`.`path/` WITH (
                FORMAT = json_each_row,
                PARTITIONED_BY = key
            ) SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = json_each_row,
                SCHEMA (
                    data String NOT NULL,
                    key Uint64 NOT NULL
                )
            )
        )", "s3_source"_a = s3SourceName, "pq_source"_a = pqSourceName, "input_topic"_a = inputTopicName), {
            .SaveState = true
        });

        auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, R"({"data": "x", "key": 0})");

        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "");

        readSession->AddDataReceivedEvent(1, R"({"data": "y", "key": 1})");

        Sleep(TDuration::Seconds(2));
        UNIT_ASSERT_VALUES_EQUAL(GetAllObjects(sourceBucket), "{\"data\":\"x\"}\n");
    }

    Y_UNIT_TEST_F(CrossJoinWithNotExistingDataSource, TStreamingTestFixture) {
        const auto connectorClient = SetupMockConnectorClient();

        constexpr char ydbSourceName[] = "ydbSourceName";
        CreateYdbSource(ydbSourceName);

        constexpr char ydbTable[] = "unknownSourceLookup";
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
                .DescribeCount = 1,
                .ListSplitsCount = 0
            });
        }

        ExecQuery(fmt::format(R"(
                SELECT
                    *
                FROM `unknown-datasource`.`unknown-topic` WITH (
                    FORMAT = raw,
                    SCHEMA (Data String NOT NULL)
                ) AS p
                CROSS JOIN (
                    SELECT * FROM `{ydb_source}`.`{table}`
                ) AS l
            )",
            "ydb_source"_a = ydbSourceName,
            "table"_a = ydbTable
        ), EStatus::SCHEME_ERROR, "Cannot find table '/Root/unknown-datasource.[unknown-topic]' because it does not exist or you do not have access permissions");
    }

    Y_UNIT_TEST_TWIN_F(ReplicatedFederativeWriting, UseColumnTable, TStreamingTestFixture) {
        constexpr char firstOutputTopic[] = "replicatedWritingOutputTopicName1";
        constexpr char secondOutputTopic[] = "replicatedWritingOutputTopicName2";
        constexpr char pqSource[] = "pqSourceName";
        CreateTopic(firstOutputTopic);
        CreateTopic(secondOutputTopic);
        CreatePqSource(pqSource);

        constexpr char solomonSink[] = "solomonSinkName";
        CreateSolomonSource(solomonSink);

        constexpr char sourceTable[] = "source";
        constexpr char rowSinkTable[] = "rowSink";
        constexpr char columnSinkTable[] = "columnSink";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{source_table}` (
                Data String NOT NULL,
                PRIMARY KEY (Data)
            ) {source_settings};
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
            "source_table"_a = sourceTable,
            "source_settings"_a = UseColumnTable ? "WITH (STORE = COLUMN)" : "",
            "row_table"_a = rowSinkTable,
            "column_table"_a = columnSinkTable
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{table}`
                (Data)
            VALUES
                ("{{\"Val\": \"ABC\"}}");)",
            "table"_a = sourceTable
        ));

        TInstant disposition = TInstant::Now();

        // Double PQ insert
        {
            ExecQuery(fmt::format(R"(
                $rows = SELECT Data FROM `{source_table}`;
                INSERT INTO `{pq_source}`.`{output_topic1}` SELECT Unwrap(CAST("[" || Data || "]" AS Json)) AS A FROM $rows;
                INSERT INTO `{pq_source}`.`{output_topic2}` SELECT Unwrap(CAST(Data || "-B" AS String)) AS B FROM $rows;)",
                "source_table"_a = sourceTable,
                "pq_source"_a = pqSource,
                "output_topic1"_a = firstOutputTopic,
                "output_topic2"_a = secondOutputTopic
            ), EStatus::SUCCESS, "", AstChecker(1, 1));

            ReadTopicMessage(firstOutputTopic, R"([{"Val": "ABC"}])", disposition);
            ReadTopicMessage(secondOutputTopic, R"({"Val": "ABC"}-B)", disposition);
            disposition = TInstant::Now();
        }

        // Double solomon insert
        {
            const TSolomonLocation firstSoLocation = {
                .ProjectId = "cloudId1",
                .FolderId = "folderId1",
                .Service = "custom1",
                .IsCloud = false,
            };
            const TSolomonLocation secondSoLocation = {
                .ProjectId = "cloudId2",
                .FolderId = "folderId2",
                .Service = "custom2",
                .IsCloud = false,
            };

            CleanupSolomon(firstSoLocation);
            CleanupSolomon(secondSoLocation);

            ExecQuery(fmt::format(R"(
                $rows = SELECT Data FROM `{source_table}`;

                INSERT INTO `{solomon_sink}`.`{first_solomon_project}/{first_solomon_folder}/{first_solomon_service}`
                SELECT Unwrap(CAST("[" || Data || "]" AS Json)) AS sensor, 1 AS value, Timestamp("2025-03-12T14:40:39Z") AS ts FROM $rows;

                INSERT INTO `{solomon_sink}`.`{second_solomon_project}/{second_solomon_folder}/{second_solomon_service}`
                SELECT Unwrap(CAST(Data || "-B" AS String)) AS sensor, 2 AS value, Timestamp("2025-03-12T14:40:39Z") AS ts FROM $rows;)",
                "source_table"_a = sourceTable,
                "solomon_sink"_a = solomonSink,
                "first_solomon_project"_a = firstSoLocation.ProjectId,
                "first_solomon_folder"_a = firstSoLocation.FolderId,
                "first_solomon_service"_a = firstSoLocation.Service,
                "second_solomon_project"_a = secondSoLocation.ProjectId,
                "second_solomon_folder"_a = secondSoLocation.FolderId,
                "second_solomon_service"_a = secondSoLocation.Service
            ), EStatus::SUCCESS, "", AstChecker(1, 1));

            std::string expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "[{\"Val\": \"ABC\"}]"
      ]
    ],
    "ts": 1741790439,
    "value": 1
  }
])";
            UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(firstSoLocation), expectedMetrics);

            expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "{\"Val\": \"ABC\"}-B"
      ]
    ],
    "ts": 1741790439,
    "value": 2
  }
])";
            UNIT_ASSERT_STRINGS_EQUAL(GetSolomonMetrics(secondSoLocation), expectedMetrics);
        }

        // Mixed external and kqp writing
        {
            ExecQuery(fmt::format(R"(
                $rows = SELECT Data FROM `{source_table}`;
                UPSERT INTO `{column_table}` SELECT Unwrap(CAST(Data || "-C" AS String)) AS C FROM $rows;
                INSERT INTO `{pq_source}`.`{output_topic}` SELECT Unwrap(CAST("[" || Data || "]" AS Json)) AS A FROM $rows;
                UPSERT INTO `{row_table}` SELECT Unwrap(CAST(Data || "-B" AS Utf8)) AS B FROM $rows;)",
                "source_table"_a = sourceTable,
                "pq_source"_a = pqSource,
                "output_topic"_a = firstOutputTopic,
                "row_table"_a = rowSinkTable,
                "column_table"_a = columnSinkTable
            ), EStatus::SUCCESS, "", AstChecker(2, 4));

            ReadTopicMessage(firstOutputTopic, R"([{"Val": "ABC"}])", disposition);
            disposition = TInstant::Now();

            const auto& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{row_table}`;
                SELECT * FROM `{column_table}`;)",
                "row_table"_a = rowSinkTable,
                "column_table"_a = columnSinkTable
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);

            CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("B").GetUtf8(), R"({"Val": "ABC"}-B)");
            });

            CheckScriptResult(results[1], 1, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("C").GetString(), R"({"Val": "ABC"}-C)");
            });
        }
    }

    Y_UNIT_TEST_F(ReadFromLocalTopicsWithAuth, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;

        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);

        constexpr char inputTopic[] = "inputTopicName";
        constexpr char outputTopic[] = "outputTopicName";
        CreateTopic(inputTopic, std::nullopt, /* local */ true);
        CreateTopic(outputTopic, std::nullopt, /* local */ true);

        auto asyncResult = GetQueryClient()->ExecuteQuery(fmt::format(R"(
                PRAGMA pq.Consumer = "test_consumer";
                INSERT INTO `{output_topic}`
                SELECT * FROM `{input_topic}` WITH (
                    STREAMING = "TRUE"
                ) LIMIT 2
            )",
            "input_topic"_a = inputTopic,
            "output_topic"_a = outputTopic
        ), TTxControl::NoTx());

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopic, "data", 0, /* local */ true);
        ReadTopicMessage(outputTopic, "data", TInstant::Now() - TDuration::Seconds(100), /* local */ true);

        // Force session reconnect
        KillTopicPqrbTablet(inputTopic);

        const auto disposition = TInstant::Now();
        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopic, "data2", 0, /* local */ true);
        ReadTopicMessage(outputTopic, "data2", disposition, /* local */ true);

        const auto result = asyncResult.ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToOneLineString());
    }

    Y_UNIT_TEST_F(ScalarFederativeWriting, TStreamingTestFixture) {
        constexpr char firstOutputTopic[] = "replicatedWritingOutputTopicName1";
        constexpr char secondOutputTopic[] = "replicatedWritingOutputTopicName2";
        constexpr char pqSource[] = "pqSourceName";
        CreateTopic(firstOutputTopic);
        CreateTopic(secondOutputTopic);
        CreatePqSource(pqSource);

        constexpr char solomonSink[] = "solomonSinkName";
        CreateSolomonSource(solomonSink);

        const TSolomonLocation soLocation = {
            .ProjectId = "cloudId1",
            .FolderId = "folderId1",
            .Service = "custom1",
            .IsCloud = false,
        };
        CleanupSolomon(soLocation);
        ExecQuery(fmt::format(R"(
            INSERT INTO `{pq_source}`.`{output_topic1}` SELECT "TestData1";
            INSERT INTO `{pq_source}`.`{output_topic2}` SELECT "TestData2" AS Data;
            INSERT INTO `{pq_source}`.`{output_topic2}`(Data) VALUES ("TestData2");

            INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
            SELECT
                13333 AS value,
                "test-insert" AS sensor,
                Timestamp("2025-03-12T14:40:39Z") AS ts;

            INSERT INTO `{solomon_sink}`.`{solomon_project}/{solomon_folder}/{solomon_service}`
                (value, sensor, ts)
            VALUES
                (23333, "test-insert-2", Timestamp("2025-03-12T14:40:39Z"));)",
            "pq_source"_a = pqSource,
            "output_topic1"_a = firstOutputTopic,
            "output_topic2"_a = secondOutputTopic,
            "solomon_sink"_a = solomonSink,
            "solomon_project"_a = soLocation.ProjectId,
            "solomon_folder"_a = soLocation.FolderId,
            "solomon_service"_a = soLocation.Service
        ), EStatus::SUCCESS, "", AstChecker(2, 5));

        ReadTopicMessage(firstOutputTopic, "TestData1");
        ReadTopicMessages(secondOutputTopic, {"TestData2", "TestData2"});

        std::string expectedMetrics = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-insert"
      ]
    ],
    "ts": 1741790439,
    "value": 13333
  },
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-insert-2"
      ]
    ],
    "ts": 1741790439,
    "value": 23333
  }
])";

        // TODO canonize order and avoid duplication
        std::string expectedMetrics2 = R"([
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-insert-2"
      ]
    ],
    "ts": 1741790439,
    "value": 23333
  },
  {
    "labels": [
      [
        "name",
        "value"
      ],
      [
        "sensor",
        "test-insert"
      ]
    ],
    "ts": 1741790439,
    "value": 13333
  }
])";
        auto results = GetSolomonMetrics(soLocation);
        if (results != expectedMetrics2) {
            UNIT_ASSERT_VALUES_EQUAL(results, expectedMetrics);
        }
    }

    Y_UNIT_TEST_F(TableModeWithPartitionPredicate, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic[] = "tableMode";

        ui32 partitionCount = 4;
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);

        WriteTopicMessage(topic, "{\"key\": \"data0\"}", 0, /* local */ true);
        WriteTopicMessage(topic, "data", 1, /* local */ true);  // wrong schema
        WriteTopicMessage(topic, "{\"key\": \"data2\"}", 2, /* local */ true);
        WriteTopicMessage(topic, "{\"key\": \"data3\"}", 3, /* local */ true);

        Sleep(TDuration::Seconds(1));

        auto test = [&](const TString& filter, ui64 rowCount, std::function<void(TResultSetParser&)> validator) {
            TString text = fmt::format(R"(
                SELECT 
                    SystemMetadata('partition_id') as partition_id,
                    SystemMetadata("offset") as offset,
                    key as data
                FROM `{topic}`
                WITH (FORMAT = "json_each_row", SCHEMA = (key String NOT NULL))
                WHERE {filter})",
                "topic"_a = topic,
                "filter"_a = filter
            );
            auto result = ExecQuery(text);
            CheckScriptResult(result[0], 3, rowCount, validator);
        };

        test("SystemMetadata('partition_id') = 0 \
           OR SystemMetadata('partition_id') >= 2", 3, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data0"
            || resultSet.ColumnParser(2).GetString() == "data2"
            || resultSet.ColumnParser(2).GetString() == "data3");
        });

        test("(SystemMetadata('partition_id') = 0 AND SystemMetadata('offset') >=0) \
           OR (SystemMetadata('partition_id') = 2 AND SystemMetadata('offset') >=1)", 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data0");
        });

        test("SystemMetadata('partition_id') = 0 and key = 'data0'", 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUint64(), 0);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(2).GetString(), "data0");
        });
    }

    Y_UNIT_TEST_F(TableModeWithOffsetPredicate, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic[] = "tableMode";

        ui32 partitionCount = 1;
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);

        WriteTopicMessage(topic, "data", 0, /* local */ true);                  // wrong schema
        WriteTopicMessage(topic, "{\"key\": \"data1\"}", 0, /* local */ true);
        WriteTopicMessage(topic, "{\"key\": \"data2\"}", 0, /* local */ true);
        WriteTopicMessage(topic, "data", 0, /* local */ true);                  // wrong schema

        Sleep(TDuration::Seconds(1));

        auto test = [&](const TString& filter, ui64 rowCount, std::function<void(TResultSetParser&)> validator) {
            TString text = fmt::format(R"(
                SELECT 
                    SystemMetadata('partition_id') as partition_id,
                    SystemMetadata("offset") as offset,
                    key as data
                FROM `{topic}`
                WITH (FORMAT = "json_each_row", SCHEMA = (key String NOT NULL))
                WHERE {filter})",
                "topic"_a = topic,
                "filter"_a = filter
            );
            auto result = ExecQuery(text);
            CheckScriptResult(result[0], 3, rowCount, validator);
        };

        test("SystemMetadata('offset') < -42", 0,  [&](TResultSetParser& /*resultSet*/) {});
        test("SystemMetadata('offset') <   0", 0,  [&](TResultSetParser& /*resultSet*/) {});
        test("SystemMetadata('offset') >  42", 0,  [&](TResultSetParser& /*resultSet*/) {});
        test("SystemMetadata('offset') = 1", 1,  [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1");
            });
        test("SystemMetadata('offset') >= 1 AND SystemMetadata('offset') < 3", 2,  [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1" || resultSet.ColumnParser(2).GetString() == "data2");
            });
        test("1 <= SystemMetadata('offset') AND 3 > SystemMetadata('offset')", 2,  [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1" || resultSet.ColumnParser(2).GetString() == "data2");
            });
        test("SystemMetadata('offset') IN (1, 2)", 2,  [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1" || resultSet.ColumnParser(2).GetString() == "data2");
            });
    }

    Y_UNIT_TEST_F(TableModeWithWriteTimePredicate, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic[] = "tableMode";

        ui32 partitionCount = 1;
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);

        WriteTopicMessage(topic, "data", 0, /* local */ true);                  // wrong schema
        WriteTopicMessage(topic, "{\"key\": \"data1\"}", 0, /* local */ true);
        WriteTopicMessage(topic, "{\"key\": \"data2\"}", 0, /* local */ true);
        WriteTopicMessage(topic, "data", 0, /* local */ true);                  // wrong schema

        auto received = ReadTopicMessagesWithWriteTime(topic, 4, TInstant{}, true);
        UNIT_ASSERT_VALUES_EQUAL(received.size(), 4);
        for (auto& [message, writeTime] : received) {
            Cerr << "message " << message << " writeTime " << writeTime << Endl;
        }


        auto test = [&](const TString& filter, ui64 rowCount, std::function<void(TResultSetParser&)> validator) {
            TString text = fmt::format(R"(
                SELECT 
                    SystemMetadata('partition_id') as partition_id,
                    SystemMetadata("write_time") as offset,
                    key as data
                FROM `{topic}`
                WITH (FORMAT = "json_each_row", SCHEMA = (key String NOT NULL))
                WHERE {filter})",
                "topic"_a = topic,
                "filter"_a = filter
            );
            auto result = ExecQuery(text);
            CheckScriptResult(result[0], 3, rowCount, validator);
        };

        test("SystemMetadata('write_time') = Timestamp(\"" + received[1].second.ToString() + "\")", 1,  [&](TResultSetParser& resultSet) {
            UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1");
        });
        
    }

    Y_UNIT_TEST_F(TableModeWithMixedPredicate, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic[] = "tableMode";

        ui32 partitionCount = 3;
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);

        WriteTopicMessage(topic, "wrong schema", 0, /* local */ true);
        WriteTopicMessage(topic, "{\"key\": \"data0\"}", 0, /* local */ true);
        WriteTopicMessage(topic, "wrong schema", 0, /* local */ true);

        WriteTopicMessage(topic, "wrong schema", 1, /* local */ true);
        WriteTopicMessage(topic, "wrong schema", 1, /* local */ true);
        WriteTopicMessage(topic, "{\"key\": \"data1\"}", 1, /* local */ true);

        WriteTopicMessage(topic, "{\"key\": \"data2\"}", 2, /* local */ true);
        WriteTopicMessage(topic, "wrong schema", 2, /* local */ true);
        WriteTopicMessage(topic, "wrong schema", 2, /* local */ true);

        Sleep(TDuration::Seconds(1));

        auto test = [&](const TString& filter, ui64 rowCount, std::function<void(TResultSetParser&)> validator) {
            TString text = fmt::format(R"(
                SELECT 
                    SystemMetadata('partition_id') as partition_id,
                    SystemMetadata("offset") as offset,
                    key as data
                FROM `{topic}`
                WITH (FORMAT = "json_each_row", SCHEMA = (key String NOT NULL))
                WHERE {filter})",
                "topic"_a = topic,
                "filter"_a = filter
            );
            auto result = ExecQuery(text);
            CheckScriptResult(result[0], 3, rowCount, validator);
        };

        test("SystemMetadata('partition_id') = 0 AND SystemMetadata('offset') = 1", 1,  [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data0");
            });
        test("SystemMetadata('partition_id') = 1 AND SystemMetadata('offset') >= 2", 1,  [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1");
            });
        test("SystemMetadata('partition_id') = 2 AND SystemMetadata('offset') < 1", 1,  [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data2");
            });
    }

    Y_UNIT_TEST_F(CreateExternalDataSourceAuthMethodIam, TStreamingWithSchemaSecretsTestFixture) {
        auto& appConfig = SetupAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableExternalDataSourceAuthMethodIam(true);
        constexpr char cloudId[] =  ""; // TODO find a way create database with cloud_id

        constexpr char inputTopicName[] = "createExternalDataSourceAuthMethodIam";
        CreateTopic(inputTopicName);
        constexpr char secretPath[] = "eds_iam_token";
        ExecQuery(fmt::format(R"(
            CREATE SECRET {secret} WITH (value = "{token}");
            )",
            "secret"_a = secretPath,
            "token"_a = BUILTIN_ACL_METADATA // TODO root@ does not work; why?
            ));

        constexpr char serviceAccountId[] = "foobar"; // not validated/used on creation
        constexpr char pqSourceName[] = "sourceNameCloud";
        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{pq_location}",
                DATABASE_NAME = "{pq_database_name}",
                AUTH_METHOD = "IAM",
                INITIAL_TOKEN_SECRET_PATH = "{secret}",
                SERVICE_ACCOUNT_ID = "{service_account_id}"
            );)",
            "pq_source"_a = pqSourceName,
            "pq_location"_a = YDB_ENDPOINT,
            "pq_database_name"_a = YDB_DATABASE,
            "secret"_a = secretPath,
            "service_account_id"_a = serviceAccountId
        ));
        {
            const auto externalDataSourceDesc = Navigate(GetRuntime(), GetRuntime().AllocateEdgeActor(), "/Root/" + std::string(pqSourceName), NSchemeCache::TSchemeCacheNavigate::EOp::OpUnknown);
            const auto& externalDataSource = externalDataSourceDesc->ResultSet.at(0);
            UNIT_ASSERT_EQUAL(externalDataSource.Kind, NSchemeCache::TSchemeCacheNavigate::EKind::KindExternalDataSource);
            UNIT_ASSERT(externalDataSource.ExternalDataSourceInfo);
            auto& info = *externalDataSource.ExternalDataSourceInfo;
            auto& description = info.Description;
            UNIT_ASSERT_VALUES_EQUAL(description.GetSourceType(), "Ydb");
            auto& auth = description.GetAuth();
            UNIT_ASSERT(auth.HasIam());
            auto& iam = auth.GetIam();
            UNIT_ASSERT(iam.HasServiceAccountId());
            UNIT_ASSERT_VALUES_EQUAL(iam.GetServiceAccountId(), serviceAccountId);
            UNIT_ASSERT(iam.HasResourceId());
            UNIT_ASSERT_VALUES_EQUAL(iam.GetResourceId(), cloudId);
        }
        // cannot verify use without some kind of "mock IAM"
    }
}


} // namespace NKikimr::NKqp
