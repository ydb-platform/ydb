#include "common.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

#include <fmt/format.h>

#include <random>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace fmt::literals;
using namespace NYql::NConnector::NTest;
using namespace NTestUtils;
using namespace NFederatedQueryTest;
using namespace NYdb::NConsoleClient::NAi;

Y_UNIT_TEST_SUITE(KqpStreamingQueriesDdl) {
    Y_UNIT_TEST_F(CreateAndAlterStreamingQuery, TStreamingWithSchemaSecretsTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

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

        {
            auto appCounters = GetCounters("tablets")->GetSubgroup("type", "SchemeShard")->GetSubgroup("category", "app");
            WaitFor(TDuration::Seconds(60), "StreamingQueryCount and RunningStreamingQueryCount reach 1", [&](TString& error) {
                auto queryCount = appCounters->GetCounter("SUM(SchemeShard/StreamingQueryCount)", false)->Val();
                auto runningCount = appCounters->GetCounter("SUM(SchemeShard/RunningStreamingQueryCount)", false)->Val();
                error = TStringBuilder() << "StreamingQueryCount=" << queryCount << ", RunningStreamingQueryCount=" << runningCount << ", expected both to be 1";
                return queryCount == 1 && runningCount == 1;
            });
        }

        WriteTopicMessage(inputTopicName, R"({"key": "key2", "value": "value2"})");
        ReadTopicMessages(outputTopicName, {"key1value1", "value2key2"});

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(2, 0);
        {
            auto appCounters = GetCounters("tablets")->GetSubgroup("type", "SchemeShard")->GetSubgroup("category", "app");
            WaitFor(TDuration::Seconds(60), "StreamingQueryCount and RunningStreamingQueryCount reach 1", [&](TString& error) {
                auto queryCount = appCounters->GetCounter("SUM(SchemeShard/StreamingQueryCount)", false)->Val();
                auto runningCount = appCounters->GetCounter("SUM(SchemeShard/RunningStreamingQueryCount)", false)->Val();
                error = TStringBuilder() << "StreamingQueryCount=" << queryCount << ", RunningStreamingQueryCount=" << runningCount;
                return queryCount == 1 && runningCount == 0;
            });
        }

        {
            const TString issuesJson = GetStreamingQueryIssues(queryName);
            Cerr << "Issues: " << issuesJson << Endl;

            TJsonParser issuesParser;
            UNIT_ASSERT(issuesParser.Parse(issuesJson));
            UNIT_ASSERT_VALUES_EQUAL(issuesParser.GetValue().GetIntegerRobust(), 1);

            issuesParser = issuesParser.GetKey("issues");
            UNIT_ASSERT_VALUES_EQUAL(issuesParser.GetValue().GetIntegerRobust(), 1);

            issuesParser = issuesParser.GetElement(0);
            UNIT_ASSERT_VALUES_EQUAL(issuesParser.GetValue().GetIntegerRobust(), 2);
            UNIT_ASSERT_VALUES_EQUAL(issuesParser.GetKey("message").GetString(), "Request was canceled by user");
            UNIT_ASSERT_VALUES_EQUAL(issuesParser.GetKey("severity").GetValue().GetIntegerSafe(), static_cast<i64>(NYql::TSeverityIds::S_INFO));
        }
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

        {
            auto appCounters = GetCounters("tablets")->GetSubgroup("type", "SchemeShard")->GetSubgroup("category", "app");
            WaitFor(TDuration::Seconds(60), "StreamingQueryCount and RunningStreamingQueryCount reach 1", [&](TString& error) {
                auto queryCount = appCounters->GetCounter("SUM(SchemeShard/StreamingQueryCount)", false)->Val();
                auto runningCount = appCounters->GetCounter("SUM(SchemeShard/RunningStreamingQueryCount)", false)->Val();
                error = TStringBuilder() << "StreamingQueryCount=" << queryCount << ", RunningStreamingQueryCount=" << runningCount;
                return queryCount == 0 && runningCount == 0;
            });
        }
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
        ), NYdb::Dev::EStatus::GENERIC_ERROR, "Streaming partition balancing is disabled. Please contact your system administrator to enable it");
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
            Sleep(TDuration::Seconds(2)); // Wait for checkpoint completion

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

    Y_UNIT_TEST_QUAD_F(CheckpointPropagationAfterSubgraphFinalization, LocalTopics, ModernChannels, TStreamingWithSchemaSecretsTestFixture) {
        NodeCount = 2;
        DqChannelsVersion = ModernChannels ? 2 : 1;
        InternalInitFederatedQuerySetupFactory = true;

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto firstInputTopicName = TStringBuilder() << Name_ << "InputTopicName1";
        const auto secondInputTopicName = TStringBuilder() << Name_ << "InputTopicName2";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(firstInputTopicName, NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(2, 2), LocalTopics);
        CreateTopic(secondInputTopicName, std::nullopt, LocalTopics);
        CreateTopic(outputTopicName, std::nullopt, LocalTopics);

        constexpr char tableName[] = "outputTable";
        ExecQuery(fmt::format(R"(
                CREATE TABLE `{table_name}` (
                    Data String NOT NULL,
                    PRIMARY KEY (Data)
                );
            )",
            "table_name"_a = tableName
        ));

        constexpr char pqSourceName[] = "pqSourceName";
        if constexpr (!LocalTopics) {
            CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);
        }

        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE"; -- Enable `LIMIT` operator in at-least-once semantic
                PRAGMA ydb.MaxTasksPerStage = "2"; -- Ensure configuration where one of chanels will be remote, and one local
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }},
                    {{ "tx": 0, "stage": 1, "tasks": 2 }},
                    {{ "tx": 0, "stage": 2, "tasks": 2 }},
                    {{ "tx": 0, "stage": 3, "tasks": 2 }},
                    {{ "tx": 0, "stage": 4, "tasks": 2 }},
                    {{ "tx": 0, "stage": 5, "tasks": 2 }}
                ] @@;

                $data = SELECT * FROM {pq_source}`{first_input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        event String
                    )
                ) LIMIT 2;

                $grouped = SELECT
                    event,
                    CAST(SOME(time) AS String) AS time,
                    CAST(COUNT(*) AS String) AS count
                FROM $data
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event;

                $processed = SELECT Unwrap(event || "-" || time || "-" || count) AS Data FROM $grouped
                UNION ALL SELECT * FROM {pq_source}`{second_input_topic}`;

                INSERT INTO {pq_source}`{output_topic}` SELECT * FROM $processed;

                UPSERT INTO `{table}` SELECT * FROM $processed;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = LocalTopics ? TStringBuilder() : TStringBuilder() << "`" << pqSourceName << "`.",
            "first_input_topic"_a = firstInputTopicName,
            "second_input_topic"_a = secondInputTopicName,
            "output_topic"_a = outputTopicName,
            "table"_a = tableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        WaitCheckpointUpdate(checkpointId);

        auto disposition = TInstant::Now();
        WriteTopicMessage(firstInputTopicName, R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})", /* partition */ 0, LocalTopics);
        WriteTopicMessage(firstInputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})", /* partition */ 1, LocalTopics);
        ReadTopicMessages(outputTopicName, {"A-2025-08-24T00:00:00.000000Z-1", "A-2025-08-25T00:00:00.000000Z-1"}, disposition, /* sort */ true, LocalTopics);
        WaitCheckpointUpdate(checkpointId);

        // Check table data (should be flushed on checkpoint)
        {
            const auto& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{table_name}` ORDER BY Data;)",
                "table_name"_a = tableName
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

            ui64 index = 0;
            const std::vector expected = {"A-2025-08-24T00:00:00.000000Z-1", "A-2025-08-25T00:00:00.000000Z-1"};
            CheckScriptResult(results[0], 1, 2, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Data").GetString(), expected[index++]);
            });

            ExecQuery(fmt::format(R"(
                DELETE FROM `{table_name}`;)",
                "table_name"_a = tableName
            ));
        }

        // Checkpoints still works
        WaitCheckpointUpdate(checkpointId);
        WaitCheckpointUpdate(checkpointId);

        disposition = TInstant::Now();
        WriteTopicMessage(secondInputTopicName, "test_message", /* partition */ 0, LocalTopics);
        ReadTopicMessage(outputTopicName, "test_message", disposition, LocalTopics);
        WaitCheckpointUpdate(checkpointId);

        // Check table data (should be flushed on checkpoint)
        {
            const auto& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{table_name}` ORDER BY Data;)",
                "table_name"_a = tableName
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

            CheckScriptResult(results[0], 1, 1, [](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Data").GetString(), "test_message");
            });
        }

        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(1, 1);

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 6));

        DropTopic(firstInputTopicName, LocalTopics);
        DropTopic(secondInputTopicName, LocalTopics);
        DropTopic(outputTopicName, LocalTopics);
    }

    Y_UNIT_TEST_TWIN_F(CheckpointPropagationWithUninitializedStatefulOperator, ModernChannels, TStreamingWithSchemaSecretsTestFixture) {
        NodeCount = 2;
        DqChannelsVersion = ModernChannels ? 2 : 1;

        const std::shared_ptr<TConnectorClientMock> connectorClient = SetupMockConnectorClient();

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto limitedInputTopicName = TStringBuilder() << Name_ << "InputTopicNameLimited";
        const auto mainInputTopicName = TStringBuilder() << Name_ << "InputTopicNameMain";
        const auto auxInputTopicName = TStringBuilder() << Name_ << "InputTopicNameAux";
        const auto outputTopicName = TStringBuilder() << Name_ << "OutputTopicName";
        CreateTopic(limitedInputTopicName, NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(2, 2));
        CreateTopic(mainInputTopicName);
        CreateTopic(auxInputTopicName, NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(2, 2));
        CreateTopic(outputTopicName);

        constexpr char outputTableName[] = "outputTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{output_table}` (
                Data String NOT NULL,
                PRIMARY KEY (Data)
            );)",
            "output_table"_a = outputTableName
        ));

        constexpr char lookupTableName[] = "lookup";
        ExecExternalQuery(fmt::format(R"(
            CREATE TABLE `{lookup_table}` (
                fqdn String,
                payload String,
                PRIMARY KEY (fqdn)
            );)",
            "lookup_table"_a = lookupTableName
        ));

        constexpr char ydbSourceName[] = "ydbSourceName";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateYdbSource(ydbSourceName);
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);

        {   // Prepare connector mock
            const std::vector<TColumn> columns = {
                {"fqdn", Ydb::Type::STRING},
                {"payload", Ydb::Type::STRING}
            };
            SetupMockConnectorTableDescription(connectorClient, {
                .TableName = lookupTableName,
                .Columns = columns,
                .DescribeCount = 2,
                .ListSplitsCount = 1
            });

            const std::vector<std::string> fqdnColumn = {"host1.example.com"};
            const std::vector<std::string> payloadColumn = {"P1"};
            SetupMockConnectorTableData(connectorClient, {
                .TableName = lookupTableName,
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

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                -- Emulate situation when right input of map join is hanging,
                -- so left input stay uninitialized with underlying group by hop.
                --
                -- Note: during checkpoint such hanging of right join input will lead to checkpoint hanging,
                -- so in test query will be restarted after zero checkpoint completion.

                PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE"; -- Enable `LIMIT` operator in at-least-once semantic
                PRAGMA ydb.MaxTasksPerStage = "2"; -- Ensure configuration where one of chanels will be remote, and one local
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }},
                    {{ "tx": 0, "stage": 1, "tasks": 2 }},
                    {{ "tx": 0, "stage": 2, "tasks": 2 }},
                    {{ "tx": 0, "stage": 3, "tasks": 2 }},
                    {{ "tx": 0, "stage": 4, "tasks": 2 }},
                    {{ "tx": 0, "stage": 5, "tasks": 2 }},
                    {{ "tx": 0, "stage": 6, "tasks": 2 }},
                    {{ "tx": 0, "stage": 7, "tasks": 2 }},
                    {{ "tx": 0, "stage": 8, "tasks": 2 }}
                ] @@;

                $left_join_stream = SELECT * FROM `{pq_source}`.`{limited_input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        event String
                    )
                );

                $left_join_stream_grouped = SELECT
                    event,
                    CAST(SOME(time) AS String) AS time,
                    CAST(COUNT(*) AS String) AS count
                FROM $left_join_stream
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event;

                -- Both sides of this join is empty, so group by hop wont be initialized at least on one side
                $joined_data = SELECT
                    Unwrap(l.event || "-" || l.time || "-" || l.count || "-" || r.payload) AS Data
                FROM $left_join_stream_grouped AS l
                LEFT JOIN `{ydb_source}`.`{lookup_table}` AS r ON l.event = r.fqdn;

                $united_data = SELECT * FROM $joined_data
                UNION ALL SELECT * FROM `{pq_source}`.`{aux_input_topic}`; -- Actually `aux_input_topic` will hit LIMIT

                $limited_data = SELECT * FROM $united_data LIMIT 2;

                $processed = SELECT * FROM $limited_data
                UNION ALL SELECT * FROM `{pq_source}`.`{main_input_topic}`;

                INSERT INTO `{pq_source}`.`{output_topic}` SELECT * FROM $processed;

                UPSERT INTO `{output_table}` SELECT * FROM $processed;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "limited_input_topic"_a = limitedInputTopicName,
            "aux_input_topic"_a = auxInputTopicName,
            "main_input_topic"_a = mainInputTopicName,
            "output_topic"_a = outputTopicName,
            "output_table"_a = outputTableName,
            "lookup_table"_a = lookupTableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        ValidateStreamingQueryAst(queryName, [](const TString& ast) {
            UNIT_ASSERT_STRING_CONTAINS(ast, "MapJoinCore");
            AstChecker(/* txCount */ 1, /* stagesCount */ 9)(ast);
            Cerr << TString(TStringBuilder() << "Ast:\n" << ast << "\n") << Flush;
        });

        Sleep(TDuration::Seconds(1)); // Wait for zero checkpoint
        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        const auto seqNo = GetLastCheckpointSeqNo(checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(seqNo, 1);

        // Check that query works
        auto disposition = TInstant::Now();
        WriteTopicMessage(limitedInputTopicName, R"({"time": "2025-08-24T00:00:00.000000Z", "event": "host1.example.com"})", /* partition */ 0);
        WriteTopicMessage(limitedInputTopicName, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "host1.example.com"})", /* partition */ 1);
        ReadTopicMessage(outputTopicName, "host1.example.com-2025-08-24T00:00:00.000000Z-1-P1", disposition);

        // Check table empty
        {
            const std::vector<TResultSet>& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{table_name}` ORDER BY Data;)",
                "table_name"_a = outputTableName
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(results[0].RowsCount(), 0);
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));

        // After restart right side of map join is hanging
        connectorClient->LockReading();
        UNIT_ASSERT_VALUES_EQUAL(GetLastCheckpointSeqNo(checkpointId), seqNo); // No checkpoints expected

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));

        // Hit limit value and mark whole join as finished
        disposition = TInstant::Now();
        WriteTopicMessage(auxInputTopicName, "test_message1", /* partition */ 0);
        WriteTopicMessage(auxInputTopicName, "test_message2", /* partition */ 1);
        ReadTopicMessages(outputTopicName, {"test_message1", "test_message2"}, disposition, /* sort */ true);
        UNIT_ASSERT_VALUES_EQUAL(GetLastCheckpointSeqNo(checkpointId), seqNo); // No checkpoints expected
        Sleep(TDuration::Seconds(1));

        // Test that limit is reached
        disposition = TInstant::Now();
        WriteTopicMessage(auxInputTopicName, "test_lost_message", /* partition */ 0);
        WriteTopicMessages(mainInputTopicName, {"test_message3", "test_message4"});
        ReadTopicMessages(outputTopicName, {"test_message3", "test_message4"}, disposition, /* sort */ true);
        UNIT_ASSERT_VALUES_EQUAL(GetLastCheckpointSeqNo(checkpointId), seqNo); // No checkpoints expected

        // Unlock right joins side and wait checkpoint
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        connectorClient->UnlockReading();
        WaitCheckpointUpdate(checkpointId);

        // Check table data (should be flushed on checkpoint)
        {
            const auto& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{table_name}` ORDER BY Data;)",
                "table_name"_a = outputTableName
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

            ui64 index = 0;
            const std::vector expected = {"test_message1", "test_message2", "test_message3", "test_message4"};
            CheckScriptResult(results[0], 1, 4, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Data").GetString(), expected[index++]);
            });
        }

        // Test that query continue working
        disposition = TInstant::Now();
        WriteTopicMessage(mainInputTopicName, "test_message5");
        ReadTopicMessage(outputTopicName, "test_message5", disposition);
        CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        WaitCheckpointUpdate(checkpointId);

        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(2, 1);

        DropTopic(limitedInputTopicName);
        DropTopic(auxInputTopicName);
        DropTopic(mainInputTopicName);
        DropTopic(outputTopicName);
        ExecExternalQuery(fmt::format(R"(
            DROP TABLE `{table_name}`;)",
            "table_name"_a = lookupTableName
        ));
    }

    Y_UNIT_TEST_TWIN_F(CheckpointPropagationWithFiniteResultAndCheckpoints, ModernChannels, TStreamingWithSchemaSecretsTestFixture) {
        NodeCount = 2;
        DqChannelsVersion = ModernChannels ? 2 : 1;
        SetupAppConfig().MutableFeatureFlags()->SetEnableStreamingQueriesCounters(false);

        const auto inputTopicName = TStringBuilder() << Name_ << "InputTopicName";
        const IMockPqGateway::TPtr pqGateway = SetupMockPqGateway({
            .LockWritingByDefault = true,
            .Topics = {{inputTopicName, {.PartitionCount = 2}}},
        });

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto firstOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName1";
        const auto secondOutputTopicName = TStringBuilder() << Name_ << "OutputTopicName2";
        CreateTopic(inputTopicName, NYdb::NTopic::TCreateTopicSettings().PartitioningSettings(2, 2));
        CreateTopic(firstOutputTopicName);
        CreateTopic(secondOutputTopicName);

        constexpr char tableName[] = "outputTable";
        ExecQuery(fmt::format(R"(
                CREATE TABLE `{table_name}` (
                    Data String NOT NULL,
                    PRIMARY KEY (Data)
                );
            )",
            "table_name"_a = tableName
        ));

        constexpr char pqSourceName[] = "pqSourceName";
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets  */ true);

        constexpr TDuration CHECKPOINT_INTERVAL = TDuration::Seconds(10);
        const auto queryName = TStringBuilder() << Name_ << "StreamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "PT{checkpoint_interval}S"
            ) AS
            DO BEGIN
                PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE"; -- Enable `LIMIT` operator in at-least-once semantic
                PRAGMA ydb.MaxTasksPerStage = "2"; -- Ensure configuration where one of chanels will be remote, and one local
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 2 }},
                    {{ "tx": 0, "stage": 1, "tasks": 1 }},
                    {{ "tx": 0, "stage": 2, "tasks": 1 }}
                ] @@;

                $data = SELECT * FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (
                        time String NOT NULL,
                        event String
                    )
                ) LIMIT 3;

                $grouped = SELECT
                    event,
                    CAST(SOME(time) AS String) AS time,
                    CAST(COUNT(*) AS String) AS count
                FROM $data
                GROUP BY
                    HOP (CAST(time AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    event;

                $processed = SELECT Unwrap(event || "-" || time || "-" || count) AS Data FROM $grouped;

                INSERT INTO `{pq_source}`.`{first_output_topic}` SELECT * FROM $processed;

                INSERT INTO `{pq_source}`.`{second_output_topic}` SELECT * FROM $processed;

                UPSERT INTO `{table}` SELECT * FROM $processed;
            END DO;)",
            "query_name"_a = queryName,
            "checkpoint_interval"_a = CHECKPOINT_INTERVAL.Seconds(),
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "first_output_topic"_a = firstOutputTopicName,
            "second_output_topic"_a = secondOutputTopicName,
            "table"_a = tableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        const auto seqNo = GetLastCheckpointSeqNo(checkpointId);

        const auto readSession0 = pqGateway->WaitReadSession(inputTopicName);
        const auto readSession1 = pqGateway->WaitReadSession(inputTopicName);
        readSession0->AddDataReceivedEvent(0, R"({"time": "2025-08-24T00:00:00.000000Z", "event": "A"})");
        readSession0->AddDataReceivedEvent(1, R"({"time": "2025-08-25T00:00:00.000000Z", "event": "A"})");

        const auto firstWriteSession = pqGateway->WaitWriteSession(firstOutputTopicName);
        firstWriteSession->Unlock();
        firstWriteSession->ExpectMessage("A-2025-08-24T00:00:00.000000Z-1");

        const auto secondWriteSession = pqGateway->WaitWriteSession(secondOutputTopicName);
        secondWriteSession->LockAcks(); // One of sinks is hanging
        secondWriteSession->Unlock();
        secondWriteSession->ExpectMessage("A-2025-08-24T00:00:00.000000Z-1");

        // Hit limit and finish most part of the graph
        readSession1->AddDataReceivedEvent(2, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        firstWriteSession->ExpectMessages({"A-2025-08-25T00:00:00.000000Z-1", "A-2025-08-26T00:00:00.000000Z-1"});
        secondWriteSession->ExpectMessages({"A-2025-08-25T00:00:00.000000Z-1", "A-2025-08-26T00:00:00.000000Z-1"});

        // Check table data (should be flushed on finish)
        {
            Sleep(TDuration::Seconds(1));
            const auto& results = ExecQuery(fmt::format(R"(
                SELECT * FROM `{table_name}` ORDER BY Data;)",
                "table_name"_a = tableName
            ));
            UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);

            ui64 index = 0;
            const std::vector expected = {"A-2025-08-24T00:00:00.000000Z-1", "A-2025-08-25T00:00:00.000000Z-1", "A-2025-08-26T00:00:00.000000Z-1"};
            CheckScriptResult(results[0], 1, 3, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Data").GetString(), expected[index++]);
            });
        }

        // Expected graph state now:
        // [source stage, finished] -> [limit stage, finished] -> [group by stage + 2x PQ sinks, waiting sink{1}] -> [table sink stage, finished]
        // Checkpoint will be injected into source stage and must pass all stages

        auto newSeqNo = CheckNoCheckpointUpdate(checkpointId, CHECKPOINT_INTERVAL / 2);
        UNIT_ASSERT_VALUES_EQUAL(newSeqNo, seqNo); // No checkpoints due to checkpointing interval

        const auto pqCountersExtractor = [&](const ui64 nodeIndex) -> std::function<ui64()> {
            const NMonitoring::TDynamicCounterPtr kqpCounters = GetCounters("kqp", nodeIndex);
            const auto sourceTracker = kqpCounters->GetSubgroup("subsystem", "DqSinkTracker");
            const auto sourceCounters = sourceTracker->GetSubgroup("sink", "PqSink");
            const auto inflyData = sourceCounters->GetCounter("InFlyData");
            const auto inFlyCheckpoints = sourceCounters->GetCounter("InFlyCheckpoints");
            const auto inFlyPendingAckCheckpoints = sourceCounters->GetCounter("InFlyPendingAckCheckpoints");

            if (inflyData->Val() != 0) {
                return [inFlyCheckpoints, inFlyPendingAckCheckpoints]() {
                    return inFlyCheckpoints->Val() + inFlyPendingAckCheckpoints->Val();
                };
            }

            return nullptr;
        };

        auto counters = pqCountersExtractor(/* nodeIndex */ 0);
        if (!counters) {
            counters = pqCountersExtractor(/* nodeIndex */ 1);
        }
        UNIT_ASSERT_C(counters, "Counters not found for PQ sink");
        UNIT_ASSERT_VALUES_EQUAL(counters(), 0);

        WaitFor(CHECKPOINT_INTERVAL, "checkpoint propagation", [&](TString& error) {
            if (GetLastCheckpointSeqNo(checkpointId) == seqNo) {
                error = "new checkpoint still is not created";
                return false;
            }

            return counters() == 1;
        });

        newSeqNo = GetLastCheckpointSeqNo(checkpointId);
        secondWriteSession->UnlockAcks();
        WaitCheckpointUpdate(checkpointId, std::pair(1, newSeqNo)); // Checkpoint should successfully finish

        readSession0->ExpectSessionClosed(TDuration::Seconds(1));
        readSession1->ExpectSessionClosed(TDuration::Seconds(1));
        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(1, 0);

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 4));

        DropTopic(inputTopicName);
        DropTopic(firstOutputTopicName);
        DropTopic(secondOutputTopicName);
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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

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
        Sleep(TDuration::Seconds(1)); // wait for checkpoint

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

    Y_UNIT_TEST_QUAD_F(StreamingQueryWithStreamLookupJoin, WithFeatureFlag, WithFullscanFlag, TStreamingTestFixture) {
        if (!WithFeatureFlag && WithFullscanFlag) {
            // legal, but nothing to check
            return;
        }
        NeedsStatsCollectors = true;
        constexpr ui32 combinations = WithFeatureFlag && !WithFullscanFlag ? 2 : 1;
        {
            auto& setupAppConfig = SetupAppConfig();
            setupAppConfig.MutableQueryServiceConfig()->SetProgressStatsPeriodMs(0);
            setupAppConfig.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(WithFeatureFlag);
            setupAppConfig.MutableFeatureFlags()->SetEnableDqSourceStreamLookupJoinFullscan(WithFullscanFlag);
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
                .DescribeCount = 2*combinations,
                // 1 for table-vs-topic, 1 for compilation, doubled for *Bad query compilation
                .ListSplitsCount = WithFeatureFlag ? (WithFullscanFlag ? 1 + 3 * 2 : 1 + 3 + 1) : 0,
                // 1 for compilation (and another one for *Bad query compilation)
                // 3 for lookups, doubled for fullscan mode
                .ValidateListSplitsArgs = false
            });

            if (WithFeatureFlag) {
                ui64 readSplitsCount = 0;
                const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
                SetupMockConnectorTableData(connectorClient, {
                    .TableName = ydbTable,
                    .Columns = columns,
                    .NumberReadSplits = (WithFullscanFlag ? 3*2 : 3),
                    .ValidateReadSplitsArgs = false,
                    .ResultFactory = [&]() {
                        readSplitsCount += 1;
                        const auto payloadColumn = readSplitsCount <= (WithFullscanFlag ? 4 : 2)
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

        auto actorsAlive = GetCounters("utils")->GetSubgroup("execpool", "User")->GetSubgroup("sensor", "ActorsAliveByActivity")->GetNamedCounter("activity", "NYql::NDq::(anonymous namespace)::TInputTransformStreamLookupBase");
        WaitFor(TDuration::Seconds(10), "ActorsAlive", [&](TString& error) {
            auto val = actorsAlive->Val();
            error = TStringBuilder() << "InputTransform actors count is " << val << ", expected 1";
            return val == 1;
        });

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

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(1, 0);

        WaitFor(TDuration::Seconds(10), "ActorsAlive", [&](TString& error) {
            auto val = actorsAlive->Val();
            error = TStringBuilder() << "InputTransform actors count is " << val << ", expected 0";
            return val == 0;
        });

        if (!WithFullscanFlag) {
            // extra check that fullscan option without fullscan feature-flag fails
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}Bad` AS
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
                    LEFT JOIN /*+ streamlookup(FullscanLimit 123) */ ANY $ydb_lookup AS l
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
            EStatus::GENERIC_ERROR,
            "EnableDqSourceStreamLookupJoinFullscan disabled, but FullscanLimit is 123");

            CheckScriptExecutionsCount(2, 0);
        }
    }

    Y_UNIT_TEST_TWIN_F(StreamingQueryWithStreamLookupJoinShuffleMode, WithFeatureFlag, TStreamingTestFixture) {
        {
            auto& setupAppConfig = SetupAppConfig();
            setupAppConfig.MutableQueryServiceConfig()->SetProgressStatsPeriodMs(0);
            setupAppConfig.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(true);
            setupAppConfig.MutableFeatureFlags()->SetEnableDqSourceStreamLookupJoinFullscan(true);
            setupAppConfig.MutableFeatureFlags()->SetEnableDqSourceStreamLookupJoinShuffleMode(WithFeatureFlag);
        }

        constexpr ui64 maxPartitions = 2;
        const TVector<NYql::NDq::EShuffleMode> shuffleModes = {
            NYql::NDq::EShuffleMode::Off,
            NYql::NDq::EShuffleMode::Map,
        };
        ui64 maxTasks = 3;
        ui64 combinations = (WithFeatureFlag ? maxPartitions*maxTasks*shuffleModes.size() : 1);
        const auto connectorClient = SetupMockConnectorClient();

        constexpr char inputTopicName[] = "sljShuffleInputTopicName";
        constexpr char outputTopicName[] = "sljShuffleOutputTopicName";
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
                .DescribeCount = 2*(combinations + !WithFeatureFlag),
                // 1 for table/topic discovery, 1 for LoadMeta
                .ListSplitsCount = (1 + 2*2)*combinations + (!WithFeatureFlag),
                // 1 for LoadMeta, (regular + fullscan) for each each of 2 lookups
                // without feature flag, only one LoadMeta
                .ValidateListSplitsArgs = false
            });

            {
                ui64 readSplitsCount = 0;
                const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
                SetupMockConnectorTableData(connectorClient, {
                    .TableName = ydbTable,
                    .Columns = columns,
                    .NumberReadSplits = 4*combinations,
                    .ValidateReadSplitsArgs = false,
                    .ResultFactory = [&]() {
                        readSplitsCount += 1;
                        const auto payloadColumn = readSplitsCount <= 2
                            ? std::vector<std::string>{"P1", "P2", "P3"}
                            : std::vector<std::string>{"P4", "P5", "P6"};
                        if (readSplitsCount == 4) {
                            readSplitsCount = 0;
                        }

                        return MakeRecordBatch(
                            MakeArray<arrow::BinaryBuilder>("fqdn", fqdnColumn, arrow::binary()),
                            MakeArray<arrow::BinaryBuilder>("payload", payloadColumn, arrow::binary())
                        );
                    }
                });
            }
        }

        for (ui64 partitions = 1; partitions <= maxPartitions; ++partitions) {
            AlterTopic(inputTopicName, NYdb::NTopic::TAlterTopicSettings{}.AlterPartitioningSettings(partitions, partitions));
            for (ui64 tasks = 1; tasks <= maxTasks; ++tasks) {
                for (auto shuffleMode : shuffleModes) {
                    bool expectedSuccess = WithFeatureFlag || shuffleMode == NYql::NDq::EShuffleMode::Off;
                    constexpr char queryName[] = "streamingQuery";
                    ExecQuery(fmt::format(R"(
                        CREATE STREAMING QUERY `{query_name}` AS
                        DO BEGIN
                            PRAGMA ydb.MaxTasksPerStage = "{tasks}";
                            -- PRAGMA ydb.OverridePlanner = @@ [
                            --    {{ "tx": 0, "stage": 0, "tasks": {tasks} }}
                            -- ] @@;
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
                            LEFT JOIN /*+ streamlookup(ShuffleMode {shuffle_mode}
                                                       TTL {ttl}) */ ANY $ydb_lookup AS l
                            ON (l.fqdn = p.host);

                            INSERT INTO `{pq_source}`.`{output_topic}`
                            SELECT Unwrap(event || "-" || payload) FROM $joined
                        END DO;)",
                        "query_name"_a = queryName,
                        "pq_source"_a = pqSourceName,
                        "ydb_source"_a = ydbSourceName,
                        "ydb_table"_a = ydbTable,
                        "input_topic"_a = inputTopicName,
                        "output_topic"_a = outputTopicName,
                        "shuffle_mode"_a = ToString(shuffleMode),
                        "ttl"_a = (tasks > 1 && partitions > 1 && shuffleMode != NYql::NDq::EShuffleMode::Off ? TDuration::Minutes(10) : TDuration::Seconds(1)).Seconds(),
                        "tasks"_a = tasks
                    ),
                    expectedSuccess ? EStatus::SUCCESS : EStatus::GENERIC_ERROR,
                    expectedSuccess ? TStringBuilder() : TStringBuilder() << "EnableDqSourceStreamLookupJoinShuffleMode disabled, but ShuffleMode is " << shuffleMode);
                    if (!expectedSuccess) {
                        return;
                    }
                    // Different scenarios:
                    // Unshuffled: second portion must come after expiring TTL (otherwise it will reuse cache)
                    // Shuffled: second portion lands in different task, so two lookups performed anyway

                    CheckScriptExecutionsCount(1, 1);

                    {
                        auto now = TInstant::Now();
                        Sleep(TDuration::Seconds(1));
                        WriteTopicMessages(inputTopicName, {
                            R"({"time": 0, "event": "A", "host": "host1.example.com"})",
                            R"({"time": 1, "event": "B", "host": "host3.example.com"})",
                            R"({"time": 2, "event": "A", "host": "host1.example.com"})",
                        });

                        ReadTopicMessages(outputTopicName, {"A-P1", "B-P3", "A-P1"}, now, /*sort=*/ true);
                    }

                    Sleep(TDuration::Seconds(1));

                    {
                        auto now = TInstant::Now();
                        Sleep(TDuration::Seconds(1));
                        WriteTopicMessages(inputTopicName, {
                            R"({"time": 3, "event": "A", "host": "host1.example.com"})",
                            R"({"time": 4, "event": "B", "host": "host3.example.com"})",
                            R"({"time": 5, "event": "A", "host": "host1.example.com"})",
                        }, partitions - 1);

                        ReadTopicMessages(outputTopicName, {"A-P4", "B-P6", "A-P4"}, now, /*sort=*/ true);
                    }

                    CheckScriptExecutionsCount(1, 1);

                    const auto results = ExecQuery(
                        "SELECT ast_compressed FROM `.metadata/script_executions`;"
                    );
                    UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
                    CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& result) {
                        const auto& ast = result.ColumnParser(0).GetOptionalString();
                        UNIT_ASSERT(ast);
                        UNIT_ASSERT_STRING_CONTAINS(*ast, "DqCnStreamLookup");
                    });
                    ExecQuery(
                        fmt::format(R"(DROP STREAMING QUERY `{query_name}`;)",
                        "query_name"_a = queryName
                    ));
                    Sleep(TDuration::Seconds(1));
                }
            }
        }
    }

    Y_UNIT_TEST_F(StreamingQueryWithStreamLookupJoinShuffleModeHash, TStreamingTestFixture) {
        {
            auto& setupAppConfig = SetupAppConfig();
            setupAppConfig.MutableQueryServiceConfig()->SetProgressStatsPeriodMs(0);
            setupAppConfig.MutableTableServiceConfig()->SetEnableDqSourceStreamLookupJoin(true);
            setupAppConfig.MutableFeatureFlags()->SetEnableDqSourceStreamLookupJoinFullscan(true);
            setupAppConfig.MutableFeatureFlags()->SetEnableDqSourceStreamLookupJoinShuffleMode(true);
        }

        constexpr ui64 partitions = 2;
        constexpr auto shuffleMode = NYql::NDq::EShuffleMode::Hash;
        ui64 tasks = 3;
        const auto connectorClient = SetupMockConnectorClient();

        constexpr char inputTopicName[] = "sljShuffleHashInputTopicName";
        constexpr char outputTopicName[] = "sljShuffleHashOutputTopicName";
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
                .DescribeCount = 1 + 1, // table/topic discovery and LoadMeta
                .ListSplitsCount = 1 + 1*2, // LoadMeta and 2 lookups
                .ValidateListSplitsArgs = false
            });

            {
                ui64 readSplitsCount = 0;
                const std::vector<std::string> fqdnColumn = {"host1.example.com", "host2.example.com", "host3.example.com"};
                SetupMockConnectorTableData(connectorClient, {
                    .TableName = ydbTable,
                    .Columns = columns,
                    .NumberReadSplits = 2, // 2 lookups (initial + lru refresh)
                    .ValidateReadSplitsArgs = false,
                    .ResultFactory = [&]() {
                        readSplitsCount += 1;
                        const auto payloadColumn = readSplitsCount <= 1
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

        AlterTopic(inputTopicName, NYdb::NTopic::TAlterTopicSettings{}.AlterPartitioningSettings(partitions, partitions));
        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "{tasks}";
                -- PRAGMA ydb.OverridePlanner = @@ [
                --    {{ "tx": 0, "stage": 0, "tasks": {tasks} }}
                -- ] @@;
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
                LEFT JOIN /*+ streamlookup(ShuffleMode {shuffle_mode}
                                           TTL {ttl} FullscanLimit 0) */ ANY $ydb_lookup AS l
                ON (l.fqdn = p.host);

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(event || "-" || payload) FROM $joined
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "ydb_source"_a = ydbSourceName,
            "ydb_table"_a = ydbTable,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "shuffle_mode"_a = ToString(shuffleMode),
            "ttl"_a = TDuration::Seconds(1).Seconds(),
            "tasks"_a = tasks
        ));
        // Hash shuffle: all messages with same key lands in one task,
        // single lookup is performed

        CheckScriptExecutionsCount(1, 1);

        // write same keys to two partitions -> they lands in same task/transform actor
        // (with ShuffleMode Map they'd land in different tasks, hence they'd take 2 lookups-per-refresh, 4 total)
        {
            auto now = TInstant::Now();
            Sleep(TDuration::Seconds(1));
            WriteTopicMessages(inputTopicName, {
                R"({"time": 0, "event": "A", "host": "host1.example.com"})",
                R"({"time": 1, "event": "B", "host": "host1.example.com"})",
                R"({"time": 2, "event": "A", "host": "host1.example.com"})",
            }, /*partition=*/0);
            WriteTopicMessages(inputTopicName, {
                R"({"time": 0, "event": "a", "host": "host1.example.com"})",
                R"({"time": 1, "event": "b", "host": "host1.example.com"})",
                R"({"time": 2, "event": "a", "host": "host1.example.com"})",
            }, partitions - 1);

            ReadTopicMessages(outputTopicName, {
                "A-P1", "B-P1", "A-P1", "a-P1", "b-P1", "a-P1"
            }, now, /*sort=*/ true);
        }

        Sleep(TDuration::Seconds(1));

        {
            auto now = TInstant::Now();
            Sleep(TDuration::Seconds(1));
            WriteTopicMessages(inputTopicName, {
                R"({"time": 0, "event": "A", "host": "host1.example.com"})",
                R"({"time": 1, "event": "B", "host": "host1.example.com"})",
                R"({"time": 2, "event": "A", "host": "host1.example.com"})",
            }, /*partition=*/0);
            WriteTopicMessages(inputTopicName, {
                R"({"time": 3, "event": "a", "host": "host1.example.com"})",
                R"({"time": 4, "event": "b", "host": "host1.example.com"})",
                R"({"time": 5, "event": "a", "host": "host1.example.com"})",
            }, partitions - 1);

            ReadTopicMessages(outputTopicName, {
                "A-P4", "B-P4", "A-P4", "a-P4", "b-P4", "a-P4"
            }, now, /*sort=*/ true);
        }

        CheckScriptExecutionsCount(1, 1);

        const auto results = ExecQuery(
            "SELECT ast_compressed FROM `.metadata/script_executions`;"
        );
        UNIT_ASSERT_VALUES_EQUAL(results.size(), 1);
        CheckScriptResult(results[0], 1, 1, [&](TResultSetParser& result) {
            const auto& ast = result.ColumnParser(0).GetOptionalString();
            UNIT_ASSERT(ast);
            UNIT_ASSERT_STRING_CONTAINS(*ast, "DqCnStreamLookup");
        });
        ExecQuery(
            fmt::format(R"(DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));
        Sleep(TDuration::Seconds(1));
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
                PRAGMA ydb.DqChannelVersion = "2";

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
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit

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

    Y_UNIT_TEST_F(StreamingQueryJoinRecalculationOnRetry, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "streamingQueryJoinRecalculationOnRetryInputTopic";
        constexpr char outputTopicName[] = "streamingQueryJoinRecalculationOnRetryOutputTopic";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char oltpTableName[] = "oltpTable";
        constexpr char olapTableName[] = "olapTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{oltp_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `{olap_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = COLUMN
            );)",
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{oltp_table}`(Key, Value)
            VALUES (1, "oltp-1");

            INSERT INTO `{olap_table}`(Key, Value)
            VALUES (1, "olap-1");)",
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    Unwrap(oltp.Value || "-" || olap.Value)
                FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = json_each_row,
                    SCHEMA (
                        Key Int32 NOT NULL
                    )
                ) AS topic
                LEFT JOIN `{oltp_table}` AS oltp ON topic.Key = oltp.Key
                LEFT JOIN `{olap_table}` AS olap ON topic.Key = olap.Key
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, R"({"Key": 1})");
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessage("oltp-1-olap-1");

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{oltp_table}`(Key, Value)
            VALUES (1, "oltp-2");

            UPSERT INTO `{olap_table}`(Key, Value)
            VALUES (1, "olap-2");)",
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, R"({"Key": 1})");
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessage("oltp-2-olap-2");
    }

    Y_UNIT_TEST_F(StreamingQueryJoinRecalculationOnManualRestart, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "streamingQueryJoinRecalculationOnManualRestartInputTopic";
        constexpr char outputTopicName[] = "streamingQueryJoinRecalculationOnManualRestartOutputTopic";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);
        CreatePqSource(pqSourceName);

        constexpr char oltpTableName[] = "oltpTable";
        constexpr char olapTableName[] = "olapTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{oltp_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `{olap_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = COLUMN
            );)",
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{oltp_table}`(Key, Value)
            VALUES (1, "oltp-1");

            INSERT INTO `{olap_table}`(Key, Value)
            VALUES (1, "olap-1");)",
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    Unwrap(oltp.Value || "-" || olap.Value)
                FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = json_each_row,
                    SCHEMA (
                        Key Int32 NOT NULL
                    )
                ) AS topic
                LEFT JOIN `{oltp_table}` AS oltp ON topic.Key = oltp.Key
                LEFT JOIN `{olap_table}` AS olap ON topic.Key = olap.Key
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName,
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"Key": 1})");
        ReadTopicMessage(outputTopicName, "oltp-1-olap-1");
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{oltp_table}`(Key, Value)
            VALUES (1, "oltp-2");

            UPSERT INTO `{olap_table}`(Key, Value)
            VALUES (1, "olap-2");)",
            "oltp_table"_a = oltpTableName,
            "olap_table"_a = olapTableName
        ));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        WriteTopicMessage(inputTopicName, R"({"Key": 1})");
        const auto disposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName, "oltp-2-olap-2", disposition);
    }

    Y_UNIT_TEST_F(StreamingQueryWithPrecompute, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "streamingQueryWithPrecomputeInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithPrecomputeOutputTopic";
        constexpr char pqSourceName[] = "pqSourceName";
        CreateTopic(inputTopicName, NTopic::TCreateTopicSettings().PartitioningSettings(2, 2));
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
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit

        const auto& result = ExecQuery("SELECT Plan, Ast FROM `.sys/streaming_queries`");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        CheckScriptResult(result[0], 2, 1, [&](TResultSetParser& resultSet) {
            AstChecker(2, 3)(resultSet.ColumnParser("Ast").GetOptionalUtf8().value_or(""));

            const auto planJson = resultSet.ColumnParser("Plan").GetOptionalUtf8().value_or("");
            Cerr << "Plan: " << planJson << Endl;
            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(planJson, &plan));

            const auto& stagePlan = plan["Plan"]["Plans"][0]["Plans"][0];
            UNIT_ASSERT_VALUES_EQUAL(stagePlan["Node Type"].GetStringSafe(), "Stage");
            UNIT_ASSERT_VALUES_EQUAL(stagePlan["Stats"]["Tasks"].GetIntegerSafe(), 2);

            const auto& sourceOp = stagePlan["Plans"][0]["Operators"].GetArraySafe()[0];
            UNIT_ASSERT_VALUES_EQUAL(sourceOp["ExternalDataSource"].GetStringSafe(), pqSourceName);
            UNIT_ASSERT_VALUES_EQUAL(sourceOp["SourceType"].GetStringSafe(), "pq");
        });

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

    Y_UNIT_TEST_F(StreamingQueryPrecomputeRecalculationOnRetry, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopicName[] = "streamingQueryPrecomputeRecalculationOnRetryInputTopic";
        constexpr char outputTopicName[] = "streamingQueryPrecomputeRecalculationOnRetryOutputTopic";
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

        const auto readSession = pqGateway->WaitReadSession(inputTopicName);
        readSession->AddDataReceivedEvent(0, "message-1");
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessage("message-1-value-1");

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{table_name}`
                (Key, Value)
            VALUES
                (1, "value-2");)",
            "table_name"_a = tableName
        ));

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        pqGateway->WaitReadSession(inputTopicName)->AddDataReceivedEvent(1, "message-2");
        pqGateway->WaitWriteSession(outputTopicName)->ExpectMessage("message-2-value-2");
    }

    Y_UNIT_TEST_F(StreamingQueryWithDifferentPrecomputeTypes, TStreamingTestFixture) {
        constexpr char oltpTableName[] = "oltpTable";
        constexpr char olapTableName[] = "olapTable";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{oltp_table_name}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `{olap_table_name}` (
                Key Int32 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = COLUMN
            );)",
            "oltp_table_name"_a = oltpTableName,
            "olap_table_name"_a = olapTableName
        ));

        ExecQuery(fmt::format(R"(
            UPSERT INTO `{oltp_table_name}`
                (Key, Value)
            VALUES
                (1, "value-1"),
                (2, "value-1");
            UPSERT INTO `{olap_table_name}`
                (Key, Value)
            VALUES
                (1, "value-1"),
                (2, "value-1");)",
            "oltp_table_name"_a = oltpTableName,
            "olap_table_name"_a = olapTableName
        ));

        constexpr char sourceBucket[] = "test_streaming_query_with_s3_join";
        constexpr char objectContent[] = R"(
{"Key": 1, "Value": "value-1"}
{"Key": 2, "Value": "value-1"})";
        CreateBucketWithObject(sourceBucket, "path/test_object.json", objectContent);

        constexpr char pqSourceName[] = "pqSourceName";
        constexpr char s3SourceName[] = "s3Source";
        CreatePqSource(pqSourceName);
        CreateS3Source(sourceBucket, s3SourceName);

        constexpr char externalTableName[] = "externalTable";
        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL TABLE `{external_table}` (
                Key Int32 NOT NULL,
                Value String NOT NULL
            ) WITH (
                DATA_SOURCE = "{external_source}",
                LOCATION = "path/test_object.json",
                FORMAT = "json_each_row"
            );)",
            "external_table"_a = externalTableName,
            "external_source"_a = s3SourceName
        ));

        constexpr char inputTopicName[] = "streamingQueryWithDifferentPrecomputeTypesInputTopicName";
        constexpr char outputTopicName[] = "streamingQueryWithDifferentPrecomputeTypesOutputTopicName";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        for (const auto& sourceName : {oltpTableName, olapTableName, externalTableName}) {
            constexpr char queryName[] = "streamingQuery";

            ExecQuery(fmt::format(R"(
                CREATE OR REPLACE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    $precompute_agg = SELECT CAST(MAX(Key) AS String) FROM `{precompute_source}`;
                    $precompute_limit = SELECT Value FROM `{precompute_source}` LIMIT 1;
                    $empty_precompute = SELECT Value FROM `{precompute_source}` WHERE Key = 3;

                    INSERT INTO `{pq_source}`.`{output_topic}`
                    SELECT
                        Unwrap(Data || $precompute_agg || $precompute_limit || ($empty_precompute ?? "<null>"))
                    FROM `{pq_source}`.`{input_topic}`;
                END DO;)",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "precompute_source"_a = sourceName,
                "input_topic"_a = inputTopicName,
                "output_topic"_a = outputTopicName
            ));

            Sleep(TDuration::Seconds(1));

            const auto disposition = TInstant::Now();
            auto message = TStringBuilder() << "test_message" << sourceName;
            WriteTopicMessage(inputTopicName, message);
            ReadTopicMessage(outputTopicName, message << "2value-1<null>", disposition);
        }
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

        Sleep(TDuration::Seconds(1));  // wait for checkpoint commit

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

        constexpr char consumerName[] = "test_consumer";
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

        WaitFor(TDuration::Seconds(10), "Wait query running", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Status FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            std::string status;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                status = resultSet.ColumnParser("Status").GetOptionalUtf8().value_or("");
            });

            error = TStringBuilder() << "Query status: " << status;
            return status == "RUNNING";
        });

        AlterTopic(inputTopicName, NYdb::NTopic::TAlterTopicSettings{}.AppendDropConsumers(consumerName));

        WaitFor(TDuration::Seconds(10), "Wait fail", [&](TString& error) {
            const auto& issues = GetStreamingQueryIssues(queryName);
            error = TStringBuilder() << "Query issues: " << issues;
            return issues.contains("no read rule provided for consumer 'test_consumer'");
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
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit

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
        TString InputTopicName;
        TString OutputTopicName;
        TString PqSourceName;
        TString QueryName;
        TString QueryText;
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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

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
        Sleep(TDuration::Seconds(1));  // wait for checkpoint commit

        WriteTopicMessage(info.InputTopicName, R"({"time": "2025-08-26T00:00:00.000000Z", "event": "A"})");
        ReadTopicMessage(info.OutputTopicName, "B-2025-08-25T00:00:00.000000Z-1", readDisposition);
        Sleep(TDuration::Seconds(1));  // wait for checkpoint commit

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
            setupAppConfig.MutableFeatureFlags()->SetEnableDqSourceStreamLookupJoinFullscan(true);
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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

        connectorClient->LockReading();
        WriteTopicMessage(inputTopicName, R"({"time": 1, "event": "B", "host": "host3.example.com"})");
        Sleep(TDuration::Seconds(2)); // wait for checkpoint commit
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
        Sleep(TDuration::Seconds(2)); // wait for checkpoint commit
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
            Sleep(TDuration::Seconds(1)); // wait for checkpoit commit
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
                    PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE";
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
                    PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE";
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

        Sleep(TDuration::Seconds(1));
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

    void CreateMultiOutputQuery(TStreamingTestFixture& self, const std::string& queryName, const std::string& pqSource,
        const std::string& inputTopic, const std::string& outputTopic1, const std::string& outputTopic2,
        const std::string& rowTable, const std::string& columnTable)
    {
        self.ExecQuery(fmt::format(R"(
            CREATE TABLE `{row_table}` (
                Key String NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );
            CREATE TABLE `{column_table}` (
                Key String NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (
                STORE = COLUMN
            );)",
            "row_table"_a = rowTable,
            "column_table"_a = columnTable
        ));

        self.ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT Key, Value FROM `{pq_source}`.`{input_topic}` WITH (
                    FORMAT = json_each_row,
                    SCHEMA (
                        Key String NOT NULL,
                        Value String NOT NULL
                    )
                );

                INSERT INTO `{pq_source}`.`{output_topic1}` SELECT Unwrap(Value || "-t1") AS Data FROM $in;
                INSERT INTO `{pq_source}`.`{output_topic2}` SELECT Unwrap(Value || "-t2") AS Data FROM $in;
                UPSERT INTO `{row_table}` SELECT Key, Unwrap(Value || "-r") AS Value FROM $in;
                UPSERT INTO `{column_table}` SELECT Key, Unwrap(Value || "-c") AS Value FROM $in;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSource,
            "input_topic"_a = inputTopic,
            "output_topic1"_a = outputTopic1,
            "output_topic2"_a = outputTopic2,
            "row_table"_a = rowTable,
            "column_table"_a = columnTable
        ));
    }

    Y_UNIT_TEST_F(StreamingQueryMultiOutputRestart, TStreamingTestFixture) {
        constexpr char inputTopic[] = "streamingQueryMultiOutputRestartInputTopic";
        constexpr char outputTopic1[] = "streamingQueryMultiOutputRestartOutputTopic1";
        constexpr char outputTopic2[] = "streamingQueryMultiOutputRestartOutputTopic2";
        constexpr char pqSource[] = "pqSourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic1);
        CreateTopic(outputTopic2);
        CreatePqSource(pqSource);

        constexpr char rowTable[] = "rowSink";
        constexpr char columnTable[] = "columnSink";
        constexpr char queryName[] = "streamingQuery";
        CreateMultiOutputQuery(*this, queryName, pqSource, inputTopic, outputTopic1, outputTopic2, rowTable, columnTable);
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopic, R"({"Key": "k1", "Value": "m1"})");
        ReadTopicMessage(outputTopic1, "m1-t1");
        ReadTopicMessage(outputTopic2, "m1-t2");
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit
        CheckTable(*this, rowTable, {{"k1", "m1-r"}});
        CheckTable(*this, columnTable, {{"k1", "m1-c"}});

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);
        Sleep(TDuration::Seconds(1));

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopic, R"({"Key": "k2", "Value": "m2"})");
        ReadTopicMessage(outputTopic1, "m2-t1", disposition);
        ReadTopicMessage(outputTopic2, "m2-t2", disposition);
        Sleep(TDuration::Seconds(1));
        CheckTable(*this, rowTable, {{"k1", "m1-r"}, {"k2", "m2-r"}});
        CheckTable(*this, columnTable, {{"k1", "m1-c"}, {"k2", "m2-c"}});
    }

    Y_UNIT_TEST_F(StreamingQueryMultiOutputCheckpointRecovery, TStreamingTestFixture) {
        constexpr char inputTopic[] = "streamingQueryMultiOutputCheckpointRecoveryInputTopic";
        constexpr char outputTopic1[] = "streamingQueryMultiOutputCheckpointRecoveryOutputTopic1";
        constexpr char outputTopic2[] = "streamingQueryMultiOutputCheckpointRecoveryOutputTopic2";
        constexpr char pqSource[] = "pqSourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic1);
        CreateTopic(outputTopic2);
        CreatePqSource(pqSource);

        constexpr char rowTable[] = "rowSink";
        constexpr char columnTable[] = "columnSink";
        constexpr char queryName[] = "streamingQuery";
        CreateMultiOutputQuery(*this, queryName, pqSource, inputTopic, outputTopic1, outputTopic2, rowTable, columnTable);
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopic, R"({"Key": "k1", "Value": "m1"})");
        ReadTopicMessage(outputTopic1, "m1-t1");
        ReadTopicMessage(outputTopic2, "m1-t2");
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit
        CheckTable(*this, rowTable, {{"k1", "m1-r"}});
        CheckTable(*this, columnTable, {{"k1", "m1-c"}});

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(1, 0);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(inputTopic, R"({"Key": "k2", "Value": "m2"})");
        const auto disposition = TInstant::Now();

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            );)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        // Every output receives exactly the message added after the checkpoint (offset recovery, no loss / no duplicates)
        ReadTopicMessage(outputTopic1, "m2-t1", disposition);
        ReadTopicMessage(outputTopic2, "m2-t2", disposition);
        Sleep(TDuration::Seconds(1));
        CheckTable(*this, rowTable, {{"k1", "m1-r"}, {"k2", "m2-r"}});
        CheckTable(*this, columnTable, {{"k1", "m1-c"}, {"k2", "m2-c"}});
    }

    Y_UNIT_TEST_F(StreamingQueryMultiOutputConsistencyOnRestart, TStreamingTestFixture) {
        const auto pqGateway = SetupMockPqGateway();

        constexpr char inputTopic[] = "streamingQueryMultiOutputConsistencyInputTopic";
        constexpr char outputTopic1[] = "streamingQueryMultiOutputConsistencyOutputTopic1";
        constexpr char outputTopic2[] = "streamingQueryMultiOutputConsistencyOutputTopic2";
        constexpr char pqSource[] = "pqSourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic1);
        CreateTopic(outputTopic2);
        CreatePqSource(pqSource);

        constexpr char rowTable[] = "rowSink";
        constexpr char columnTable[] = "columnSink";
        constexpr char queryName[] = "streamingQuery";
        CreateMultiOutputQuery(*this, queryName, pqSource, inputTopic, outputTopic1, outputTopic2, rowTable, columnTable);
        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        const auto readSession = pqGateway->WaitReadSession(inputTopic);
        readSession->AddDataReceivedEvent(0, R"({"Key": "k1", "Value": "m1"})");
        pqGateway->WaitWriteSession(outputTopic1)->ExpectMessage("m1-t1");
        pqGateway->WaitWriteSession(outputTopic2)->ExpectMessage("m1-t2");
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit
        CheckTable(*this, rowTable, {{"k1", "m1-r"}});
        CheckTable(*this, columnTable, {{"k1", "m1-c"}});

        // Automatic restart via read session failure
        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        pqGateway->WaitReadSession(inputTopic)->AddDataReceivedEvent(1, R"({"Key": "k2", "Value": "m2"})");

        // All outputs stay consistent after restart: exactly the second message, no re-delivery of the first
        pqGateway->WaitWriteSession(outputTopic1)->ExpectMessage("m2-t1");
        pqGateway->WaitWriteSession(outputTopic2)->ExpectMessage("m2-t2");
        Sleep(TDuration::Seconds(1));
        CheckTable(*this, rowTable, {{"k1", "m1-r"}, {"k2", "m2-r"}});
        CheckTable(*this, columnTable, {{"k1", "m1-c"}, {"k2", "m2-c"}});
    }

    Y_UNIT_TEST_F(StreamingQueryMultiOutputInvalidConfigurations, TStreamingTestFixture) {
        constexpr char inputTopic[] = "streamingQueryMultiOutputInvalidInputTopic";
        constexpr char outputTopic[] = "streamingQueryMultiOutputInvalidOutputTopic";
        constexpr char pqSource[] = "pqSourceName";
        constexpr char otherPqSource[] = "otherPqSourceName";
        CreateTopic(inputTopic);
        CreateTopic(outputTopic);
        CreatePqSource(pqSource);
        CreatePqSource(otherPqSource);

        constexpr char rowTable[] = "rowSink";
        constexpr char columnTable[] = "columnSink";
        ExecQuery(fmt::format(R"(
            CREATE TABLE `{row_table}` (
                Value String NOT NULL,
                PRIMARY KEY (Value)
            );
            CREATE TABLE `{column_table}` (
                Value String NOT NULL,
                PRIMARY KEY (Value)
            ) WITH (
                STORE = COLUMN 
            );)",
            "row_table"_a = rowTable,
            "column_table"_a = columnTable
        ));

        for (const auto& sink : {
            TStringBuilder() << "nonExistentSource`.`" << outputTopic,
            TStringBuilder() << rowTable << "Unk"
        }) {
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `streamingQuery` AS
                DO BEGIN
                    INSERT INTO `{sink}`
                    SELECT Data FROM `{pq_source}`.`{input_topic}`
                END DO;)",
                "pq_source"_a = pqSource,
                "input_topic"_a = inputTopic,
                "sink"_a = sink
            ), EStatus::SCHEME_ERROR, "does not exist");
        }

        for (const auto& mode : {"UPSERT", "REPLACE"}) {
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `streamingQuery` AS
                DO BEGIN
                    {mode} INTO `{pq_source}`.`{output_topic}`
                    SELECT Data FROM `{pq_source}`.`{input_topic}`
                END DO;)",
                "mode"_a = mode,
                "pq_source"_a = pqSource,
                "input_topic"_a = inputTopic,
                "output_topic"_a = outputTopic
            ), EStatus::GENERIC_ERROR, TStringBuilder() << "Write mode '" << to_lower(TString(mode)) << "' is not supported for external entities");
        }

        for (const auto& table : {rowTable, columnTable}) {
            for (const auto& mode : {"INSERT", "REPLACE"}) {
                ExecQuery(fmt::format(R"(
                    CREATE STREAMING QUERY `streamingQuery` AS
                    DO BEGIN
                        {mode} INTO `{table}`
                        SELECT Unwrap(Value) AS Value FROM `{pq_source}`.`{input_topic}` WITH (
                            FORMAT = json_each_row,
                            SCHEMA (
                                Value String NOT NULL
                            )
                        )
                    END DO;)",
                    "mode"_a = mode,
                    "table"_a = table,
                    "pq_source"_a = pqSource,
                    "input_topic"_a = inputTopic
                ), EStatus::GENERIC_ERROR, "Only UPSERT writing mode is supported for YDB writes inside streaming queries");
            }
        }

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `streamingQuery` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` SELECT Data FROM `{pq_source}`.`{input_topic}`;
                SELECT * FROM `{pq_source}`.`{input_topic}`;
            END DO;)",
            "pq_source"_a = pqSource,
            "input_topic"_a = inputTopic,
            "output_topic"_a = outputTopic
        ), EStatus::GENERIC_ERROR, "Results is not allowed for streaming queries, please use INSERT to record the query result");
    }

    Y_UNIT_TEST_F(DropStreamingQueryDuringRetries, TStreamingWithSchemaSecretsTestFixture) {
        constexpr char topic[] = "dropStreamingQueryDuringRetriesTopic";
        constexpr char pqSource[] = "pqSource";
        CreateTopic(topic);
        CreatePqSource(pqSource);
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        const auto queryName = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{topic}`
                SELECT * FROM `{pq_source}`.`{topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSource,
            "topic"_a = topic
        ));

        WaitFor(TDuration::Seconds(10), "Wait query running", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Status FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            TString status;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                status = resultSet.ColumnParser("Status").GetOptionalUtf8().value_or("");
            });

            error = TStringBuilder() << "Query status: " << status;
            return status == "RUNNING";
        });

        DropTopic(topic);

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
                UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Status").GetOptionalUtf8(), "STOPPED");
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
        Sleep(TDuration::Seconds(2)); // Wait for checkpoint

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

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 3));

        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit

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

    Y_UNIT_TEST_F(StreamingQueryWithTwoGroupByHopsOnSameKey, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "streamingQueryWithTwoGroupByHopsOnSameKeyInputTopic";
        constexpr char outputTopicName1[] = "streamingQueryWithTwoGroupByHopsOnSameKeyOutputTopic1";
        constexpr char outputTopicName2[] = "streamingQueryWithTwoGroupByHopsOnSameKeyOutputTopic2";
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
                        time1 String NOT NULL,
                        time2 String NOT NULL,
                        key String
                    )
                );

                $grouped1 = SELECT
                    key,
                    SOME(time1) AS time1,
                    CAST(COUNT(*) AS String) AS count
                FROM $pq_source
                GROUP BY
                    HOP (CAST(time1 AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    key;

                $grouped2 = SELECT
                    key,
                    SOME(time2) AS time2,
                    CAST(COUNT(*) AS String) AS count
                FROM $pq_source
                GROUP BY
                    HOP (CAST(time2 AS Timestamp), "PT1H", "PT1H", "PT0H"),
                    key;

                INSERT INTO `{pq_source}`.`{output_topic1}`
                SELECT Unwrap(key || "-" || time1 || "-" || count) FROM $grouped1;

                INSERT INTO `{pq_source}`.`{output_topic2}`
                SELECT Unwrap(key || "-" || time2 || "-" || count) FROM $grouped2;
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
            R"({"time1": "2025-08-24T00:00:00.000000Z", "time2": "2028-08-24T00:00:00.000000Z", "key": "A"})",
            R"({"time1": "2025-08-25T00:00:00.000000Z", "time2": "2028-08-25T00:00:00.000000Z", "key": "B"})",
        });
        ReadTopicMessage(outputTopicName1, "A-2025-08-24T00:00:00.000000Z-1");
        ReadTopicMessage(outputTopicName2, "A-2028-08-24T00:00:00.000000Z-1");

        ValidateStreamingQueryAst(queryName, AstChecker(/* txCount */ 1, /* stagesCount */ 3));

        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));

        const auto disposition = TInstant::Now();
        WriteTopicMessage(inputTopicName, R"({"time1": "2025-08-26T00:00:00.000000Z", "time2": "2028-08-26T00:00:00.000000Z", "key": "C"})");
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
            "query_name"_a = queryName
        ));
        CheckScriptExecutionsCount(2, 1);

        ReadTopicMessage(outputTopicName1, "B-2025-08-25T00:00:00.000000Z-1", disposition);
        ReadTopicMessage(outputTopicName2, "B-2028-08-25T00:00:00.000000Z-1", disposition);
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

        const auto& result2 = ExecQuery(fmt::format(R"(SELECT * FROM `{topic}` LIMIT 1)","topic"_a = topic));
        CheckScriptResult(result2[0], 1, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetString(), "data");
        });

        const auto& result3 = ExecQuery(fmt::format(R"(SELECT * FROM `{topic}` WITH(STREAMING="FALSE"))","topic"_a = topic));
        CheckScriptResult(result3[0], 1, partitionCount, [&](TResultSetParser& resultSet) {
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

    Y_UNIT_TEST_F(StreamingQueryWithFlattenListBy, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "streamingQueryWithFlattenListByInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithFlattenListByOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"sql(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Item FROM `{pq_source}`.`{input_topic}`
                FLATTEN LIST BY (String::SplitToList(Data, ",") AS Item)
            END DO;)sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "A,B,C,D,E");
        ReadTopicMessages(outputTopicName, {"A", "B", "C", "D", "E"});
    }

    Y_UNIT_TEST_F(StreamingQueryWithOffsetAndLimit, TStreamingTestFixture) {
        constexpr char inputTopicName[] = "streamingQueryWithOffsetAndLimitInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithOffsetAndLimitOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"sql(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE";
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
                LIMIT 1 OFFSET 1
            END DO;)sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessages(inputTopicName, {"A", "B", "C"});
        ReadTopicMessage(outputTopicName, "B");

        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(1, 0);
    }

    Y_UNIT_TEST_TWIN_F(StreamingQueryWithProcess, EnableKqpConstraintsTransformer, TStreamingTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableKqpConstraintsTransformer(EnableKqpConstraintsTransformer);

        constexpr char inputTopicName[] = "streamingQueryWithProcessInputTopic";
        constexpr char outputTopicName[] = "streamingQueryWithProcessOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"sql(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                {opt_pragma};

                $s = SELECT Data || "-1" AS D1, Data || "-2" AS D2 FROM `{pq_source}`.`{input_topic}` LIMIT 1;

                $serialize_json = ($input)->{{
                    $serialize = YQL::Udf(AsAtom("ClickHouseClient.SerializeFormat"), Void(), TupleType(TupleType(TypeOf($input))), AsAtom("json_each_row"));
                    return Yql::Map($serialize($input), ($out)->(<|Data: $out|>));
                }};

                $p = PROCESS $s USING $serialize_json(TableRows());

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Unwrap(Data) FROM $p WHERE Data IS NOT NULL;
            END DO;)sql",
            "opt_pragma"_a = EnableKqpConstraintsTransformer ? "PRAGMA ydb.OptValidateStreamingConstraints = \"false\";" : "",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "X");
        ReadTopicMessage(outputTopicName, "{\"D1\":\"X-1\",\"D2\":\"X-2\"}\n");

        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(1, 0);
    }

    Y_UNIT_TEST_TWIN_F(StreamingQueryWithCdcReading, UseLocalTopics, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;

        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(UseLocalTopics);

        constexpr char outputTopic[] = "outputTopicName";
        CreateTopic(outputTopic, std::nullopt, UseLocalTopics);

        constexpr char outPqSource[] = "outSourceName";
        CreatePqSource(outPqSource);

        constexpr char inputPqSource[] = "inputSourceName";
        ExecQuery(fmt::format(
            R"sql(
                CREATE EXTERNAL DATA SOURCE `{pq_source}` WITH (
                    SOURCE_TYPE = "Ydb",
                    LOCATION = "{pq_location}",
                    DATABASE_NAME = "{pq_database_name}",
                    AUTH_METHOD = "NONE"
                );
            )sql",
            "pq_source"_a = inputPqSource,
            "pq_location"_a = GetInternalDriver()->GetConfig().GetEndpoint(),
            "pq_database_name"_a = GetInternalDriver()->GetConfig().GetDatabase()
        ));

        constexpr char tableName[] = "tableName";
        ExecQuery(fmt::format(R"sql(
            CREATE TABLE `{t}` (
                id String NOT NULL,
                val Int64 NOT NULL,
                PRIMARY KEY (id)
            );
        )sql", "t"_a = tableName));

        constexpr char changefeedName[] = "changelog";
        ExecQuery(fmt::format(R"sql(
            ALTER TABLE `{t}` ADD CHANGEFEED `{c}` WITH (
                MODE = 'NEW_AND_OLD_IMAGES',
                FORMAT = 'JSON',
                RETENTION_PERIOD = Interval('PT1H')
            );
        )sql", "t"_a = tableName, "c"_a = changefeedName));

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"sql(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $messages = SELECT
                    Unwrap(Yson::ConvertTo(
                        Data, Struct<
                            newImage: Struct<val: Int64>,
                            oldImage: Yson?,
                            key: Tuple<String>
                        >
                    )) AS Parsed
                FROM {input_source}`{table}/{changefeed}`
                WITH (
                    FORMAT = json_as_string,
                    SCHEMA (
                        Data Json
                    )
                );

                INSERT INTO {output_source}`{output_name}`
                SELECT
                    ToBytes(Unwrap(Yson2::SerializeJson(Yson::From(AsStruct(
                        String::Base64Decode(Parsed.key.0) AS id,
                        Parsed.newImage.val AS val
                    )))))
                FROM $messages
                WHERE Parsed.oldImage IS NULL
            END DO;)sql",
            "table"_a = tableName,
            "changefeed"_a = changefeedName,
            "output_name"_a = outputTopic,
            "input_source"_a = UseLocalTopics ? TStringBuilder() : TStringBuilder() << inputPqSource << ".",
            "output_source"_a = UseLocalTopics ? TStringBuilder() : TStringBuilder() << outPqSource << ".",
            "query_name"_a = queryName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        ExecQuery(fmt::format(R"sql(
            UPSERT INTO `{t}`(id, val) VALUES ("X", 42);
            UPSERT INTO `{t}`(id, val) VALUES ("X", 13);
        )sql", "t"_a = tableName));

        ReadTopicMessage(outputTopic, R"({"id":"X","val":42})", TInstant::Now() - TDuration::Seconds(100), UseLocalTopics);
    }

    Y_UNIT_TEST_F(ReadTopicSchemaWithYdbPrefixIsProhibited, TStreamingTestFixture) {
        const std::string sourceName = "schema_with_ydb_prefix_source";
        CreatePqSource(sourceName);

        const std::string topicName = "schema_with_ydb_prefix_topic";
        CreateTopic(topicName);

        // Schema column name starting with __ydb_ should be rejected
        ExecQuery(fmt::format(R"(
            SELECT * FROM `{source}`.`{topic}` WITH (
                FORMAT = "json_each_row",
                SCHEMA (
                    __ydb_my_field String NOT NULL,
                    value String NOT NULL
                )
            )
            LIMIT 1;)",
            "source"_a = sourceName,
            "topic"_a = topicName
        ),
        EStatus::GENERIC_ERROR,
        "names starting with '__ydb_' are reserved for system columns");
    }

    Y_UNIT_TEST_F(StreamingQueryInvalidationAfterCreation, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "streamingQueryInvalidationAfterCreationInputTopic1";
        constexpr char outputTopicName[] = "streamingQueryInvalidationAfterCreationOutputTopic";
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
                    SELECT * FROM `{pq_source}`.`{input_topic}`;
                END DO;
            )sql",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "test_message");
        ReadTopicMessage(outputTopicName, "test_message");

        constexpr ui64 changesCount = 10;
        for (ui64 i = 0; i < changesCount; ++i) {
            ExecQuery(fmt::format(
                R"sql(
                    ALTER STREAMING QUERY `{query_name}` SET (FORCE = TRUE) AS
                    DO BEGIN
                        PRAGMA ydb.OverridePlanner = "invalid";
                        INSERT INTO `{pq_source}`.`{output_topic}`
                        SELECT * FROM `{pq_source}`.`{input_topic}`;
                    END DO;
                )sql",
                "query_name"_a = queryName,
                "pq_source"_a = pqSourceName,
                "input_topic"_a = inputTopicName,
                "output_topic"_a = outputTopicName
            ), EStatus::GENERIC_ERROR, "Invalid override planner settings");
        }

        {
            const auto& result = ExecQuery("SELECT Status, Issues FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            CheckScriptResult(result[0], 2, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_STRING_CONTAINS(resultSet.ColumnParser("Issues").GetOptionalUtf8().value_or(""), "Invalid override planner settings");
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("Status").GetOptionalUtf8().value_or(""), "FAILED");
            });
        }

        {
            const auto& result = ExecQuery("SELECT COUNT(*) AS count FROM `.metadata/script_executions`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser("count").GetUint64(), std::min(changesCount, static_cast<ui64>(4)));
            });
        }
    }

    Y_UNIT_TEST_F(StreamingQueryPlaningErrorRetry, TStreamingTestFixture) {
        auto& appConfig = SetupAppConfig();
        appConfig.MutableTableServiceConfig()->MutableResourceManager()->SetComputeActorsCount(500);
        appConfig.MutableQueryServiceConfig()->SetQueryArtifactsCompressionMethod("zstd_6");

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr ui32 partitionCount = 1000;
        constexpr char inputTopicName[] = "createAndAlterStreamingQueryInputTopic";
        constexpr char outputTopicName[] = "createAndAlterStreamingQueryOutputTopic";
        CreateTopic(inputTopicName, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount));
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                PRAGMA ydb.MaxTasksPerStage = "1000";
                PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 0, "tasks": 1000 }}
                ] @@;

                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Data FROM `{pq_source}`.`{input_topic}`
                GROUP BY Data, HOP(CurrentUtcTimestamp(TableRow()), "PT10S", "PT10S", "PT10S");
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        WaitFor(TDuration::Seconds(60), "wait streaming query issues", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Status, Issues FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            bool hasIssues = false;
            CheckScriptResult(result[0], 2, 1, [&](TResultSetParser& resultSet) {
                const TString issuesJson = resultSet.ColumnParser("Issues").GetOptionalUtf8().value_or("");
                hasIssues = issuesJson.contains("Previous query retries") && issuesJson.contains("Not enough resources to execute query");
                error = TStringBuilder() << "issues: " << issuesJson;

                const auto status = *resultSet.ColumnParser("Status").GetOptionalUtf8();
                if (!IsIn({"RUNNING", "SUSPENDED"}, status)) {
                    UNIT_FAIL("Unexpected query status: " << status);
                }
            });

            return hasIssues;
        });
    }

    Y_UNIT_TEST_F(StreamingQueryRestartsWhenUpsertTableDeletedWhileRunning, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "sqRestartsUpsertMissingTableRuntimeInputTopic";
        constexpr char pqSourceName[] = "sqRestartsUpsertMissingTableRuntimePqSource";
        constexpr char outputTableName[] = "sqRestartsUpsertMissingTableRuntime";
        constexpr char queryName[] = "sqRestartsUpsertMissingTableRuntimeQuery";

        CreateTopic(inputTopicName);
        CreatePqSource(pqSourceName);

        ExecQuery(fmt::format(R"(
            CREATE TABLE `{output_table}` (
                Key String NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            );)",
            "output_table"_a = outputTableName
        ));

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                UPSERT INTO `{output_table}`
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
            "output_table"_a = outputTableName
        ));

        CheckScriptExecutionsCount(1, 1);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, R"({"Key": "key1", "Value": "value1"})");
        Sleep(TDuration::Seconds(1)); // wait for checkpoint commit
        CheckTable(*this, outputTableName, {{"key1", "value1"}});

        ExecQuery(fmt::format(R"(DROP TABLE `{output_table}`;)",
            "output_table"_a = outputTableName
        ));

        // Trigger another batch so the write actor tries to write to the now-deleted table.
        WriteTopicMessage(inputTopicName, R"({"Key": "key2", "Value": "value2"})");

        WaitFor(TDuration::Seconds(60), "Wait for execution restart after table drop", [&](TString& error) {
            const auto& result = ExecQuery(
                R"sql(SELECT lease_generation FROM `.metadata/script_executions`;)sql"
            );
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            i64 generation = 0;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                generation = resultSet.ColumnParser(0).GetOptionalInt64().value_or(0);
            });
            error = TStringBuilder() << "Lease generation: " << generation;
            return generation > 1;
        });
    }

    Y_UNIT_TEST_F(CheckpointSupportValidationForCallables, TStreamingTestFixture) {
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "checkpointSupportValidationForCallablesInputTopic";
        constexpr char outputTopicName[] = "checkpointSupportValidationForCallablesOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY streamingQueryFailed AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}` LIMIT 1
            END DO;)",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Checkpoints are not supported for LIMIT operator, query may produce unstable results");

        constexpr char queryNameLimit[] = "streamingQueryLimitRun";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}1` AS
            DO BEGIN
                PRAGMA ydb.OptValidateStreamingCheckpoints = "FALSE";
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Data || "1" FROM `{pq_source}`.`{input_topic}` LIMIT 1
            END DO;)",
            "query_name"_a = queryNameLimit,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}2` AS
            DO BEGIN
                PRAGMA ydb.DisableCheckpoints = "TRUE";
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Data || "2" FROM `{pq_source}`.`{input_topic}` LIMIT 1
            END DO;)",
            "query_name"_a = queryNameLimit,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(3, 2);
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "test_message");
        ReadTopicMessages(outputTopicName, {"test_message1", "test_message2"});

        Sleep(TDuration::Seconds(1));
        CheckScriptExecutionsCount(3, 0);

        constexpr char queryNameTakeWhile[] = "streamingQueryTakeWhile";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT
                    String::JoinFromList(
                        ListTakeWhile(String::SplitToList(Data, ","), ($x) -> (LEN($x) <= 3)),
                        ","
                    )
                FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "query_name"_a = queryNameTakeWhile,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(4, 1);
        const auto disposition = TInstant::Now();
        Sleep(TDuration::Seconds(1));

        WriteTopicMessage(inputTopicName, "t,es,t_m,essa,gexxx");
        ReadTopicMessage(outputTopicName, "t,es,t_m", disposition);

        ValidateStreamingQueryAst(queryNameTakeWhile, [](const TString& ast) {
            UNIT_ASSERT_STRING_CONTAINS(ast, "TakeWhile");
        });
    }

    TTestInfo SetupCheckpointIntervalTest(TStreamingTestFixture& self, const TString& queryName) {
        TTestInfo info = {
            .InputTopicName = TStringBuilder() << queryName << "Input" << self.Name_,
            .OutputTopicName = TStringBuilder() << queryName << "Output" << self.Name_,
            .PqSourceName = "pqSourceName",
            .QueryName = queryName
        };
        info.QueryText = fmt::format(R"(
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`
            )",
            "pq_source"_a = info.PqSourceName,
            "input_topic"_a = info.InputTopicName,
            "output_topic"_a = info.OutputTopicName
        );

        self.CreateTopic(info.InputTopicName);
        self.CreateTopic(info.OutputTopicName);

        return info;
    }

    Y_UNIT_TEST_F(CheckpointIntervalSettingValidation, TStreamingTestFixture) {
        CheckpointPeriod = TDuration::Days(1);

        constexpr char queryName[] = "streamingQuery";
        const auto info = SetupCheckpointIntervalTest(*this, queryName);
        CreatePqSource(info.PqSourceName);

        const auto createQuery = [&](const std::string& checkpointInterval) {
            return fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` WITH (
                    RUN = FALSE,
                    CHECKPOINT_INTERVAL = "{checkpoint_interval}"
                ) AS DO BEGIN{query_text}END DO;)",
                "query_name"_a = info.QueryName,
                "checkpoint_interval"_a = checkpointInterval,
                "query_text"_a = info.QueryText
            );
        };

        const auto alterQuery = [&](const std::string& checkpointInterval) {
            return fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    CHECKPOINT_INTERVAL = "{checkpoint_interval}"
                );)",
                "query_name"_a = info.QueryName,
                "checkpoint_interval"_a = checkpointInterval
            );
        };

        // Init script execution tables

        ExecAndWaitScript("SELECT 42;");

        // Invalid intervals are rejected on create

        ExecQuery(createQuery("10s"), EStatus::BAD_REQUEST, "Invalid properties for creation new streaming query");
        ExecQuery(createQuery("10s"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration: 10s");
        ExecQuery(createQuery(""), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration:");
        ExecQuery(createQuery("PT1S1M"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration: PT1S1M");
        ExecQuery(createQuery("P100000D"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration: P100000D");
        ExecQuery(createQuery("-PT1S"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is should be non-negative interval, but got: -PT1S");

        // Valid interval is saved as is (query is not created by failed operations above)

        ExecQuery(createQuery("PT0.5S"));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "PT0.5S");
        CheckScriptExecutionsCount(1, 0);

        // Invalid intervals are rejected on alter, previous value is preserved

        ExecQuery(alterQuery("10s"), EStatus::BAD_REQUEST, "Invalid properties for alter streaming query");
        ExecQuery(alterQuery("10s"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration: 10s");
        ExecQuery(alterQuery(""), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration:");
        ExecQuery(alterQuery("PT1S1M"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration: PT1S1M");
        ExecQuery(alterQuery("P100000D"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is not a valid ISO 8601 duration: P100000D");
        ExecQuery(alterQuery("-PT1S"), EStatus::BAD_REQUEST, "CHECKPOINT_INTERVAL property is should be non-negative interval, but got: -PT1S");
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "PT0.5S");

        // Valid interval with all components is accepted on alter

        ExecQuery(alterQuery("P1DT2H3M4.5S"));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "P1DT2H3M4.5S");

        // Interval is preserved by alter of another property

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RESOURCE_POOL = "default"
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "P1DT2H3M4.5S");

        // Interval is reset to default by create or replace without setting

        ExecQuery(fmt::format(R"(
            CREATE OR REPLACE STREAMING QUERY `{query_name}` WITH (
                RUN = FALSE
            ) AS DO BEGIN{query_text}END DO;)",
            "query_name"_a = info.QueryName,
            "query_text"_a = info.QueryText
        ));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "");

        CheckScriptExecutionsCount(1, 0);
    }

    Y_UNIT_TEST_F(CheckpointIntervalSettingCreation, TStreamingTestFixture) {
        CheckpointPeriod = TDuration::Days(1);

        const auto pqGateway = SetupMockPqGateway();
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char defaultQueryName[] = "defaultIntervalStreamingQuery";
        constexpr char fastQueryName[] = "fastIntervalStreamingQuery";
        const auto defaultInfo = SetupCheckpointIntervalTest(*this, defaultQueryName);
        const auto fastInfo = SetupCheckpointIntervalTest(*this, fastQueryName);
        CreatePqSource(defaultInfo.PqSourceName);

        // Query without setting uses cluster wide checkpointing period

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN{query_text}END DO;)",
            "query_name"_a = defaultInfo.QueryName,
            "query_text"_a = defaultInfo.QueryText
        ));
        CheckStreamingQueryProperty(defaultQueryName, "checkpoint_interval", "");

        // Query with setting overrides cluster wide checkpointing period

        constexpr char checkpointInterval[] = "PT0.2S";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "{checkpoint_interval}"
            ) AS DO BEGIN{query_text}END DO;)",
            "query_name"_a = fastInfo.QueryName,
            "checkpoint_interval"_a = checkpointInterval,
            "query_text"_a = fastInfo.QueryText
        ));
        CheckStreamingQueryProperty(fastQueryName, "checkpoint_interval", checkpointInterval);

        CheckScriptExecutionsCount(2, 2);
        const auto defaultReadSession = pqGateway->WaitReadSession(defaultInfo.InputTopicName);
        const auto readSession = pqGateway->WaitReadSession(fastInfo.InputTopicName);

        const auto fastCheckpointId = GetStreamingQueryCheckpointId(fastQueryName);
        const auto defaultCheckpointId = GetStreamingQueryCheckpointId(defaultQueryName);
        UNIT_ASSERT_VALUES_UNEQUAL(fastCheckpointId, defaultCheckpointId);

        // Checkpoints are performed with interval from query setting

        WaitCheckpointUpdate(fastCheckpointId);
        WaitCheckpointUpdate(fastCheckpointId);

        // And query without setting still uses cluster wide period

        CheckNoCheckpointUpdate(defaultCheckpointId);

        // Query with checkpoint interval processes data as usual

        readSession->AddDataReceivedEvent(0, "test_message");
        pqGateway->WaitWriteSession(fastInfo.OutputTopicName)->ExpectMessage("test_message");

        // Setting is restored from operation meta after internal retry

        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        pqGateway->WaitReadSession(fastInfo.InputTopicName);
        WaitStreamingQueryStatus(fastQueryName);

        defaultReadSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        pqGateway->WaitReadSession(defaultInfo.InputTopicName);
        WaitStreamingQueryStatus(defaultQueryName);

        UNIT_ASSERT_VALUES_EQUAL(GetStreamingQueryCheckpointId(fastQueryName), fastCheckpointId);
        UNIT_ASSERT_VALUES_EQUAL(GetStreamingQueryCheckpointId(defaultQueryName), defaultCheckpointId);
        WaitCheckpointUpdate(fastCheckpointId);
        WaitCheckpointUpdate(fastCheckpointId);
        CheckNoCheckpointUpdate(defaultCheckpointId);
    }

    Y_UNIT_TEST_F(CheckpointIntervalSettingAlter, TStreamingTestFixture) {
        CheckpointPeriod = TDuration::Days(1);

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char queryName[] = "streamingQuery";
        const auto info = SetupCheckpointIntervalTest(*this, queryName);
        CreatePqSource(info.PqSourceName);

        const auto alterQuery = [&](const std::string& checkpointInterval) {
            return fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (
                    CHECKPOINT_INTERVAL = "{checkpoint_interval}"
                );)",
                "query_name"_a = info.QueryName,
                "checkpoint_interval"_a = checkpointInterval
            );
        };

        // Query is created without setting, so cluster wide checkpointing period is used

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN{query_text}END DO;)",
            "query_name"_a = info.QueryName,
            "query_text"_a = info.QueryText
        ));
        CheckScriptExecutionsCount(1, 1);
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "");
        WaitStreamingQueryStatus(queryName);

        const auto checkpointId = GetStreamingQueryCheckpointId(queryName);
        CheckNoCheckpointUpdate(checkpointId);

        // Alter sets up checkpoint interval and restarts query without checkpoint loss

        constexpr char checkpointInterval[] = "PT0.2S";
        ExecQuery(alterQuery(checkpointInterval));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", checkpointInterval);
        WaitStreamingQueryStatus(queryName);
        UNIT_ASSERT_VALUES_EQUAL(GetStreamingQueryCheckpointId(queryName), checkpointId);

        WaitCheckpointUpdate(checkpointId);
        WaitCheckpointUpdate(checkpointId);

        // Query is working after restart

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(info.InputTopicName, "test_message");
        ReadTopicMessage(info.OutputTopicName, "test_message");

        // Alter of another property preserves checkpoint interval

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RESOURCE_POOL = "default"
            );)",
            "query_name"_a = info.QueryName
        ));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "PT0.2S");
        WaitStreamingQueryStatus(queryName);

        WaitCheckpointUpdate(checkpointId);
        WaitCheckpointUpdate(checkpointId);

        // Alter changes checkpoint interval

        ExecQuery(alterQuery("P1D"));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", "P1D");
        WaitStreamingQueryStatus(queryName);
        UNIT_ASSERT_VALUES_EQUAL(GetStreamingQueryCheckpointId(queryName), checkpointId);

        CheckNoCheckpointUpdate(checkpointId);
    }

    Y_UNIT_TEST_F(ZeroCheckpointIntervalSetting, TStreamingTestFixture) {
        CheckpointPeriod = TDuration::Days(1);

        const auto pqGateway = SetupMockPqGateway();
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char queryName[] = "fastIntervalStreamingQuery";
        const auto info = SetupCheckpointIntervalTest(*this, queryName);
        CreatePqSource(info.PqSourceName);

        // Query with zero checkpointing period

        constexpr char checkpointInterval[] = "PT0S";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` WITH (
                CHECKPOINT_INTERVAL = "{checkpoint_interval}"
            ) AS DO BEGIN{query_text}END DO;)",
            "query_name"_a = info.QueryName,
            "checkpoint_interval"_a = checkpointInterval,
            "query_text"_a = info.QueryText
        ));
        CheckStreamingQueryProperty(queryName, "checkpoint_interval", checkpointInterval);

        CheckScriptExecutionsCount(1, 1);
        const auto readSession = pqGateway->WaitReadSession(info.InputTopicName);
        const auto checkpointId = GetStreamingQueryCheckpointId(queryName);

        WaitCheckpointUpdate(checkpointId);
        WaitCheckpointUpdate(checkpointId);

        // Query with zero checkpoint interval processes data as usual

        readSession->AddDataReceivedEvent(0, "test_message");
        pqGateway->WaitWriteSession(info.OutputTopicName)->ExpectMessage("test_message");
    }

    Y_UNIT_TEST_F(DeliveryGuarantyWriteSettingDisabled, TStreamingTestFixture) {
        SetupAppConfig().MutableFeatureFlags()->SetEnableExactlyOnceTopicsWriting(false);

        constexpr char inputTopicName[] = "deliveryGuarantyWriteSettingDisabledInputTopic";
        constexpr char outputTopicName[] = "deliveryGuarantyWriteSettingDisabledOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY streamingQuery AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Exactly once delivery guarantee is disabled. Please contact your system administrator to enable it.");

        // Test settings validation
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY streamingQuery AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (
                    DLIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Unknown setting 'dliveryguarantee'");

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY streamingQuery AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (
                    DELIVERY_GUARANTEE
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Expected `DELIVERY_GUARANTEE` = value");

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY streamingQuery AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (
                    DELIVERY_GUARANTEE = "none"
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "`DELIVERY_GUARANTEE` must be 'exactly_once' or 'at_least_once'");
    }

    Y_UNIT_TEST_F(DeliveryGuarantyWriteSettingEnabled, TStreamingWithSchemaSecretsTestFixture) {
        {
            auto& featureFlags = *SetupAppConfig().MutableFeatureFlags();
            featureFlags.SetEnableExactlyOnceTopicsWriting(true);
            featureFlags.SetEnableStreamingQueriesPqSinkDeduplication(true);
        }

        constexpr char inputTopicName[] = "deliveryGuarantyWriteSettingDisabledInputTopic";
        constexpr char outputTopicName[] = "deliveryGuarantyWriteSettingDisabledOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        constexpr char pqSourceNameNoAuth[] = "sourceNameNoAuth";
        CreatePqSourceBasicAuth(pqSourceName, /* useSchemaSecrets */ true);
        CreatePqSource(pqSourceNameNoAuth);

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY deliveryGuarantyWriteSettingWithNoAuth AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "pq_source"_a = pqSourceNameNoAuth,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "Authorization is required for setting `DELIVERY_GUARANTEE` = 'exactly_once'");

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY deliveryGuarantyWriteSettingWithDeduplication AS
            DO BEGIN
                PRAGMA pq.EnableDeduplication = "TRUE";
                INSERT INTO `{pq_source}`.`{output_topic}` WITH (
                    DELIVERY_GUARANTEE = "exactly_once"
                ) SELECT * FROM `{pq_source}`.`{input_topic}`
            END DO;)",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::GENERIC_ERROR, "`DELIVERY_GUARANTEE` = 'exactly_once' is not supported with enabled deduplication");

        ExecQuery(fmt::format(R"(
            INSERT INTO `{pq_source}`.`{output_topic}` WITH (
                DELIVERY_GUARANTEE = "exactly_once"
            ) SELECT "test_message")",
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ), EStatus::SUCCESS, "`DELIVERY_GUARANTEE` = 'exactly_once' cannot be used in current query context, falling back to default 'at_least_once'");

        ReadTopicMessage(outputTopicName, "test_message");
    }

    Y_UNIT_TEST_F(BackPressureOnWritingIntoTopics, TStreamingTestFixture) {
        SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager()->SetMaxTotalChannelBuffersSize(1_MB);

        const auto pqGateway = SetupMockPqGateway(TMockPqGatewaySettings{.LockWritingByDefault = true});

        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");

        constexpr char inputTopicName[] = "backPressureOnWritingIntoTopicsInputTopic";
        constexpr char outputTopicName[] = "backPressureOnWritingIntoTopicsOutputTopic";
        CreateTopic(inputTopicName);
        CreateTopic(outputTopicName);

        constexpr char pqSourceName[] = "sourceName";
        CreatePqSource(pqSourceName);

        const auto queryName = "streamingQuery";
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT * FROM `{pq_source}`.`{input_topic}`;
            END DO;)",
            "query_name"_a = queryName,
            "pq_source"_a = pqSourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        CheckScriptExecutionsCount(1, 1);

        const auto& checkpointId = GetStreamingQueryCheckpointId(queryName);
        const auto readSession = pqGateway->WaitReadSession(inputTopicName);
        const auto writeSession = pqGateway->WaitWriteSession(outputTopicName);

        ui64 messageSentIdx = 0;
        const TString largeMessage(512_KB, 'x');

        for (ui64 i = 0; i < 10; ++i) {
            readSession->AddDataReceivedEvent(0, TStringBuilder() << largeMessage << messageSentIdx++);
        }

        CheckNoCheckpointUpdate(checkpointId); // Checkpoint should wait for unwritten data
        writeSession->EnsureEmpty(); // No continuation token was provided
        UNIT_ASSERT_VALUES_EQUAL(readSession->GetInflightEventsCount(), 0);

        ui64 messageReceivedIdx = 0;
        writeSession->Unlock();
        WaitCheckpointUpdate(checkpointId);

        std::vector<TString> messages;
        messages.reserve(messageSentIdx);
        while (messageReceivedIdx < messageSentIdx) {
            messages.emplace_back(TStringBuilder() << largeMessage << messageReceivedIdx++);
        }
        writeSession->ExpectMessages(messages);

        WaitCheckpointUpdate(checkpointId);

        // Check that inflight acks accounted into memory
        writeSession->LockAcks();

        bool messageConsumed = true;
        constexpr ui64 messagesCount = 1000;
        for (ui64 i = 0; i < messagesCount; ++i) {
            readSession->AddDataReceivedEvent(0, TStringBuilder() << largeMessage << messageSentIdx++);

            if (messageConsumed) {
                try {
                    WaitFor(TDuration::Seconds(1), "read input message", [&] {
                        return readSession->GetInflightEventsCount() == 0;
                    }, TDuration::MicroSeconds(1), /* throwException */ true);
                } catch (const std::exception&) {
                    messageConsumed = false;
                }
            }
        }

        // Graph should be stopped by back pressure

        CheckNoCheckpointUpdate(checkpointId); // Checkpoint should wait for acks
        UNIT_ASSERT(!messageConsumed);
        UNIT_ASSERT_GE(readSession->GetInflightEventsCount(), 1); // Messages waiting by back pressure

        const auto& writtenData = writeSession->ExtractData();
        UNIT_ASSERT_GE(messagesCount, writtenData.size() + 1);
        for (const auto& message : writtenData) {
            UNIT_ASSERT_VALUES_EQUAL(message, TStringBuilder() << largeMessage << messageReceivedIdx++);
        }

        // Resume after stop by back pressure

        writeSession->UnlockAcks();
        WaitCheckpointUpdate(checkpointId);

        messages.clear();
        messages.reserve(messageSentIdx - messageReceivedIdx);
        while (messageReceivedIdx < messageSentIdx) {
            messages.emplace_back(TStringBuilder() << largeMessage << messageReceivedIdx++);
        }
        writeSession->ExpectMessages(messages);

        WaitCheckpointUpdate(checkpointId);
        UNIT_ASSERT_VALUES_EQUAL(readSession->GetInflightEventsCount(), 0);
    }
}

} // namespace NKikimr::NKqp
