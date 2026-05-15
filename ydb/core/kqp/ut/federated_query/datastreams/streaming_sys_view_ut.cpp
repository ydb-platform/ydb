#include "common.h"

#include <ydb/core/kqp/common/kqp_script_executions.h>

#include <fmt/format.h>


namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYql::NConnector::NApi;
using namespace NYql::NConnector::NTest;

Y_UNIT_TEST_SUITE(KqpStreamingQueriesSysView) {
    Y_UNIT_TEST_F(ReadNotCreatedSysView, TStreamingSysViewTestFixture) {
        Setup();
        CheckSysView({});
    }

    Y_UNIT_TEST_F(ReadRangesForSysView, TStreamingSysViewTestFixture) {
        Setup();

        StartQuery("A");
        StartQuery("B");
        StartQuery("C");
        StartQuery("D");
        StartQuery("E");
        Sleep(STATS_WAIT_DURATION);

        CheckSysView({
            {"A"}, {"B"}, {"C"}, {"D"}, {"E"}
        });

        CheckSysView({
            {"B"}, {"C"}, {"D"}
        }, "Path > '/Root/A' AND Path < '/Root/E'");

        CheckSysView({
            {"C"}, {"D"}, {"E"}
        }, "Path > '/Root/B'");

        CheckSysView({
            {"A"}, {"B"}
        }, "Path < '/Root/C'");

        CheckSysView({
            {"C"}, {"D"}, {"E"}
        }, "Path >= '/Root/C' AND Path <= '/Root/E'");

        CheckSysView({
            {"D"}, {"E"}
        }, "Path >= '/Root/D'");

        CheckSysView({
            {"A"}, {"B"}
        }, "Path <= '/Root/B'");

        CheckSysView({
            {"C"}
        }, "Path = '/Root/C'");
    }

    Y_UNIT_TEST_F(ReadWithoutAuth, TStreamingSysViewTestFixture) {
        QueryClientSettings = TClientSettings();
        Setup();

        StartQuery("A");
        StartQuery("B");
        StartQuery("C");
        Sleep(STATS_WAIT_DURATION);

        CheckSysView({
            {"A"}, {"B"}, {"C"}
        });
    }

    Y_UNIT_TEST_F(SortOrderForSysView, TStreamingSysViewTestFixture) {
        Setup();

        StartQuery("A");
        StartQuery("B");
        StartQuery("C");
        StartQuery("D");
        Sleep(STATS_WAIT_DURATION);

        CheckSysView({
            {"A"}, {"B"}, {"C"}, {"D"}
        }, "", "Path ASC");

        CheckSysView({
            {"C"}, {"D"}
        }, "Path >= '/Root/C'", "Path ASC");

        CheckSysView({
            {"D"}, {"C"}, {"B"}, {"A"}
        }, "", "Path DESC");

        CheckSysView({
            {"D"}, {"C"}
        }, "Path >= '/Root/C'", "Path DESC");
    }

    Y_UNIT_TEST_F(SysViewColumnsValues, TStreamingSysViewTestFixture) {
        const auto pqGateway = SetupMockPqGateway();
        Setup();

        constexpr char queryName[] = "streamingQuery";
        ExecQuery(fmt::format(R"(
            GRANT ALL ON `/Root` TO `root@builtin`;
            CREATE RESOURCE POOL MyResourcePool WITH (CONCURRENT_QUERY_LIMIT = "-1");
            CREATE STREAMING QUERY `{query_name}` WITH (
                RUN = FALSE,
                RESOURCE_POOL = "MyResourcePool"
            ) AS DO BEGIN{text}END DO)",
            "query_name"_a = queryName,
            "text"_a = GetQueryText(queryName)
        ));

        CheckSysView({{
            .Name = queryName,
            .Status = "CREATED",
            .Run = false,
            .Pool = "MyResourcePool",
        }});

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = TRUE
            ))",
            "query_name"_a = queryName
        ));

        auto readSession = pqGateway->WaitReadSession(TString{InputTopic});
        readSession->AddDataReceivedEvent(0, "test0");
        pqGateway->WaitWriteSession(TString{OutputTopic})->ExpectMessage("test0");

        {
            Sleep(STATS_WAIT_DURATION);
            const auto result = CheckSysView({{
                .Name = queryName,
                .Status = "RUNNING",
                .Run = true,
                .Pool = "MyResourcePool",
            }})[0];

            UNIT_ASSERT_VALUES_EQUAL(result.PreviousExecutionIds.size(), 0);
            const auto operation = GetScriptExecutionOperation(OperationIdFromExecutionId(TString{result.ExecutionId}));
            UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().ExecStatus, EExecStatus::Running);
        }

        auto failAt = TInstant::Now();
        readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Test pq session failure")});
        readSession = pqGateway->WaitReadSession(TString{InputTopic});
        readSession->AddDataReceivedEvent(0, "test0");
        pqGateway->WaitWriteSession(TString{OutputTopic})->ExpectMessage("test0");

        {
            Sleep(TDuration::Seconds(1));
            const auto result = CheckSysView({{
                .Name = queryName,
                .Status = "RUNNING",
                .Issues = "Test pq session failure",
                .Run = true,
                .Pool = "MyResourcePool",
                .RetryCount = 1,
                .LastFailAt = failAt,
            }})[0];

            UNIT_ASSERT_VALUES_EQUAL(result.PreviousExecutionIds.size(), 0);
            const auto operation = GetScriptExecutionOperation(OperationIdFromExecutionId(TString{result.ExecutionId}));
            UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().ExecStatus, EExecStatus::Running);
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (
                RUN = FALSE
            ))",
            "query_name"_a = queryName
        ));

        {
            const auto result = CheckSysView({{
                .Name = queryName,
                .Status = "STOPPED",
                .Issues = "Request was canceled by user",
                .Run = false,
                .Pool = "MyResourcePool",
                .RetryCount = 1,
                .LastFailAt = failAt,
                .CheckPlan = true,
            }})[0];

            UNIT_ASSERT_VALUES_EQUAL(result.PreviousExecutionIds.size(), 1);
            UNIT_ASSERT(result.ExecutionId.empty());
            const auto operation = GetScriptExecutionOperation(OperationIdFromExecutionId(TString{result.PreviousExecutionIds[0]}), /* checkStatus */ false);
            const auto& status = operation.Status();
            UNIT_ASSERT_VALUES_EQUAL_C(status.GetStatus(), EStatus::CANCELLED, status.GetIssues().ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(operation.Metadata().ExecStatus, EExecStatus::Canceled);
        }
    }

    Y_UNIT_TEST_F(SysViewForFinishedStreamingQueries, TStreamingSysViewTestFixture) {
        Setup();

        const std::vector<std::string> texts = {
            fmt::format(R"(
                ;INSERT INTO `{pq_source}`.`{output_topic}`
                /* A */
                SELECT * FROM `{pq_source}`.`{input_topic}`
                LIMIT 1;)",
                "pq_source"_a = PQ_SOURCE,
                "output_topic"_a = OutputTopic,
                "input_topic"_a = InputTopic
            ), fmt::format(R"(
                ;PRAGMA ydb.OverridePlanner = @@ [
                    {{ "tx": 0, "stage": 10, "tasks": 1 }}
                ] @@;
                INSERT INTO `{pq_source}`.`{output_topic}`
                /* B */
                SELECT * FROM `{pq_source}`.`{input_topic}`;)",
                "pq_source"_a = PQ_SOURCE,
                "output_topic"_a = OutputTopic,
                "input_topic"_a = InputTopic
            )
        };

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY A AS DO BEGIN{text}END DO)",
            "text"_a = texts[0]
        ));

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY B AS DO BEGIN{text}END DO)",
            "text"_a = texts[1]
        ), EStatus::GENERIC_ERROR);

        Sleep(TDuration::Seconds(1));
        WriteTopicMessage(InputTopic, "test0");
        ReadTopicMessage(OutputTopic, "test0");
        Sleep(TDuration::Seconds(1));

        CheckSysView({{
            .Name = "A",
            .Status = "COMPLETED",
            .Text = texts[0],
            .CheckPlan = true,
        }, {
            .Name = "B",
            .Status = "CREATED",
            .Text = texts[1],
        }});
    }

    Y_UNIT_TEST_F(SysViewForSuspendedStreamingQueries, TStreamingSysViewTestFixture) {
        Setup();

        const std::string text = fmt::format(R"(
            PRAGMA pq.Consumer = "unknown";
            INSERT INTO `{pq_source}`.`{output_topic}`
            SELECT * FROM `{pq_source}`.`{input_topic}`;)",
            "pq_source"_a = PQ_SOURCE,
            "output_topic"_a = OutputTopic,
            "input_topic"_a = InputTopic
        );

        const auto start = TInstant::Now();
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY A AS DO BEGIN{text}END DO)",
            "text"_a = text
        ));

        const auto timeout = TDuration::Seconds(20);
        WaitFor(timeout, "Wait query suspend", [&](TString& error) {
            const auto& result = ExecQuery("SELECT Status, SuspendedUntil FROM `.sys/streaming_queries`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            std::string status;
            std::optional<TInstant> suspendedUntil;
            CheckScriptResult(result[0], 2, 1, [&](TResultSetParser& resultSet) {
                status = *resultSet.ColumnParser("Status").GetOptionalUtf8();
                suspendedUntil = resultSet.ColumnParser("SuspendedUntil").GetOptionalTimestamp();
            });

            if (status != "SUSPENDED") {
                error = TStringBuilder() << "Query status is not SUSPENDED, but " << status;
                return false;
            }

            const auto delta = abs(suspendedUntil->SecondsFloat() - start.SecondsFloat());
            UNIT_ASSERT_GE(timeout.SecondsFloat(), delta);
            return true;
        });
    }

    Y_UNIT_TEST_F(ReadPartOfSysViewColumns, TStreamingSysViewTestFixture) {
        Setup();

        StartQuery("A");

        const auto& result = ExecQuery(R"(
            SELECT
                Path,
                Text,
                Run,
                ResourcePool
            FROM `.sys/streaming_queries`
        )");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

        std::vector<TSysViewResult> results;
        CheckScriptResult(result[0], 4, 1, [&](TResultSetParser& resultSet) {
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Path").GetOptionalUtf8(), "/Root/A");
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Text").GetOptionalUtf8(), GetQueryText("A"));
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("Run").GetOptionalBool(), true);
            UNIT_ASSERT_VALUES_EQUAL(*resultSet.ColumnParser("ResourcePool").GetOptionalUtf8(), "default");
        });
    }

    Y_UNIT_TEST_F(ReadSysViewWithRowCountBackPressure, TStreamingSysViewTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1_KB);
        Setup();

        constexpr ui64 NUMBER_OF_QUERIES = 2010;

        std::vector<TSysViewRow> rows;
        rows.reserve(NUMBER_OF_QUERIES);
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto name = TStringBuilder() << "query-" << i;
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{name}` WITH (
                    RUN = FALSE
                ) AS DO BEGIN{text}END DO)",
                "name"_a = name,
                "text"_a = GetQueryText(name)
            ));
            rows.push_back({
                .Name = name,
                .Status = "CREATED",
                .Run = false,
            });
        }

        CheckSysView(rows);
    }

    Y_UNIT_TEST_F(ReadSysViewWithRowSizeBackPressure, TStreamingSysViewTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1_KB);
        Setup();

        constexpr ui64 NUMBER_OF_QUERIES = 50;
        const std::string payload(100_KB, 'x');

        std::vector<TSysViewRow> rows;
        rows.reserve(NUMBER_OF_QUERIES);
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto name = TStringBuilder() << "query-" << i;
            const auto text = TStringBuilder() << GetQueryText(name) << " /* " << payload << " */";
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{name}` WITH (
                    RUN = FALSE
                ) AS DO BEGIN{text}END DO)",
                "name"_a = name,
                "text"_a = text
            ));
            rows.push_back({
                .Name = name,
                .Status = "CREATED",
                .Text = text,
                .Run = false,
            });
        }

        CheckSysView(rows);
    }

    Y_UNIT_TEST_F(ReadSysViewWithMetadataSizeBackPressure, TStreamingSysViewTestFixture) {
        LogSettings.Freeze = true;
        SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager()->SetChannelBufferSize(1_KB);
        Setup();

        constexpr ui64 NUMBER_OF_QUERIES = 50;
        const std::string payload(50_KB, 'x');

        std::vector<TSysViewRow> rows;
        std::vector<std::string> resultMessages;
        rows.reserve(NUMBER_OF_QUERIES);
        resultMessages.reserve(NUMBER_OF_QUERIES);
        for (ui64 i = 0; i < NUMBER_OF_QUERIES; ++i) {
            const auto name = TStringBuilder() << "query-" << i;
            const std::string text = fmt::format(R"(
                ;INSERT INTO `{pq_source}`.`{output_topic}`
                SELECT Data || "{payload}" FROM `{pq_source}`.`{input_topic}`
                LIMIT 1;)",
                "payload"_a = payload,
                "pq_source"_a = PQ_SOURCE,
                "input_topic"_a = InputTopic,
                "output_topic"_a = OutputTopic
            );
            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{name}`
                AS DO BEGIN{text}END DO)",
                "name"_a = name,
                "text"_a = text
            ));
            rows.push_back({
                .Name = name,
                .Status = "COMPLETED",
                .Ast = payload,
                .Text = text,
                .Run = true,
            });
            resultMessages.emplace_back(TStringBuilder() << "test" << payload);
        }

        WriteTopicMessage(InputTopic, "test");
        ReadTopicMessages(OutputTopic, resultMessages);

        WaitFor(TDuration::Seconds(60), "Wait for queries to complete", [&](TString& error) {
            const auto& result = ExecQuery("SELECT COUNT(*) FROM `.metadata/script_execution_leases`");
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);

            ui64 count = 0;
            CheckScriptResult(result[0], 1, 1, [&](TResultSetParser& resultSet) {
                count = resultSet.ColumnParser(0).GetUint64();
            });

            error = TStringBuilder() << "running queries: " << count;
            return count == 0;
        });

        Sleep(STATS_WAIT_DURATION);
        CheckSysView(rows);
    }
}

} // namespace NKikimr::NKqp
