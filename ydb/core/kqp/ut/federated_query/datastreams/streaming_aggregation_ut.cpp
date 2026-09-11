#include "common.h"

#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/tx/datashard/const.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <library/cpp/json/json_reader.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <limits>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NYdb;

namespace {

struct TAggregationSettings {
    ui32 Tasks = 1;
    ui32 Partitions = 1;
    bool DisableCheckpoints = true;
    bool SelectDistinct = false;
    TString Prelude;
    TString ExpectedError;
};

class TStreamingAggregationTestFixture : public TStreamingTestFixture {
public:
    void CreateStateTable(bool useStateTable) {
        HasStateTable = useStateTable;
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        if (HasStateTable) {
            ExecQuery(R"(
                CREATE TABLE aggregationState (
                    key String NOT NULL,
                    state String,
                    PRIMARY KEY (key)
                );
            )");
        }
    }

    void CreateAggregationTopics(ui32 partitions = 1) {
        InputTopic = TStringBuilder() << "aggregationInput_" << CreateGuidAsString();
        OutputTopic = TStringBuilder() << "aggregationOutput_" << CreateGuidAsString();
        CreateTopic(InputTopic, NTopic::TCreateTopicSettings().PartitioningSettings(partitions, partitions));
        CreateTopic(OutputTopic);
        CreatePqSource("source");
    }

    void StartAggregation(bool useStateTable, const TString& columns, const TString& result,
                          const TString& keys = "key", const TString& filter = "",
                          const TAggregationSettings& settings = {}) {
        CreateStateTable(useStateTable);
        CreateAggregationTopics(settings.Partitions);

        TString planner;
        if (settings.Tasks > 1) {
            planner = fmt::format(R"(
                PRAGMA ydb.OverridePlanner = @@ [
                    {{"tx": 0, "stage": 0, "tasks": {tasks}}},
                    {{"tx": 0, "stage": 1, "tasks": {tasks}}}
                ] @@;
            )", "tasks"_a = settings.Tasks);
        }

        ExecQuery(fmt::format(R"sql(
            CREATE STREAMING QUERY aggregation AS DO BEGIN
                PRAGMA ydb.DisableCheckpoints = "{disable_checkpoints}";
                PRAGMA ydb.MaxTasksPerStage = "{tasks}";
                {planner}
                {prelude}

                INSERT INTO `source`.`{output}`
                SELECT {distinct} Unwrap({result}) AS Data
                FROM `source`.`{input}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA ({columns})
                )
                {filter}
                GROUP /*+ streaming({state_table}) */ BY {keys};
            END DO;
        )sql",
            "input"_a = InputTopic,
            "output"_a = OutputTopic,
            "columns"_a = columns,
            "result"_a = result,
            "keys"_a = keys,
            "filter"_a = filter,
            "state_table"_a = HasStateTable ? "'/Root/aggregationState'" : "",
            "disable_checkpoints"_a = settings.DisableCheckpoints ? "TRUE" : "FALSE",
            "distinct"_a = settings.SelectDistinct ? "DISTINCT" : "",
            "tasks"_a = settings.Tasks,
            "planner"_a = planner,
            "prelude"_a = settings.Prelude),
            settings.ExpectedError ? EStatus::GENERIC_ERROR : EStatus::SUCCESS,
            settings.ExpectedError);

        if (settings.ExpectedError) {
            return;
        }

        WaitStreamingQueryStatus("aggregation");
        ValidateStreamingQueryAst("aggregation", [](const TString& ast) {
            UNIT_ASSERT_STRING_CONTAINS(ast, "StreamingAggregation");
        });
    }

    void WriteAndCheck(const std::vector<std::string>& input, const std::vector<std::string>& expected) {
        WriteTopicMessages(InputTopic, input);
        CheckOutput(expected);
    }

    void CheckOutput(const std::vector<std::string>& expected) {
        ExpectedOutput.insert(ExpectedOutput.end(), expected.begin(), expected.end());
        // Read the entire output so checks across batches cannot miss earlier updates.
        ReadTopicMessages(OutputTopic, ExpectedOutput, TInstant::Zero(), /* sort */ true);
    }

    void FinishAggregation() {
        CheckPersistedState();
        ExecQuery("DROP STREAMING QUERY aggregation;");
    }

    void CheckPersistedState(const TString& table = "aggregationState") {
        if (HasStateTable) {
            const auto result = ExecQuery(fmt::format(R"(
                SELECT COUNT(*) AS keys, COUNT(state) AS states FROM `{}`;
            )", table));
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            CheckScriptResult(result[0], 2, 1, [](TResultSetParser& row) {
                const auto keys = row.ColumnParser("keys").GetUint64();
                UNIT_ASSERT_C(keys > 0, "The table-backed aggregation must persist evicted keys");
                UNIT_ASSERT_VALUES_EQUAL(row.ColumnParser("states").GetUint64(), keys);
            });
        }
    }

    void CheckFiniteResult(const TString& query, std::vector<std::string> expected, bool expectShuffle = true) {
        const auto result = GetQueryClient()->ExecuteQuery(query, NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)
                .ClientTimeout(TEST_OPERATION_TIMEOUT)).ExtractValueSync();
        CheckFiniteResult(result, std::move(expected), expectShuffle);
    }

    void CheckFiniteResult(const NQuery::TExecuteQueryResult& result, std::vector<std::string> expected,
                           bool expectShuffle = true) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetAst());
        const auto ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        TResultSetParser parser(result.GetResultSet(0));
        std::vector<std::string> actual;
        while (parser.TryNextRow()) {
            actual.push_back(parser.ColumnParser("Data").GetString());
        }
        Sort(actual);
        Sort(expected);
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
        UNIT_ASSERT_STRING_CONTAINS(ast, "StreamingAggregation");
        if (expectShuffle) {
            UNIT_ASSERT_STRING_CONTAINS(ast, "DqCnHashShuffle");
        }
        CheckPersistedState();
    }

    void CheckStaticInput(bool useStateTable) {
        CreateStateTable(useStateTable);
        CheckFiniteResult(fmt::format(R"(
            $input = AsList(
                AsStruct("a" AS key, 5l AS value),
                AsStruct("a" AS key, -2l AS value),
                AsStruct("b" AS key, 10l AS value),
                AsStruct("a" AS key, 7l AS value),
                AsStruct("b" AS key, -10l AS value),
                AsStruct("c" AS key, 1l AS value),
                AsStruct("d" AS key, 2l AS value),
                AsStruct("e" AS key, 3l AS value));
            SELECT Unwrap(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
            FROM AS_TABLE($input)
            GROUP /*+ streaming({}) */ BY key;
        )", useStateTable ? "'/Root/aggregationState'" : ""),
            {"a:1:5", "a:2:3", "b:1:10", "a:3:10", "b:2:0", "c:1:1", "d:1:2", "e:1:3"},
            /* expectShuffle */ false);
    }

    void CheckFiniteTopicInput(bool useStateTable, bool streamingInput = false) {
        const auto pqGateway = streamingInput ? SetupMockPqGateway() : nullptr;
        SetupAppConfig().MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        CreateStateTable(useStateTable);
        InputTopic = TStringBuilder() << "finiteAggregationInput_" << CreateGuidAsString();
        CreateTopic(InputTopic);
        CreatePqSource("source");
        const std::vector<std::string> messages = {
            R"({"key":"a","value":5})", R"({"key":"b","value":10})",
            R"({"key":"a","value":-2})", R"({"key":"b","value":-10})",
            R"({"key":"a","value":1000})",
        };
        if (!streamingInput) {
            WriteTopicMessages(InputTopic, messages);
        }
        const auto query = fmt::format(R"(
            PRAGMA ydb.MaxTasksPerStage = "1";
            {consumer}
            $input = SELECT * FROM `source`.`{input}` WITH (
                STREAMING = "{streaming}", FORMAT = "json_each_row",
                SCHEMA (key String NOT NULL, value Int64 NOT NULL)) LIMIT 4;
            SELECT Unwrap(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
            FROM $input
            GROUP /*+ streaming({state_table}) */ BY key;
        )", "input"_a = InputTopic, "state_table"_a = useStateTable ? "'/Root/aggregationState'" : "",
            "streaming"_a = streamingInput ? "TRUE" : "FALSE",
            "consumer"_a = streamingInput ? "" : R"(PRAGMA pq.Consumer = "test_consumer";)");
        auto result = GetQueryClient()->ExecuteQuery(query, NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)
                .ClientTimeout(TEST_OPERATION_TIMEOUT));
        NTestUtils::IMockPqReadSession::TPtr readSession;
        if (streamingInput) {
            // Send after the read session opens: streaming reads start at the current time.
            readSession = pqGateway->WaitReadSession(InputTopic);
            for (size_t i = 0; i < messages.size(); ++i) {
                readSession->AddDataReceivedEvent(i, TString(messages[i]));
            }
        }
        CheckFiniteResult(result.ExtractValueSync(),
            {"a:1:5", "b:1:10", "a:2:3", "b:2:0"});
        if (readSession) {
            readSession->ExpectSessionClosed();
        }
    }

    void CheckTableInput(bool useStateTable) {
        CreateStateTable(useStateTable);
        ExecQuery(R"(
            CREATE TABLE aggregationInput (
                id Uint64 NOT NULL, key String NOT NULL, value Int64 NOT NULL,
                PRIMARY KEY (id)
            );
        )");
        ExecQuery(R"(
            UPSERT INTO aggregationInput (id, key, value) VALUES
                (1, "a", 5), (2, "b", 10), (3, "a", 5), (4, "b", 10), (5, "a", 5);
        )");
        // Equal values per key make running results independent of table scan order.
        CheckFiniteResult(fmt::format(R"(
            PRAGMA ydb.MaxTasksPerStage = "1";
            SELECT Unwrap(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
            FROM aggregationInput
            GROUP /*+ streaming({}) */ BY key;
        )", useStateTable ? "'/Root/aggregationState'" : ""),
            {"a:1:5", "a:2:10", "a:3:15", "b:1:10", "b:2:20"});
    }

    void CheckGroupingSets(bool useStateTable, const TString& keys, const std::vector<ui32>& masks,
                           bool useGroupingCase = false) {
        TString result = R"(
            COALESCE(key, "*") || ":" || COALESCE(CAST(subkey AS String), "*")
                || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)
        )";
        if (useGroupingCase) {
            result = R"((CASE GROUPING(key, subkey)
                WHEN 0 THEN "detail" WHEN 1 THEN "key" WHEN 2 THEN "subkey" ELSE "total"
                END) || ":" || )" + result;
        }
        StartAggregation(useStateTable, "key String NOT NULL, subkey Uint64 NOT NULL, value Int64 NOT NULL",
            result, keys);
        struct TRow {
            TString Input;
            std::array<std::string, 4> Output;
        };
        const std::vector<TRow> rows = {
            {R"({"key":"a","subkey":1,"value":2})", {"a:1:1:2", "a:*:1:2", "*:1:1:2", "*:*:1:2"}},
            {R"({"key":"a","subkey":2,"value":3})", {"a:2:1:3", "a:*:2:5", "*:2:1:3", "*:*:2:5"}},
            {R"({"key":"b","subkey":1,"value":4})", {"b:1:1:4", "b:*:1:4", "*:1:2:6", "*:*:3:9"}},
            {R"({"key":"a","subkey":1,"value":-1})", {"a:1:2:1", "a:*:3:4", "*:1:3:5", "*:*:4:8"}},
        };
        const std::array<std::string, 4> labels = {"detail:", "key:", "subkey:", "total:"};
        for (const auto& row : rows) {
            std::vector<std::string> expected;
            for (const auto mask : masks) {
                expected.push_back((useGroupingCase ? labels[mask] : "") + row.Output[mask]);
            }
            WriteAndCheck({row.Input}, expected);
        }
        FinishAggregation();
    }

    void CheckRestart(bool useStateTable, bool injectFailure) {
        const auto pqGateway = SetupMockPqGateway();
        StartAggregation(useStateTable, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String))");
        auto readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent({
            {0, R"({"key":"a","value":5})", {}},
            {1, R"({"key":"a","value":2})", {}},
            {2, R"({"key":"b","value":10})", {}},
        });
        auto writeSession = pqGateway->WaitWriteSession(OutputTopic);
        writeSession->ExpectMessages({"a:1:5", "a:2:7", "b:1:10"}, true);
        CheckPersistedState();

        if (injectFailure) {
            readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Aggregation restart test")});
        } else {
            ExecQuery("ALTER STREAMING QUERY aggregation SET (RUN = FALSE);");
            readSession->ExpectSessionClosed();
            writeSession->ExpectSessionClosed();
            CheckScriptExecutionsCount(1, 0);
            ExecQuery("ALTER STREAMING QUERY aggregation SET (RUN = TRUE);");
        }

        auto nextReadSession = pqGateway->WaitReadSession(InputTopic);
        UNIT_ASSERT(nextReadSession != readSession);
        readSession->ExpectSessionClosed();
        writeSession->ExpectSessionClosed();
        writeSession->EnsureEmpty();
        WaitStreamingQueryStatus("aggregation");
        // Failure recovery reuses the execution; manual ALTER creates a new one.
        CheckScriptExecutionsCount(injectFailure ? 1 : 2, 1);

        // Only evicted state (a) survives in the table; cached state (b) is lost.
        nextReadSession->AddDataReceivedEvent({
            {3, R"({"key":"a","value":1})", {}},
            {4, R"({"key":"a","value":-2})", {}},
            {5, R"({"key":"b","value":3})", {}},
        });
        auto nextWriteSession = pqGateway->WaitWriteSession(OutputTopic);
        nextWriteSession->ExpectMessages(useStateTable
            ? std::vector<TString>{"a:3:8", "a:4:6", "b:1:3"}
            : std::vector<TString>{"a:1:1", "a:2:-1", "b:1:3"}, true);
        FinishAggregation();
        nextReadSession->ExpectSessionClosed();
        nextWriteSession->ExpectSessionClosed();
        nextWriteSession->EnsureEmpty();
    }

    void CheckAlterAggregation(bool useStateTable) {
        const auto pqGateway = SetupMockPqGateway();
        CreateStateTable(useStateTable);
        CreateAggregationTopics();
        const auto setQuery = [&](bool create, bool aggregate) {
            ExecQuery(fmt::format(R"(
                {ddl} STREAMING QUERY aggregation {settings} AS DO BEGIN
                    PRAGMA ydb.DisableCheckpoints = "TRUE";
                    PRAGMA ydb.MaxTasksPerStage = "1";
                    INSERT INTO `source`.`{output}`
                    SELECT Unwrap({result}) AS Data
                    FROM `source`.`{input}` WITH (
                        FORMAT = "json_each_row", SCHEMA (key String NOT NULL, value Int64 NOT NULL))
                    {group};
                END DO;
            )", "ddl"_a = create ? "CREATE" : "ALTER", "settings"_a = create ? "" : "SET (FORCE = TRUE)",
                "input"_a = InputTopic, "output"_a = OutputTopic,
                "result"_a = aggregate
                    ? R"(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String))"
                    : R"(key || ":" || CAST(value AS String))",
                "group"_a = aggregate
                    ? fmt::format("GROUP /*+ streaming({}) */ BY key", useStateTable ? "'/Root/aggregationState'" : "")
                    : ""));
            WaitStreamingQueryStatus("aggregation");
            ValidateStreamingQueryAst("aggregation", [aggregate](const TString& ast) {
                UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("StreamingAggregation"), aggregate, ast);
            });
        };

        setQuery(true, false);
        auto readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent({
            {0, R"({"key":"a","value":5})", {}}, {1, R"({"key":"a","value":5})", {}},
        });
        auto writeSession = pqGateway->WaitWriteSession(OutputTopic);
        writeSession->ExpectMessages({"a:5", "a:5"});

        setQuery(false, true);
        readSession->ExpectSessionClosed();
        writeSession->ExpectSessionClosed();
        writeSession->EnsureEmpty();
        readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent({
            {2, R"({"key":"a","value":2})", {}}, {3, R"({"key":"a","value":3})", {}},
            {4, R"({"key":"b","value":7})", {}},
        });
        writeSession = pqGateway->WaitWriteSession(OutputTopic);
        writeSession->ExpectMessages({"a:1:2", "a:2:5", "b:1:7"}, true);
        CheckPersistedState();

        setQuery(false, false);
        readSession->ExpectSessionClosed();
        writeSession->ExpectSessionClosed();
        writeSession->EnsureEmpty();
        readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent({
            {5, R"({"key":"a","value":-1})", {}}, {6, R"({"key":"b","value":4})", {}},
        });
        writeSession = pqGateway->WaitWriteSession(OutputTopic);
        writeSession->ExpectMessages({"a:-1", "b:4"}, true);
        FinishAggregation();
        readSession->ExpectSessionClosed();
        writeSession->ExpectSessionClosed();
        writeSession->EnsureEmpty();
        const auto result = ExecQuery("SELECT COUNT(*) AS count FROM `.sys/streaming_queries` WHERE Path = '/Root/aggregation';");
        CheckScriptResult(result.at(0), 1, 1, [](TResultSetParser& row) {
            UNIT_ASSERT_VALUES_EQUAL(row.ColumnParser("count").GetUint64(), 0);
        });
    }

    void CheckOversizedState(bool largeKey) {
        const auto pqGateway = SetupMockPqGateway();
        const ui64 limit = largeKey ? NDataShard::NLimits::MaxWriteKeySize : NDataShard::NLimits::MaxWriteValueSize;
        constexpr ui64 chunkSize = 64 * 1024;
        const ui64 chunks = limit / chunkSize + 1;
        const TString repeated = fmt::format(
            R"(String::JoinFromList(ListReplicate(String::LeftPad(value, {}ul, value), {}ul), ""))",
            chunkSize, chunks);
        StartAggregation(true, "key String NOT NULL, value String NOT NULL",
            largeKey ? "CAST(COUNT(*) AS String)" : fmt::format("CAST(LENGTH(SOME({})) AS String)", repeated),
            largeKey ? repeated + " AS large_key" : "key");
        const auto readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent(0, R"({"key":"a","value":"x"})");
        const auto writeSession = pqGateway->WaitWriteSession(OutputTopic);
        writeSession->ExpectMessage(largeKey ? "1" : ToString(chunkSize * chunks));
        // The second key evicts the oversized key/value and forces a state-table write.
        readSession->AddDataReceivedEvent(1, R"({"key":"b","value":"y"})");
        NTestUtils::WaitFor(TEST_OPERATION_TIMEOUT, "oversized aggregation state error", [&](TString& error) {
            error = GetStreamingQueryIssues("aggregation");
            return error.Contains("Streaming aggregation UPSERT failed for table /Root/aggregationState")
                && error.Contains(largeKey ? "Row key size of" : "Row cell size of")
                && error.Contains(ToString(limit));
        });
        ExecQuery("DROP STREAMING QUERY aggregation;");
        writeSession->ExpectSessionClosed();
        writeSession->EnsureEmpty();
        const auto result = ExecQuery("SELECT COUNT(*) AS count FROM aggregationState;");
        CheckScriptResult(result.at(0), 1, 1, [](TResultSetParser& row) {
            UNIT_ASSERT_VALUES_EQUAL(row.ColumnParser("count").GetUint64(), 0);
        });
    }

    void CheckMultipleAggregations(bool useStateTable, bool extraSinks) {
        const auto pqGateway = SetupMockPqGateway();
        CreateStateTable(useStateTable);
        CreateAggregationTopics();
        const TString secondOutput = TStringBuilder() << OutputTopic << "_subkey";
        const TString rawOutput = TStringBuilder() << OutputTopic << "_raw";
        CreateTopic(secondOutput);
        if (useStateTable) {
            ExecQuery(R"(
                CREATE TABLE subkeyAggregationState (
                    key String NOT NULL, state String, PRIMARY KEY (key));
            )");
        }
        TString sinks;
        if (extraSinks) {
            CreateTopic(rawOutput);
            ExecQuery(R"(
                CREATE TABLE rawRows (
                    id Uint64 NOT NULL, key String NOT NULL, subkey String NOT NULL, value Int64 NOT NULL,
                    PRIMARY KEY (id));
            )");
            sinks = fmt::format(R"(
                INSERT INTO `source`.`{}`
                SELECT Unwrap(CAST(id AS String) || ":" || key || ":" || subkey || ":" || CAST(value AS String)) AS Data
                FROM $input;
                -- With checkpoints disabled, finish this branch to flush the table sink.
                UPSERT INTO rawRows SELECT id, key, subkey, value FROM $input LIMIT 4;
            )", rawOutput);
        }
        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY aggregation AS DO BEGIN
                PRAGMA ydb.DisableCheckpoints = "TRUE";
                PRAGMA ydb.MaxTasksPerStage = "1";
                $input = SELECT * FROM `source`.`{input}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (id Uint64 NOT NULL, key String NOT NULL, subkey String NOT NULL, value Int64 NOT NULL));
                INSERT INTO `source`.`{output}`
                SELECT Unwrap(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
                FROM $input GROUP /*+ streaming({state_table}) */ BY key;
                INSERT INTO `source`.`{second_output}`
                SELECT Unwrap(subkey || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
                FROM $input GROUP /*+ streaming({second_state_table}) */ BY subkey;
                {sinks}
            END DO;
        )", "input"_a = InputTopic, "output"_a = OutputTopic, "second_output"_a = secondOutput,
            "state_table"_a = useStateTable ? "'/Root/aggregationState'" : "",
            "second_state_table"_a = useStateTable ? "'/Root/subkeyAggregationState'" : "", "sinks"_a = sinks));
        WaitStreamingQueryStatus("aggregation");
        ValidateStreamingQueryAst("aggregation", [](const TString& ast) {
            const auto parsed = NYql::ParseAst(ast);
            UNIT_ASSERT_C(parsed.IsOk(), parsed.Issues.ToString());
            NYql::TExprContext ctx;
            NYql::TExprNode::TPtr root;
            UNIT_ASSERT_C(NYql::CompileExpr(*parsed.Root, root, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());
            ui32 sources = 0;
            ui32 aggregations = 0;
            bool nonzeroShuffleInput = false;
            NYql::VisitExpr(root, [&](const NYql::TExprNode::TPtr& node) {
                sources += node->IsCallable("DqSource");
                aggregations += node->IsCallable("StreamingAggregation");
                if (node->IsCallable("DqCnHashShuffle")) {
                    const auto& output = node->Head();
                    UNIT_ASSERT(output.IsCallable("TDqOutput"));
                    nonzeroShuffleInput |= output.Child(1)->Content() != "0";
                }
                return true;
            });
            UNIT_ASSERT_VALUES_EQUAL_C(sources, 1, ast);
            UNIT_ASSERT_VALUES_EQUAL_C(aggregations, 2, ast);
            UNIT_ASSERT_C(nonzeroShuffleInput, ast);
        });
        const auto readSession = pqGateway->WaitReadSession(InputTopic);
        readSession->AddDataReceivedEvent({
            {0, R"({"id":1,"key":"a","subkey":"x","value":2})", {}},
            {1, R"({"id":2,"key":"a","subkey":"y","value":3})", {}},
        });
        const auto byKey = pqGateway->WaitWriteSession(OutputTopic);
        const auto bySubkey = pqGateway->WaitWriteSession(secondOutput);
        const auto raw = extraSinks ? pqGateway->WaitWriteSession(rawOutput) : nullptr;
        byKey->ExpectMessages({"a:1:2", "a:2:5"}, true);
        bySubkey->ExpectMessages({"x:1:2", "y:1:3"}, true);
        if (raw) {
            raw->ExpectMessages({"1:a:x:2", "2:a:y:3"}, true);
        }
        readSession->AddDataReceivedEvent({
            {2, R"({"id":3,"key":"b","subkey":"x","value":5})", {}},
            {3, R"({"id":4,"key":"a","subkey":"x","value":-1})", {}},
        });
        byKey->ExpectMessages({"b:1:5", "a:3:4"}, true);
        bySubkey->ExpectMessages({"x:2:7", "x:3:6"}, true);
        if (raw) {
            raw->ExpectMessages({"3:b:x:5", "4:a:x:-1"}, true);
            NTestUtils::WaitFor(TEST_OPERATION_TIMEOUT, "raw table sink rows", [&]() {
                const auto result = ExecQuery("SELECT id, key, subkey, value FROM rawRows ORDER BY id;");
                if (result.at(0).RowsCount() != 4) {
                    return false;
                }
                TResultSetParser rows(result.at(0));
                std::vector<TString> actual;
                while (rows.TryNextRow()) {
                    actual.push_back(fmt::format("{}:{}:{}:{}", rows.ColumnParser("id").GetUint64(),
                        rows.ColumnParser("key").GetString(), rows.ColumnParser("subkey").GetString(),
                        rows.ColumnParser("value").GetInt64()));
                }
                UNIT_ASSERT_VALUES_EQUAL(actual, (std::vector<TString>{"1:a:x:2", "2:a:y:3", "3:b:x:5", "4:a:x:-1"}));
                return true;
            });
        }
        CheckPersistedState("subkeyAggregationState");
        FinishAggregation();
        readSession->ExpectSessionClosed();
        for (const auto& writer : {byKey, bySubkey, raw}) {
            if (writer) {
                writer->ExpectSessionClosed();
                writer->EnsureEmpty();
            }
        }
    }

    void CheckInputVolume(bool useStateTable, ui32 inputSizeMiB) {
        StartAggregation(useStateTable, "key String NOT NULL, value Int64 NOT NULL, payload String NOT NULL", R"(
            key || ":" || CAST(COUNT(*) AS String)
                || ":" || CAST(SUM(value) AS String)
                || ":" || CAST(MIN(value) AS String)
                || ":" || CAST(MAX(value) AS String)
                || ":" || CAST(SUM(LENGTH(payload)) AS String)
        )");

        constexpr ui64 MiB = 1024 * 1024;
        constexpr ui64 MessageSize = 64 * 1024;
        constexpr ui64 MessagesPerBatch = MiB / MessageSize;
        struct TExpectedState {
            ui64 Count = 0;
            i64 Sum = 0;
            i64 Min = std::numeric_limits<i64>::max();
            i64 Max = std::numeric_limits<i64>::min();
            ui64 PayloadBytes = 0;
        };
        std::array<TExpectedState, 4> states;
        ui64 inputBytes = 0;

        for (ui32 batch = 0; batch < inputSizeMiB; ++batch) {
            // Reuse a real SDK writer within each batch instead of opening a session per row.
            auto writer = GetTopicClient()->CreateSimpleBlockingWriteSession(NTopic::TWriteSessionSettings()
                .Path(InputTopic)
                .PartitionId(0));
            std::vector<std::string> expected;
            for (ui64 row = 0; row < MessagesPerBatch; ++row) {
                const ui64 index = batch * MessagesPerBatch + row;
                // Adjacent equal keys cover cache hits; revisits cover loading evicted states.
                const ui64 key = (index / 2) % states.size();
                const i64 value = static_cast<i64>(index % 101) - 50;
                const auto prefix = fmt::format(R"({{"key":"{}","value":{},"payload":")", key, value);
                const std::string payload(MessageSize - prefix.size() - 2, 'a' + key);
                const std::string message = prefix + payload + R"("})";
                UNIT_ASSERT_VALUES_EQUAL(message.size(), MessageSize);
                UNIT_ASSERT_C(writer->Write(NTopic::TWriteMessage(message), nullptr, TEST_OPERATION_TIMEOUT),
                    "Failed to write input row " << index);
                inputBytes += message.size();

                auto& state = states[key];
                ++state.Count;
                state.Sum += value;
                state.Min = std::min(state.Min, value);
                state.Max = std::max(state.Max, value);
                state.PayloadBytes += payload.size();
                expected.emplace_back(fmt::format("{}:{}:{}:{}:{}:{}", key,
                    state.Count, state.Sum, state.Min, state.Max, state.PayloadBytes));
            }
            UNIT_ASSERT_C(writer->Close(TEST_OPERATION_TIMEOUT), "Input messages were not acknowledged");
            CheckOutput(expected);
        }

        UNIT_ASSERT_VALUES_EQUAL(inputBytes, inputSizeMiB * MiB);
        FinishAggregation();
    }

    void CheckDistributedAggregation(bool useStateTable, ui32 nodes, ui32 tasks) {
        NodeCount = nodes;
        auto& resources = *SetupAppConfig().MutableTableServiceConfig()->MutableResourceManager();
        resources.SetMaxNonParallelTasksExecutionLimit(0);
        resources.SetMaxNonParallelTopStageExecutionLimit(0);
        NFederatedQueryTest::WaitResourcesPublish(*GetKikimrRunner());

        constexpr ui32 Partitions = 4;
        constexpr ui32 Keys = 32;
        StartAggregation(useStateTable, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String))",
            "key", "", {.Tasks = tasks, .Partitions = Partitions});

        ui64 count = 0;
        i64 sum = 0;
        for (const i64 value : {3, -1, 5}) {
            for (ui32 partition = 0; partition < Partitions; ++partition) {
                std::vector<std::string> input;
                for (ui32 key = 0; key < Keys; ++key) {
                    input.emplace_back(fmt::format(R"({{"key":"key{}","value":{}}})", key, value));
                }
                WriteTopicMessages(InputTopic, input, partition);
            }
            // Every partition sends every key. Equal values within a round make the
            // expected updates independent of the order in which channels are read.
            std::vector<std::string> expected;
            for (ui32 key = 0; key < Keys; ++key) {
                for (ui32 update = 1; update <= Partitions; ++update) {
                    expected.emplace_back(fmt::format("key{}:{}:{}", key, count + update, sum + update * value));
                }
            }
            CheckOutput(expected);
            count += Partitions;
            sum += Partitions * value;
        }
        CheckExecutionPlacement(tasks, nodes);
        FinishAggregation();
    }

    void CheckExecutionPlacement(ui32 tasks, ui32 nodes) {
        NTestUtils::WaitFor(TEST_OPERATION_TIMEOUT, "distributed aggregation statistics", [&](TString& error) {
            const auto result = ExecQuery("SELECT Plan FROM `.sys/streaming_queries` WHERE Path = '/Root/aggregation';");
            TString planText;
            CheckScriptResult(result.at(0), 1, 1, [&](TResultSetParser& row) {
                planText = row.ColumnParser("Plan").GetOptionalUtf8().value_or("");
            });
            error = planText;
            if (!planText) {
                return false;
            }
            NJson::TJsonValue plan;
            UNIT_ASSERT(NJson::ReadJsonTree(planText, &plan));
            bool hasParallelStage = false;
            const std::function<void(const NJson::TJsonValue&)> visit = [&](const NJson::TJsonValue& value) {
                if (value.IsMap()) {
                    if (value.Has("Stats") && value["Stats"]["PhysicalStageId"].GetIntegerRobust() == 1
                        && value["Stats"]["Tasks"].GetIntegerRobust() == tasks) {
                        hasParallelStage = true;
                    }
                    for (const auto& [_, child] : value.GetMapSafe()) {
                        visit(child);
                    }
                } else if (value.IsArray()) {
                    for (const auto& child : value.GetArraySafe()) {
                        visit(child);
                    }
                }
            };
            visit(plan);
            // Streaming queries collect FULL statistics, which omit per-node task counts.
            // Check the live compute actor counters on each node instead.
            for (ui32 node = 0; node < nodes; ++node) {
                if (!GetCounters("kqp", node)->GetCounter("RM/ComputeActors", false)->Val()) {
                    error = TStringBuilder() << "No compute actors on node " << node << "; plan: " << planText;
                    return false;
                }
            }
            return hasParallelStage;
        });
    }

    void CheckStateTableFailure(bool writeFailure) {
        StartAggregation(true, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(SUM(value) AS String))");
        ExecQuery("DROP TABLE aggregationState;");
        if (writeFailure) {
            // Missing keys can be read, but serialized states cannot be written to Uint64.
            ExecQuery(R"(
                CREATE TABLE aggregationState (
                    key String NOT NULL,
                    state Uint64,
                    PRIMARY KEY (key)
                );
            )");
        }
        WriteTopicMessages(InputTopic, {
            R"({"key":"a","value":5})",
            R"({"key":"b","value":10})",
        });
        const TString operation = writeFailure ? "UPSERT" : "SELECT";
        NTestUtils::WaitFor(TEST_OPERATION_TIMEOUT, "state table failure", [&](TString& error) {
            error = GetStreamingQueryIssues("aggregation");
            return error.Contains(TStringBuilder() << "Streaming aggregation " << operation << " failed for table /Root/aggregationState");
        });
        ExecQuery("DROP STREAMING QUERY aggregation;");
    }

private:
    bool HasStateTable = false;
    TString InputTopic;
    TString OutputTopic;
    std::vector<std::string> ExpectedOutput;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpStreamingAggregation) {
    Y_UNIT_TEST_F(MultipleStateTablePathsAreRejected, TStreamingAggregationTestFixture) {
        ExecQuery(R"(
            CREATE TABLE aggregationInput (
                key String NOT NULL, value Int64 NOT NULL,
                PRIMARY KEY (key)
            );
        )");
        ExecQuery(R"(
            SELECT key, SUM(value) AS value
            FROM aggregationInput
            GROUP /*+ streaming('/Root/first', '/Root/second') */ BY key;
        )", EStatus::GENERIC_ERROR, "Streaming aggregation accepts at most one state table path");
    }

    Y_UNIT_TEST_TWIN_F(StaticInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckStaticInput(UseStateTable);
    }

    Y_UNIT_TEST_TWIN_F(StaticJoinInput, UseStateTable, TStreamingAggregationTestFixture) {
        CreateStateTable(UseStateTable);
        CheckFiniteResult(fmt::format(R"(
            $left = AsList(
                AsStruct("a" AS key, 5l AS value),
                AsStruct("a" AS key, 5l AS value),
                AsStruct("b" AS key, 10l AS value),
                AsStruct("b" AS key, 10l AS value),
                AsStruct("b" AS key, 10l AS value),
                AsStruct("c" AS key, 1l AS value),
                AsStruct("d" AS key, 2l AS value),
                AsStruct("e" AS key, 3l AS value));
            $right = AsList(AsStruct("a" AS key), AsStruct("b" AS key), AsStruct("c" AS key),
                AsStruct("d" AS key), AsStruct("e" AS key));
            SELECT Unwrap(l.key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(l.value) AS String)) AS Data
            FROM AS_TABLE($left) AS l JOIN AS_TABLE($right) AS r ON l.key = r.key
            GROUP /*+ streaming({}) */ BY l.key;
        )", UseStateTable ? "'/Root/aggregationState'" : ""),
            {"a:1:5", "a:2:10", "b:1:10", "b:2:20", "b:3:30", "c:1:1", "d:1:2", "e:1:3"},
            /* expectShuffle */ false);
    }

    Y_UNIT_TEST_TWIN_F(FiniteTopicInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckFiniteTopicInput(UseStateTable);
    }

    Y_UNIT_TEST_TWIN_F(FiniteStreamingTopicInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckFiniteTopicInput(UseStateTable, true);
    }

    Y_UNIT_TEST_TWIN_F(TableInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckTableInput(UseStateTable);
    }

    Y_UNIT_TEST_TWIN_F(ManualRestart, UseStateTable, TStreamingAggregationTestFixture) {
        CheckRestart(UseStateTable, false);
    }

    Y_UNIT_TEST_TWIN_F(RestartAfterPqFailure, UseStateTable, TStreamingAggregationTestFixture) {
        CheckRestart(UseStateTable, true);
    }

    Y_UNIT_TEST_TWIN_F(AlterAddAndRemoveAggregation, UseStateTable, TStreamingAggregationTestFixture) {
        CheckAlterAggregation(UseStateTable);
    }

    Y_UNIT_TEST_TWIN_F(OversizedStateIsRejected, LargeKey, TStreamingAggregationTestFixture) {
        CheckOversizedState(LargeKey);
    }

    Y_UNIT_TEST_TWIN_F(MultipleAggregationsOverOneInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckMultipleAggregations(UseStateTable, false);
    }

    Y_UNIT_TEST_TWIN_F(MultipleAggregationsAndRawSinks, UseStateTable, TStreamingAggregationTestFixture) {
        CheckMultipleAggregations(UseStateTable, true);
    }

    Y_UNIT_TEST_TWIN_F(SomeAggregate, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64", R"(
            key || ":" || CAST(COUNT(*) AS String) || ":" || COALESCE(CAST(SOME(value) AS String), "null")
        )", "key", "", {.Prelude = "PRAGMA EmitAggApply;"});
        // Equal non-null values avoid depending on which value SOME chooses.
        WriteAndCheck({
            R"({"key":"a","value":null})", R"({"key":"b","value":7})",
            R"({"key":"a","value":3})", R"({"key":"b","value":null})",
            R"({"key":"a","value":3})", R"({"key":"a","value":null})",
        }, {"a:1:null", "b:1:7", "a:2:3", "b:2:7", "a:3:3", "a:4:3"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(GroupByExpression, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key Int64 NOT NULL, value Int64 NOT NULL", R"(
            CAST(bucket AS String) || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)
        )", "key % 2 AS bucket");
        WriteAndCheck({
            R"({"key":1,"value":5})", R"({"key":2,"value":10})",
            R"({"key":3,"value":-2})", R"({"key":4,"value":-10})",
        }, {"1:1:5", "0:1:10", "1:2:3", "0:2:0"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(Rollup, UseStateTable, TStreamingAggregationTestFixture) {
        CheckGroupingSets(UseStateTable, "ROLLUP(key, subkey)", {0, 1, 3});
    }

    Y_UNIT_TEST_TWIN_F(Cube, UseStateTable, TStreamingAggregationTestFixture) {
        CheckGroupingSets(UseStateTable, "CUBE(key, subkey)", {0, 1, 2, 3});
    }

    Y_UNIT_TEST_TWIN_F(GroupingSets, UseStateTable, TStreamingAggregationTestFixture) {
        CheckGroupingSets(UseStateTable, "GROUPING SETS ((key), (subkey))", {1, 2});
    }

    Y_UNIT_TEST_TWIN_F(CaseGrouping, UseStateTable, TStreamingAggregationTestFixture) {
        CheckGroupingSets(UseStateTable, "CUBE(key, subkey)", {0, 1, 2, 3}, true);
    }

    Y_UNIT_TEST_TWIN_F(SessionWindowsAreRejected, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL", "CAST(SUM(value) AS String)",
            "key, SessionWindow(value, 10) AS session_start", "",
            {.ExpectedError = "Session windows are not supported for streaming aggregation"});
    }

    Y_UNIT_TEST_TWIN_F(HoppingWindowsAreRejected, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL", "CAST(SUM(value) AS String)",
            R"(key, HOP(CurrentUtcTimestamp(TableRow()), "PT10S", "PT10S", "PT10S"))", "",
            {.ExpectedError = "Hopping windows are not supported for streaming aggregation"});
    }

    Y_UNIT_TEST_TWIN_F(RunningCountAndSum, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String))");
        WriteAndCheck({
            R"({"key":"a","value":5})",
            R"({"key":"a","value":-2})",
            R"({"key":"b","value":10})",
            R"({"key":"a","value":7})",
            R"({"key":"b","value":-10})",
        }, {"a:1:5", "a:2:3", "b:1:10", "a:3:10", "b:2:0"});
        // A separate input batch checks state across yielding while the source is idle.
        WriteAndCheck({
            R"({"key":"b","value":3})",
            R"({"key":"a","value":-10})",
        }, {"b:3:3", "a:4:0"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(NullableValuesAndMultipleAggregates, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64", R"(
            key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(COUNT(value) AS String)
                || ":" || COALESCE(CAST(SUM(value) AS String), "null")
                || ":" || COALESCE(CAST(MIN(value) AS String), "null")
                || ":" || COALESCE(CAST(MAX(value) AS String), "null")
                || ":" || COALESCE(CAST(AVG(value) AS String), "null")
        )", "key", "", {.Prelude = "PRAGMA EmitAggApply;"});
        WriteAndCheck({
            R"({"key":"a","value":null})",
            R"({"key":"b","value":10})",
            R"({"key":"a","value":-2})",
            R"({"key":"a","value":5})",
            R"({"key":"b","value":null})",
            R"({"key":"a","value":null})",
            R"({"key":"b","value":-4})",
        }, {
            "a:1:0:null:null:null:null", "b:1:1:10:10:10:10",
            "a:2:1:-2:-2:-2:-2", "a:3:2:3:-2:5:1.5",
            "b:2:1:10:10:10:10", "a:4:2:3:-2:5:1.5", "b:3:2:6:-4:10:3",
        });
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(CompositeKeysAndFilter, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, subkey Uint64 NOT NULL, value Int64 NOT NULL, keep Bool NOT NULL", R"(
            key || ":" || CAST(subkey AS String) || ":" || CAST(COUNT(*) AS String)
                || ":" || CAST(SUM(value * 2) AS String)
        )", "key, subkey", "WHERE keep");
        WriteAndCheck({
            R"({"key":"a","subkey":1,"value":2,"keep":true})",
            R"({"key":"a","subkey":2,"value":3,"keep":true})",
            R"({"key":"a","subkey":1,"value":1000,"keep":false})",
            R"({"key":"b","subkey":1,"value":4,"keep":true})",
            R"({"key":"a","subkey":1,"value":-1,"keep":true})",
            R"({"key":"a","subkey":2,"value":5,"keep":true})",
            R"({"key":"b","subkey":1,"value":-4,"keep":true})",
        }, {"a:1:1:4", "a:2:1:6", "b:1:1:8", "a:1:2:2", "a:2:2:16", "b:1:2:0"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(NullableGroupKeys, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String, value Int64 NOT NULL", R"(
            COALESCE(key, "null") || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)
        )");
        WriteAndCheck({
            R"({"key":null,"value":1})",
            R"({"key":"","value":2})",
            R"({"key":"a","value":4})",
            R"({"key":null,"value":3})",
            R"({"key":"","value":5})",
            R"({"key":"a","value":6})",
        }, {"null:1:1", ":1:2", "a:1:4", "null:2:4", ":2:7", "a:2:10"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(AggregateListPreservesDuplicates, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64", R"(
            key || ":[" || String::JoinFromList(
                ListMap(ListSort(AGGREGATE_LIST(value)), ($v) -> (CAST($v AS String))), ",") || "]"
        )");
        WriteAndCheck({
            R"({"key":"a","value":3})",
            R"({"key":"a","value":3})",
            R"({"key":"b","value":null})",
            R"({"key":"a","value":-2})",
            R"({"key":"b","value":5})",
            R"({"key":"a","value":null})",
            R"({"key":"b","value":5})",
        }, {"a:[3]", "a:[3,3]", "b:[]", "a:[-2,3,3]", "b:[5]", "a:[-2,3,3]", "b:[5,5]"});
        WriteAndCheck({
            R"({"key":"a","value":8})",
            R"({"key":"b","value":-1})",
        }, {"a:[-2,3,3,8]", "b:[-1,5,5]"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(ConditionalAndKeyedAggregates, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL, label String NOT NULL, keep Bool NOT NULL", R"(
            key || ":" || CAST(COUNT_IF(keep) AS String)
                || ":" || COALESCE(CAST(SUM_IF(value, keep) AS String), "null")
                || ":" || CAST(BOOL_AND(keep) AS String)
                || ":" || CAST(BOOL_OR(keep) AS String)
                || ":" || MIN_BY(label, value) || ":" || MAX_BY(label, value)
        )");
        WriteAndCheck({
            R"({"key":"a","value":5,"label":"x","keep":true})",
            R"({"key":"b","value":-3,"label":"b","keep":false})",
            R"({"key":"a","value":2,"label":"y","keep":false})",
            R"({"key":"a","value":-4,"label":"z","keep":true})",
            R"({"key":"b","value":7,"label":"c","keep":true})",
            R"({"key":"a","value":6,"label":"w","keep":true})",
            R"({"key":"b","value":-5,"label":"d","keep":false})",
        }, {
            "a:1:5:true:true:x:x", "b:0:null:false:false:b:b",
            "a:1:5:false:true:y:x", "a:2:1:false:true:z:x",
            "b:1:7:false:true:b:c", "a:3:7:false:true:z:w", "b:1:7:false:true:d:c",
        });
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(UdafWithComplexKeyStateAndResult, UseStateTable, TStreamingAggregationTestFixture) {
        const TString prelude = R"(
            $init = ($item) -> (<|
                Stats: <|Count: 1ul, Total: $item.value, Weighted: $item.value|>,
                Values: AsList($item.value),
                Labels: AsList($item.label)
            |>);
            $update = ($state, $item) -> (<|
                Stats: <|
                    Count: $state.Stats.Count + 1ul,
                    Total: $state.Stats.Total + $item.value,
                    Weighted: $state.Stats.Weighted + Unwrap(CAST($state.Stats.Count + 1ul AS Int64)) * $item.value
                |>,
                Values: ListExtend($state.Values, AsList($item.value)),
                Labels: ListExtend($state.Labels, AsList($item.label))
            |>);
            $merge = ($left, $right) -> (<|
                Stats: <|
                    Count: $left.Stats.Count + $right.Stats.Count,
                    Total: $left.Stats.Total + $right.Stats.Total,
                    Weighted: $left.Stats.Weighted + $right.Stats.Weighted
                        + Unwrap(CAST($left.Stats.Count AS Int64)) * $right.Stats.Total
                |>,
                Values: ListExtend($left.Values, $right.Values),
                Labels: ListExtend($left.Labels, $right.Labels)
            |>);
            $finish = ($state) -> (<|
                Summary: AsTuple($state.Stats.Count, $state.Stats.Total, $state.Stats.Weighted),
                Values: ListSort($state.Values),
                Labels: String::JoinFromList(ListSort($state.Labels), ",")
            |>);
            $factory = AggregationFactory("UDAF", $init, $update, $merge, $finish);
            $make_key = ($tenant, $bucket) -> (
                AsStruct($tenant AS tenant, AsTuple($bucket, $bucket % 2ul) AS bucket)
            );
            $render = ($key, $result) -> (
                COALESCE($key.tenant, "null") || "/" || CAST($key.bucket.0 AS String)
                    || "/" || CAST($key.bucket.1 AS String)
                    || ":" || CAST($result.Summary.0 AS String)
                    || ":" || CAST($result.Summary.1 AS String)
                    || ":" || CAST($result.Summary.2 AS String)
                    || ":[" || String::JoinFromList(ListMap($result.Values, ($v) -> (CAST($v AS String))), ",")
                    || "]:" || $result.Labels
            );
        )";
        StartAggregation(UseStateTable, "tenant String, bucket Uint64 NOT NULL, value Int64 NOT NULL, label String NOT NULL",
            R"($render(group_key, AGGREGATE_BY(AsStruct(value AS value, label AS label), $factory)))",
            "$make_key(tenant, bucket) AS group_key", "",
            {.Prelude = prelude});
        WriteAndCheck({
            R"({"tenant":"a","bucket":1,"value":3,"label":"c"})",
            R"({"tenant":"a","bucket":1,"value":-2,"label":"a"})",
            R"({"tenant":"a","bucket":2,"value":7,"label":"b"})",
            R"({"tenant":null,"bucket":1,"value":4,"label":"n"})",
            R"({"tenant":"","bucket":1,"value":9,"label":"e"})",
            R"({"tenant":"a","bucket":1,"value":5,"label":"b"})",
        }, {
            "a/1/1:1:3:3:[3]:c", "a/1/1:2:1:-1:[-2,3]:a,c",
            "a/2/0:1:7:7:[7]:b", "null/1/1:1:4:4:[4]:n", "/1/1:1:9:9:[9]:e",
            "a/1/1:3:6:14:[-2,3,5]:a,b,c",
        });
        WriteAndCheck({
            R"({"tenant":null,"bucket":1,"value":-1,"label":"m"})",
            R"({"tenant":"a","bucket":2,"value":1,"label":"a"})",
            R"({"tenant":"","bucket":1,"value":2,"label":"d"})",
        }, {"null/1/1:2:3:2:[-1,4]:m,n", "a/2/0:2:8:9:[1,7]:a,b", "/1/1:2:11:13:[2,9]:d,e"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(MultipleTasksOnOneNode, UseStateTable, TStreamingAggregationTestFixture) {
        CheckDistributedAggregation(UseStateTable, 1, 4);
    }

    Y_UNIT_TEST_TWIN_F(MultipleTasksOnMultipleNodes, UseStateTable, TStreamingAggregationTestFixture) {
        CheckDistributedAggregation(UseStateTable, 3, 6);
    }

    Y_UNIT_TEST_TWIN_F(RepeatedUpdatesAreNotUnique, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(MIN(value) AS String))");
        WriteAndCheck({
            R"({"key":"a","value":7})", R"({"key":"a","value":7})",
            R"({"key":"b","value":3})", R"({"key":"a","value":7})",
        }, {"a:7", "a:7", "b:3", "a:7"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(DistinctAggregateIsRejected, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(COUNT(DISTINCT value) AS String))", "key", "",
            {.ExpectedError = "DISTINCT aggregation is not supported for mode: StreamingAggregation"});
    }

    Y_UNIT_TEST_TWIN_F(DistinctStreamingOutputIsRejected, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(SUM(value) AS String))", "key", "",
            {.SelectDistinct = true, .ExpectedError = "Aggregation of streaming input without windows is not supported"});
    }

    Y_UNIT_TEST_TWIN_F(CheckpointsAreRejected, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(SUM(value) AS String))", "key", "",
            {.DisableCheckpoints = false,
             .ExpectedError = "Unsupported callable for streaming processing with checkpoints: 'StreamingAggregation'"});
    }

    Y_UNIT_TEST_TWIN_F(DataCorrectnessFor1MiBInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckInputVolume(UseStateTable, 1);
    }

    Y_UNIT_TEST_TWIN_F(DataCorrectnessFor10MiBInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckInputVolume(UseStateTable, 10);
    }

    Y_UNIT_TEST_TWIN_F(DataCorrectnessFor100MiBInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckInputVolume(UseStateTable, 100);
    }

    Y_UNIT_TEST_TWIN_F(StateTableFailureIsReported, WriteFailure, TStreamingAggregationTestFixture) {
        CheckStateTableFailure(WriteFailure);
    }
} // Y_UNIT_TEST_SUITE(KqpStreamingAggregation)

} // namespace NKikimr::NKqp
