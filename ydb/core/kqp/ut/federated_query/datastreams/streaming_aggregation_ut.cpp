#include "common.h"

#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/kqp/common/events/query.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/core/tx/datashard/const.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>

#include <library/cpp/json/json_reader.h>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <limits>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NYdb;

namespace {

class TStateTableQueryProxy final : public TActorBootstrapped<TStateTableQueryProxy> {
public:
    using THandler = std::function<bool(const TEvKqp::TEvQueryRequest&, TEvKqp::TEvQueryResponse&)>;

    TStateTableQueryProxy(const TActorId& proxy, THandler handler)
        : Proxy(proxy)
        , Handler(std::move(handler))
    {}

    void Bootstrap() {
        Become(&TStateTableQueryProxy::StateWork);
    }

    STFUNC(StateWork) {
        if (ev->GetTypeRewrite() == TEvents::TEvPoison::EventType) {
            PassAway();
            return;
        }
        if (ev->GetTypeRewrite() == TEvKqp::TEvQueryRequest::EventType) {
            const auto& request = *ev->Get<TEvKqp::TEvQueryRequest>();
            if (request.GetQuery().Contains("DECLARE $key AS String;")
                && request.GetQuery().Contains("`/Root/aggregationState`")) {
                auto response = MakeHolder<TEvKqp::TEvQueryResponse>(MakeIntrusive<NActors::TProtoArenaHolder>());
                if (Handler(request, *response)) {
                    Send(ev->Sender, response.Release(), 0, ev->Cookie);
                    return;
                }
            }
        }
        Send(ev->Forward(Proxy));
    }

private:
    const TActorId Proxy;
    const THandler Handler;
};

// The fixture uses real actor threads, so intercept requests through the local service.
class TScopedStateTableQueryProxy {
public:
    TScopedStateTableQueryProxy(TTestActorRuntime& runtime, TStateTableQueryProxy::THandler handler)
        : ActorSystem(*runtime.GetActorSystem(0))
        , ServiceId(MakeKqpProxyID(runtime.GetNodeId()))
        , OriginalProxy(ActorSystem.LookupLocalService(ServiceId))
        , Proxy(ActorSystem.Register(new TStateTableQueryProxy(OriginalProxy, std::move(handler))))
    {
        ActorSystem.RegisterLocalService(ServiceId, Proxy);
    }

    ~TScopedStateTableQueryProxy() {
        ActorSystem.RegisterLocalService(ServiceId, OriginalProxy);
        ActorSystem.Send(Proxy, new TEvents::TEvPoison());
    }

private:
    TActorSystem& ActorSystem;
    const TActorId ServiceId;
    const TActorId OriginalProxy;
    const TActorId Proxy;
};

TString MakeStateTableQuery(bool tableInput = false) {
    return fmt::format(R"sql(
        PRAGMA ydb.EnableStreamingAggregation = "TRUE";
        PRAGMA ydb.StreamingAggregationStateTablePath = "/Root/aggregationState";
        PRAGMA ydb.MaxTasksPerStage = "1";
        SELECT Unwrap(key || ":" || CAST(SUM(value) AS String)) AS Data
        FROM {input}
        GROUP BY key;
    )sql", "input"_a = tableInput ? "aggregationInput" : R"(
        AS_TABLE(AsList(
            AsStruct("a" AS key, 1l AS value),
            AsStruct("b" AS key, 2l AS value),
            AsStruct("a" AS key, 1l AS value)))
    )");
}

struct TAggregationSettings {
    ui32 Tasks = 1;
    ui32 Partitions = 1;
    bool DisableCheckpoints = true;
    bool ExpectHopping = false;
    TString Prelude;
    TString ExpectedError;
};

const TString ParentIndexUdafPrelude = R"(
    $init = ($item, $parent) -> (<|InitParent: $parent, UpdateParent: $parent, Total: $item, Count: 1u|>);
    $update = ($state, $item, $parent) -> (<|
        InitParent: $state.InitParent,
        UpdateParent: $parent,
        Total: $state.Total + $item,
        Count: $state.Count + 1u
    |>);
    $factory = AggregationFactory("UDAF", $init, $update, NULL);
    $render = ($left, $right) -> (Ensure(
        String::JoinFromList(ListMap(ListSort(AsList($left.InitParent, $right.InitParent)),
            ($parent) -> (CAST($parent AS String))), ",")
            || ":" || CAST($left.Total AS String) || ":" || CAST($right.Total AS String)
            || ":" || CAST($left.Count AS String),
        $left.InitParent == $left.UpdateParent AND $right.InitParent == $right.UpdateParent
            AND $left.Count == $right.Count,
        "Aggregation parent index changed between init and update"
    ));
)";

// Different arguments keep two independent handlers even if their factories are identical.
const TString ParentIndexUdafResult = R"($render(AGGREGATE_BY(value, $factory), AGGREGATE_BY(-value, $factory)))";

class TStreamingAggregationTestFixture : public TStreamingTestFixture {
public:
    TStreamingAggregationTestFixture()
        : AggregationAppConfig(SetupAppConfig())
    {
        auto& featureFlags = *AggregationAppConfig.MutableFeatureFlags();
        featureFlags.SetEnableStreamingAggregation(true);
        // Streaming aggregation currently requires constraint validation to be disabled.
        featureFlags.SetEnableKqpConstraintsTransformer(false);
    }

    NKikimrConfig::TAppConfig& AggregationAppConfig;

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
                PRAGMA ydb.EnableStreamingAggregation = "TRUE";
                PRAGMA ydb.StreamingAggregationStateTablePath = "{state_table}";
                {planner}
                {prelude}

                INSERT INTO `source`.`{output}`
                SELECT Unwrap({result}) AS Data
                FROM `source`.`{input}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA ({columns})
                )
                {filter}
                {group};
            END DO;
        )sql",
            "input"_a = InputTopic,
            "output"_a = OutputTopic,
            "columns"_a = columns,
            "result"_a = result,
            "group"_a = keys ? fmt::format("GROUP BY {}", keys) : "",
            "filter"_a = filter,
            "state_table"_a = HasStateTable ? "/Root/aggregationState" : "",
            "tasks"_a = settings.Tasks,
            "disable_checkpoints"_a = settings.DisableCheckpoints ? "TRUE" : "FALSE",
            "planner"_a = planner,
            "prelude"_a = settings.Prelude),
            settings.ExpectedError ? EStatus::GENERIC_ERROR : EStatus::SUCCESS,
            settings.ExpectedError);

        if (settings.ExpectedError) {
            return;
        }

        WaitStreamingQueryStatus("aggregation");
        ValidateStreamingQueryAst("aggregation", [expectHopping = settings.ExpectHopping](const TString& ast) {
            if (expectHopping) {
                UNIT_ASSERT_STRING_CONTAINS(ast, "MultiHoppingCore");
                UNIT_ASSERT_C(!ast.Contains("KqpStreamingAggregation"), ast);
            } else {
                UNIT_ASSERT_STRING_CONTAINS(ast, "KqpStreamingAggregation");
            }
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
            const auto result = ExecQuery(fmt::format("SELECT key, state FROM `{}`;", table));
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            UNIT_ASSERT_C(result[0].RowsCount() > 0, "The table-backed aggregation must persist evicted keys");
            TResultSetParser rows(result[0]);
            while (rows.TryNextRow()) {
                UNIT_ASSERT(rows.ColumnParser("state").GetOptionalString());
            }
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
        UNIT_ASSERT_STRING_CONTAINS(ast, "KqpStreamingAggregation");
        if (expectShuffle) {
            UNIT_ASSERT_STRING_CONTAINS(ast, "DqCnHashShuffle");
        }
        CheckPersistedState();
    }

    void CheckStaticInput(bool useStateTable) {
        CreateStateTable(useStateTable);
        CheckFiniteResult(fmt::format(R"(
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            PRAGMA ydb.StreamingAggregationStateTablePath = "{}";
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
            GROUP BY key;
        )", useStateTable ? "/Root/aggregationState" : ""),
            {"a:1:5", "a:2:3", "b:1:10", "a:3:10", "b:2:0", "c:1:1", "d:1:2", "e:1:3"},
            /* expectShuffle */ false);
    }

    void CheckFiniteTopicInput(bool useStateTable, bool streamingInput = false) {
        const auto pqGateway = streamingInput ? SetupMockPqGateway() : nullptr;
        AggregationAppConfig.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
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
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            PRAGMA ydb.StreamingAggregationStateTablePath = "{state_table}";
            {consumer}
            $input = SELECT * FROM `source`.`{input}` WITH (
                STREAMING = "{streaming}", FORMAT = "json_each_row",
                SCHEMA (key String NOT NULL, value Int64 NOT NULL)) LIMIT 4;
            SELECT Unwrap(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
            FROM $input
            GROUP BY key;
        )", "input"_a = InputTopic, "state_table"_a = useStateTable ? "/Root/aggregationState" : "",
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
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            PRAGMA ydb.StreamingAggregationStateTablePath = "{}";
            PRAGMA ydb.MaxTasksPerStage = "1";
            SELECT Unwrap(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
            FROM aggregationInput
            GROUP BY key;
        )", useStateTable ? "/Root/aggregationState" : ""),
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
            result, keys, "", {.ExpectedError = useStateTable
                ? "At most one streaming aggregation with a state table is allowed per query" : ""});
        if (useStateTable) {
            return;
        }
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

    void CheckAggregationExecutionsCount(ui64 expectedExecutionsCount, ui64 expectedLeasesCount) {
        const auto result = ExecQuery(R"(
            SELECT * FROM `.metadata/script_executions`;
            SELECT * FROM `.metadata/script_execution_leases`;
        )");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(result[0].RowsCount(), expectedExecutionsCount);
        UNIT_ASSERT_VALUES_EQUAL(result[1].RowsCount(), expectedLeasesCount);
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
            CheckAggregationExecutionsCount(1, 0);
            ExecQuery("ALTER STREAMING QUERY aggregation SET (RUN = TRUE);");
        }

        auto nextReadSession = pqGateway->WaitReadSession(InputTopic);
        UNIT_ASSERT(nextReadSession != readSession);
        readSession->ExpectSessionClosed();
        writeSession->ExpectSessionClosed();
        writeSession->EnsureEmpty();
        WaitStreamingQueryStatus("aggregation");
        // Failure recovery reuses the execution; manual ALTER creates a new one.
        CheckAggregationExecutionsCount(injectFailure ? 1 : 2, 1);

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

    void WaitAggregationCheckpoint() {
        // Avoid SQL aggregation here: the feature flag rewrites metadata queries too.
        const auto latest = ExecQuery(R"(
            SELECT coordinator_generation, seq_no
            FROM `.metadata/streaming/checkpoints/checkpoints_metadata`
            WHERE graph_id LIKE "%/Root/aggregation"
            ORDER BY coordinator_generation DESC, seq_no DESC LIMIT 1;
        )");
        UNIT_ASSERT_VALUES_EQUAL(latest.size(), 1);
        NFq::TCheckpointId bound(0, 0);
        TResultSetParser latestRows(latest[0]);
        if (latestRows.TryNextRow()) {
            bound = {*latestRows.ColumnParser("coordinator_generation").GetOptionalUint64(),
                *latestRows.ColumnParser("seq_no").GetOptionalUint64()};
        }

        // Wait for a checkpoint started after the preceding batch was processed,
        // so an older in-flight checkpoint cannot satisfy the wait.
        NTestUtils::WaitFor(TEST_OPERATION_TIMEOUT, "streaming aggregation checkpoint", [&](TString& error) {
            const auto result = ExecQuery(fmt::format(R"(
                SELECT coordinator_generation, seq_no
                FROM `.metadata/streaming/checkpoints/checkpoints_metadata`
                WHERE graph_id LIKE "%/Root/aggregation" AND status = {}ut
                ORDER BY coordinator_generation DESC, seq_no DESC LIMIT 1;
            )", static_cast<ui8>(NFq::ECheckpointStatus::Completed)));
            UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
            TResultSetParser rows(result[0]);
            if (!rows.TryNextRow()) {
                error = "No completed checkpoints";
                return false;
            }
            const NFq::TCheckpointId current(*rows.ColumnParser("coordinator_generation").GetOptionalUint64(),
                *rows.ColumnParser("seq_no").GetOptionalUint64());
            error = TStringBuilder() << "Last completed checkpoint " << current.CoordinatorGeneration << "." << current.SeqNo;
            return bound < current;
        });
    }

    struct TCheckpointBatch {
        std::vector<TString> Input;
        std::vector<TString> Output;
    };

    void CheckCheckpointRecovery(const TString& columns, const TString& result, const TString& keys,
                                 const std::vector<TCheckpointBatch>& batches, bool injectFailure = true,
                                 const TString& prelude = "") {
        const auto pqGateway = SetupMockPqGateway();
        StartAggregation(false, columns, result, keys, "", {.DisableCheckpoints = false, .Prelude = prelude});
        auto readSession = pqGateway->WaitReadSession(InputTopic);
        auto writeSession = pqGateway->WaitWriteSession(OutputTopic);
        ui64 offset = 0;
        for (size_t i = 0; i < batches.size(); ++i) {
            for (const auto& input : batches[i].Input) {
                readSession->AddDataReceivedEvent(offset++, input);
            }
            writeSession->ExpectMessages(batches[i].Output, true);
            WaitAggregationCheckpoint();
            writeSession->EnsureEmpty();
            if (i + 1 == batches.size()) {
                break;
            }

            if (injectFailure) {
                readSession->AddCloseSessionEvent(EStatus::UNAVAILABLE, {NIssue::TIssue("Aggregation checkpoint recovery test")});
            } else {
                ExecQuery("ALTER STREAMING QUERY aggregation SET (RUN = FALSE);");
                readSession->ExpectSessionClosed();
                writeSession->ExpectSessionClosed();
                ExecQuery("ALTER STREAMING QUERY aggregation SET (RUN = TRUE);");
            }
            readSession->ExpectSessionClosed();
            writeSession->ExpectSessionClosed();
            writeSession->EnsureEmpty();
            readSession = pqGateway->WaitReadSession(InputTopic);
            writeSession = pqGateway->WaitWriteSession(OutputTopic);
            WaitStreamingQueryStatus("aggregation");
        }
        FinishAggregation();
        readSession->ExpectSessionClosed();
        writeSession->ExpectSessionClosed();
        writeSession->EnsureEmpty();
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
                    PRAGMA ydb.EnableStreamingAggregation = "TRUE";
                    PRAGMA ydb.StreamingAggregationStateTablePath = "{state_table}";
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
                "group"_a = aggregate ? "GROUP BY key" : "",
                "state_table"_a = useStateTable ? "/Root/aggregationState" : ""));
            WaitStreamingQueryStatus("aggregation");
            ValidateStreamingQueryAst("aggregation", [aggregate](const TString& ast) {
                UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("KqpStreamingAggregation"), aggregate, ast);
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
        const auto result = ExecQuery("SELECT Path FROM `.sys/streaming_queries` WHERE Path = '/Root/aggregation';");
        UNIT_ASSERT_VALUES_EQUAL(result.at(0).RowsCount(), 0);
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
        const auto result = ExecQuery("SELECT key FROM aggregationState;");
        UNIT_ASSERT_VALUES_EQUAL(result.at(0).RowsCount(), 0);
    }

    void CheckMultipleAggregations(bool useStateTable, bool extraSinks) {
        const auto pqGateway = SetupMockPqGateway();
        CreateStateTable(useStateTable);
        CreateAggregationTopics();
        const TString secondOutput = TStringBuilder() << OutputTopic << "_subkey";
        const TString rawOutput = TStringBuilder() << OutputTopic << "_raw";
        CreateTopic(secondOutput);
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
                PRAGMA ydb.EnableStreamingAggregation = "TRUE";
                PRAGMA ydb.StreamingAggregationStateTablePath = "{state_table}";
                $input = SELECT * FROM `source`.`{input}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (id Uint64 NOT NULL, key String NOT NULL, subkey String NOT NULL, value Int64 NOT NULL));
                INSERT INTO `source`.`{output}`
                SELECT Unwrap(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
                FROM $input GROUP BY key;
                INSERT INTO `source`.`{second_output}`
                SELECT Unwrap(subkey || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)) AS Data
                FROM $input GROUP BY subkey;
                {sinks}
            END DO;
        )", "input"_a = InputTopic, "output"_a = OutputTopic, "second_output"_a = secondOutput,
            "state_table"_a = useStateTable ? "/Root/aggregationState" : "",
            "sinks"_a = sinks),
            useStateTable ? EStatus::GENERIC_ERROR : EStatus::SUCCESS,
            useStateTable ? "At most one streaming aggregation with a state table is allowed per query" : "");
        if (useStateTable) {
            return;
        }
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
                aggregations += node->IsCallable("KqpStreamingAggregation");
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
        auto& resources = *AggregationAppConfig.MutableTableServiceConfig()->MutableResourceManager();
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

    void CheckUdafUnpersistableSavedState(bool useStateTable, bool checkpointsEnabled, bool wrapSavedState) {
        const TString prelude = TStringBuilder() << R"(
            $init = ($item) -> (Stat::TDigest_Create($item));
            $update = ($state, $item) -> (Stat::TDigest_AddValue($state, $item));
            $finish = ($state) -> (Stat::TDigest_GetPercentile($state, 0.5));
        )" << (wrapSavedState ? R"(
            $save = ($state) -> (AsStruct($state AS payload));
            $load = ($saved) -> ($saved.payload);
            $factory = AggregationFactory("UDAF", $init, $update, NULL, $finish, $save, $load);
        )" : R"(
            $factory = AggregationFactory("UDAF", $init, $update, NULL, $finish);
        )");
        const bool needsPersistence = useStateTable || checkpointsEnabled;
        StartAggregation(useStateTable, "key String NOT NULL, value Double NOT NULL",
            R"(key || ":" || CAST(AGGREGATE_BY(value, $factory) AS String))", "key", "",
            {.DisableCheckpoints = !checkpointsEnabled,
             .Prelude = prelude,
             .ExpectedError = needsPersistence ? "Expected persistable data, but got:" : ""});
        if (needsPersistence) {
            return;
        }

        WriteAndCheck({
            R"({"key":"a","value":10})", R"({"key":"a","value":30})",
            R"({"key":"b","value":100})", R"({"key":"a","value":50})",
            R"({"key":"b","value":200})",
        }, {"a:10", "a:20", "b:100", "a:30", "b:150"});
        FinishAggregation();
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
    Y_UNIT_TEST_TWIN_F(FeatureFlagAndPragmaControlRewrite, Enabled, TStreamingAggregationTestFixture) {
        AggregationAppConfig.MutableFeatureFlags()->SetEnableStreamingAggregation(Enabled);
        for (const TString& pragma : {TString(), TString("FALSE"), TString("TRUE")}) {
            const bool streamingEnabled = Enabled && pragma == "TRUE";
            for (const bool keyed : {false, true}) {
                const auto query = fmt::format(R"(
                    PRAGMA EmitAggApply;
                    {enable_streaming_aggregation}
                    PRAGMA ydb.StreamingAggregationStateTablePath = "{state_table}";
                    $input = AsList(AsStruct("a" AS key, 2l AS value), AsStruct("a" AS key, 3l AS value));
                    SELECT Unwrap(CAST(SUM(value) AS String)) AS Data
                    FROM AS_TABLE($input) {group};
                )", "enable_streaming_aggregation"_a = pragma.empty() ? ""
                        : fmt::format(R"(PRAGMA ydb.EnableStreamingAggregation = "{}";)", pragma),
                    "state_table"_a = streamingEnabled ? "" : "/Root/nonexistent",
                    "group"_a = keyed ? "GROUP BY key" : "");
                const auto result = GetQueryClient()->ExecuteQuery(query, NQuery::TTxControl::NoTx(),
                    NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)
                        .ClientTimeout(TEST_OPERATION_TIMEOUT)).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetStats());
                UNIT_ASSERT(result.GetStats()->GetAst());
                const TString ast = *result.GetStats()->GetAst();
                UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("KqpStreamingAggregation"), streamingEnabled, ast);
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
                TResultSetParser rows(result.GetResultSet(0));
                std::vector<std::string> actual;
                while (rows.TryNextRow()) {
                    actual.push_back(rows.ColumnParser("Data").GetString());
                }
                Sort(actual);
                UNIT_ASSERT_VALUES_EQUAL(actual, (streamingEnabled ? std::vector<std::string>{"2", "5"} : std::vector<std::string>{"5"}));
            }
        }
    }

    Y_UNIT_TEST_TWIN_F(CheckpointRecovery, InjectFailure, TStreamingAggregationTestFixture) {
        CheckCheckpointRecovery("key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String))", "key", {
                {{R"({"key":"a","value":5})", R"({"key":"a","value":2})", R"({"key":"b","value":10})"},
                    {"a:1:5", "a:2:7", "b:1:10"}},
                {{R"({"key":"a","value":-2})", R"({"key":"b","value":3})", R"({"key":"c","value":1})"},
                    {"a:3:5", "b:2:13", "c:1:1"}},
                // Save and restart restored state without consuming any new input.
                {{}, {}},
                {{R"({"key":"a","value":1})", R"({"key":"b","value":-3})", R"({"key":"c","value":4})"},
                    {"a:4:6", "b:3:10", "c:2:5"}},
            }, InjectFailure);
    }

    Y_UNIT_TEST_F(CheckpointKeylessRecovery, TStreamingAggregationTestFixture) {
        CheckCheckpointRecovery("value Int64 NOT NULL",
            R"(CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String))", "", {
                {{R"({"value":5})", R"({"value":3})"}, {"1:5", "2:8"}},
                {{R"({"value":-2})"}, {"3:6"}},
            });
    }

    Y_UNIT_TEST_F(CheckpointEmptyStateRecovery, TStreamingAggregationTestFixture) {
        CheckCheckpointRecovery("key String NOT NULL, value Int64 NOT NULL",
            R"(key || ":" || CAST(SUM(value) AS String))", "key", {
                {{}, {}},
                {{R"({"key":"a","value":5})"}, {"a:5"}},
                {{R"({"key":"a","value":-2})"}, {"a:3"}},
            });
    }

    Y_UNIT_TEST_F(CheckpointResourceStateRecovery, TStreamingAggregationTestFixture) {
        // AGGREGATE_LIST has a resource state and needs the trait's save/load handlers.
        CheckCheckpointRecovery("key String, subkey Uint64 NOT NULL, value Int64", R"(
            COALESCE(key, "null") || ":" || CAST(subkey AS String) || ":[" || String::JoinFromList(
                ListMap(ListSort(AGGREGATE_LIST(value)), ($v) -> (CAST($v AS String))), ",") || "]"
        )", "key, subkey", {
            {{R"({"key":null,"subkey":1,"value":5})", R"({"key":null,"subkey":1,"value":null})",
                R"({"key":"a","subkey":2,"value":3})"}, {"null:1:[5]", "null:1:[5]", "a:2:[3]"}},
            {{R"({"key":null,"subkey":1,"value":-2})", R"({"key":"a","subkey":2,"value":7})",
                R"({"key":null,"subkey":2,"value":1})"}, {"null:1:[-2,5]", "a:2:[3,7]", "null:2:[1]"}},
            {{R"({"key":null,"subkey":1,"value":4})", R"({"key":"a","subkey":2,"value":null})"},
                {"null:1:[-2,4,5]", "a:2:[3,7]"}},
        });
    }

    Y_UNIT_TEST_TWIN_F(CheckpointsWithStateTableValidation, ValidateCheckpoints, TStreamingAggregationTestFixture) {
        StartAggregation(true, "key String NOT NULL, value Int64 NOT NULL", R"(CAST(SUM(value) AS String))", "key", "",
            {.DisableCheckpoints = false,
             .Prelude = ValidateCheckpoints ? "" : "PRAGMA ydb.OptValidateStreamingCheckpoints = \"FALSE\";",
             .ExpectedError = ValidateCheckpoints ? "Checkpoints are not supported for streaming aggregation with a state table" : ""});
        if (!ValidateCheckpoints) {
            ExecQuery("DROP STREAMING QUERY aggregation;");
        }
    }

    Y_UNIT_TEST_F(CheckpointMultiplePercentiles, TStreamingAggregationTestFixture) {
        // Both outputs share one TDigest resource, including its save/load handlers.
        CheckCheckpointRecovery("key String NOT NULL, value Int64", R"(
            key || ":" || COALESCE(CAST(PERCENTILE(value, 0.50) AS String), "null")
                || ":" || COALESCE(CAST(PERCENTILE(value, 0.95) AS String), "null")
        )", "key", {
            {{R"({"key":"a","value":null})", R"({"key":"a","value":10})",
                R"({"key":"a","value":20})", R"({"key":"b","value":100})"},
                {"a:null:null", "a:10:10", "a:15:20", "b:100:100"}},
            {{R"({"key":"a","value":30})", R"({"key":"b","value":200})"},
                {"a:20:30", "b:150:200"}},
        });
    }

    Y_UNIT_TEST_F(KeylessAggregation, TStreamingAggregationTestFixture) {
        StartAggregation(false, "key String NOT NULL, value Int64 NOT NULL",
            R"(CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String))", "");
        WriteAndCheck({R"({"key":"a","value":5})", R"({"key":"b","value":3})"}, {"1:5", "2:8"});
        WriteAndCheck({R"({"key":"a","value":-2})"}, {"3:6"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(AggregationWithoutHandlers, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String", R"(COALESCE(key, "null"))");
        WriteAndCheck({R"({"key":"a"})", R"({"key":"b"})", R"({"key":"a"})", R"({"key":null})"},
            {"a", "b", "a", "null"});
        WriteAndCheck({R"({"key":"b"})", R"({"key":null})", R"({"key":"a"})"}, {"b", "null", "a"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(UnusedGlobalAggregation, EmptyInput, TStreamingAggregationTestFixture) {
        const auto result = GetQueryClient()->ExecuteQuery(fmt::format(R"(
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            $input = AsList(AsStruct(1l AS value), AsStruct(2l AS value), AsStruct(3l AS value));
            SELECT "marker" AS Data FROM (
                SELECT COUNT(*) AS count FROM AS_TABLE($input) WHERE {}
            );
        )", EmptyInput ? "FALSE" : "TRUE"), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)
                .ClientTimeout(TEST_OPERATION_TIMEOUT)).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Data").GetString(), "marker");

        // The unused global aggregate becomes one row before streaming rewriting, even for empty input.
        UNIT_ASSERT(result.GetStats());
        UNIT_ASSERT(result.GetStats()->GetAst());
        const auto ast = *result.GetStats()->GetAst();
        UNIT_ASSERT_C(ast.find("KqpStreamingAggregation") == std::string::npos, ast);
    }

    Y_UNIT_TEST_TWIN_F(StaticInput, UseStateTable, TStreamingAggregationTestFixture) {
        CheckStaticInput(UseStateTable);
    }

    Y_UNIT_TEST_TWIN_F(StaticJoinInput, UseStateTable, TStreamingAggregationTestFixture) {
        CreateStateTable(UseStateTable);
        CheckFiniteResult(fmt::format(R"(
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            PRAGMA ydb.StreamingAggregationStateTablePath = "{}";
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
            GROUP BY l.key;
        )", UseStateTable ? "/Root/aggregationState" : ""),
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

    Y_UNIT_TEST_QUAD_F(MultipleFiniteAggregations, UseStateTable, Nested, TStreamingAggregationTestFixture) {
        CreateStateTable(UseStateTable);
        const TString query = fmt::format(R"sql(
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            PRAGMA ydb.StreamingAggregationStateTablePath = "{}";
            $input = AsList(
                AsStruct("a" AS key, 1l AS value),
                AsStruct("b" AS key, 2l AS value),
                AsStruct("a" AS key, 3l AS value));
            $first = SELECT key, SUM(value) AS value FROM AS_TABLE($input) GROUP BY key;
            {}
        )sql", UseStateTable ? "/Root/aggregationState" : "", Nested
            ? "SELECT key, SUM(value) FROM $first GROUP BY key;"
            : "SELECT * FROM $first; SELECT key, COUNT(*) FROM AS_TABLE($input) GROUP BY key;");
        const auto results = ExecQuery(query, UseStateTable ? EStatus::GENERIC_ERROR : EStatus::SUCCESS,
            UseStateTable ? "At most one streaming aggregation with a state table is allowed per query" : "");
        if constexpr (!UseStateTable) {
            UNIT_ASSERT_VALUES_EQUAL(results.size(), Nested ? 1 : 2);
            for (const auto& result : results) {
                UNIT_ASSERT_VALUES_EQUAL(result.RowsCount(), 3);
            }
        }
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

    Y_UNIT_TEST_F(GroupingSetsWithIntersectingKeys, TStreamingAggregationTestFixture) {
        StartAggregation(/* useStateTable */ false, "a String NOT NULL, b String NOT NULL, value Int64 NOT NULL", R"(
            (CASE GROUPING(a, b) WHEN 1 THEN "a:" ELSE "b:" END)
                || COALESCE(a, b) || ":" || CAST(COUNT(*) AS String) || ":" || CAST(SUM(value) AS String)
        )", "GROUPING SETS ((a), (b))");
        // Both grouping sets contain the same String keys, but must keep independent accumulators.
        WriteAndCheck({
            R"({"a":"x","b":"x","value":1})",
            R"({"a":"x","b":"y","value":10})",
            R"({"a":"y","b":"y","value":100})",
            R"({"a":"x","b":"x","value":1000})",
        }, {
            "a:x:1:1", "b:x:1:1",
            "a:x:2:11", "b:y:1:10",
            "a:y:1:100", "b:y:2:110",
            "a:x:3:1011", "b:x:2:1001",
        });
        WriteAndCheck({R"({"a":"y","b":"x","value":-1})"}, {"a:y:2:99", "b:x:3:1000"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(CaseGrouping, UseStateTable, TStreamingAggregationTestFixture) {
        CheckGroupingSets(UseStateTable, "CUBE(key, subkey)", {0, 1, 2, 3}, true);
    }

    Y_UNIT_TEST_TWIN_F(SessionWindowsAreRejected, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL", "CAST(SUM(value) AS String)",
            "key, SessionWindow(value, 10) AS session_start", "",
            {.ExpectedError = "Session windows are not supported for streaming aggregation"});
    }

    Y_UNIT_TEST_TWIN_F(HoppingWindowsTakePrecedence, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL", "CAST(SUM(value) AS String)",
            R"(key, HOP(CurrentUtcTimestamp(TableRow()), "PT10S", "PT10S", "PT10S"))", "",
            {.ExpectHopping = true});
        ExecQuery("DROP STREAMING QUERY aggregation;");
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

    Y_UNIT_TEST_TWIN_F(MultiplePercentiles, Keyless, TStreamingAggregationTestFixture) {
        const TString result = TStringBuilder() << (Keyless ? "\"all\"" : "key") << R"(
            || ":" || CAST(COUNT(*) AS String)
            || ":" || COALESCE(CAST(PERCENTILE(value, 0.50) AS String), "null")
            || ":" || COALESCE(CAST(PERCENTILE(value, 0.95) AS String), "null")
        )";
        StartAggregation(false, "key String NOT NULL, value Int64", result, Keyless ? "" : "key");
        WriteAndCheck({
            R"({"key":"a","value":null})",
            R"({"key":"a","value":10})",
            R"({"key":"a","value":20})",
            R"({"key":"b","value":100})",
            R"({"key":"b","value":null})",
            R"({"key":"a","value":30})",
        }, Keyless
            ? std::vector<std::string>{"all:1:null:null", "all:2:10:10", "all:3:15:20",
                "all:4:20:100", "all:5:20:100", "all:6:25:100"}
            : std::vector<std::string>{"a:1:null:null", "a:2:10:10", "a:3:15:20",
                "b:1:100:100", "b:2:100:100", "a:4:20:30"});
        WriteAndCheck({R"({"key":"a","value":40})", R"({"key":"b","value":200})"}, Keyless
            ? std::vector<std::string>{"all:7:30:100", "all:8:35:200"}
            : std::vector<std::string>{"a:5:25:40", "b:3:150:200"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(ProjectedGroupKeys, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String, subkey Uint64 NOT NULL, value Int64", R"(
            CAST(COUNT(*) AS String) || ":" || COALESCE(CAST(SUM(value) AS String), "null")
        )", "key, subkey", "", {.Prelude = "PRAGMA EmitAggApply;"});
        WriteAndCheck({
            R"({"key":null,"subkey":1,"value":null})",
            R"({"key":"a","subkey":1,"value":10})",
            R"({"key":null,"subkey":1,"value":5})",
            R"({"key":"a","subkey":2,"value":30})",
            R"({"key":"b","subkey":1,"value":40})",
        }, {"1:null", "1:10", "2:5", "1:30", "1:40"});
        WriteAndCheck({R"({"key":"a","subkey":1,"value":2})", R"({"key":null,"subkey":1,"value":-1})"},
            {"2:12", "3:4"});
        FinishAggregation();
    }

    Y_UNIT_TEST_F(ProjectedPercentiles, TStreamingAggregationTestFixture) {
        StartAggregation(false, "key String NOT NULL, value Int64", R"(
            COALESCE(CAST(PERCENTILE(value, 0.50) AS String), "null")
                || ":" || COALESCE(CAST(PERCENTILE(value, 0.95) AS String), "null")
        )", "key");
        WriteAndCheck({R"({"key":"a","value":null})", R"({"key":"a","value":10})",
            R"({"key":"a","value":20})", R"({"key":"b","value":100})"},
            {"null:null", "10:10", "15:20", "100:100"});
        WriteAndCheck({R"({"key":"a","value":30})", R"({"key":"b","value":200})"},
            {"20:30", "150:200"});
        FinishAggregation();
    }

    Y_UNIT_TEST_F(ProjectedEmptyOutput, TStreamingAggregationTestFixture) {
        CheckFiniteResult(R"(
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            $input = AsList(AsStruct("a" AS key), AsStruct("b" AS key), AsStruct("a" AS key));
            SELECT Unwrap(CAST(COUNT(*) AS String)) AS Data FROM (
                SELECT key FROM AS_TABLE($input) GROUP BY key
            );
        )", {"1", "2", "3"}, /* expectShuffle */ false);
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

    Y_UNIT_TEST_TWIN_F(FloatingPointGroupKeys, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL", R"(
            CASE WHEN number == 0.0 THEN "zero" WHEN number == 1.0 THEN "one" ELSE "nan" END
                || ":" || CAST(COUNT(*) AS String)
        )", "Unwrap(CAST(key AS Double)) AS number");
        // Equal floating-point keys must match both in the cache and after eviction.
        WriteAndCheck({
            R"({"key":"0"})",
            R"({"key":"-0"})",
            R"({"key":"1"})",
            R"({"key":"0"})",
            R"({"key":"nan"})",
            R"({"key":"-nan"})",
            R"({"key":"1"})",
            R"({"key":"nan"})",
            R"({"key":"-0"})",
        }, {"zero:1", "zero:2", "one:1", "zero:3", "nan:1", "nan:2", "one:2", "nan:3", "zero:4"});
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

    Y_UNIT_TEST_TWIN_F(UdafWithCustomStateSerialization, UseStateTable, TStreamingAggregationTestFixture) {
        const TString prelude = R"(
            $init = ($item) -> (Stat::TDigest_Create($item));
            $update = ($state, $item) -> (Stat::TDigest_AddValue($state, $item));
            $merge = ($left, $right) -> (Stat::TDigest_Merge($left, $right));
            $finish = ($state) -> (Stat::TDigest_GetPercentile($state, 0.5));
            $save = ($state) -> (AsStruct(Stat::TDigest_Serialize($state) AS payload));
            $load = ($saved) -> (Stat::TDigest_Deserialize($saved.payload));
            $factory = AggregationFactory("UDAF", $init, $update, $merge, $finish, $save, $load);
        )";
        StartAggregation(UseStateTable, "key String NOT NULL, value Double NOT NULL",
            R"(key || ":" || CAST(AGGREGATE_BY(value, $factory) AS String))", "key", "",
            {.Prelude = prelude});
        // The live state is a resource; only the custom save result can be packed.
        // Repeated keys exercise cache hits, while alternating keys force save/load through the table.
        WriteAndCheck({
            R"({"key":"a","value":10})",
            R"({"key":"a","value":30})",
            R"({"key":"b","value":100})",
            R"({"key":"c","value":1000})",
            R"({"key":"a","value":50})",
            R"({"key":"b","value":200})",
            R"({"key":"a","value":70})",
            R"({"key":"c","value":2000})",
        }, {"a:10", "a:20", "b:100", "c:1000", "a:30", "b:150", "a:40", "c:1500"});
        WriteAndCheck({
            R"({"key":"a","value":90})",
            R"({"key":"b","value":300})",
            R"({"key":"c","value":3000})",
        }, {"a:50", "b:200", "c:2000"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(UdafWithRowState, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL", R"(
            key || ":" || CAST(AGGREGATE_BY(AsStruct(value AS value), $factory).value AS String)
        )", "key", "", {.Prelude = R"(
            $init = ($item) -> ($item);
            $update = ($state, $item) -> (AsStruct($state.value + $item.value AS value));
            $factory = AggregationFactory("UDAF", $init, $update, NULL);
        )"});
        WriteAndCheck({
            R"({"key":"a","value":1})", R"({"key":"a","value":2})",
            R"({"key":"b","value":10})", R"({"key":"a","value":4})",
            R"({"key":"b","value":20})",
        }, {"a:1", "a:3", "b:10", "a:7", "b:30"});
        WriteAndCheck({R"({"key":"a","value":8})", R"({"key":"b","value":30})"}, {"a:15", "b:60"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(UdafWithoutMergeAndOptionalHandlers, UseStateTable, TStreamingAggregationTestFixture) {
        // A struct argument lets the UDAF handle nulls itself: the automatic optional wrapper requires merge.
        const TString prelude = R"(
            $init = ($item) -> ($item.value);
            $update = ($state, $item) -> (COALESCE($state + $item.value, $state, $item.value));
            $factory = AggregationFactory("UDAF", $init, $update, NULL);
        )";
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64",
            R"(key || ":" || COALESCE(CAST(AGGREGATE_BY(AsStruct(value AS value), $factory) AS String), "null"))", "key", "",
            {.Prelude = prelude});
        WriteAndCheck({
            R"({"key":"a","value":null})", R"({"key":"b","value":10})",
            R"({"key":"a","value":3})", R"({"key":"a","value":2})",
            R"({"key":"b","value":null})", R"({"key":"a","value":-1})",
            R"({"key":"b","value":4})",
        }, {"a:null", "b:10", "a:3", "a:5", "b:10", "a:4", "b:14"});
        FinishAggregation();
    }

    Y_UNIT_TEST_F(UdafWithoutMergeWithNullableInputIsRejected, TStreamingAggregationTestFixture) {
        StartAggregation(false, "key String NOT NULL, value Int64",
            R"(COALESCE(CAST(AGGREGATE_BY(value, $factory) AS String), "null"))", "key", "",
            {.Prelude = R"(
                $factory = AggregationFactory("UDAF", ($item) -> ($item), ($state, $item) -> ($state + $item), NULL);
            )",
             .ExpectedError = "Mismatch lambda return type, Void != Int64"});
    }

    Y_UNIT_TEST_TWIN_F(UdafWithDefaultValue, OptionalDefault, TStreamingAggregationTestFixture) {
        const TString prelude = fmt::format(R"(
            $init = ($item) -> ($item);
            $update = ($state, $item) -> ($state + $item);
            $merge = ($left, $right) -> ($left + $right);
            $identity = ($state) -> ($state);
            $factory = AggregationFactory("UDAF", $init, $update, $merge, $identity, $identity, $identity, {default});
        )", "default"_a = OptionalDefault ? "Just(42l)" : "42l");
        StartAggregation(false, "value Int64", R"(CAST(AGGREGATE_BY(value, $factory) AS String))", "", "",
            {.Prelude = prelude});
        WriteAndCheck({R"({"value":null})", R"({"value":5})", R"({"value":null})", R"({"value":2})"},
            {"42", "5", "5", "7"});
        FinishAggregation();
    }

    Y_UNIT_TEST_QUAD_F(UdafDefaultTypeValidation, Keyed, OptionalDefault, TStreamingAggregationTestFixture) {
        const TString prelude = fmt::format(R"(
            $init = ($item) -> ($item);
            $update = ($state, $item) -> ($state + $item);
            $identity = ($state) -> ($state);
            $factory = AggregationFactory("UDAF", $init, $update, NULL, $identity, $identity, $identity, {default});
        )", "default"_a = OptionalDefault ? "Just(42l)" : "42.0");
        StartAggregation(false, "key String NOT NULL, value Int64 NOT NULL",
            R"(CAST(AGGREGATE_BY(value, $factory) AS String))", Keyed ? "key" : "", "",
            {.Prelude = prelude, .ExpectedError = Keyed ? ""
                : "Default value type requires an unsupported conversion in streaming aggregation"});
        if (Keyed) {
            // Defaults are not used for keyed aggregation.
            WriteAndCheck({R"({"key":"a","value":1})", R"({"key":"a","value":2})",
                R"({"key":"b","value":10})"}, {"1", "3", "10"});
            FinishAggregation();
        }
    }

    Y_UNIT_TEST_OCTET_F(UdafResultTypes, Keyed, Optional, HasDefault, TStreamingAggregationTestFixture) {
        const auto query = fmt::format(R"(
            PRAGMA ydb.EnableStreamingAggregation = "TRUE";
            $init = ($item) -> ($item);
            $update = ($state, $item) -> ($state + $item);
            $merge = ($left, $right) -> ($left + $right);
            $identity = ($state) -> ($state);
            $finish = ($state) -> ({finish});
            $factory = AggregationFactory("UDAF", $init, $update, $merge, $finish, $identity, $identity, {default});
            $input = AsList(
                AsStruct("a" AS key, 1l AS value),
                AsStruct("a" AS key, 2l AS value),
                AsStruct("b" AS key, 3l AS value),
                AsStruct("a" AS key, 4l AS value));
            SELECT value, Unwrap(CAST(value AS String)) AS Data FROM (
                SELECT AGGREGATE_BY(value, $factory) AS value FROM AS_TABLE($input) {group_by}
            );
        )", "finish"_a = Optional ? "Just($state)" : "$state", "default"_a = HasDefault ? "42l" : "NULL",
            "group_by"_a = Keyed ? "GROUP BY key" : "");
        const auto result = GetQueryClient()->ExecuteQuery(query, NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)
                .ClientTimeout(TEST_OPERATION_TIMEOUT)).ExtractValueSync();
        CheckFiniteResult(result, Keyed ? std::vector<std::string>{"1", "3", "3", "7"}
            : std::vector<std::string>{"1", "3", "6", "10"}, /* expectShuffle */ false);

        // Check the public result type as well as the per-row values and the streaming plan.
        const auto columns = result.GetResultSet(0).GetColumnsMeta();
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2);
        const auto valueColumn = std::find_if(columns.begin(), columns.end(),
            [](const auto& column) { return column.Name == "value"; });
        UNIT_ASSERT(valueColumn != columns.end());
        TTypeParser type(valueColumn->Type);
        if (Keyed ? Optional : !HasDefault) {
            UNIT_ASSERT_VALUES_EQUAL(type.GetKind(), TTypeParser::ETypeKind::Optional);
            type.OpenOptional();
        }
        UNIT_ASSERT_VALUES_EQUAL(type.GetKind(), TTypeParser::ETypeKind::Primitive);
        UNIT_ASSERT_VALUES_EQUAL(type.GetPrimitive(), EPrimitiveType::Int64);
    }

    Y_UNIT_TEST_F(CheckpointUdafWithoutMerge, TStreamingAggregationTestFixture) {
        const TString prelude = R"(
            $init = ($item) -> (Stat::TDigest_Create($item));
            $update = ($state, $item) -> (Stat::TDigest_AddValue($state, $item));
            $finish = ($state) -> (Stat::TDigest_GetPercentile($state, 0.5));
            $save = ($state) -> (AsStruct(Stat::TDigest_Serialize($state) AS payload));
            $load = ($saved) -> (Stat::TDigest_Deserialize($saved.payload));
            $factory = AggregationFactory("UDAF", $init, $update, NULL, $finish, $save, $load);
        )";
        CheckCheckpointRecovery("key String NOT NULL, value Double NOT NULL",
            R"(key || ":" || CAST(AGGREGATE_BY(value, $factory) AS String))", "key", {
                {{R"({"key":"a","value":10})", R"({"key":"a","value":30})",
                    R"({"key":"b","value":100})"}, {"a:10", "a:20", "b:100"}},
                {{R"({"key":"a","value":50})", R"({"key":"b","value":200})"}, {"a:30", "b:150"}},
                {{}, {}},
                {{R"({"key":"a","value":70})", R"({"key":"b","value":300})"}, {"a:40", "b:200"}},
            }, /* injectFailure */ true, prelude);
    }

    Y_UNIT_TEST_TWIN_F(UdafParentIndices, UseStateTable, TStreamingAggregationTestFixture) {
        StartAggregation(UseStateTable, "key String NOT NULL, value Int64 NOT NULL",
            TStringBuilder() << "key || \":\" || " << ParentIndexUdafResult, "key", "",
            {.Prelude = ParentIndexUdafPrelude});
        // Consecutive keys update cached state; alternating keys exercise table eviction and reload.
        WriteAndCheck({
            R"({"key":"a","value":2})", R"({"key":"a","value":3})",
            R"({"key":"b","value":10})", R"({"key":"a","value":4})",
            R"({"key":"b","value":-1})",
        }, {"a:0,1:2:-2:1", "a:0,1:5:-5:2", "b:0,1:10:-10:1", "a:0,1:9:-9:3", "b:0,1:9:-9:2"});
        WriteAndCheck({
            R"({"key":"c","value":7})", R"({"key":"a","value":-2})", R"({"key":"b","value":1})",
        }, {"c:0,1:7:-7:1", "a:0,1:7:-7:4", "b:0,1:10:-10:3"});
        FinishAggregation();
    }

    Y_UNIT_TEST_TWIN_F(CheckpointUdafParentIndices, Keyless, TStreamingAggregationTestFixture) {
        if (Keyless) {
            CheckCheckpointRecovery("value Int64 NOT NULL", ParentIndexUdafResult, "", {
                {{R"({"value":2})", R"({"value":3})"}, {"0,1:2:-2:1", "0,1:5:-5:2"}},
                {{R"({"value":4})"}, {"0,1:9:-9:3"}},
                {{}, {}},
                {{R"({"value":-1})"}, {"0,1:8:-8:4"}},
            }, /* injectFailure */ true, ParentIndexUdafPrelude);
        } else {
            CheckCheckpointRecovery("key String NOT NULL, value Int64 NOT NULL",
                TStringBuilder() << "key || \":\" || " << ParentIndexUdafResult, "key", {
                    {{R"({"key":"a","value":2})", R"({"key":"a","value":3})",
                        R"({"key":"b","value":10})"}, {"a:0,1:2:-2:1", "a:0,1:5:-5:2", "b:0,1:10:-10:1"}},
                    {{R"({"key":"a","value":4})", R"({"key":"b","value":-1})"}, {"a:0,1:9:-9:3", "b:0,1:9:-9:2"}},
                    {{}, {}},
                    {{R"({"key":"a","value":-2})", R"({"key":"c","value":7})"}, {"a:0,1:7:-7:4", "c:0,1:7:-7:1"}},
                }, /* injectFailure */ true, ParentIndexUdafPrelude);
        }
    }

    Y_UNIT_TEST_TWIN_F(UdafResourceStateWithoutPersistence, WrapSavedState, TStreamingAggregationTestFixture) {
        CheckUdafUnpersistableSavedState(/* useStateTable */ false, /* checkpointsEnabled */ false, WrapSavedState);
    }

    Y_UNIT_TEST_TWIN_F(UdafMissingSerializerIsRejected, UseStateTable, TStreamingAggregationTestFixture) {
        CheckUdafUnpersistableSavedState(UseStateTable, /* checkpointsEnabled */ !UseStateTable, /* wrapSavedState */ false);
    }

    Y_UNIT_TEST_TWIN_F(UdafUnpersistableSerializerIsRejected, UseStateTable, TStreamingAggregationTestFixture) {
        CheckUdafUnpersistableSavedState(UseStateTable, /* checkpointsEnabled */ !UseStateTable, /* wrapSavedState */ true);
    }

    Y_UNIT_TEST_F(UdafNonComputableSerializerIsRejected, TStreamingAggregationTestFixture) {
        StartAggregation(false, "key String NOT NULL, value Int64 NOT NULL",
            R"(CAST(AGGREGATE_BY(value, $factory) AS String))", "key", "", {.Prelude = R"(
                $init = ($item) -> ($item);
                $update = ($state, $item) -> ($state + $item);
                $finish = ($state) -> ($state);
                $save = ($state) -> (TypeOf($state));
                $load = ($type) -> (0l);
                $factory = AggregationFactory("UDAF", $init, $update, NULL, $finish, $save, $load);
            )", .ExpectedError = "Expected computable data"});
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
            {.ExpectedError = "DISTINCT aggregation is not supported for mode: KqpStreamingAggregation"});
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

    Y_UNIT_TEST_F(StateTableInServerlessDatabase, TStreamingAggregationTestFixture) {
        DynamicNodeCount = 1;
        StoragePoolTypes = {"test"};
        QueryClientSettings.Database("/Root/serverless");

        auto& server = GetKikimrRunner()->GetTestServer();
        Tests::TTenants tenants(&server);
        Ydb::Cms::CreateDatabaseRequest sharedRequest;
        sharedRequest.set_path("/Root/shared");
        auto& storage = *sharedRequest.mutable_shared_resources()->add_storage_units();
        storage.set_unit_kind("test");
        storage.set_count(1);
        tenants.CreateTenant(std::move(sharedRequest));

        TPortManager portManager;
        server.EnableGRpc(portManager.GetPort(), tenants.List("/Root/shared").front(), "/Root/shared");

        Ydb::Cms::CreateDatabaseRequest serverlessRequest;
        serverlessRequest.set_path("/Root/serverless");
        serverlessRequest.mutable_serverless_resources()->set_shared_database_path("/Root/shared");
        tenants.CreateTenant(std::move(serverlessRequest));

        ExecQuery(R"(
            GRANT ALL ON `/Root/serverless` TO `)" BUILTIN_ACL_ROOT R"(`;
            CREATE TABLE aggregationState (
                key String NOT NULL,
                state String,
                PRIMARY KEY (key)
            );
        )");
        const TString inputTopic = TStringBuilder() << "aggregationInput_" << CreateGuidAsString();
        const TString outputTopic = TStringBuilder() << "aggregationOutput_" << CreateGuidAsString();
        CreateTopic(inputTopic);
        CreateTopic(outputTopic);
        CreatePqSource("source");

        // A relative state table path must resolve in the serverless query's database.
        ExecQuery(fmt::format(R"sql(
            CREATE STREAMING QUERY aggregation AS DO BEGIN
                PRAGMA ydb.DisableCheckpoints = "TRUE";
                PRAGMA ydb.MaxTasksPerStage = "1";
                PRAGMA ydb.EnableStreamingAggregation = "TRUE";
                PRAGMA ydb.StreamingAggregationStateTablePath = "aggregationState";
                INSERT INTO `source`.`{output}`
                SELECT Unwrap(key || ":" || CAST(SUM(value) AS String)) AS Data
                FROM `source`.`{input}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA (key String NOT NULL, value Int64 NOT NULL)
                )
                GROUP BY key;
            END DO;
        )sql", "input"_a = inputTopic, "output"_a = outputTopic));
        WaitStreamingQueryStatus("serverless/aggregation");
        ValidateStreamingQueryAst("serverless/aggregation", [](const TString& ast) {
            UNIT_ASSERT_STRING_CONTAINS(ast, "KqpStreamingAggregation");
        });

        // Alternating keys forces eviction and restoration through the state table.
        WriteTopicMessages(inputTopic, {
            R"({"key":"a","value":1})",
            R"({"key":"b","value":2})",
            R"({"key":"a","value":3})",
        });
        ReadTopicMessages(outputTopic, {"a:1", "a:4", "b:2"}, TInstant::Zero(), /* sort */ true);
        WriteTopicMessages(inputTopic, {
            R"({"key":"b","value":5})",
            R"({"key":"a","value":6})",
        });
        ReadTopicMessages(outputTopic, {"a:1", "a:4", "a:10", "b:2", "b:7"}, TInstant::Zero(), /* sort */ true);

        const auto result = ExecQuery("SELECT state FROM aggregationState;");
        UNIT_ASSERT_VALUES_EQUAL(result.size(), 1);
        UNIT_ASSERT_C(result[0].RowsCount() > 0, "Evicted aggregation states must be stored in the serverless database");
        TResultSetParser rows(result[0]);
        while (rows.TryNextRow()) {
            UNIT_ASSERT(rows.ColumnParser("state").GetOptionalString());
        }
        ExecQuery("DROP STREAMING QUERY aggregation;");
    }

    Y_UNIT_TEST_QUAD_F(StateTableUsesQueryUserToken, TableInput, DataQuery, TStreamingAggregationTestFixture) {
        CreateStateTable(true);
        const TString user = "aggregation_user@builtin";
        ExecQuery(fmt::format(R"(
            GRANT CONNECT ON `/Root` TO `{user}`;
            GRANT "ydb.generic.read", "ydb.generic.write" ON `/Root/aggregationState` TO `{user}`;
        )", "user"_a = user));
        if constexpr (TableInput) {
            ExecQuery(R"(
                CREATE TABLE aggregationInput (
                    id Uint64 NOT NULL, key String NOT NULL, value Int64 NOT NULL,
                    PRIMARY KEY (id)
                );
            )");
            ExecQuery(R"(
                UPSERT INTO aggregationInput (id, key, value) VALUES
                    (1, "a", 1), (2, "b", 2), (3, "a", 1);
            )");
            ExecQuery(fmt::format("GRANT \"ydb.generic.read\" ON `/Root/aggregationInput` TO `{}`;", user));
        }

        struct TRequests {
            std::atomic<ui32> Selects = 0;
            std::atomic<ui32> Upserts = 0;
            std::atomic<bool> SameIdentity = true;
        };
        const auto requests = std::make_shared<TRequests>();
        TScopedStateTableQueryProxy proxy(GetRuntime(), [requests, user](const auto& request, auto&) {
            if (request.GetQuery().Contains("UPSERT INTO")) {
                ++requests->Upserts;
            } else {
                ++requests->Selects;
            }
            if (request.GetUserToken()->GetUserSID() != user || request.GetDatabase() != "/Root") {
                requests->SameIdentity = false;
            }
            return false;
        });

        if constexpr (DataQuery) {
            NYdb::NTable::TTableClient client(*GetInternalDriver(), NYdb::NTable::TClientSettings().AuthToken(user));
            const auto sessionResult = client.CreateSession().ExtractValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto session = sessionResult.GetSession();
            const auto result = session.ExecuteDataQuery(MakeStateTableQuery(TableInput),
                NYdb::NTable::TTxControl::BeginTx().CommitTx(),
                NYdb::NTable::TExecDataQuerySettings().ClientTimeout(TEST_OPERATION_TIMEOUT)).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            TResultSetParser parser(result.GetResultSet(0));
            std::vector<std::string> actual;
            while (parser.TryNextRow()) {
                actual.push_back(parser.ColumnParser("Data").GetString());
            }
            Sort(actual);
            UNIT_ASSERT_VALUES_EQUAL(actual, (std::vector<std::string>{"a:1", "a:2", "b:2"}));
            CheckPersistedState();
        } else {
            NQuery::TQueryClient client(*GetInternalDriver(), NQuery::TClientSettings().AuthToken(user));
            const auto result = client.ExecuteQuery(MakeStateTableQuery(TableInput), NQuery::TTxControl::NoTx(),
                NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)
                    .ClientTimeout(TEST_OPERATION_TIMEOUT)).ExtractValueSync();
            CheckFiniteResult(result, {"a:1", "a:2", "b:2"}, TableInput);
        }
        UNIT_ASSERT(requests->Selects.load() > 0);
        UNIT_ASSERT(requests->Upserts.load() > 0);
        UNIT_ASSERT(requests->SameIdentity.load());
    }

    Y_UNIT_TEST_TWIN_F(StateTableEnforcesQueryUserPermissions, WriteFailure, TStreamingAggregationTestFixture) {
        CreateStateTable(true);
        const TString user = "aggregation_user@builtin";
        ExecQuery(fmt::format("GRANT CONNECT ON `/Root` TO `{}`;", user));
        if constexpr (WriteFailure) {
            ExecQuery(fmt::format("GRANT \"ydb.generic.read\" ON `/Root/aggregationState` TO `{}`;", user));
        }
        NQuery::TQueryClient client(*GetInternalDriver(), NQuery::TClientSettings().AuthToken(user));
        const auto result = client.ExecuteQuery(MakeStateTableQuery(), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().ClientTimeout(TEST_OPERATION_TIMEOUT)
                .RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0))).ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "State table access must use the query user's permissions");
        const TString operation = WriteFailure ? "UPSERT" : "SELECT";
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
            TStringBuilder() << "Streaming aggregation " << operation << " failed for table /Root/aggregationState");
    }

    Y_UNIT_TEST_TWIN_F(StateTableDoesNotRetryUnauthorized, WriteFailure, TStreamingAggregationTestFixture) {
        CreateStateTable(true);
        const auto attempts = std::make_shared<std::atomic<ui32>>(0);
        TScopedStateTableQueryProxy proxy(GetRuntime(), [attempts](const auto& request, auto& response) {
            if (request.GetQuery().Contains("UPSERT INTO") != WriteFailure) {
                return false;
            }
            ++*attempts;
            response.Record.SetYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
            response.Record.MutableResponse()->AddQueryIssues()->set_message("Injected state table permission failure");
            return true;
        });

        const auto result = GetQueryClient()->ExecuteQuery(MakeStateTableQuery(), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().ClientTimeout(TEST_OPERATION_TIMEOUT)
                .RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0))).ExtractValueSync();
        UNIT_ASSERT_C(!result.IsSuccess(), "Unauthorized state table queries must fail the aggregation");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Injected state table permission failure");
        UNIT_ASSERT_VALUES_EQUAL(attempts->load(), 1);
    }

    Y_UNIT_TEST_QUAD_F(StateTableRetriesTransientFailures, WriteFailure, ExhaustRetries, TStreamingAggregationTestFixture) {
        CreateStateTable(true);
        const auto attempts = std::make_shared<std::atomic<ui32>>(0);
        TScopedStateTableQueryProxy proxy(GetRuntime(), [attempts](const auto& request, auto& response) {
            if (request.GetQuery().Contains("UPSERT INTO") != WriteFailure) {
                return false;
            }
            constexpr std::array statuses = {Ydb::StatusIds::UNAVAILABLE, Ydb::StatusIds::ABORTED,
                Ydb::StatusIds::OVERLOADED, Ydb::StatusIds::UNDETERMINED};
            const auto attempt = attempts->fetch_add(1);
            if (!ExhaustRetries && attempt >= statuses.size()) {
                return false;
            }
            response.Record.SetYdbStatus(statuses[attempt % statuses.size()]);
            response.Record.MutableResponse()->AddQueryIssues()->set_message("Injected state table transient failure");
            return true;
        });

        const auto result = GetQueryClient()->ExecuteQuery(MakeStateTableQuery(), NQuery::TTxControl::NoTx(),
            NQuery::TExecuteQuerySettings().StatsMode(NQuery::EStatsMode::Full)
                .ClientTimeout(TEST_OPERATION_TIMEOUT)
                .RetrySettings(NRetry::TRetryOperationSettings().MaxRetries(0))).ExtractValueSync();
        if constexpr (ExhaustRetries) {
            UNIT_ASSERT_C(!result.IsSuccess(), "Exhausted retries must fail the aggregation");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Injected state table transient failure");
            UNIT_ASSERT(attempts->load() > 1);
        } else {
            CheckFiniteResult(result, {"a:1", "a:2", "b:2"}, false);
            UNIT_ASSERT(attempts->load() >= 5);
        }
    }

    Y_UNIT_TEST_TWIN_F(StateTableFailureIsReported, WriteFailure, TStreamingAggregationTestFixture) {
        CheckStateTableFailure(WriteFailure);
    }
} // Y_UNIT_TEST_SUITE(KqpStreamingAggregation)

} // namespace NKikimr::NKqp
