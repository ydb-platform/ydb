#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpQueryService) {
    Y_UNIT_TEST(StreamExecuteQueryPure) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto params = TParamsBuilder()
            .AddParam("$value").Int64(17).Build()
            .Build();

        auto it = db.StreamExecuteQuery(R"(
            SELECT $value;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 count = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                count += resultSet.RowsCount();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(count, 1);
    }

    Y_UNIT_TEST(ExecuteQueryPure) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto params = TParamsBuilder()
            .AddParam("$value").Int64(17).Build()
            .Build();

        auto result = db.ExecuteQuery(R"(
            SELECT $value;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[17]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(StreamExecuteQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto it = db.StreamExecuteQuery(R"(
            SELECT Key, Value2 FROM TwoShard WHERE Value2 > 0;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 count = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto resultSet = streamPart.ExtractResultSet();
                count += resultSet.RowsCount();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(count, 2);
    }

    Y_UNIT_TEST(ExecuteQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            SELECT Key, Value2 FROM TwoShard WHERE Value2 > 0 ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            [[3u];[1]];
            [[4000000003u];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ExecuteQueryPg) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings()
            .Syntax(ESyntax::Pg);

        auto result = db.ExecuteQuery(R"(
            SELECT * FROM (VALUES
                (1::int8, 'one'),
                (2::int8, 'two'),
                (3::int8, 'three')
            ) AS t;
        )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([
            ["1";"one"];
            ["2";"two"];
            ["3";"three"]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    //KIKIMR-18492
    Y_UNIT_TEST(ExecuteQueryPgTableSelect) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        auto settings = TExecuteQuerySettings()
            .Syntax(ESyntax::Pg);
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteSchemeQuery(R"(
                --!syntax_pg
                CREATE TABLE test (id int8,PRIMARY KEY (id)))"
            ).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            auto db = kikimr.GetQueryClient();
            auto result = db.ExecuteQuery(
                "SELECT * FROM test",
                TTxControl::BeginTx().CommitTx(), settings
            ).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(ExecuteQueryScalar) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            SELECT COUNT(*) FROM EightShard;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[24u]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ExecuteQueryMultiResult) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings()
            .StatsMode(EStatsMode::Basic);

        auto result = db.ExecuteQuery(R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);

        CompareYson(R"([
            [[1];[202u];["Value2"]];
            [[1];[502u];["Value2"]];
            [[1];[802u];["Value2"]]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([
            [[1u];["One"];[-1]];
            [[2u];["Two"];[0]];
            [[3u];["Three"];[1]]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST(ExecuteQueryMultiScalar) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            SELECT COUNT(*) FROM EightShard;
            SELECT COUNT(*) FROM TwoShard;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[24u]])", FormatResultSetYson(result.GetResultSet(0)));
        CompareYson(R"([[6u]])", FormatResultSetYson(result.GetResultSet(1)));
    }

    Y_UNIT_TEST(StreamExecuteQueryMultiResult) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto it = db.StreamExecuteQuery(R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT 2;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

        ui64 lastResultSetIndex = 0;
        ui64 count = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                if (streamPart.GetResultSetIndex() != lastResultSetIndex) {
                    UNIT_ASSERT_VALUES_EQUAL(streamPart.GetResultSetIndex(), lastResultSetIndex + 1);
                    ++lastResultSetIndex;
                }

                auto resultSet = streamPart.ExtractResultSet();
                count += resultSet.RowsCount();
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(count, 7);
    }

    Y_UNIT_TEST(ExecuteQueryWrite) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            UPSERT INTO TwoShard (Key, Value2) VALUES(0, 101);

            SELECT Value2 FROM TwoShard WHERE Key = 0;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[101]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ExplainQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto params = TParamsBuilder()
            .AddParam("$value").Int64(17).Build()
            .Build();

        auto settings = TExecuteQuerySettings()
            .ExecMode(EExecMode::Explain);

        auto result = db.ExecuteQuery(R"(
            SELECT $value;
        )", TTxControl::NoTx(), params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetResultSets().empty());

        UNIT_ASSERT(result.GetStats().Defined());
        UNIT_ASSERT(result.GetStats()->GetPlan().Defined());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(plan));
    }

    Y_UNIT_TEST(ExecStats) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto params = TParamsBuilder()
            .AddParam("$value").Uint32(10).Build()
            .Build();

        auto settings = TExecuteQuerySettings()
            .StatsMode(EStatsMode::Basic);

        auto result = db.ExecuteQuery(R"(
            SELECT * FROM TwoShard WHERE Key < $value;
        )", TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 3);
        UNIT_ASSERT(result.GetStats().Defined());
        UNIT_ASSERT(!result.GetStats()->GetPlan().Defined());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
    }

    Y_UNIT_TEST(ExecStatsPlan) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto params = TParamsBuilder()
            .AddParam("$value").Uint32(10).Build()
            .Build();

        auto settings = TExecuteQuerySettings()
            .StatsMode(EStatsMode::Full);

        auto result = db.ExecuteQuery(R"(
            SELECT * FROM TwoShard WHERE Key < $value;
        )", TTxControl::BeginTx().CommitTx(), params, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 3);
        UNIT_ASSERT(result.GetStats().Defined());
        UNIT_ASSERT(result.GetStats()->GetPlan().Defined());

        NJson::TJsonValue plan;
        NJson::ReadJsonTree(*result.GetStats()->GetPlan(), &plan, true);

        auto stages = FindPlanStages(plan);

        i64 totalTasks = 0;
        for (const auto& stage : stages) {
            totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("TotalTasks").GetIntegerSafe();
        }
        UNIT_ASSERT_VALUES_EQUAL(totalTasks, 2);
    }

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver, i32 tries = -1) {
        NYdb::NOperation::TOperationClient client(ydbDriver);
        NThreading::TFuture<NYdb::NQuery::TScriptExecutionOperation> op;
        do {
            if (!op.Initialized()) {
                Sleep(TDuration::MilliSeconds(10));
            }
            if (tries > 0) {
                --tries;
            }
            op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(operationId);
            UNIT_ASSERT_C(op.GetValueSync().Status().IsSuccess(), op.GetValueSync().Status().GetStatus() << ":" << op.GetValueSync().Status().GetIssues().ToString());
        } while (!op.GetValueSync().Ready() && tries != 0);
        return op.GetValueSync();
    }

    Y_UNIT_TEST(ExecuteScript) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Execute);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Metadata().ScriptContent.Text, "SELECT 42");

        auto checkFetch = [&](const auto& executionOrOperation) {
            TFetchScriptResultsResult results = db.FetchScriptResults(executionOrOperation, 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);
        };

        checkFetch(scriptExecutionOperation.Metadata().ExecutionId);
        checkFetch(scriptExecutionOperation);
    }

    Y_UNIT_TEST(ExecuteMultiScript) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42; SELECT 101;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Execute);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Metadata().ScriptContent.Text, "SELECT 42; SELECT 101;");

        {
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId, 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);
        }
        {
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId, 1).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 101);
        }
    }

    void ValidatePlan(const TString& plan) {
        UNIT_ASSERT(!plan.empty());
        UNIT_ASSERT(plan != "{}");
        NJson::TJsonValue jsonPlan;
        NJson::ReadJsonTree(plan, &jsonPlan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(jsonPlan));
    }

    Y_UNIT_TEST(ExplainScript) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings().ExecMode(Ydb::Query::EXEC_MODE_EXPLAIN);
        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();

        UNIT_ASSERT_EQUAL(scriptExecutionOperation.Metadata().ExecMode, EExecMode::Explain);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Explain);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Metadata().ScriptContent.Text, "SELECT 42");

        ValidatePlan(readyOp.Metadata().ExecStats.query_plan());
    }

    Y_UNIT_TEST(ParseScript) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings().ExecMode(Ydb::Query::EXEC_MODE_PARSE);
        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();

        // TODO: change when parse mode will be supported
        UNIT_ASSERT_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::BAD_REQUEST, scriptExecutionOperation.Status().GetStatus());
        UNIT_ASSERT(scriptExecutionOperation.Status().GetIssues().Size() == 1);
        UNIT_ASSERT_EQUAL_C(scriptExecutionOperation.Status().GetIssues().back().GetMessage(), "Query mode is not supported yet", scriptExecutionOperation.Status().GetIssues().ToString());

    }

    Y_UNIT_TEST(ValidateScript) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings().ExecMode(Ydb::Query::EXEC_MODE_VALIDATE);
        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();

        // TODO: change when validate mode will be supported
        UNIT_ASSERT_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::BAD_REQUEST, scriptExecutionOperation.Status().GetStatus());
        UNIT_ASSERT(scriptExecutionOperation.Status().GetIssues().Size() == 1);
        UNIT_ASSERT_EQUAL_C(scriptExecutionOperation.Status().GetIssues().back().GetMessage(), "Query mode is not supported yet", scriptExecutionOperation.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteScriptWithUnspecifiedMode) {
                auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings().ExecMode(Ydb::Query::EXEC_MODE_UNSPECIFIED);
        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();

        UNIT_ASSERT_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::BAD_REQUEST, scriptExecutionOperation.Status().GetStatus());
        UNIT_ASSERT(scriptExecutionOperation.Status().GetIssues().Size() == 1);
        UNIT_ASSERT_EQUAL_C(scriptExecutionOperation.Status().GetIssues().back().GetMessage(), "Query mode is not specified", scriptExecutionOperation.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteScriptPg) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings()
            .Syntax(Ydb::Query::SYNTAX_PG);

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT * FROM (VALUES
                (1::int8, 'one'),
                (2::int8, 'two'),
                (3::int8, 'three')
            ) AS t;
        )", settings).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Execute);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ScriptContent.Syntax, ESyntax::Pg);

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation, 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        CompareYson(R"([
            ["1";"one"];
            ["2";"two"];
            ["3";"three"]
        ])", FormatResultSetYson(results.GetResultSet()));
    }


    void ExecuteScriptWithStatsMode (Ydb::Query::StatsMode statsMode) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings()
            .StatsMode(statsMode);

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        auto readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Execute);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Metadata().ScriptContent.Text, "SELECT 42");

        if (statsMode == Ydb::Query::STATS_MODE_NONE) {
            return;
        }

        // TODO: more checks?
        UNIT_ASSERT_C(readyOp.Metadata().ExecStats.query_phases_size() == 1, readyOp.Metadata().ExecStats);
        UNIT_ASSERT_C(readyOp.Metadata().ExecStats.total_duration_us() > 0, readyOp.Metadata().ExecStats);

        if (statsMode == Ydb::Query::STATS_MODE_BASIC) {
            return;
        }

        ValidatePlan(readyOp.Metadata().ExecStats.query_plan());
    }
    Y_UNIT_TEST(ExecuteScriptStatsBasic) {
        ExecuteScriptWithStatsMode(Ydb::Query::STATS_MODE_BASIC);
    }

    Y_UNIT_TEST(ExecuteScriptStatsFull) {
        ExecuteScriptWithStatsMode(Ydb::Query::STATS_MODE_FULL);
    }

    Y_UNIT_TEST(ExecuteScriptStatsProfile) {
        ExecuteScriptWithStatsMode(Ydb::Query::STATS_MODE_PROFILE);
    }

    Y_UNIT_TEST(ExecuteScriptStatsNone) {
        ExecuteScriptWithStatsMode(Ydb::Query::STATS_MODE_NONE);
    }

    Y_UNIT_TEST(ListScriptExecutions) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        NYdb::NOperation::TOperationClient client(kikimr.GetDriver());
        auto list = client.List<NYdb::NQuery::TScriptExecutionOperation>(42).ExtractValueSync();
        UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
        UNIT_ASSERT(list.GetList().empty());

        constexpr ui64 ScriptExecutionsCount = 100;
        std::set<TString> ops;
        for (ui64 i = 0; i < ScriptExecutionsCount; ++i) {
            auto scriptExecutionOperation = db.ExecuteScript(R"(
                SELECT 42
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);
            ops.emplace(scriptExecutionOperation.Metadata().ExecutionId);
        }
        UNIT_ASSERT_VALUES_EQUAL(ops.size(), ScriptExecutionsCount);

        std::set<TString> listedOps;
        ui64 listed = 0;
        TString nextPageToken;
        while (true) {
            auto list = client.List<NYdb::NQuery::TScriptExecutionOperation>(42, nextPageToken).ExtractValueSync();
            UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
            UNIT_ASSERT(list.GetList().size() <= 42);
            for (const auto& op : list.GetList()) {
                ++listed;
                UNIT_ASSERT_C(listedOps.emplace(op.Metadata().ExecutionId).second, op.Metadata().ExecutionId);
            }
            nextPageToken = list.NextPageToken();
            if (!nextPageToken) {
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(listed, ScriptExecutionsCount);
        UNIT_ASSERT_EQUAL(ops, listedOps);
    }

    Y_UNIT_TEST(ForgetScriptExecution) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        NYdb::NOperation::TOperationClient client(kikimr.GetDriver());
        auto list = client.List<NYdb::NQuery::TScriptExecutionOperation>(42).ExtractValueSync();
        UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
        UNIT_ASSERT(list.GetList().empty());

        constexpr ui64 ScriptExecutionsCount = 100;
        std::set<TString> ops;
        for (ui64 i = 0; i < ScriptExecutionsCount; ++i) {
            auto scriptExecutionOperation = db.ExecuteScript(R"(
                SELECT 42
            )").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);
            ops.emplace(scriptExecutionOperation.Metadata().ExecutionId);
        }
        UNIT_ASSERT_VALUES_EQUAL(ops.size(), ScriptExecutionsCount);

        std::set<TString> listedOps;
        ui64 listed = 0;
        std::set<TString> rememberedOps = ops;
        bool forgetNextOperation = true;
        list = client.List<NYdb::NQuery::TScriptExecutionOperation>(ScriptExecutionsCount).ExtractValueSync();
        UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
        UNIT_ASSERT(list.GetList().size() == ScriptExecutionsCount);
        for (const auto& op : list.GetList()) {
            ++listed;
            UNIT_ASSERT_C(listedOps.emplace(op.Metadata().ExecutionId).second, op.Metadata().ExecutionId);
            if (forgetNextOperation) {
                auto status = client.Forget(op.Id()).ExtractValueSync();
                UNIT_ASSERT_C(status.GetStatus() == NYdb::EStatus::SUCCESS || status.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED ||
                          status.GetStatus() == NYdb::EStatus::ABORTED, status.GetIssues().ToString());
                if (status.GetStatus() == NYdb::EStatus::SUCCESS) {
                    rememberedOps.erase(op.Metadata().ExecutionId);
                }
            }
            forgetNextOperation = !forgetNextOperation;
        }
        UNIT_ASSERT_VALUES_EQUAL(listed, ScriptExecutionsCount);
        UNIT_ASSERT_EQUAL(ops, listedOps);

        std::set<TString> listedOpsAfterForget;
        list = client.List<NYdb::NQuery::TScriptExecutionOperation>(ScriptExecutionsCount, {}).ExtractValueSync();
        UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
        for (const auto& op : list.GetList()) {
            UNIT_ASSERT_C(listedOpsAfterForget.emplace(op.Metadata().ExecutionId).second, op.Metadata().ExecutionId);
        }

        UNIT_ASSERT_EQUAL(rememberedOps, listedOpsAfterForget);
    }

    Y_UNIT_TEST(ForgetScriptExecutionOnLongQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        // TODO: really long query
        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NOperation::TOperationClient opClient(kikimr.GetDriver());
        TStatus forgetStatus = {EStatus::STATUS_UNDEFINED, NYql::TIssues()};
        while (forgetStatus.GetStatus() != NYdb::EStatus::SUCCESS) {
            forgetStatus = opClient.Forget(scriptExecutionOperation.Id()).ExtractValueSync();
            UNIT_ASSERT_C(forgetStatus.GetStatus() == NYdb::EStatus::SUCCESS || forgetStatus.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED ||
                          forgetStatus.GetStatus() == NYdb::EStatus::ABORTED, forgetStatus.GetIssues().ToString());
        }
        UNIT_ASSERT_C(forgetStatus.GetStatus() == NYdb::EStatus::SUCCESS, forgetStatus.GetIssues().ToString());
        forgetStatus = opClient.Forget(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(forgetStatus.GetStatus() == NYdb::EStatus::NOT_FOUND, forgetStatus.GetIssues().ToString());

    }

    Y_UNIT_TEST(ForgetScriptExecutionRace) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NOperation::TOperationClient opClient(kikimr.GetDriver());
        WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());

        std::vector<NYdb::TAsyncStatus> forgetFutures(3);
        for (auto& f : forgetFutures) {
            f = opClient.Forget(scriptExecutionOperation.Id());
        }

        i32 successCount = 0;
        for (auto& f : forgetFutures) {
            auto forgetStatus = f.ExtractValueSync();
            UNIT_ASSERT_C(forgetStatus.GetStatus() == NYdb::EStatus::SUCCESS || forgetStatus.GetStatus() == NYdb::EStatus::NOT_FOUND ||
                          forgetStatus.GetStatus() == NYdb::EStatus::ABORTED, forgetStatus.GetIssues().ToString());
            if (forgetStatus.GetStatus() == NYdb::EStatus::SUCCESS) {
                ++successCount;
            }
        }

        UNIT_ASSERT(successCount == 1);

        auto op = opClient.Get<NYdb::NQuery::TScriptExecutionOperation>(scriptExecutionOperation.Id()).ExtractValueSync();
        auto forgetStatus = opClient.Forget(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(op.Status().GetStatus() == NYdb::EStatus::NOT_FOUND, op.Status().GetStatus());
        UNIT_ASSERT_C(forgetStatus.GetStatus() == NYdb::EStatus::NOT_FOUND, forgetStatus.GetIssues().ToString());
    }

    Y_UNIT_TEST(CancelScriptExecution) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NOperation::TOperationClient opClient(kikimr.GetDriver());
        std::vector<NYdb::TAsyncStatus> cancelFutures(3);
        // Check races also
        for (auto& f : cancelFutures) {
            f = opClient.Cancel(scriptExecutionOperation.Id());
        }

        for (auto& f : cancelFutures) {
            auto cancelStatus = f.ExtractValueSync();
            UNIT_ASSERT_C(cancelStatus.GetStatus() == NYdb::EStatus::SUCCESS || cancelStatus.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED, cancelStatus.GetIssues().ToString());
        }

        auto op = opClient.Get<NYdb::NQuery::TScriptExecutionOperation>(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(op.Status().IsSuccess(), op.Status().GetIssues().ToString());
        UNIT_ASSERT_C(op.Ready(), op.Status().GetIssues().ToString());
        UNIT_ASSERT(op.Metadata().ExecStatus == EExecStatus::Completed || op.Metadata().ExecStatus == EExecStatus::Canceled);
        UNIT_ASSERT_EQUAL(op.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT(op.Status().GetStatus() == NYdb::EStatus::SUCCESS || op.Status().GetStatus() == NYdb::EStatus::CANCELLED);

        // Check cancel for completed query
        auto cancelStatus = opClient.Cancel(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(cancelStatus.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED, cancelStatus.GetIssues().ToString());
    }

    Y_UNIT_TEST(CloseConnection) {
        auto kikimr = DefaultKikimrRunner();

        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        int maxTimeoutMs = 100;

        for (int i = 1; i < maxTimeoutMs; i++) {
            auto it = kikimr.GetQueryClient().StreamExecuteQuery(R"(
                SELECT * FROM `/Root/EightShard` WHERE Text = "Value1" ORDER BY Key;
            )", TTxControl::BeginTx().CommitTx(), TExecuteQuerySettings().ClientTimeout(TDuration::MilliSeconds(i))).GetValueSync();

            if (it.IsSuccess()) {
                try {
                    for (;;) {
                        auto streamPart = it.ReadNext().GetValueSync();
                        if (!streamPart.IsSuccess()) {
                            break;
                        }
                    }
                } catch (const TStreamReadError& ex) {
                    UNIT_ASSERT_VALUES_EQUAL(ex.Status, NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
                } catch (const std::exception& ex) {
                    UNIT_ASSERT_C(false, "unknown exception during the test");
                }
            } else {
                UNIT_ASSERT_VALUES_EQUAL(it.GetStatus(), NYdb::EStatus::CLIENT_DEADLINE_EXCEEDED);
            }
        }

        WaitForZeroSessions(counters);

        for (const auto& service: kikimr.GetTestServer().GetGRpcServer().GetServices()) {
            UNIT_ASSERT_VALUES_EQUAL(service->RequestsInProgress(), 0);
            UNIT_ASSERT(!service->IsUnsafeToShutdown());
        }
    }

    Y_UNIT_TEST(ExecuteScriptFailsWithForgetAfter) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", NYdb::NQuery::TExecuteScriptSettings().ForgetAfter(TDuration::Days(1))).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::UNSUPPORTED, scriptExecutionOperation.Status().GetIssues().ToString());
    }

    TScriptExecutionOperation CreateScriptExecutionOperation(size_t numberOfRows, NYdb::NQuery::TQueryClient& db, const NYdb::TDriver& ydbDriver) {
        TString sql = "SELECT * FROM AS_TABLE([";
        for (size_t i = 0; i < numberOfRows; ++i) {
            sql.append(TStringBuilder() << "<|idx:" << i << "|>");
            if (i + 1 < numberOfRows) {
                sql.append(',');
            } else {
                sql.append("]);");
            }
        }

        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), ydbDriver);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);

        return scriptExecutionOperation;
    }

    Y_UNIT_TEST(InvalidFetchToken) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = CreateScriptExecutionOperation(1, db, kikimr.GetDriver());

        auto settings = TFetchScriptResultsSettings();
        settings.FetchToken("?");

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation, 0, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(results.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(EmptyNextFetchToken) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        constexpr size_t NUMBER_OF_ROWS = 10;
        auto scriptExecutionOperation = CreateScriptExecutionOperation(NUMBER_OF_ROWS, db, kikimr.GetDriver());

        auto settings = TFetchScriptResultsSettings();
        settings.RowsLimit(NUMBER_OF_ROWS);

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation, 0, settings).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
        UNIT_ASSERT(results.GetNextFetchToken().Empty());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), NUMBER_OF_ROWS);
    }

    Y_UNIT_TEST(TestPaging) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        constexpr size_t NUMBER_OF_ROWS = 10;
        auto scriptExecutionOperation = CreateScriptExecutionOperation(NUMBER_OF_ROWS, db, kikimr.GetDriver());

        constexpr size_t ROWS_LIMIT = 2;
        auto settings = TFetchScriptResultsSettings();
        settings.RowsLimit(ROWS_LIMIT);

        constexpr size_t NUMBER_OF_TESTS = 3;
        for (size_t i = 0; i < NUMBER_OF_TESTS && (i + 1) * ROWS_LIMIT <= NUMBER_OF_ROWS; ++i) {
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation, 0, settings).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), ROWS_LIMIT);
            for (size_t j = 0; j < ROWS_LIMIT; ++j) {
                UNIT_ASSERT(resultSet.TryNextRow());
                UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), i * ROWS_LIMIT + j);
            }

            settings.FetchToken(results.GetNextFetchToken());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
