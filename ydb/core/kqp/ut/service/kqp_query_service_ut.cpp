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

        auto result = db.ExecuteQuery(R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

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

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver) {
        NYdb::NOperation::TOperationClient client(ydbDriver);
        NThreading::TFuture<NYdb::NQuery::TScriptExecutionOperation> op;
        do {
            if (!op.Initialized()) {
                Sleep(TDuration::MilliSeconds(10));
            }
            op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(operationId);
            UNIT_ASSERT_C(op.GetValueSync().Status().IsSuccess(), op.GetValueSync().Status().GetStatus() << ":" << op.GetValueSync().Status().GetIssues().ToString());
        } while (!op.GetValueSync().Ready());
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
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Execute);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Metadata().ScriptContent.Text, "SELECT 42");

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Metadata().ExecutionId).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);
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
        UNIT_ASSERT(op.Ready());
        UNIT_ASSERT(op.Metadata().ExecStatus == EExecStatus::Completed || op.Metadata().ExecStatus == EExecStatus::Canceled);
        UNIT_ASSERT_EQUAL(op.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT(op.Status().GetStatus() == NYdb::EStatus::SUCCESS || op.Status().GetStatus() == NYdb::EStatus::CANCELLED);

        // Check cancel for completed query
        auto cancelStatus = opClient.Cancel(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(cancelStatus.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED, cancelStatus.GetIssues().ToString());
    }
}

} // namespace NKqp
} // namespace NKikimr
