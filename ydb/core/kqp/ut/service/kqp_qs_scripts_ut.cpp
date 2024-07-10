#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <ydb/core/kqp/counters/kqp_counters.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpQueryServiceScripts) {
    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionOperation(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver, i32 tries = -1) {
        NYdb::NOperation::TOperationClient client(ydbDriver);
        while(1) {
            auto op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(operationId).GetValueSync();
            if (op.Ready() || tries == 0) {
                return op;
            }
            UNIT_ASSERT_C(op.Status().IsSuccess(), op.Status().GetStatus() << ":" << op.Status().GetIssues().ToString());
            if (tries > 0) {
                --tries;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    void CheckScriptResults(TScriptExecutionOperation scriptExecutionOperation, TScriptExecutionOperation readyOp, TQueryClient& db) {
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Execute);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Metadata().ScriptContent.Text, "SELECT 42");
        UNIT_ASSERT_VALUES_EQUAL(readyOp.Metadata().ResultSetsMeta.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readyOp.Metadata().ResultSetsMeta.front().Columns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(readyOp.Metadata().ResultSetsMeta.front().Columns[0].Name, "column0");
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ResultSetsMeta.front().Columns[0].Type.GetProto().type_id(), Ydb::Type::PrimitiveTypeId::Type_PrimitiveTypeId_INT32);

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
        UNIT_ASSERT(resultSet.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);
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
        CheckScriptResults(scriptExecutionOperation, readyOp, db);
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
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);
        }
        {
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 1).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 1);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 101);
        }
    }

    Y_UNIT_TEST(ExecuteScriptWithWorkloadManager) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        auto kikimr = TKikimrRunner(TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true)
            .SetEnableScriptExecutionOperations(true));
        auto db = kikimr.GetQueryClient();

        TExecuteScriptSettings settings;

        {  // Existing pool
            settings.PoolId("default");

            auto scripOp = db.ExecuteScript("SELECT 42", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scripOp.Status().GetStatus(), EStatus::SUCCESS, scripOp.Status().GetIssues().ToString());
            CheckScriptResults(scripOp, WaitScriptExecutionOperation(scripOp.Id(), kikimr.GetDriver()), db);
        }

        {  // Not existing pool (check workload manager enabled)
            settings.PoolId("another_pool_id");

            auto scripOp = db.ExecuteScript("SELECT 42", settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scripOp.Status().GetStatus(), EStatus::SUCCESS, scripOp.Status().GetIssues().ToString());

            auto readyOp = WaitScriptExecutionOperation(scripOp.Id(), kikimr.GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Failed, readyOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT_EQUAL_C(readyOp.Status().GetStatus(), EStatus::NOT_FOUND, readyOp.Status().GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Resource pool another_pool_id not found");
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Failed to resolve pool id another_pool");
            UNIT_ASSERT_STRING_CONTAINS(readyOp.Status().GetIssues().ToString(), "Query failed during adding/waiting in workload pool");
        }
    }

    void ValidatePlan(const TMaybe<TString>& plan) {
        UNIT_ASSERT(plan);
        UNIT_ASSERT(plan != "{}");
        NJson::TJsonValue jsonPlan;
        NJson::ReadJsonTree(plan.GetRef(), &jsonPlan, true);
        UNIT_ASSERT(ValidatePlanNodeIds(jsonPlan));
    }

    Y_UNIT_TEST(ExplainScript) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings().ExecMode(EExecMode::Explain);
        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();

        UNIT_ASSERT_EQUAL(scriptExecutionOperation.Metadata().ExecMode, EExecMode::Explain);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecStatus, EExecStatus::Completed);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecMode, EExecMode::Explain);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT_STRING_CONTAINS(readyOp.Metadata().ScriptContent.Text, "SELECT 42");

        ValidatePlan(readyOp.Metadata().ExecStats.GetPlan());
        UNIT_ASSERT(readyOp.Metadata().ExecStats.GetAst());
    }

    Y_UNIT_TEST(ParseScript) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteScriptSettings().ExecMode(EExecMode::Parse);
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

        auto settings = TExecuteScriptSettings().ExecMode(EExecMode::Validate);
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

        auto settings = TExecuteScriptSettings().ExecMode(EExecMode::Unspecified);
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
            .Syntax(ESyntax::Pg);

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

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        CompareYson(R"([
            ["1";"one"];
            ["2";"two"];
            ["3";"three"]
        ])", FormatResultSetYson(results.GetResultSet()));
    }

    Y_UNIT_TEST(ExecuteScriptWithParameters) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto params = TParamsBuilder()
            .AddParam("$value").Int64(17).Build()
            .Build();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            DECLARE $value As Int64;
            SELECT $value;
        )", params).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        CompareYson(R"([
            [17]
        ])", FormatResultSetYson(results.GetResultSet()));
    }

    void ExecuteScriptWithStatsMode(EStatsMode statsMode) {
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

        if (statsMode == EStatsMode::None) {
            return;
        }

        // TODO: more checks?
        UNIT_ASSERT_C(TProtoAccessor().GetProto(readyOp.Metadata().ExecStats).query_phases_size() == 1, readyOp.Metadata().ExecStats.ToString());
        UNIT_ASSERT_C(readyOp.Metadata().ExecStats.GetTotalDuration() > TDuration::Zero(), readyOp.Metadata().ExecStats.ToString());

        if (statsMode == EStatsMode::Basic) {
            return;
        }

        ValidatePlan(readyOp.Metadata().ExecStats.GetPlan());
        UNIT_ASSERT(readyOp.Metadata().ExecStats.GetAst());
    }

    Y_UNIT_TEST(ExecuteScriptStatsBasic) {
        ExecuteScriptWithStatsMode(EStatsMode::Basic);
    }

    Y_UNIT_TEST(ExecuteScriptStatsFull) {
        ExecuteScriptWithStatsMode(EStatsMode::Full);
    }

    Y_UNIT_TEST(ExecuteScriptStatsProfile) {
        ExecuteScriptWithStatsMode(EStatsMode::Profile);
    }

    Y_UNIT_TEST(ExecuteScriptStatsNone) {
        ExecuteScriptWithStatsMode(EStatsMode::None);
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
            UNIT_ASSERT_C(forgetStatus.GetStatus() == NYdb::EStatus::SUCCESS || forgetStatus.GetStatus() == NYdb::EStatus::NOT_FOUND, forgetStatus.GetIssues().ToString());
            if (forgetStatus.GetStatus() == NYdb::EStatus::SUCCESS) {
                ++successCount;
            }
        }

        UNIT_ASSERT(successCount >= 1);

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
        UNIT_ASSERT_C(op.Ready(), op.Status().GetIssues().ToString());
        UNIT_ASSERT_C(op.Metadata().ExecStatus == EExecStatus::Completed || op.Metadata().ExecStatus == EExecStatus::Canceled, op.Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL(op.Metadata().ExecutionId, scriptExecutionOperation.Metadata().ExecutionId);
        UNIT_ASSERT(op.Status().GetStatus() == NYdb::EStatus::SUCCESS || op.Status().GetStatus() == NYdb::EStatus::CANCELLED);

        // Check cancel for completed query
        auto cancelStatus = opClient.Cancel(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(cancelStatus.GetStatus() == NYdb::EStatus::PRECONDITION_FAILED, cancelStatus.GetIssues().ToString());
    }

    NYdb::NQuery::TScriptExecutionOperation WaitScriptExecutionFail(const NYdb::TOperation::TOperationId& operationId, const NYdb::TDriver& ydbDriver) {
        NYdb::NOperation::TOperationClient client(ydbDriver);
        NThreading::TFuture<NYdb::NQuery::TScriptExecutionOperation> op;
        do {
            if (!op.Initialized()) {
                Sleep(TDuration::MilliSeconds(10));
            }
            op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(operationId);
        } while (op.GetValueSync().Status().IsSuccess());
        return op.GetValueSync();
    }

    void ExpectExecStatus(EExecStatus status, const TScriptExecutionOperation op, const NYdb::TDriver& ydbDriver) {
        auto readyOp = WaitScriptExecutionOperation(op.Id(), ydbDriver);
        UNIT_ASSERT_C(readyOp.Ready(), readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT(readyOp.Metadata().ExecStatus == status);
        UNIT_ASSERT_EQUAL(readyOp.Metadata().ExecutionId, op.Metadata().ExecutionId);
    }

    void ExecuteScriptWithSettings(const TExecuteScriptSettings& settings, EExecStatus status, TString query = "SELECT 1;") {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto op = db.ExecuteScript(query, settings).ExtractValueSync();
        ExpectExecStatus(status, op, kikimr.GetDriver());
    }

    Y_UNIT_TEST(ExecuteScriptWithCancelAfter) {
        TString query = R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )";
        auto settings = TExecuteScriptSettings().CancelAfter(TDuration::MilliSeconds(1));
        ExecuteScriptWithSettings(settings, EExecStatus::Canceled, query);

        settings = TExecuteScriptSettings().CancelAfter(TDuration::Seconds(10));
        ExecuteScriptWithSettings(settings, EExecStatus::Completed);
    }

    Y_UNIT_TEST(ExecuteScriptWithTimeout) {
        TString query = R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )";
        auto settings = TExecuteScriptSettings().OperationTimeout(TDuration::MilliSeconds(1));
        ExecuteScriptWithSettings(settings, EExecStatus::Failed, query);

        settings = TExecuteScriptSettings().OperationTimeout(TDuration::Seconds(100));
        ExecuteScriptWithSettings(settings, EExecStatus::Completed);
    }

    Y_UNIT_TEST(ExecuteScriptWithCancelAfterAndTimeout) {
        TString query = R"(
            SELECT * FROM EightShard WHERE Text = "Value2" AND Data = 1 ORDER BY Key;
            SELECT * FROM TwoShard WHERE Key < 10 ORDER BY Key;
        )";
        auto settings = TExecuteScriptSettings().CancelAfterWithTimeout(TDuration::MilliSeconds(1), TDuration::Seconds(100));
        ExecuteScriptWithSettings(settings, EExecStatus::Canceled, query);

        settings = TExecuteScriptSettings().CancelAfterWithTimeout(TDuration::Seconds(100), TDuration::MilliSeconds(1));
        ExecuteScriptWithSettings(settings, EExecStatus::Failed, query);
    }

    void CheckScriptOperationExpires(const TExecuteScriptSettings &settings) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());


        auto readyOp = WaitScriptExecutionFail(scriptExecutionOperation.Id(), kikimr.GetDriver());
        UNIT_ASSERT_C(readyOp.Status().GetStatus() == EStatus::NOT_FOUND, readyOp.Status().GetStatus() << ":" << readyOp.Status().GetIssues().ToString());

        NOperation::TOperationClient client(kikimr.GetDriver());

        auto list = client.List<NYdb::NQuery::TScriptExecutionOperation>(100).ExtractValueSync();
        UNIT_ASSERT_C(list.IsSuccess(), list.GetIssues().ToString());
        UNIT_ASSERT(list.GetList().size() == 0);

        auto status = client.Forget(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(status.GetStatus() == NYdb::EStatus::NOT_FOUND, status.GetIssues().ToString());

        status = client.Cancel(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(status.GetStatus() == NYdb::EStatus::NOT_FOUND, status.GetIssues().ToString());

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.GetStatus() == NYdb::EStatus::NOT_FOUND, results.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteScriptWithForgetAfter) {
        CheckScriptOperationExpires(TExecuteScriptSettings().ForgetAfter(TDuration::MilliSeconds(50)));
    }

    void CheckScriptResultsExpire(const TExecuteScriptSettings& settings) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = db.ExecuteScript(R"(
            SELECT 42
        )", settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr.GetDriver());
        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_C(resultSet.TryNextRow(), results.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), 42);

        while (results.GetStatus() != NYdb::EStatus::NOT_FOUND) {
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());
            results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            Sleep(TDuration::MilliSeconds(10));
        }

        UNIT_ASSERT_C(results.GetStatus() == NYdb::EStatus::NOT_FOUND, results.GetIssues().ToString());
        NOperation::TOperationClient client(kikimr.GetDriver());
        auto op = client.Get<TScriptExecutionOperation>(scriptExecutionOperation.Id()).ExtractValueSync();
        UNIT_ASSERT_C(op.Status().IsSuccess(), op.Status().GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteScriptWithResultsTtl) {
        CheckScriptResultsExpire(TExecuteScriptSettings().ResultsTtl(TDuration::MilliSeconds(2000)));
    }

    Y_UNIT_TEST(ExecuteScriptWithResultsTtlAndForgetAfter) {

        auto settings = TExecuteScriptSettings().ResultsTtl(TDuration::MilliSeconds(2000)).ForgetAfter(TDuration::Seconds(30));
        CheckScriptResultsExpire(settings);

        settings = TExecuteScriptSettings().ResultsTtl(TDuration::Minutes(1)).ForgetAfter(TDuration::MilliSeconds(500));
        CheckScriptOperationExpires(settings);
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

        TExecuteScriptSettings settings;
        settings.StatsMode(EStatsMode::Full);

        auto scriptExecutionOperation = db.ExecuteScript(sql, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());

        auto readyOperation = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), ydbDriver);
        UNIT_ASSERT_EQUAL(readyOperation.Metadata().ExecStatus, EExecStatus::Completed);

        return readyOperation;
    }

    Y_UNIT_TEST(InvalidFetchToken) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = CreateScriptExecutionOperation(1, db, kikimr.GetDriver());

        auto settings = TFetchScriptResultsSettings();
        settings.FetchToken("?");

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0, settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(results.GetStatus(), EStatus::BAD_REQUEST);
    }

    Y_UNIT_TEST(EmptyNextFetchToken) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        constexpr size_t NUMBER_OF_ROWS = 10;
        auto scriptExecutionOperation = CreateScriptExecutionOperation(NUMBER_OF_ROWS, db, kikimr.GetDriver());

        auto settings = TFetchScriptResultsSettings();
        settings.RowsLimit(NUMBER_OF_ROWS);

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0, settings).ExtractValueSync();
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
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0, settings).ExtractValueSync();
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

    Y_UNIT_TEST(TestTruncatedByRows) {
        constexpr size_t ROWS_LIMIT = 2000;

        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->set_scriptresultrowslimit(ROWS_LIMIT);

        auto kikimr = DefaultKikimrRunner({}, appCfg);
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperationTruncated = CreateScriptExecutionOperation(ROWS_LIMIT + 1, db, kikimr.GetDriver());
        TFetchScriptResultsResult resultsTruncated = db.FetchScriptResults(scriptExecutionOperationTruncated.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(resultsTruncated.IsSuccess(), resultsTruncated.GetIssues().ToString());
        UNIT_ASSERT(resultsTruncated.GetResultSet().Truncated());

        auto scriptExecutionOperationNotTruncated = CreateScriptExecutionOperation(ROWS_LIMIT, db, kikimr.GetDriver());
        TFetchScriptResultsResult resultsNotTruncated = db.FetchScriptResults(scriptExecutionOperationNotTruncated.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(resultsNotTruncated.IsSuccess(), resultsNotTruncated.GetIssues().ToString());
        UNIT_ASSERT(!resultsNotTruncated.GetResultSet().Truncated());
    }

    Y_UNIT_TEST(TestTruncatedBySize) {
        constexpr size_t NUMER_ROWS = 500;

        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->set_scriptresultsizelimit(NUMER_ROWS / 2);

        auto kikimr = DefaultKikimrRunner({}, appCfg);
        auto db = kikimr.GetQueryClient();

        auto scriptExecutionOperation = CreateScriptExecutionOperation(NUMER_ROWS, db, kikimr.GetDriver());

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        UNIT_ASSERT(results.GetResultSet().Truncated());
    }

    Y_UNIT_TEST(TestFetchMoreThanLimit) {
        constexpr size_t NUMER_BATCHES = 5;
        constexpr size_t ROWS_LIMIT = 20;

        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableTableServiceConfig()->MutableQueryLimits()->set_resultrowslimit(ROWS_LIMIT);

        auto kikimr = DefaultKikimrRunner({}, appCfg);
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = CreateScriptExecutionOperation(NUMER_BATCHES * ROWS_LIMIT + 1, db, kikimr.GetDriver());

        auto settings = TFetchScriptResultsSettings();
        settings.RowsLimit(NUMER_BATCHES * ROWS_LIMIT);

        TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0, settings).ExtractValueSync();
        UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

        TResultSetParser resultSet(results.ExtractResultSet());
        UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), NUMER_BATCHES * ROWS_LIMIT);
        for (size_t i = 0; i < NUMER_BATCHES * ROWS_LIMIT; ++i) {
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetInt32(), i);
        }
    }

    Y_UNIT_TEST(Tcl) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto op = db.ExecuteScript(R"(
            SELECT 1;
            COMMIT;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(op.Status().GetStatus(), EStatus::SUCCESS, op.Status().GetIssues().ToString());

        auto readyOp = WaitScriptExecutionOperation(op.Id(), kikimr.GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Status().GetStatus(), EStatus::GENERIC_ERROR, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT(HasIssue(readyOp.Status().GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));

        op = db.ExecuteScript(R"(
            SELECT 1;
            ROLLBACK;
        )").ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(op.Status().GetStatus(), EStatus::SUCCESS, op.Status().GetIssues().ToString());

        readyOp = WaitScriptExecutionOperation(op.Id(), kikimr.GetDriver());
        UNIT_ASSERT_VALUES_EQUAL_C(readyOp.Status().GetStatus(), EStatus::GENERIC_ERROR, readyOp.Status().GetIssues().ToString());
        UNIT_ASSERT(HasIssue(readyOp.Status().GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
    }

    Y_UNIT_TEST(TestAstWithCompression) {
        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableQueryServiceConfig()->SetQueryArtifactsCompressionMinSize(0);
        appCfg.MutableQueryServiceConfig()->SetQueryArtifactsCompressionMethod("zstd_6");

        auto kikimr = DefaultKikimrRunner({}, appCfg);
        auto db = kikimr.GetQueryClient();
        auto scriptExecutionOperation = CreateScriptExecutionOperation(1, db, kikimr.GetDriver());

        UNIT_ASSERT_STRING_CONTAINS(scriptExecutionOperation.Metadata().ExecStats.GetAst().GetRef(), "\"idx\" (DataType 'Int32)");
    }
}

} // namespace NKqp
} // namespace NKikimr
