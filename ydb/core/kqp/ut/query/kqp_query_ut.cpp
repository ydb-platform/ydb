#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>

#include <library/cpp/json/json_reader.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

static void CheckStatusAfterTimeout(TSession& session, const TString& query, const TTxControl& txControl) {
    const TInstant start = TInstant::Now();
    while (true) {
        auto result = session.ExecuteDataQuery(query, txControl).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS || result.GetStatus() == EStatus::SESSION_BUSY, result.GetIssues().ToString());
        if (result.GetStatus() == EStatus::SUCCESS) {
            break;
        }

        UNIT_ASSERT_C(TInstant::Now() - start < TDuration::Seconds(30), "Unable to cancel processing after client lost");
        // Do not fire too much CPU
        Sleep(TDuration::MilliSeconds(10));
    }
}

Y_UNIT_TEST_SUITE(KqpQuery) {
    Y_UNIT_TEST(PreparedQueryInvalidate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto prepareResult = session.PrepareDataQuery(Q_(R"(
            SELECT * FROM `/Root/Test`
        )")).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(prepareResult.GetStatus(), EStatus::SUCCESS);

        auto query = prepareResult.GetQuery();

        auto result = query.Execute(TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        auto alterResult = session.AlterTable("/Root/Test",
            TAlterTableSettings()
                .AppendDropColumns("Comment")
        ).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(alterResult.GetStatus(), EStatus::SUCCESS);

        ui32 retries = 0;
        do {
            ++retries;
            if (retries > 5) {
                UNIT_FAIL("Too many retries.");
            }

            result = query.Execute(TTxControl::BeginTx().CommitTx()).GetValueSync();
        } while (result.GetStatus() == EStatus::UNAVAILABLE || result.GetStatus() == EStatus::ABORTED);

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
        UNIT_ASSERT_VALUES_EQUAL(counters.RecompileRequestGet()->Val(), 1);
    }

    Y_UNIT_TEST(QueryCache) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test`;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(query, txControl, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

        result = session.ExecuteDataQuery(query, txControl, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

        stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);
    }

    Y_UNIT_TEST(QueryCacheTtl) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetCompileQueryCacheTTLSec(2);

        TKikimrRunner kikimr(appConfig);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test`;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        TExecDataQuerySettings execSettings;
        execSettings.KeepInQueryCache(true);
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(query, txControl, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

        result = session.ExecuteDataQuery(query, txControl, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), true);

        execSettings.KeepInQueryCache(false);

        auto delay = TDuration::Seconds(appConfig.GetTableServiceConfig().GetCompileQueryCacheTTLSec());
        for (int i = 0; i < 10; ++i) {
            Sleep(delay);

            result = session.ExecuteDataQuery(query, txControl, execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);

            stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
            if (!stats.compilation().from_cache())
                break;
        }

        result = session.ExecuteDataQuery(query, txControl, execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
    }

    Y_UNIT_TEST(QueryCacheInvalidate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test`;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        auto result = session.ExecuteDataQuery(query, txControl, TExecDataQuerySettings().KeepInQueryCache(true))
            .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto dataQuery = result.GetQuery();
        UNIT_ASSERT(dataQuery);

        auto alterResult = session.AlterTable("/Root/Test", TAlterTableSettings().AppendDropColumns("Comment"))
            .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), EStatus::SUCCESS, alterResult.GetIssues().ToString());

        result = dataQuery->Execute(txControl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(query, txControl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
        UNIT_ASSERT_VALUES_EQUAL(counters.RecompileRequestGet()->Val(), 1);
    }

    Y_UNIT_TEST(QueryCachePermissionsLoss) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto setPermissions = [&](bool allow) {
            NYdb::NScheme::TPermissions permissions("user0@builtin",
                {"ydb.deprecated.describe_schema", "ydb.deprecated.select_row"}
            );

            auto permissionsSettings = allow
                ? NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                : NYdb::NScheme::TModifyPermissionsSettings().AddRevokePermissions(permissions);

            auto schemeClient = kikimr.GetSchemeClient();
            auto result = schemeClient.ModifyPermissions("/Root/Test", permissionsSettings).ExtractValueSync();
            AssertSuccessResult(result);
        };

        auto query = Q_(R"(
            SELECT * FROM `/Root/Test`
        )");

        setPermissions(true);
        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto db = NYdb::NTable::TTableClient(driver);

            auto session = db.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        setPermissions(false);
        {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto db = NYdb::NTable::TTableClient(driver);

            auto session = db.CreateSession().GetValueSync().GetSession();

            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            result.GetIssues().PrintTo(Cerr);
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(QueryTimeout) {
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(app));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/TwoShard`;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gSkipRepliesFailPoint.Enable(-1, -1, 1);

        auto result = session.ExecuteDataQuery(
            query,
            txControl,
            TExecDataQuerySettings()
                .OperationTimeout(TDuration::MilliSeconds(50))
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::TIMEOUT);

        NDataShard::gSkipRepliesFailPoint.Disable();

        const TInstant start = TInstant::Now();
        // Check session is ready or busy, but eventualy must be ready
        while (true) {
            result = session.ExecuteDataQuery(query, txControl).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS || result.GetStatus() == EStatus::SESSION_BUSY, result.GetIssues().ToString());
            if (result.GetStatus() == EStatus::SUCCESS) {
                break;
            }
            UNIT_ASSERT_C(TInstant::Now() - start < TDuration::Seconds(30), "Unable to cancel processing after timeout status");
            // Do not fire too much CPU
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    Y_UNIT_TEST(QueryTimeoutImmediate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key == 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gSkipRepliesFailPoint.Enable(-1, -1, 1);

        auto result = session.ExecuteDataQuery(
            query,
            txControl,
            TExecDataQuerySettings()
                .OperationTimeout(TDuration::MilliSeconds(50))
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::TIMEOUT);

        NDataShard::gSkipRepliesFailPoint.Disable();

        // Check session is ready or busy (both possible)
        result = session.ExecuteDataQuery(query, txControl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus() == EStatus::SUCCESS || result.GetStatus() == EStatus::SESSION_BUSY, true, result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(QueryClientTimeout, EnableImmediateEffects) {
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        app.MutableTableServiceConfig()->SetEnableKqpImmediateEffects(EnableImmediateEffects);
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(app);

        TKikimrRunner kikimr(serverSettings);

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);

        auto query = Q_(R"(
            SELECT * FROM `/Root/TwoShard`;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gSkipRepliesFailPoint.Enable(-1, -1, 2);

        auto result = session.ExecuteDataQuery(
            query,
            txControl,
            TExecDataQuerySettings()
                .UseClientTimeoutForOperation(false)
                .ClientTimeout(TDuration::Seconds(3))
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);

        NDataShard::gSkipRepliesFailPoint.Disable();

        CheckStatusAfterTimeout(session, query, txControl);
    }

    Y_UNIT_TEST(QueryClientTimeoutPrecompiled) {
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(app));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);

        auto query = Q_(R"(
            SELECT * FROM `/Root/TwoShard`;
        )");

        auto prepareResult = session.PrepareDataQuery(
            query
        ).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL(prepareResult.GetStatus(), EStatus::SUCCESS);

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gSkipRepliesFailPoint.Enable(-1, -1, 1);

        auto result = prepareResult.GetQuery().Execute(
            txControl,
            TExecDataQuerySettings()
                .UseClientTimeoutForOperation(false)
                .ClientTimeout(TDuration::Seconds(3))
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);

        NDataShard::gSkipRepliesFailPoint.Disable();

        CheckStatusAfterTimeout(session, query, txControl);
    }

    Y_UNIT_TEST(QueryCancel) {
        NKikimrConfig::TAppConfig app;
        app.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(app));
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/TwoShard`
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gCancelTxFailPoint.Enable(-1, -1, 1);

        auto result = session.ExecuteDataQuery(
            query,
            txControl
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CANCELLED);

        NDataShard::gCancelTxFailPoint.Disable();

        // Check session is ready
        result = session.ExecuteDataQuery(query, txControl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(QueryCancelImmediate) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableKqpDataQuerySourceRead(false);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig);
        TKikimrRunner kikimr{settings};
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/TwoShard` WHERE Key == 1;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gCancelTxFailPoint.Enable(-1, -1, 0);

        auto result = session.ExecuteDataQuery(
            query,
            txControl
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CANCELLED);

        NDataShard::gCancelTxFailPoint.Disable();

        // Check session is ready
        result = session.ExecuteDataQuery(query, txControl).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
    }

    Y_UNIT_TEST(QueryCancelWrite) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES
                (4, "Four"),
                (4000000000u, "BigZero");
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gCancelTxFailPoint.Enable(-1, -1, 0);

        auto result = session.ExecuteDataQuery(
            query,
            txControl
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NDataShard::gCancelTxFailPoint.Disable();
    }

    Y_UNIT_TEST(QueryCancelWriteImmediate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            UPSERT INTO `/Root/TwoShard` (Key, Value1) VALUES
                (4, "Four");
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gCancelTxFailPoint.Enable(-1, -1, 0);

        auto result = session.ExecuteDataQuery(
            query,
            txControl
        ).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        NDataShard::gCancelTxFailPoint.Disable();
    }

    Y_UNIT_TEST(QueryResultsTruncated) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Tmp` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        const ui32 RowsCount = 1000;

        auto replaceQuery = Q1_(R"(
            DECLARE $rows AS
                List<Struct<
                    Key: Uint64?,
                    Value: String?
                >>;

            REPLACE INTO `/Root/Tmp`
            SELECT * FROM AS_TABLE($rows);
        )");

        {
            auto paramsBuilder = session.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");

            rowsParam.BeginList();
            for (ui32 i = 0; i < RowsCount; ++i) {
                rowsParam.AddListItem()
                    .BeginStruct()
                    .AddMember("Key")
                        .OptionalUint64(i)
                    .AddMember("Value")
                        .OptionalString(ToString(i))
                    .EndStruct();
            }
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Tmp`;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(!result.GetResultSet(0).Truncated());
        UNIT_ASSERT(result.GetResultSet(0).RowsCount() == RowsCount);

        {
            auto paramsBuilder = session.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");

            rowsParam.BeginList();
            rowsParam.AddListItem()
                .BeginStruct()
                .AddMember("Key")
                    .OptionalUint64(RowsCount)
                .AddMember("Value")
                    .OptionalString(ToString(RowsCount))
                .EndStruct();
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Tmp`;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetResultSet(0).Truncated());
        UNIT_ASSERT(result.GetResultSet(0).RowsCount() == RowsCount);

         {
            auto paramsBuilder = session.GetParamsBuilder();
            auto& rowsParam = paramsBuilder.AddParam("$rows");

            rowsParam.BeginList();
            rowsParam.AddListItem()
                .BeginStruct()
                .AddMember("Key")
                    .OptionalUint64(RowsCount + 1)
                .AddMember("Value")
                    .OptionalString(ToString(RowsCount + 1))
                .EndStruct();
            rowsParam.EndList();
            rowsParam.Build();

            auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/Tmp`;
        )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetResultSet(0).Truncated());
        UNIT_ASSERT(result.GetResultSet(0).RowsCount() == RowsCount);

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).name(), "/Root/Tmp");
        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases(0).table_access(0).reads().rows(), 1001);
    }

    Y_UNIT_TEST(YqlSyntaxV0) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(R"(
            --!syntax_v0
            SELECT * FROM [/Root/KeyValue] WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());

        result = session.ExecuteDataQuery(R"(
            --!syntax_v1
            SELECT * FROM [/Root/KeyValue] WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT(!result.IsSuccess());

        result = session.ExecuteDataQuery(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key = 1;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT(result.IsSuccess());
    }

    Y_UNIT_TEST(YqlTableSample) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_KqpYqlSyntaxVersion");
        setting.SetValue("1");

        TKikimrRunner kikimr({setting});
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const TString query(Q_(R"(SELECT * FROM `/Root/Test` TABLESAMPLE SYSTEM(1.0);)"));
        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::UNSUPPORTED);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("ATOM evaluation is not supported in YDB queries.");
        }));
    }

    Y_UNIT_TEST(QueryExplain) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExplainDataQuery(R"(
            SELECT * FROM `/Root/Test` WHERE Group = 1 AND Name > "Name";
        )").GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        Cerr << "AST:" << Endl << result.GetAst() << Endl;
        Cerr << "Plan:" << Endl << result.GetPlan() << Endl;

        auto astRes = NYql::ParseAst(result.GetAst());
        UNIT_ASSERT(astRes.IsOk());
        NYql::TExprContext exprCtx;
        NYql::TExprNode::TPtr exprRoot;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

        UNIT_ASSERT(NJson::ValidateJson(result.GetPlan()));
    }

    Y_UNIT_TEST(RewriteIfPresentToMap) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        const TString query = Q_(R"(
                declare $key as Uint64;
                declare $text as String;
                declare $data as Int32;

                update `/Root/EightShard`
                set Text = $text, Data = $data
                where Length(Text) != 7 and Data = $data and Key = $key;

                upsert into `/Root/EightShard` (Key, Text, Data) values
                    ($key, $text || "_10", $data + 100);
            )");

        auto params = TParamsBuilder()
                .AddParam("$key").Uint64(1).Build()
                .AddParam("$text").String("foo").Build()
                .AddParam("$data").Int32(100).Build()
                .Build();

        auto prepareResult = session.PrepareDataQuery(query).ExtractValueSync();
        UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());

        auto result = prepareResult.GetQuery().Execute(TTxControl::BeginTx().CommitTx(), params).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Pure) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT 1 + 1;
        )");

        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx().CommitTx()
        ).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[2]])",
            FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Now) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT YQL::Now(), YQL::Now();
        )");

        const ui32 QueriesCount = 5;

        TSet<ui64> timestamps;
        for (ui32 i = 0; i < QueriesCount; ++i) {
            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());

            auto value = parser.ColumnParser(0).GetUint64();
            UNIT_ASSERT(value == parser.ColumnParser(1).GetUint64());
            timestamps.insert(value);
        }

        UNIT_ASSERT_VALUES_EQUAL(timestamps.size(), QueriesCount);

        timestamps.clear();
        for (ui32 i = 0; i < QueriesCount; ++i) {
            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx(),
                TExecDataQuerySettings().KeepInQueryCache(true)
            ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());

            auto value = parser.ColumnParser(0).GetUint64();
            UNIT_ASSERT(value == parser.ColumnParser(1).GetUint64());
            timestamps.insert(value);
        }

        UNIT_ASSERT_VALUES_EQUAL(timestamps.size(), QueriesCount);
    }

    Y_UNIT_TEST(RandomNumber) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT YQL::RandomNumber(), YQL::RandomNumber(), RandomNumber(1);
        )");

        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx().CommitTx()
        ).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());

        auto value = parser.ColumnParser(0).GetUint64();
        UNIT_ASSERT(value == parser.ColumnParser(1).GetUint64());
        UNIT_ASSERT(value != parser.ColumnParser(2).GetUint64());
    }

    Y_UNIT_TEST(RandomUuid) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT CAST(YQL::RandomUuid() AS Utf8), CAST(YQL::RandomUuid() AS Utf8);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();

        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());

        auto value = parser.ColumnParser(0).GetUtf8();
        UNIT_ASSERT(value == parser.ColumnParser(1).GetUtf8());
    }

    Y_UNIT_TEST(CurrentUtcTimestamp) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT YQL::CurrentUtcTimestamp(), YQL::CurrentUtcTimestamp();
        )");

        TInstant timestamp;

        {
            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx(),
                TExecDataQuerySettings().KeepInQueryCache(true)
            ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());

            timestamp = parser.ColumnParser(0).GetTimestamp();
            UNIT_ASSERT(timestamp == parser.ColumnParser(1).GetTimestamp());
        }

        {
            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx(),
                TExecDataQuerySettings().KeepInQueryCache(true)
            ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

            TResultSetParser parser(result.GetResultSet(0));
            UNIT_ASSERT(parser.TryNextRow());

            auto value = parser.ColumnParser(0).GetTimestamp();
            UNIT_ASSERT(value == parser.ColumnParser(1).GetTimestamp());
            UNIT_ASSERT(timestamp != value);
        }
    }

    Y_UNIT_TEST(UnsafeTimestampCastV0) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TsTest` (
                Key Timestamp,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        const TString query = R"(
            --!syntax_v0
            DECLARE $key AS Uint64;
            DECLARE $value AS String;

            UPSERT INTO `/Root/TsTest` (Key, Value) VALUES
                ($key, $value);
        )";

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").String("foo").Build()
            .Build();

        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx().CommitTx(),
            params
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(UnsafeTimestampCastV1) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/TsTest` (
                Key Timestamp,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync().IsSuccess());

        const TString query = Q1_(R"(
            DECLARE $key AS Uint64;
            DECLARE $value AS String;

            UPSERT INTO `/Root/TsTest` (Key, Value) VALUES
                ($key, $value);
        )");

        auto params = TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$value").String("foo").Build()
            .Build();

        auto result = session.ExecuteDataQuery(
            query,
            TTxControl::BeginTx().CommitTx(),
            params
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(QueryStats) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        NYdb::NTable::TExecDataQuerySettings execSettings;
        execSettings.CollectQueryStats(ECollectQueryStatsMode::Basic);

        auto result = session.ExecuteDataQuery(Q_(R"(
            UPSERT INTO `/Root/EightShard`
            SELECT
                Key,
                Value1 AS Text,
                Value2 AS Data
            FROM `/Root/TwoShard`
            WHERE Key < 10;
        )"), TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto& stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());

        TString statsStr;
        NProtoBuf::TextFormat::PrintToString(stats, &statsStr);
        Cerr << statsStr << Endl;

        uint64_t totalCpuTimeUs = 0;

        UNIT_ASSERT(stats.process_cpu_time_us() > 0);
        UNIT_ASSERT(stats.total_cpu_time_us() > 0);
        UNIT_ASSERT(stats.total_duration_us() > 0);
        totalCpuTimeUs += stats.process_cpu_time_us();

        UNIT_ASSERT(stats.has_compilation());
        auto& compile = stats.compilation();
        UNIT_ASSERT_VALUES_EQUAL(compile.from_cache(), false);
        UNIT_ASSERT(compile.duration_us() > 0);
        UNIT_ASSERT(compile.cpu_time_us() > 0);
        totalCpuTimeUs += compile.cpu_time_us();

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 3);
        totalCpuTimeUs += stats.query_phases(0).cpu_time_us();

        auto& phase0 = stats.query_phases(1);
        UNIT_ASSERT(phase0.duration_us() > 0);
        UNIT_ASSERT(phase0.cpu_time_us() > 0);
        totalCpuTimeUs += phase0.cpu_time_us();

        UNIT_ASSERT_VALUES_EQUAL(phase0.table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(phase0.table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(phase0.table_access(0).reads().rows(), 3);
        UNIT_ASSERT(phase0.table_access(0).reads().bytes() > 0);
        UNIT_ASSERT(!phase0.table_access(0).has_updates());
        UNIT_ASSERT(!phase0.table_access(0).has_deletes());

        auto& phase1 = stats.query_phases(2);
        UNIT_ASSERT(phase1.duration_us() > 0);
        UNIT_ASSERT(phase1.cpu_time_us() > 0);
        totalCpuTimeUs += phase1.cpu_time_us();
        UNIT_ASSERT_VALUES_EQUAL(phase1.table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(phase1.table_access(0).name(), "/Root/EightShard");
        UNIT_ASSERT(!phase1.table_access(0).has_reads());
        UNIT_ASSERT_VALUES_EQUAL(phase1.table_access(0).updates().rows(), 3);
        UNIT_ASSERT(phase1.table_access(0).updates().bytes() > 0);
        UNIT_ASSERT(!phase1.table_access(0).has_deletes());

        UNIT_ASSERT_VALUES_EQUAL(stats.total_cpu_time_us(), totalCpuTimeUs);
    }

    Y_UNIT_TEST(RowsLimit) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_ResultRowsLimit");
        setting.SetValue("5");

        TKikimrRunner kikimr({setting});
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/EightShard` WHERE Text = "Value2";
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT(result.GetResultSet(0).Truncated());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
    }

    Y_UNIT_TEST(RowsLimitServiceOverride) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_ResultRowsLimit");
        setting.SetValue("5");

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->MutableQueryLimits()->SetResultRowsLimit(6);

        TKikimrRunner kikimr(appConfig, {setting});
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/EightShard` WHERE Text = "Value2";
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT(result.GetResultSet(0).Truncated());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 6);
    }

    Y_UNIT_TEST(NoEvaluate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteDataQuery(Q_(R"(
                DEFINE ACTION $hello() AS
                    SELECT "Hello!";
                END DEFINE;

                EVALUATE IF true
                    DO $hello()
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("EVALUATE IF is not supported in YDB queries.");
            }));
        }

        {
            auto result = session.ExecuteDataQuery(Q1_(R"(
                EVALUATE FOR $i IN AsList(1, 2, 3) DO BEGIN
                    SELECT $i;
                END DO;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("EVALUATE is not supported in YDB queries.");
            }));
        }

        {
            auto params = db.GetParamsBuilder()
                    .AddParam("$table").String("StringValue").Build()
                    .AddParam("$login").String("LoginString").Build()
                    .AddParam("$email").String("Email@String").Build()
                    .AddParam("$id").Uint64(1).Build()
                    .Build();

            auto result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                DECLARE  $id  AS Uint64;
                DECLARE  $login AS String;
                DECLARE  $email AS String;

                SELECT $id, $login, $email;
            )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

            CompareYson(
                    R"([[1u;"LoginString";"Email@String"]])",
                    FormatResultSetYson(result.GetResultSet(0))
            );

            result = session.ExecuteDataQuery(R"(
                --!syntax_v1
                DECLARE  $table AS String;
                DECLARE  $id  AS Uint64;
                DECLARE  $login AS String;
                DECLARE  $email AS String;

                INSERT INTO $table ( id, login, email ) VALUES ($id, $login, $email );
            )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("ATOM evaluation is not supported in YDB queries.");
            }));
        }
    }

    Y_UNIT_TEST(UdfTerminate) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue`
            WHERE TestUdfs::TestFilterTerminate(Cast(Key as Int64) ?? 0, 10)
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        if (result.GetStatus() == EStatus::GENERIC_ERROR) {
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR, [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("Execution failed");
            }));
        } else {
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::PRECONDITION_FAILED);
        }
    }

    Y_UNIT_TEST(UdfMemoryLimit) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // NOTE: 10MB is greater than default memory allocation for datashard tx.
        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT * FROM `/Root/KeyValue`
            WHERE Value < TestUdfs::RandString(10000000);
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        result.GetIssues().PrintTo(Cerr);
    }

    Y_UNIT_TEST(DdlInDataQuery) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            CREATE TABLE `/Root/Tmp` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("can't be performed in data query");
        }));

        result = session.ExecuteDataQuery(Q_(R"(
            DROP TABLE `/Root/KeyValue`;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("not supported");
        }));

        result = session.ExecuteDataQuery(Q_(R"(
            ALTER TABLE `/Root/KeyValue` DROP COLUMN Value;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR, [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("not supported");
        }));
    }

    Y_UNIT_TEST(DyNumberCompare) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        // Compare DyNumber
        auto result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("13.1" AS DyNumber);
            $dn2 = CAST("10.2" AS DyNumber);

            SELECT
                $dn1 = $dn2,
                $dn1 != $dn2,
                $dn1 > $dn2,
                $dn1 <= $dn2;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        CompareYson(R"([[[%false];[%true];[%true];[%false]]])", FormatResultSetYson(result.GetResultSet(0)));

        // Compare to float
        result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("13.1" AS DyNumber);

            SELECT
                $dn1 = 13.1,
                $dn1 != 13.1,
                $dn1 > 10.2,
                $dn1 <= 10.2,
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        // Compare to int
        result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("15" AS DyNumber);

            SELECT
                $dn1 = 15,
                $dn1 != 15,
                $dn1 > 10,
                $dn1 <= 10,
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);

        // Compare to decimal
        result = session.ExecuteDataQuery(Q1_(R"(
            $dn1 = CAST("13.1" AS DyNumber);
            $dc1 = CAST("13.1" AS Decimal(22,9));

            SELECT
                $dn1 = $dc1,
                $dn1 != $dc1,
                $dn1 > $dc1,
                $dn1 <= $dc1,
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
    }

    Y_UNIT_TEST(SelectNull) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q_(R"(
            SELECT Null
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        AssertSuccessResult(result);

        auto rs = TResultSetParser(result.GetResultSet(0));
        UNIT_ASSERT(rs.TryNextRow());

        auto& cp = rs.ColumnParser(0);

        UNIT_ASSERT_VALUES_EQUAL(TTypeParser::ETypeKind::Null, cp.GetKind());
    }

    Y_UNIT_TEST(MultipleCurrentUtcTimestamp) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q1_(R"(
            SELECT * FROM `/Root/Logs` WHERE Ts > Cast(CurrentUtcTimestamp() as Int64)
            UNION ALL
            SELECT * FROM `/Root/Logs` WHERE Ts < Cast(CurrentUtcTimestamp() as Int64);
        )");

        auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(SelectWhereInSubquery) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT * FROM `/Root/KeyValue` WHERE Key IN (SELECT Key FROM `/Root/EightShard`);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(SelectCountAsteriskFromVar) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            $count = SELECT COUNT(*) FROM `/Root/KeyValue`;
            SELECT * FROM $count;
            SELECT $count;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(UpdateWhereInSubquery) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            UPDATE `/Root/KeyValue` SET Value = 'NewValue' WHERE Key IN (SELECT Key FROM `/Root/EightShard`);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DeleteWhereInSubquery) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            DELETE FROM `/Root/KeyValue` WHERE Key IN (SELECT Key FROM `/Root/EightShard`);
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(TryToUpdateNonExistentColumn) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            UPDATE `/Root/KeyValue` SET NonExistentColumn = 'NewValue' WHERE Key = 1;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(QuerySpecialTypes) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto result = session.ExecuteDataQuery(Q1_(R"(
            SELECT null;
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q1_(R"(
            SELECT [];
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = session.ExecuteDataQuery(Q1_(R"(
            SELECT {};
        )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

} // namespace NKqp
} // namespace NKikimr
