#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/testlib/common_helper.h>
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

        const auto setPermissions = [&](const std::set<TString>& permissions) {
            const TVector<TString> allPermissions = {
                "describe_schema",
                "select_row",
                "update_row",
                "erase_row",
            };

            TVector<TString> grantPermissions;
            TVector<TString> revokePermissions;

            for (const auto& permission : allPermissions) {
                if (permissions.contains(permission)) {
                    grantPermissions.push_back("ydb.deprecated." + permission);
                } else {
                    revokePermissions.push_back("ydb.deprecated." + permission);
                }
            }

            auto permissionsSettings =
                NYdb::NScheme::TModifyPermissionsSettings()
                .AddGrantPermissions(NYdb::NScheme::TPermissions("user0@builtin", grantPermissions))
                .AddRevokePermissions(NYdb::NScheme::TPermissions("user0@builtin", revokePermissions));

            auto schemeClient = kikimr.GetSchemeClient();
            auto result = schemeClient.ModifyPermissions("/Root/Test", permissionsSettings).ExtractValueSync();
            AssertSuccessResult(result);
        };

        const auto check = [&](
                const auto& query,
                const std::optional<NYdb::TParams>& params,
                const NYdb::EStatus expectedStatus,
                const std::optional<TString> expectedIssue = std::nullopt) {
            auto driverConfig = TDriverConfig()
                .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("user0@builtin");
            auto driver = TDriver(driverConfig);
            auto db = NYdb::NTable::TTableClient(driver);

            auto session = db.CreateSession().GetValueSync().GetSession();
            const auto result = params
                ? session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), *params).ExtractValueSync()
                : session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToString());
            if (expectedIssue) {
                UNIT_ASSERT_C(result.GetIssues().ToString().Contains(*expectedIssue), result.GetIssues().ToString());
            }
        };

        const auto select_query = Q_(R"(
            SELECT * FROM `/Root/Test`
        )");

        const auto params = db.GetParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                .AddListItem()
                    .BeginStruct()
                        .AddMember("In_Group").OptionalUint32(42)
                        .AddMember("In_Name").OptionalString("test_name")
                        .AddMember("In_Amount").OptionalUint64(4242)
                    .EndStruct()
                .EndList()
                .Build()
            .Build();

        const auto insert_query = Q_(R"(
            DECLARE $rows AS List<Struct<
                In_Group : Uint32?,
                In_Name : String?,
                In_Amount : Uint64?
            >>;

            INSERT INTO `/Root/Test`
            SELECT
                In_Group AS Group,
                In_Name AS Name,
                In_Amount AS Amount
            FROM AS_TABLE($rows)
        )");

        const auto delete_query = Q_(R"(
            DECLARE $rows AS List<Struct<
                In_Group : Uint32?,
                In_Name : String?,
                In_Amount : Uint64?
            >>;

            DELETE FROM `/Root/Test`
            ON SELECT
                In_Group AS Group,
                In_Name AS Name,
                In_Amount AS Amount
            FROM AS_TABLE($rows)
        )");

        setPermissions({"describe_schema", "select_row", "update_row", "erase_row"});
        check(select_query, std::nullopt, EStatus::SUCCESS);
        check(insert_query, params, EStatus::SUCCESS);
        check(delete_query, params, EStatus::SUCCESS);

        setPermissions({"describe_schema", "select_row"});
        check(select_query, std::nullopt, EStatus::SUCCESS);
        check(insert_query, params, EStatus::ABORTED, "AccessDenied");
        check(delete_query, params, EStatus::ABORTED, "AccessDenied");

        setPermissions({"describe_schema"});
        check(select_query, std::nullopt, EStatus::ABORTED, "AccessDenied");
        check(insert_query, params, EStatus::ABORTED, "AccessDenied");
        check(delete_query, params, EStatus::ABORTED, "AccessDenied");

        setPermissions({});
        check(select_query, std::nullopt, EStatus::SCHEME_ERROR);
        check(insert_query, params, EStatus::SCHEME_ERROR);
        check(delete_query, params, EStatus::SCHEME_ERROR);

        setPermissions({"select_row", "update_row", "erase_row"});
        check(select_query, std::nullopt, EStatus::SCHEME_ERROR);
        check(insert_query, params, EStatus::SCHEME_ERROR);
        check(delete_query, params, EStatus::SCHEME_ERROR);
    }

    Y_UNIT_TEST(QueryTimeout) {
        NKikimrConfig::TAppConfig app;
        TKikimrRunner kikimr(TKikimrSettings().SetAppConfig(app));

        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        auto query = Q_(R"(
            SELECT * FROM `/Root/TwoShard`;
        )");

        auto txControl = TTxControl::BeginTx().CommitTx();

        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER {
            NDataShard::gSkipReadIteratorResultFailPoint.Disable();
        };

        auto result = session.ExecuteDataQuery(
            query,
            txControl,
            TExecDataQuerySettings()
                .OperationTimeout(TDuration::MilliSeconds(50))
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::TIMEOUT);

        NDataShard::gSkipReadIteratorResultFailPoint.Disable();

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

    Y_UNIT_TEST(QueryClientTimeout) {
        NKikimrConfig::TAppConfig app;
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

        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER {
            NDataShard::gSkipReadIteratorResultFailPoint.Disable();
        };

        auto result = session.ExecuteDataQuery(
            query,
            txControl,
            TExecDataQuerySettings()
                .UseClientTimeoutForOperation(false)
                .ClientTimeout(TDuration::Seconds(3))
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);

        NDataShard::gSkipReadIteratorResultFailPoint.Disable();

        CheckStatusAfterTimeout(session, query, txControl);
    }

    Y_UNIT_TEST(QueryClientTimeoutPrecompiled) {
        NKikimrConfig::TAppConfig app;
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

        NDataShard::gSkipReadIteratorResultFailPoint.Enable(-1);
        Y_DEFER {
            NDataShard::gSkipReadIteratorResultFailPoint.Disable();
        };

        auto result = prepareResult.GetQuery().Execute(
            txControl,
            TExecDataQuerySettings()
                .UseClientTimeoutForOperation(false)
                .ClientTimeout(TDuration::Seconds(3))
        ).ExtractValueSync();

        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::CLIENT_DEADLINE_EXCEEDED);

        NDataShard::gSkipReadIteratorResultFailPoint.Disable();

        CheckStatusAfterTimeout(session, query, txControl);
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

        {
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

        {
            auto query = Q_(R"(
                SELECT Int8("-1");
            )");

            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[-1]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto query = Q_(R"(
                SELECT Int8("-1") + Int8("-1");
            )");

            auto result = session.ExecuteDataQuery(
                query,
                TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson(R"([[-2]])",
                FormatResultSetYson(result.GetResultSet(0)));
        }

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

        UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 2);

        auto& phase0 = stats.query_phases(0);
        UNIT_ASSERT(phase0.duration_us() > 0);
        UNIT_ASSERT(phase0.cpu_time_us() > 0);
        totalCpuTimeUs += phase0.cpu_time_us();

        UNIT_ASSERT_VALUES_EQUAL(phase0.table_access().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(phase0.table_access(0).name(), "/Root/TwoShard");
        UNIT_ASSERT_VALUES_EQUAL(phase0.table_access(0).reads().rows(), 3);
        UNIT_ASSERT(phase0.table_access(0).reads().bytes() > 0);
        UNIT_ASSERT(!phase0.table_access(0).has_updates());
        UNIT_ASSERT(!phase0.table_access(0).has_deletes());

        auto& phase1 = stats.query_phases(1);
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

    Y_UNIT_TEST(GenericQueryNoRowsLimit) {
        auto setting = NKikimrKqp::TKqpSetting();
        setting.SetName("_ResultRowsLimit");
        setting.SetValue("5");

        TKikimrRunner kikimr({setting});
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(Q_(R"(
            SELECT * FROM `/Root/EightShard` WHERE Text = "Value2";
        )"), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        UNIT_ASSERT(!result.GetResultSet(0).Truncated());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 8);
    }

    Y_UNIT_TEST(GenericQueryNoRowsLimitLotsOfRows) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetQueryClient();

        CreateLargeTable(kikimr, 1000, 10, 10, 5000, 10);

        auto result = db.ExecuteQuery(Q_(R"(
            SELECT * FROM `/Root/LargeTable`;
        )"), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(!result.GetResultSet(0).Truncated());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 10000);

        result = db.ExecuteQuery(Q_(R"(
            SELECT * FROM `/Root/LargeTable` LIMIT 5000;
        )"), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(!result.GetResultSet(0).Truncated());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5000);
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

        const auto issueChecker = [](const NYql::TIssue& issue) {
            return issue.GetMessage().Contains("can't be performed in data query");
        };

        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION, issueChecker));

        result = session.ExecuteDataQuery(Q_(R"(
            DROP TABLE `/Root/KeyValue`;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION, issueChecker));

        result = session.ExecuteDataQuery(Q_(R"(
            ALTER TABLE `/Root/KeyValue` DROP COLUMN Value;
        )"), TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        result.GetIssues().PrintTo(Cerr);
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::GENERIC_ERROR);
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION, issueChecker));
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

    Y_UNIT_TEST(QueryFromSqs) {
        TKikimrRunner kikimr;
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        TString user = "uuuuuuuuuuuuuuuuuuuuuuuu";
        TString queue = "qqqqqqqqqqqqqqqqqqqqqqqq";

        {
            const auto status = session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/SQS/Test` (
                    Account String,
                    QueueName String,
                    Version Uint64,
                    PRIMARY KEY(Account, QueueName)
                )
            )").GetValueSync();
            UNIT_ASSERT(status.IsSuccess());

            auto replaceQuery = Q1_(R"(
                DECLARE $rows AS
                    List<Struct<
                        Account: String,
                        QueueName: String,
                        Version: Uint64
                    >>;

                REPLACE INTO `/Root/SQS/Test`
                SELECT * FROM AS_TABLE($rows);
            )");


            auto paramsBuilder = session.GetParamsBuilder();
            paramsBuilder
                .AddParam("$rows")
                .BeginList()
                .AddListItem()
                .BeginStruct()
                    .AddMember("Version")
                        .Uint64(0)
                    .AddMember("Account")
                        .String(user)
                    .AddMember("QueueName")
                        .String(queue)
                .EndStruct()
                .EndList()
                .Build();

            auto result = session.ExecuteDataQuery(replaceQuery, TTxControl::BeginTx().CommitTx(),
                paramsBuilder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto query = Q_(R"(
                SELECT * FROM `/Root/SQS/Test`;
            )");
            NYdb::NTable::TExecDataQuerySettings execSettings;
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT(!result.GetResultSet(0).Truncated());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }

        {
            auto query = "SELECT * FROM `/Root/SQS/Test` WHERE Account='" + user + "'";;
            NYdb::NTable::TExecDataQuerySettings execSettings;
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT(!result.GetResultSet(0).Truncated());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }

        {
            auto query = "SELECT * FROM `/Root/SQS/Test` WHERE QueueName='" + queue + "'";;
            NYdb::NTable::TExecDataQuerySettings execSettings;
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT(!result.GetResultSet(0).Truncated());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }

        {
            auto query = "SELECT * FROM `/Root/SQS/Test` WHERE Account='" + user + "' AND QueueName='" + queue + "'";;
            NYdb::NTable::TExecDataQuerySettings execSettings;
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), execSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT(!result.GetResultSet(0).Truncated());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }
    }

    Y_UNIT_TEST(DictJoin) {
        TKikimrRunner kikimr;
        auto client = kikimr.GetQueryClient();

        {
            const TString sql = R"(
                --!syntax_v1

                $lsource = SELECT 'test' AS ldata;
                $rsource = SELECT 'test' AS rdata;

                $left = SELECT ROW_NUMBER() OVER w AS r, ldata FROM $lsource WINDOW w AS ();
                $right = SELECT ROW_NUMBER() OVER w AS r, rdata FROM $rsource WINDOW w AS ();

                $result  = SELECT ldata, rdata FROM $left AS tl INNER JOIN $right AS tr ON tl.r = tr.r;

                SELECT * FROM $result;
            )";
            auto result = client.ExecuteQuery(
                sql,
                NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OlapCreateAsSelect_Simple) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            CREATE TABLE `/Root/Source` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        Tests::NCommon::TLoggerInit(kikimr).SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS").Initialize();

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source` (Col1, Col2) VALUES
                    (1u, 1), (100u, 100), (10u, 10);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination1` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS SELECT 1u As Col1, 1 As Col2;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination1`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;[1]]])");
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination2` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination2` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;[1]];[10u;[10]];[100u;[100]]])");
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination3` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(prepareResult.IsSuccess());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination3` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;[1]];[10u;[10]];[100u;[100]]])");
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination4` (
                    Col1,
                    Col2,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS VALUES (1, 2), (3, 4);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("AS VALUES statement is not supported for CreateTableAs."),
                prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination5` (
                    Col1 Uint32 NOT NULL,
                    Col2 Uint64,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS VALUES (1, 2), (3, 4);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("Column types are not supported for CREATE TABLE AS"),
                prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination6` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("Query can be executed only in per-statement mode (NoTx)"),
                prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination7` (
                    Col3,
                    Col4,
                    PRIMARY KEY (Col3)
                )
                PARTITION BY HASH(Col3)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS SELECT * FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("CREATE TABLE AS with columns is not supported"),
                prepareResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OltpCreateAsSelect_Simple) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false)
            .SetEnableTempTables(true);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            CREATE TABLE `/Root/Source` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            );
        )";

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source` (Col1, Col2) VALUES
                    (1u, 1), (100u, 100), (10u, 10);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination1` (
                    PRIMARY KEY (Col1)
                )
                AS SELECT Col2 As Col1, Col1 As Col2
                FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2 FROM `/Root/Destination1`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[1];[1u]];[[10];[10u]];[[100];[100u]]])");
        }
    }

    Y_UNIT_TEST(OltpCreateAsSelect_Disable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(false);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false)
            .SetEnableTempTables(true);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            CREATE TABLE `/Root/Source` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            );
        )";

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source` (Col1, Col2) VALUES
                    (1u, 1), (100u, 100), (10u, 10);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination1` (
                    PRIMARY KEY (Col1)
                )
                AS SELECT Col2 As Col1, Col1 As Col2
                FROM `/Root/Source`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("Creating table with data is not supported."),
                prepareResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OlapCreateAsSelect_Complex) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        {
            const TString query = R"(
                CREATE TABLE `/Root/Source1` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
            )";

            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source1` (Col1, Col2) VALUES
                    (1u, 1), (100u, 100), (10u, 10);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            const TString query = R"(
                CREATE TABLE `/Root/Source2` (
                    Col1 Uint64 NOT NULL,
                    Col2 String,
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
            )";

            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/Source2` (Col1, Col2) VALUES
                    (1u, 'test1'), (100u, 'test2'), (10u, 'test3');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/Destination1` (
                    PRIMARY KEY (Col1)
                )
                PARTITION BY HASH(Col1)
                WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4)
                AS SELECT l.Col1 As Col1, l.Col2 As Col2, r.Col2 As Col3
                FROM `/Root/Source1` l JOIN `/Root/Source2` r
                ON l.Col1 = r.Col1
                WHERE l.Col1 != 10u;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2, Col3 FROM `/Root/Destination1` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;[1];["test1"]];[100u;[100];["test2"]]])");
        }
    }


    void RunQuery (const TString& query, auto& session, bool expectOk = true) {
        auto qResult = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        if (!qResult.IsSuccess()) {
            Cerr << "Query failed, status: " << qResult.GetStatus() << ": " << qResult.GetIssues().ToString() << Endl;
        }
        UNIT_ASSERT(qResult.IsSuccess() == expectOk);
    };

    struct TEntryCheck {
        NYdb::NScheme::ESchemeEntryType Type;
        TString Name;
        bool IsExpected;
        bool WasFound = false;
    };

    TEntryCheck ExpectedTopic(const TString& name) {
        return TEntryCheck{NYdb::NScheme::ESchemeEntryType::Topic, name, true};
    }
    TEntryCheck UnexpectedTopic(const TString& name) {
        return TEntryCheck{NYdb::NScheme::ESchemeEntryType::Topic, name, false};
    }

    void CheckDirEntry(TKikimrRunner& kikimr, TVector<TEntryCheck>& entriesToCheck) {
        auto res = kikimr.GetSchemeClient().ListDirectory("/Root").GetValueSync();
        for (const auto& entry : res.GetChildren()) {
            Cerr << "Scheme entry: " << entry << Endl;
            for (auto& checkEntry : entriesToCheck) {
                if (checkEntry.Name != entry.Name)
                    continue;
                if (checkEntry.IsExpected) {
                    UNIT_ASSERT_C(entry.Type == checkEntry.Type, checkEntry.Name);
                    checkEntry.WasFound = true;
                } else {
                    UNIT_ASSERT_C(entry.Type != checkEntry.Type, checkEntry.Name);
                }
            }
        }
        for (auto& checkEntry : entriesToCheck) {
            if (checkEntry.IsExpected) {
                UNIT_ASSERT_C(checkEntry.WasFound, checkEntry.Name);
            }
        }
    }

    Y_UNIT_TEST(CreateAndDropTopic) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        serverSettings.PQConfig.SetRequireCredentialsInNewProtocol(false);
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto client = kikimr.GetQueryClient();
        auto session = client.GetSession().GetValueSync().GetSession();
        auto pq = NYdb::NTopic::TTopicClient(kikimr.GetDriver(),
                                             NYdb::NTopic::TTopicClientSettings().Database("/Root").AuthToken("root@builtin"));

        {
            const auto queryCreateTopic = Q_(R"(
                --!syntax_v1
                CREATE TOPIC `/Root/TempTopic` (CONSUMER cons1);
            )");
            RunQuery(queryCreateTopic, session);
            Cerr << "Topic created\n";
            auto desc = pq.DescribeTopic("/Root/TempTopic").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetConsumers().size(), 1);
        }
        {
            const auto queryCreateTopic = Q_(R"(
                --!syntax_v1
                CREATE TOPIC IF NOT EXISTS `/Root/TempTopic` (CONSUMER cons1, CONSUMER cons2);
            )");
            RunQuery(queryCreateTopic, session);
            auto desc = pq.DescribeTopic("/Root/TempTopic").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetConsumers().size(), 1);
        }
        {
            const auto queryCreateTopic = Q_(R"(
                --!syntax_v1
                CREATE TOPIC `/Root/TempTopic` (CONSUMER cons1, CONSUMER cons2, CONSUMER cons3);
            )");
            RunQuery(queryCreateTopic, session, false);
            auto desc = pq.DescribeTopic("/Root/TempTopic").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetConsumers().size(), 1);
        }

        TVector<TEntryCheck> entriesToCheck = {ExpectedTopic("TempTopic")};
        CheckDirEntry(kikimr, entriesToCheck);
        {
            const auto query = Q_(R"(
                --!syntax_v1
                Drop TOPIC `/Root/TempTopic`;
            )");
            RunQuery(query, session);
            Cerr << "Topic dropped\n";
            TVector<TEntryCheck> entriesToCheck = {UnexpectedTopic("TempTopic")};
            CheckDirEntry(kikimr, entriesToCheck);
        }
        {
            const auto query = Q_(R"(
                --!syntax_v1
                Drop TOPIC IF EXISTS `/Root/TempTopic`;
            )");
            RunQuery(query, session);
        }
        {
            const auto query = Q_(R"(
                --!syntax_v1
                Drop TOPIC `/Root/TempTopic`;
            )");
            RunQuery(query, session, false);
        }
    }

    Y_UNIT_TEST(CreateAndAlterTopic) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr{serverSettings};
        auto client = kikimr.GetQueryClient(NYdb::NQuery::TClientSettings{}.AuthToken("root@builtin"));
        auto session = client.GetSession().GetValueSync().GetSession();
        auto pq = NYdb::NTopic::TTopicClient(kikimr.GetDriver(),
                                             NYdb::NTopic::TTopicClientSettings().Database("/Root").AuthToken("root@builtin"));

        {
            const auto queryCreateTopic = Q_(R"(
                --!syntax_v1
                CREATE TOPIC `/Root/TempTopic` (CONSUMER cons1);
            )");
            RunQuery(queryCreateTopic, session);

            auto desc = pq.DescribeTopic("/Root/TempTopic").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitioningSettings().GetMinActivePartitions(), 1);
        }
        {
            const auto query = Q_(R"(
                --!syntax_v1
                ALTER TOPIC `/Root/TempTopic` SET (min_active_partitions = 10);
            )");
            RunQuery(query, session);
            auto desc = pq.DescribeTopic("/Root/TempTopic").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitioningSettings().GetMinActivePartitions(), 10);
        }
        {
            const auto query = Q_(R"(
                --!syntax_v1
                ALTER TOPIC IF EXISTS `/Root/TempTopic` SET (min_active_partitions = 15);
            )");
            RunQuery(query, session);
            auto desc = pq.DescribeTopic("/Root/TempTopic").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetPartitioningSettings().GetMinActivePartitions(), 15);
        }

        {
            const auto query = Q_(R"(
                --!syntax_v1
                ALTER TOPIC `/Root/NoSuchTopic` SET (min_active_partitions = 10);
            )");
            RunQuery(query, session, false);

            TVector<TEntryCheck> entriesToCheck = {UnexpectedTopic("NoSuchTopic")};
            CheckDirEntry(kikimr, entriesToCheck);
        }
        {
            const auto query = Q_(R"(
                --!syntax_v1
                ALTER TOPIC IF EXISTS `/Root/NoSuchTopic` SET (min_active_partitions = 10);
            )");
            RunQuery(query, session);
            TVector<TEntryCheck> entriesToCheck = {UnexpectedTopic("NoSuchTopic")};
            CheckDirEntry(kikimr, entriesToCheck);
        }
    }
    Y_UNIT_TEST(CreateOrDropTopicOverTable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr{serverSettings};
        auto tableClient = kikimr.GetTableClient();

        {
            auto tcSession = tableClient.CreateSession().GetValueSync().GetSession();
            UNIT_ASSERT(tcSession.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TmpTable` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync().IsSuccess());
            tcSession.Close();
        }

        auto client = kikimr.GetQueryClient(NYdb::NQuery::TClientSettings{}.AuthToken("root@builtin"));
        auto session = client.GetSession().GetValueSync().GetSession();

        TVector<TEntryCheck> entriesToCheck = {TEntryCheck{.Type = NYdb::NScheme::ESchemeEntryType::Table,
                                                           .Name = "TmpTable", .IsExpected = true}};
        {
            const auto queryCreateTopic = Q_(R"(
                --!syntax_v1
                CREATE TOPIC `/Root/TmpTable` (CONSUMER cons1);
            )");
            RunQuery(queryCreateTopic, session, false);
            CheckDirEntry(kikimr, entriesToCheck);

        }
        {
            const auto queryCreateTopic = Q_(R"(
                --!syntax_v1
                CREATE TOPIC IF NOT EXISTS `/Root/TmpTable` (CONSUMER cons1);
            )");
            RunQuery(queryCreateTopic, session, false);
            CheckDirEntry(kikimr, entriesToCheck);
        }
        {
            const auto queryDropTopic = Q_(R"(
                --!syntax_v1
                DROP TOPIC `/Root/TmpTable`;
            )");
            RunQuery(queryDropTopic, session, false);
        }
        {
            const auto queryDropTopic = Q_(R"(
                --!syntax_v1
                DROP TOPIC IF EXISTS `/Root/TmpTable`;
            )");
            RunQuery(queryDropTopic, session, false);
            CheckDirEntry(kikimr, entriesToCheck);
        }
        {
            auto tcSession = tableClient.CreateSession().GetValueSync().GetSession();
            auto type = TTypeBuilder().BeginOptional().Primitive(EPrimitiveType::Uint64).EndOptional().Build();
            auto alter = TAlterTableSettings().AppendAddColumns(TColumn("NewColumn", type));

            auto alterResult = tcSession.AlterTable("/Root/TmpTable", alter
                            ).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(alterResult.GetStatus(), EStatus::SUCCESS);
        }


    }
}

} // namespace NKqp
} // namespace NKikimr
