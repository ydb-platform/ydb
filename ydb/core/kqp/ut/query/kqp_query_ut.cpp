#include <fmt/format.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>

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

    Y_UNIT_TEST(ExecuteDataQueryCollectMeta) {
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();

        {
            UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )").GetValueSync().IsSuccess());
        }

        {
            const TString query(Q1_(R"(
                SELECT * FROM `/Root/TestTable`;
            )"));

            {
                auto settings = TExecDataQuerySettings();
                settings.CollectQueryStats(ECollectQueryStatsMode::Full);

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();

                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString().c_str());

                auto stats = result.GetStats();
                UNIT_ASSERT(stats.has_value());

                UNIT_ASSERT_C(stats->GetMeta().has_value(), "Query result meta is empty");

                TStringStream in;
                in << stats->GetMeta().value();
                NJson::TJsonValue value;
                ReadJsonTree(&in, &value);

                UNIT_ASSERT_C(value.IsMap(), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("query_id"), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("version"), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("query_parameter_types"), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("table_metadata"), "Incorrect Meta");
                UNIT_ASSERT_C(value["table_metadata"].IsArray(), "Incorrect Meta: table_metadata type should be an array");
                UNIT_ASSERT_C(value.Has("created_at"), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("query_syntax"), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("query_database"), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("query_cluster"), "Incorrect Meta");
                UNIT_ASSERT_C(!value.Has("query_plan"), "Incorrect Meta");
                UNIT_ASSERT_C(value.Has("query_type"), "Incorrect Meta");
            }

            {
                auto settings = TExecDataQuerySettings();
                settings.CollectQueryStats(ECollectQueryStatsMode::Basic);

                auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();

                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString().c_str());

                auto stats = result.GetStats();
                UNIT_ASSERT(stats.has_value());

                UNIT_ASSERT_C(!stats->GetMeta().has_value(),  "Query result meta should be empty, but it's not");
            }
        }
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

            std::vector<std::string> grantPermissions;
            std::vector<std::string> revokePermissions;

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
                ? session.ExecuteDataQuery(std::string{query}, TTxControl::BeginTx().CommitTx(), *params).ExtractValueSync()
                : session.ExecuteDataQuery(std::string{query}, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), expectedStatus, result.GetIssues().ToString());
            if (expectedIssue) {
                UNIT_ASSERT_C(result.GetIssues().ToString().contains(*expectedIssue), result.GetIssues().ToString());
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
        TKikimrRunner kikimr{ TKikimrSettings() };

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
        TKikimrSettings serverSettings;
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
        TKikimrSettings serverSettings;
        TKikimrRunner kikimr(serverSettings);
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
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const auto& issue) {
            return issue.GetMessage().contains("ATOM evaluation is not supported in YDB queries.");
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

    Y_UNIT_TEST_TWIN(QueryStats, UseSink) {
        TKikimrSettings serverSettings;
        serverSettings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        TKikimrRunner kikimr(serverSettings);
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

        if (UseSink) {
            UNIT_ASSERT_VALUES_EQUAL(stats.query_phases().size(), 1);

            auto& phase0 = stats.query_phases(0);
            UNIT_ASSERT(phase0.duration_us() > 0);
            UNIT_ASSERT(phase0.cpu_time_us() > 0);
            totalCpuTimeUs += phase0.cpu_time_us();
            UNIT_ASSERT_VALUES_EQUAL(phase0.table_access().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(phase0.table_access(0).name(), "/Root/EightShard");
            UNIT_ASSERT(!phase0.table_access(0).has_reads());
            UNIT_ASSERT_VALUES_EQUAL(phase0.table_access(0).updates().rows(), 3);
            UNIT_ASSERT_VALUES_EQUAL(phase0.table_access(1).reads().rows(), 3);
            UNIT_ASSERT(phase0.table_access(0).updates().bytes() > 0);
            UNIT_ASSERT(phase0.table_access(1).reads().bytes() > 0);
            UNIT_ASSERT(!phase0.table_access(0).has_deletes());

            UNIT_ASSERT_VALUES_EQUAL(stats.total_cpu_time_us(), totalCpuTimeUs);
        } else {
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
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const auto& issue) {
                return issue.GetMessage().contains("EVALUATE IF is not supported in YDB queries.");
            }));
        }

        {
            auto result = session.ExecuteDataQuery(Q1_(R"(
                EVALUATE FOR $i IN AsList(1, 2, 3) DO BEGIN
                    SELECT $i;
                END DO;
            )"), TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::UNSUPPORTED, result.GetIssues().ToString());
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const auto& issue) {
                return issue.GetMessage().contains("EVALUATE is not supported in YDB queries.");
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
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_UNSUPPORTED, [](const auto& issue) {
                return issue.GetMessage().contains("ATOM evaluation is not supported in YDB queries.");
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
            UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::DEFAULT_ERROR, [](const auto& issue) {
                return issue.GetMessage().contains("Execution failed");
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

        const auto issueChecker = [](const auto& issue) {
            return issue.GetMessage().contains("can't be performed in data query");
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

     Y_UNIT_TEST(OlapTemporary) {
        auto settings = TKikimrSettings().SetEnableTempTables(true).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();
        auto session1 = client.GetSession().GetValueSync().GetSession();
        {
            auto result = session1.ExecuteQuery(R"(
                CREATE TEMP TABLE `/Root/test/TestTable` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = COLUMN);)",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        auto session2 = client.GetSession().GetValueSync().GetSession();
        {
            // Session2 can't use tmp table
            auto result = session2.ExecuteQuery(R"(
                SELECT * FROM `/Root/test/TestTable`;
                )",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_C(
                result.GetIssues().ToString().contains("does not exist or you do not have access permissions."),
                result.GetIssues().ToString());
        }

        {
            // Session1 can use tmp table
            auto result = session1.ExecuteQuery(R"(
                SELECT * FROM `/Root/test/TestTable`;
                )",
                NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OlapCreateAsSelect_Simple) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableMoveColumnTable(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags).SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
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

        auto client = kikimr.GetQueryClient();
        {
            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

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
            CompareYson(output, R"([[1u;1]])");
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
                prepareResult.GetIssues().ToString().contains("AS VALUES statement is not supported for CreateTableAs."),
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
                prepareResult.GetIssues().ToString().contains("Column types are not supported for CREATE TABLE AS"),
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
                prepareResult.GetIssues().ToString().contains("CTAS statement can be executed only in NoTx mode."),
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
                prepareResult.GetIssues().ToString().contains("CREATE TABLE AS with columns is not supported"),
                prepareResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OltpCreateAsSelect_Simple) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetEnableTempTables(true).SetAuthToken("user0@builtin");
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        {
            auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("root@builtin");
            auto driver = TDriver(driverConfig);
            auto schemeClient = NYdb::NScheme::TSchemeClient(driver);

            NYdb::NScheme::TPermissions permissions("user0@builtin",
                {"ydb.generic.read", "ydb.generic.write"}
            );
            auto result = schemeClient.ModifyPermissions("/Root",
                NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
            ).ExtractValueSync();
            AssertSuccessResult(result);
        }

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
            CompareYson(output, R"([[[1];1u];[[10];10u];[[100];100u]])");
        }
    }

    Y_UNIT_TEST(OltpCreateAsSelect_Disable) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
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
                prepareResult.GetIssues().ToString().contains("Creating table with data is not supported."),
                prepareResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(OlapCreateAsSelect_Complex) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableMoveColumnTable(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags).SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
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

    Y_UNIT_TEST(MixedCreateAsSelect) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableMoveColumnTable(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags).SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        {
            const TString query = R"(
                CREATE TABLE `/Root/SourceColumn` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = COLUMN);

                CREATE TABLE `/Root/SourceRow` (
                    Col1 Uint64 NOT NULL,
                    Col2 String,
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW);
            )";

            auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/SourceColumn` (Col1, Col2) VALUES
                    (1u, 1), (100u, 100), (10u, 10);
                REPLACE INTO `/Root/SourceRow` (Col1, Col2) VALUES
                    (1u, 'test1'), (100u, 'test2'), (10u, 'test3');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }


        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/DestinationColumn` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = COLUMN)
                AS SELECT l.Col1 As Col1, l.Col2 As Col2, r.Col2 As Col3
                FROM `/Root/SourceColumn` l JOIN `/Root/SourceRow` r
                ON l.Col1 = r.Col1
                WHERE l.Col1 != 10u;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }
        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/DestinationRow` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW)
                AS SELECT l.Col1 As Col1, l.Col2 As Col2, r.Col2 As Col3
                FROM `/Root/SourceColumn` l JOIN `/Root/SourceRow` r
                ON l.Col1 = r.Col1
                WHERE l.Col1 != 10u;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2, Col3 FROM `/Root/DestinationColumn` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;[1];["test1"]];[100u;[100];["test2"]]])");
        }
        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT Col1, Col2, Col3 FROM `/Root/DestinationRow` ORDER BY Col1 ASC;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[1u;[1];["test1"]];[100u;[100];["test2"]]])");
        }
    }

    Y_UNIT_TEST_TWIN(TableSink_ReplaceDataShardDataQuery, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(UseSink);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/DataShard` (
                Col1 Uint32 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 16,
                UNIFORM_PARTITIONS = 16);

            CREATE TABLE `/Root/DataShard2` (
                Col1 Uint32 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 17,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 17,
                UNIFORM_PARTITIONS = 17);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto prepareResult = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (10u, "test1", 10), (20u, "test2", 11), (2147483647u, "test3", 12), (2147483640u, NULL, 13);
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/DataShard`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([[4u]])", FormatResultSetYson(it.GetResultSet(0)));
        }

        {
            auto prepareResult = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/DataShard2` SELECT * FROM `/Root/DataShard`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/DataShard2`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([[4u]])", FormatResultSetYson(it.GetResultSet(0)));
        }

        {
            auto prepareResult = session.ExecuteDataQuery(R"(
                REPLACE INTO `/Root/DataShard2` (Col1, Col2, Col3) VALUES
                    (11u, "test1", 10), (21u, "test2", 11), (2147483646u, "test3", 12), (2147483641u, NULL, 13);
                SELECT COUNT(*) FROM `/Root/DataShard`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = session.ExecuteDataQuery(R"(
                SELECT COUNT(*) FROM `/Root/DataShard2`;
            )", NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            CompareYson(R"([[8u]])", FormatResultSetYson(it.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(CreateAsSelect_BadCases) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableMoveColumnTable(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags).SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableHtapTx(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        TKikimrRunner kikimr(settings);

        const TString query = R"(
            CREATE TABLE `/Root/ColSrc` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);

            CREATE TABLE `/Root/RowSrc` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            WITH (STORE = ROW, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColSrc` (Col1, Col2) VALUES (1u, 1), (100u, 100), (10u, 10);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/RowSrc` (Col1, Col2) VALUES (1u, 1), (100u, 100), (10u, 10);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE OR REPLACE TABLE `/Root/RowDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT * FROM `/Root/RowSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "OR REPLACE feature is supported only for EXTERNAL DATA SOURCE and EXTERNAL TABLE", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE IF NOT EXISTS TABLE `/Root/RowDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT * FROM `/Root/RowSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "no viable alternative at input 'CREATE IF'", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/RowDst` (
                    INDEX idx GLOBAL ON Col2,
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT * FROM `/Root/RowSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "extraneous input", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/RowDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT Col1, 1 / (Col2 - 100) As Col2 FROM `/Root/RowSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/RowDst` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[1u;[0]];[10u;[0]];[100u;#]])", FormatResultSetYson(result.GetResultSet(0)));

            result = client.ExecuteQuery(R"(
                DROP TABLE `/Root/RowDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/RowDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT Col2 AS Col1, Col1 As Col2 FROM `/Root/RowSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/RowDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());


            result = client.ExecuteQuery(R"(
                DROP TABLE `/Root/RowDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/ColDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = COLUMN) AS
                SELECT Col2 AS Col1, Col1 As Col2 FROM `/Root/ColSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Nullable key column 'Col1", result.GetIssues().ToString());

            result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/ColDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/ColDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = COLUMN) AS
                SELECT Unwrap(Col2) AS Col1, Col1 As Col2 FROM `/Root/ColSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = client.ExecuteQuery(R"(
                SELECT * FROM `/Root/ColDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = client.ExecuteQuery(R"(
                DROP TABLE `/Root/ColDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/RowlDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = COLUMN) AS
                SELECT NotFound AS Col1, Col1 As Col2 FROM `/Root/RowSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "not found", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/RowSrc` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT 1 AS Col1, 2 As Col2;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "path exist", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/A` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT 1 AS Col1, 2 As Col2;

                CREATE TABLE `/Root/B` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT 1 AS Col1, 2 As Col2;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Several CTAS statement can't be used without per-statement mode", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/A` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT 1 AS Col1, 2 As Col2;

                REPLACE INTO `/Root/ColSrc` (Col1, Col2) VALUES (1u, 1), (100u, 100), (10u, 10);
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "CTAS statement can't be used with other statements without per-statement mode", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/A` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT 1 AS Col1, 2 As Col2;

                SELECT * FROM `/Root/ColSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "CTAS statement can't be used with other statements without per-statement mode", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/RowDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT * FROM `/Root/ColSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = client.ExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/RowDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[3u]])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto result = client.ExecuteQuery(R"(
                CREATE TABLE `/Root/ColDst` (
                    PRIMARY KEY (Col1)
                )
                WITH (STORE = ROW) AS
                SELECT * FROM `/Root/RowSrc`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            result = client.ExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/ColDst`;
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[3u]])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_TWIN(ReadOverloaded, StreamLookup) {
        TKikimrSettings settings = TKikimrSettings().SetUseRealThreads(false);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });
        auto writeSession = kikimr.RunCall([&] { return db.CreateSession().GetValueSync().GetSession(); });

        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        
        kikimr.RunCall([&]{ CreateSampleTablesWithIndex(session, false /* no need in table data */); return true; });

        {
            const TString query(StreamLookup
                ? Q1_(R"(
                        SELECT Value FROM `/Root/SecondaryKeys` VIEW Index WHERE Fk = 1
                    )")
                : Q1_(R"(
                        SELECT COUNT(a.Key) FROM `/Root/SecondaryKeys` as a;
                    )"));

            auto grab = [&](TAutoPtr<IEventHandle> &ev) -> auto {
                if (ev->GetTypeRewrite() == TEvDataShard::TEvReadResult::EventType) {
                    auto* msg = ev->Get<TEvDataShard::TEvReadResult>();
                    msg->Record.MutableStatus()->SetCode(::Ydb::StatusIds::OVERLOADED);
                }

                return TTestActorRuntime::EEventAction::PROCESS;
            };

            runtime.SetObserverFunc(grab);
            auto future = kikimr.RunInThreadPool([&]{
                auto txc = TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx();
                return session.ExecuteDataQuery(query, txc).ExtractValueSync();
            });

            auto result = runtime.WaitFuture(future);
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(result.GetStatus() == NYdb::EStatus::OVERLOADED);
        }
    }

    Y_UNIT_TEST(TableSinkWithSubquery) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/table1` (
                p1 Utf8,
                PRIMARY KEY (p1)
            )
            WITH (
                STORE = ROW
            );

            CREATE TABLE `/Root/table2` (
                p1 Utf8,
                PRIMARY KEY (p1)
            )
            WITH (
                STORE = ROW
            );
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();

        {
            auto prepareResult = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/table1` (p1) VALUES ("a") , ("b"), ("c");
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                $data2 = Cast(AsList() As List<Struct<p1: Utf8>>);

                /* query */
                SELECT d1.p1 AS p1,
                FROM `/Root/table1` AS d1
                CROSS JOIN AS_TABLE($data2) AS d2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(R"(
                $data2 = Cast(AsList() As List<Struct<p1: Utf8>>);

                /* query */
                INSERT INTO `/Root/table1`
                SELECT d1.p1 AS p1,
                FROM `/Root/table2` AS d1
                CROSS JOIN AS_TABLE($data2) AS d2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_QUAD(CreateAsSelectTypes, NotNull, IsOlap) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableMoveColumnTable(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags).SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();

        {
            const TString createSource = std::format(R"(
                CREATE TABLE `/Root/Source` (
                    Key         Int8    NOT NULL,
                    CInt8       Int8         {0},
                    CUint8      Uint8        {0},
                    CInt16      Int16        {0},
                    CUint16     Uint16       {0},
                    CInt32      Int32        {0},
                    CUint32     Uint32       {0},
                    CInt64      Int64        {0},
                    CUint64     Uint64       {0},
                    CFloat      Float        {0},
                    CDouble     Double       {0},
                    CDate       Date         {0},
                    CDatetime       Datetime     {0},
                    CTimestamp      Timestamp    {0},
                    CDate32     Date32       {0},
                    CDatetime64     Datetime64   {0},
                    CTimestamp64        Timestamp64  {0},
                    CString     String       {0},
                    CUtf8       Utf8         {0},
                    CYson       Yson         {0},
                    CJson       Json         {0},
                    CJsonDocument       JsonDocument {0},
                    {1}
                    PRIMARY KEY (Key)
                );
                )",
                NotNull ? "NOT NULL" : "",
                IsOlap ? "" : std::format(R"(
                    CBool       Bool         {0},
                    CInterval       Interval     {0},
                    CInterval64     Interval64   {0},
                    CUuid       Uuid         {0},
                    CDyNumber       DyNumber     {0},)",
                    NotNull ? "NOT NULL" : ""));

            auto result = client.ExecuteQuery(createSource, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(std::format(R"(
                UPSERT INTO `/Root/Source` (
                    Key
                    , CInt8
                    , CUint8
                    , CInt16
                    , CUint16
                    , CInt32
                    , CUint32
                    , CInt64
                    , CUint64
                    , CFloat
                    , CDouble
                    , CDate
                    , CDatetime
                    , CTimestamp
                    , CDate32
                    , CDatetime64
                    , CTimestamp64
                    , CString
                    , CUtf8
                    , CYson
                    , CJson
                    , CJsonDocument
                    {0}
                )
                VALUES (
                    0
                    , 42
                    , 42
                    , 42
                    , 42
                    , 42
                    , 42
                    , 42
                    , 42
                    , CAST(42.0 AS Float)
                    , 42.0
                    , Date("2025-01-01")
                    , Datetime("2025-01-01T00:00:00Z")
                    , Timestamp("2025-01-01T00:00:00Z")
                    , Date("2025-01-01")
                    , Datetime("2025-01-01T00:00:00Z")
                    , Timestamp("2025-01-01T00:00:00Z")
                    , String("test")
                    , Utf8("test")
                    , Yson("<a=1>[3;%false]")
                    , Json(@@{{"a":1,"b":null}}@@)
                    , JsonDocument('{{"a":1,"b":null}}')
                    {1}
                );
            )",
            IsOlap ? "" : ", CBool, CInterval, CInterval64, CUuid, CDyNumber",
            IsOlap ? "" : ", False, Interval(\"P1DT2H3M4.567890S\"), Interval(\"P1DT2H3M4.567890S\"), Uuid(\"f9d5cc3f-f1dc-4d9c-b97e-766e57ca4ccb\"), DyNumber(\"42\")"),
            NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(std::format(R"(
                CREATE TABLE `/Root/Destination` (
                    PRIMARY KEY (Key)
                )
                WITH (STORE = {0})
                AS SELECT *
                FROM `/Root/Source`;
            )", IsOlap ? "COLUMN" : "ROW"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }
            
        {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            auto desc = session.DescribeTable("/Root/Destination").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());

            auto columns = desc.GetTableDescription().GetTableColumns();
            for (const auto& column : columns) {
                if (column.Name == "Key") {
                    continue;
                }

                UNIT_ASSERT(!column.NotNull);

                THashMap<TString, TString> nameToType = {
                    {"CBool",           "Bool"},
                    {"CInt8",           "Int8"},
                    {"CUint8",          "Uint8"},
                    {"CInt16",          "Int16"},
                    {"CUint16",         "Uint16"},
                    {"CInt32",          "Int32"},
                    {"CUint32",         "Uint32"},
                    {"CInt64",          "Int64"},
                    {"CUint64",         "Uint64"},
                    {"CFloat",          "Float"},
                    {"CDouble",         "Double"},
                    {"CDate",           "Date"},
                    {"CDatetime",       "Datetime"},
                    {"CTimestamp",      "Timestamp"},
                    {"CInterval",       "Interval"},
                    {"CDate32",         "Date32"},
                    {"CDatetime64",     "Datetime64"},
                    {"CTimestamp64",    "Timestamp64"},
                    {"CInterval64",     "Interval64"},
                    {"CString",         "String"},
                    {"CUtf8",           "Utf8"},
                    {"CYson",           "Yson"},
                    {"CJson",           "Json"},
                    {"CUuid",           "Uuid"},
                    {"CJsonDocument",   "JsonDocument"},
                    {"CDyNumber",       "DyNumber"},
                };
                if (!NotNull) {
                    for (auto& [_, type] : nameToType) {
                        type += "?";
                    }
                }

                UNIT_ASSERT_VALUES_EQUAL_C(nameToType.at(column.Name), column.Type.ToString(), column.Name);
            }
        }
    }

    Y_UNIT_TEST_TWIN(CreateAsSelectBadTypes, IsOlap) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetEnableTempTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery(std::format(R"(
                CREATE TABLE `/Root/Destination` (
                    PRIMARY KEY (Key)
                )
                WITH (STORE = {0})
                AS SELECT 1 AS Key, AsList(1, 2, 3, 4, 5) AS Value;
            )", IsOlap ? "COLUMN" : "ROW"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Invalid type for column: Value.", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(std::format(R"(
                CREATE TABLE `/Root/Destination` (
                    PRIMARY KEY (Key)
                )
                WITH (STORE = {0})
                AS SELECT 1 AS Key, NULL AS Value;
            )", IsOlap ? "COLUMN" : "ROW"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Invalid type for column: Value.", result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery(std::format(R"(
                CREATE TABLE `/Root/Destination` (
                    PRIMARY KEY (Key)
                )
                WITH (STORE = {0})
                AS SELECT 1 AS Key, [] AS Value;
            )", IsOlap ? "COLUMN" : "ROW"), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Invalid type for column: Value.", result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(CreateAsSelectPath, UseTablePathPrefix) {
        const auto dirPath = UseTablePathPrefix ? "" : "/Root/test/";
        const auto pragma = UseTablePathPrefix ? "PRAGMA TablePathPrefix(\"/Root/test\");" : "";

        auto settings = TKikimrSettings().SetWithSampleTables(false).SetEnableTempTables(true).SetAuthToken("user0@builtin");;
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        {
            auto driverConfig = TDriverConfig()
            .SetEndpoint(kikimr.GetEndpoint())
                .SetAuthToken("root@builtin");
            auto driver = TDriver(driverConfig);
            auto schemeClient = NYdb::NScheme::TSchemeClient(driver);

            {
                auto result = schemeClient.MakeDirectory("/Root/test").ExtractValueSync();
                AssertSuccessResult(result);
            }
            {
                NYdb::NScheme::TPermissions permissions("user0@builtin",
                    {"ydb.generic.read", "ydb.generic.write"}
                );
                auto result = schemeClient.ModifyPermissions("/Root/test",
                    NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)
                ).ExtractValueSync();
                AssertSuccessResult(result);
            }
        }

        const TString query = std::format(R"(
            {1}
            CREATE TABLE `{0}Source` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            );
        )", dirPath, pragma);

        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        {
            auto prepareResult = client.ExecuteQuery(std::format(R"(
                {1}
                REPLACE INTO `{0}Source` (Col1, Col2) VALUES
                    (1u, 1), (100u, 100), (10u, 10);
            )", dirPath, pragma), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(std::format(R"(
                {1}
                CREATE TABLE `{0}Destination1` (
                    PRIMARY KEY (Col1)
                )
                AS SELECT Col2 As Col1, Col1 As Col2
                FROM `{0}Source`;
            )", dirPath, pragma), NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(std::format(R"(
                {1}
                SELECT Col1, Col2 FROM `{0}Destination1`;
            )", dirPath, pragma), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[1];1u];[[10];10u];[[100];100u]])");
        }
    }

    Y_UNIT_TEST_TWIN(UpdateThenDelete, UseSink) {
        auto settings = TKikimrSettings().SetWithSampleTables(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);

        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetTableClient();

        {
            const TString query = R"(
                DECLARE $data AS List<Struct<
                    Key: String,
                    Value: String
                >>;

                UPSERT INTO KeyValue2 SELECT * FROM AS_TABLE($data);

                DELETE FROM KeyValue2 ON SELECT * FROM KeyValue2 AS a LEFT ONLY JOIN AS_TABLE($data) AS b USING (Key);
            )";

            TTypeBuilder builder;
            builder
                .BeginStruct()
                    .AddMember("Key", TTypeBuilder().Primitive(NYdb::EPrimitiveType::String).Build())
                    .AddMember("Value", TTypeBuilder().Primitive(NYdb::EPrimitiveType::String).Build())
                .EndStruct();

            auto params = client.GetParamsBuilder()
                .AddParam("$data")
                    .EmptyList(builder.Build())
                    .Build()
                .Build();

            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx(), std::move(params)).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        }
        {
            const TString query = R"(
                SELECT * FROM KeyValue2;
            )";

            auto session = client.CreateSession().GetValueSync().GetSession();
            auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            Cerr << FormatResultSetYson(result.GetResultSet(0)) << Endl;
            UNIT_ASSERT_VALUES_EQUAL(0, result.GetResultSet(0).RowsCount());
        }
    }

    Y_UNIT_TEST(ExecuteWriteQuery) {
        using namespace fmt::literals;

        TKikimrRunner kikimr;
        auto client = kikimr.GetQueryClient();

        {   // Just generate table
            const auto sql = fmt::format(R"(
                CREATE TABLE test_table (
                    PRIMARY KEY (id)
                ) AS SELECT
                    ROW_NUMBER() OVER w AS id, data
                FROM
                    AS_TABLE(ListReplicate(<|data: '{data}'|>, 500000))
                WINDOW
                    w AS (ORDER BY data))",
                "data"_a = std::string(137, 'a')
            );
            const auto result = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        Cerr << TInstant::Now() << " --------------- Start update ---------------\n";

        const auto hangingResult = client.ExecuteQuery(R"(
            UPDATE test_table SET data = "a"
        )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(hangingResult.GetStatus(), EStatus::SUCCESS, hangingResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateAsSelectView) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        TKikimrRunner kikimr(settings);

        Tests::NCommon::TLoggerInit(kikimr).SetComponents({ NKikimrServices::TX_COLUMNSHARD }, "CS").Initialize();

        auto client = kikimr.GetQueryClient();

        {
            auto result = client.ExecuteQuery( R"(
                CREATE TABLE `l_source` (
                    id Uint64,
                    num Uint64,
                    unused String,
                    PRIMARY KEY (id)
                );

                CREATE TABLE `r_source` (
                    id Uint64,
                    id2 Uint64,
                    unused String,
                    PRIMARY KEY (id)
                );
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto result = client.ExecuteQuery( R"(
                CREATE VIEW `l`
                with (security_invoker = TRUE)
                AS (
                    SELECT
                        id,
                        num
                    FROM
                        `l_source`
                );

                CREATE VIEW `r` 
                with (security_invoker = TRUE)
                AS (
                    SELECT
                        id,
                        id2
                    FROM
                        `r_source`
                );
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                INSERT INTO `/Root/l_source` (id, num) VALUES
                    (1u, 1u), (100u, 100u), (10u, 10u);
                INSERT INTO `/Root/r_source` (id, id2) VALUES
                    (1u, 1u), (100u, 100u), (10u, 10u);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `table1`
                    (PRIMARY KEY (id))
                AS (
                    SELECT
                        id, num
                    FROM `l`
                )
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT id, num FROM `/Root/table1` ORDER BY id;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[1u];[1u]];[[10u];[10u]];[[100u];[100u]]])");
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                CREATE TABLE `table2`
                    (PRIMARY KEY (id2))
                AS (
                    SELECT
                        r.id2 AS id2,
                        sum(l.num) AS num
                    FROM `l` AS l
                    LEFT JOIN `r` AS r ON l.id = r.id
                    GROUP BY r.id2
                )
            )", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT id2, num FROM `/Root/table2` ORDER BY id2;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[[1u];[1u]];[[10u];[10u]];[[100u];[100u]]])");
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
