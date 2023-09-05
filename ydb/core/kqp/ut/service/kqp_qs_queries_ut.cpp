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

Y_UNIT_TEST_SUITE(KqpQueryService) {
    Y_UNIT_TEST(SessionFromPoolError) {
        auto kikimr = DefaultKikimrRunner();
        auto settings = NYdb::NQuery::TClientSettings().Database("WrongDB");
        auto db = kikimr.GetQueryClient(settings);

        auto result = db.GetSession().GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(SessionFromPoolSuccess) {
        auto kikimr = DefaultKikimrRunner();
        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        {
            auto db = kikimr.GetQueryClient();

            TString id;
            {
                auto result = db.GetSession().GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetSession().GetId());
                id = result.GetSession().GetId();
            }
            {
                auto result = db.GetSession().GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetSession().GetId(), id);
            }
        }
        WaitForZeroSessions(counters);
    }

    Y_UNIT_TEST(QueryOnClosedSession) {
        auto kikimr = DefaultKikimrRunner();
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);

        {
            auto db = kikimr.GetQueryClient();

            TString id;
            {
                auto result = db.GetSession().GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetSession().GetId());
                auto session = result.GetSession();
                id = session.GetId();

                bool allDoneOk = true;
                NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

                UNIT_ASSERT(allDoneOk);

                auto execResult = session.ExecuteQuery("SELECT 1;",
                    NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

                UNIT_ASSERT_VALUES_EQUAL(execResult.GetStatus(), EStatus::BAD_SESSION);
            }
            // closed session must be removed from session pool
            {
                auto result = db.GetSession().GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                UNIT_ASSERT(result.GetSession().GetId() != id);
            }
        }
        WaitForZeroSessions(counters);
    }

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

    Y_UNIT_TEST(Write) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            UPSERT INTO TwoShard (Key, Value2) VALUES(0, 101);

            SELECT Value2 FROM TwoShard WHERE Key = 0;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[101]]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(Explain) {
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

    Y_UNIT_TEST(Ddl) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            CREATE TABLE TestDdl (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetResultSets().empty());

        result = db.ExecuteQuery(R"(
            UPSERT INTO TestDdl (Key, Value) VALUES (1, "One");
            SELECT * FROM TestDdl;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[[1u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));

        result = db.ExecuteQuery(R"(
            DROP TABLE TestDdl;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            SELECT * FROM TestDdl;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DdlCache) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings()
            .StatsMode(EStatsMode::Basic);

        auto result = db.ExecuteQuery(R"(
            CREATE TABLE TestDdl (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )", TTxControl::NoTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

        {
            // TODO: Switch to query service.
            auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

            UNIT_ASSERT(session.ExecuteSchemeQuery(R"(
                DROP TABLE TestDdl;
            )").GetValueSync().IsSuccess());
        }

        result = db.ExecuteQuery(R"(
            CREATE TABLE TestDdl (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )", TTxControl::NoTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
        UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
    }

    Y_UNIT_TEST(DdlTx) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            CREATE TABLE TestDdl (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(DdlMixedDml) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            CREATE TABLE TestDdl (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );

            UPSERT INTO TestDdl (Key, Value) VALUES (1, "One");
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_MIXED_SCHEME_DATA_TX));
    }

    Y_UNIT_TEST(DmlNoTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            UPSERT INTO KeyValue (Key, Value) VALUES (3, "Three");
            SELECT * FROM KeyValue;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(MaterializeTxResults) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            DELETE FROM KeyValue;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            SELECT * FROM KeyValue;
        )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
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
}

} // namespace NKqp
} // namespace NKikimr
