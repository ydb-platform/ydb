#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/public/lib/ut_helpers/ut_helpers_query.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>
#include <ydb/public/sdk/cpp/client/ydb_types/operation/operation.h>

#include <ydb/core/kqp/counters/kqp_counters.h>

#include <fmt/format.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace fmt::literals;

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
            DECLARE $value As Int64;
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
            DECLARE $value As Int64;
            SELECT $value;
        )", TTxControl::BeginTx().CommitTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        CompareYson(R"([[17]])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(StreamExecuteQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        {
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

        {
            auto it = db.StreamExecuteQuery(R"(
                SELECT Key, Value2 FROM TwoShard WHERE false ORDER BY Key > 0;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());

            ui32 rsCount = 0;
            ui32 columns = 0;
            for (;;) {
                auto streamPart = it.ReadNext().GetValueSync();
                if (!streamPart.IsSuccess()) {
                    UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                    break;
                }

                if (streamPart.HasResultSet()) {
                    auto resultSet = streamPart.ExtractResultSet();
                    columns = resultSet.ColumnsCount();
                    CompareYson(R"([])", FormatResultSetYson(resultSet));
                    rsCount++;
                }
            }

            UNIT_ASSERT_VALUES_EQUAL(rsCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(columns, 2);
        }
    }

    void CheckQueryResult(TExecuteQueryResult result) {
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        CompareYson(R"([
            [[3u];[1]];
            [[4000000003u];[1]]
        ])", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(ExecuteQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        const TString query = "SELECT Key, Value2 FROM TwoShard WHERE Value2 > 0 ORDER BY Key";
        auto result = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        CheckQueryResult(result);
    }

    Y_UNIT_TEST(ExecuteQueryExplicitBeginCommitRollback) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        auto sessionResult = db.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), EStatus::SUCCESS, sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        {
            auto beginTxResult = session.BeginTransaction(TTxSettings::OnlineRO()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(beginTxResult.GetStatus(), EStatus::BAD_REQUEST, beginTxResult.GetIssues().ToString());
            UNIT_ASSERT(beginTxResult.GetIssues());
        }

        {
            auto beginTxResult = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(beginTxResult.GetStatus(), EStatus::SUCCESS, beginTxResult.GetIssues().ToString());

            auto transaction = beginTxResult.GetTransaction();
            auto commitTxResult = transaction.Commit().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(commitTxResult.GetStatus(), EStatus::SUCCESS, commitTxResult.GetIssues().ToString());

            auto rollbackTxResult = transaction.Rollback().ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(rollbackTxResult.GetStatus(), EStatus::NOT_FOUND, rollbackTxResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ExecuteQueryExplicitTxTLI) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        auto sessionResult = db.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), EStatus::SUCCESS, sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        auto beginTxResult = session.BeginTransaction(TTxSettings::SerializableRW()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(beginTxResult.GetStatus(), EStatus::SUCCESS, beginTxResult.GetIssues().ToString());
        auto transaction = beginTxResult.GetTransaction();
        UNIT_ASSERT(transaction.IsActive());

        {
            const TString query = "UPDATE TwoShard SET Value2 = 0";
            auto result = transaction.GetSession().ExecuteQuery(query, TTxControl::Tx(transaction.GetId())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {

            const TString query = "UPDATE TwoShard SET Value2 = 1";
            auto result = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto commitTxResult = transaction.Commit().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(commitTxResult.GetStatus(), EStatus::ABORTED, commitTxResult.GetIssues().ToString());
    }

    Y_UNIT_TEST(ExecuteQueryInteractiveTx) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        auto sessionResult = db.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), EStatus::SUCCESS, sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        {
            const TString query = "UPDATE TwoShard SET Value2 = 0";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto transaction = result.GetTransaction();
            UNIT_ASSERT(transaction->IsActive());

            auto checkResult = [&](TString expected) {
                auto selectRes = db.ExecuteQuery(
                    "SELECT * FROM TwoShard ORDER BY Key",
                    TTxControl::BeginTx().CommitTx()
                ).ExtractValueSync();

                UNIT_ASSERT_C(selectRes.IsSuccess(), selectRes.GetIssues().ToString());
                CompareYson(expected, FormatResultSetYson(selectRes.GetResultSet(0)));
            };
            checkResult(R"([[[1u];["One"];[-1]];[[2u];["Two"];[0]];[[3u];["Three"];[1]];[[4000000001u];["BigOne"];[-1]];[[4000000002u];["BigTwo"];[0]];[[4000000003u];["BigThree"];[1]]])");

            auto txRes = transaction->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(txRes.GetStatus(), EStatus::SUCCESS, txRes.GetIssues().ToString());

            checkResult(R"([[[1u];["One"];[0]];[[2u];["Two"];[0]];[[3u];["Three"];[0]];[[4000000001u];["BigOne"];[0]];[[4000000002u];["BigTwo"];[0]];[[4000000003u];["BigThree"];[0]]])");
        }

        {
            const TString query = "UPDATE TwoShard SET Value2 = 1";
            auto result = session.ExecuteQuery(query, TTxControl::BeginTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            auto transaction = result.GetTransaction();
            UNIT_ASSERT(transaction->IsActive());

            const TString query2 = "UPDATE KeyValue SET Value = 'Vic'";
            auto result2 = session.ExecuteQuery(query2, TTxControl::Tx(transaction->GetId())).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result2.GetStatus(), EStatus::SUCCESS, result2.GetIssues().ToString());
            auto transaction2 = result2.GetTransaction();
            UNIT_ASSERT(transaction2->IsActive());

            auto checkResult = [&](TString table, TString expected) {
                auto selectRes = db.ExecuteQuery(
                    Sprintf("SELECT * FROM %s ORDER BY Key", table.data()),
                    TTxControl::BeginTx().CommitTx()
                ).ExtractValueSync();

                UNIT_ASSERT_C(selectRes.IsSuccess(), selectRes.GetIssues().ToString());
                CompareYson(expected, FormatResultSetYson(selectRes.GetResultSet(0)));
            };
            checkResult("TwoShard", R"([[[1u];["One"];[0]];[[2u];["Two"];[0]];[[3u];["Three"];[0]];[[4000000001u];["BigOne"];[0]];[[4000000002u];["BigTwo"];[0]];[[4000000003u];["BigThree"];[0]]])");
            checkResult("KeyValue", R"([[[1u];["One"]];[[2u];["Two"]]])");
            auto txRes = transaction->Commit().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(txRes.GetStatus(), EStatus::SUCCESS, txRes.GetIssues().ToString());

            checkResult("KeyValue", R"([[[1u];["Vic"]];[[2u];["Vic"]]])");
            checkResult("TwoShard", R"([[[1u];["One"];[1]];[[2u];["Two"];[1]];[[3u];["Three"];[1]];[[4000000001u];["BigOne"];[1]];[[4000000002u];["BigTwo"];[1]];[[4000000003u];["BigThree"];[1]]])");
        }
    }

    Y_UNIT_TEST(ExecuteQueryInteractiveTxCommitWithQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        auto sessionResult = db.GetSession().ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(sessionResult.GetStatus(), EStatus::SUCCESS, sessionResult.GetIssues().ToString());
        auto session = sessionResult.GetSession();

        const TString query = "UPDATE TwoShard SET Value2 = 0";
        auto result = session.ExecuteQuery(query, TTxControl::BeginTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        auto transaction = result.GetTransaction();
        UNIT_ASSERT(transaction->IsActive());

        auto checkResult = [&](TString expected) {
            auto selectRes = db.ExecuteQuery(
                "SELECT * FROM TwoShard ORDER BY Key",
                TTxControl::BeginTx().CommitTx()
            ).ExtractValueSync();

            UNIT_ASSERT_C(selectRes.IsSuccess(), selectRes.GetIssues().ToString());
            CompareYson(expected, FormatResultSetYson(selectRes.GetResultSet(0)));
        };
        checkResult(R"([[[1u];["One"];[-1]];[[2u];["Two"];[0]];[[3u];["Three"];[1]];[[4000000001u];["BigOne"];[-1]];[[4000000002u];["BigTwo"];[0]];[[4000000003u];["BigThree"];[1]]])");

        result = session.ExecuteQuery("UPDATE TwoShard SET Value2 = 1 WHERE Key = 1",
            TTxControl::Tx(transaction->GetId()).CommitTx()).ExtractValueSync();;
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(!result.GetTransaction()->IsActive());

        checkResult(R"([[[1u];["One"];[1]];[[2u];["Two"];[0]];[[3u];["Three"];[0]];[[4000000001u];["BigOne"];[0]];[[4000000002u];["BigTwo"];[0]];[[4000000003u];["BigThree"];[0]]])");
    }


    Y_UNIT_TEST(ForbidInteractiveTxOnImplicitSession) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        const TString query = "SELECT 1";
        UNIT_ASSERT_EXCEPTION(db.ExecuteQuery(query, TTxControl::BeginTx()).ExtractValueSync(), NYdb::TContractViolation);
    }

    Y_UNIT_TEST(ExecuteRetryQuery) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        const TString query = "SELECT Key, Value2 FROM TwoShard WHERE Value2 > 0 ORDER BY Key";
        auto queryFunc = [&query](TSession session) -> TAsyncExecuteQueryResult {
            return session.ExecuteQuery(query, TTxControl::BeginTx().CommitTx());
        };
        auto resultRetryFunc = db.RetryQuery(std::move(queryFunc)).GetValueSync();
        CheckQueryResult(resultRetryFunc);
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
                CREATE TABLE test (id int16,PRIMARY KEY (id)))"
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

    Y_UNIT_TEST(ExecuteDDLStatusCodeSchemeError) {
        TKikimrRunner kikimr(NKqp::TKikimrSettings().SetWithSampleTables(false));
        {
            auto db = kikimr.GetQueryClient();
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE unsupported_TzTimestamp (key Int32, payload TzTimestamp, primary key(key)))",
                TTxControl::NoTx()
            ).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
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
            DECLARE $value As Int64;
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
            DECLARE $value As Uint32;
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
            DECLARE $value As Uint32;
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
            if (stage.GetMapSafe().contains("Stats")) {
                totalTasks += stage.GetMapSafe().at("Stats").GetMapSafe().at("Tasks").GetIntegerSafe();
            }
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

        enum EEx {
            Empty,
            IfExists,
            IfNotExists,
        };

        auto checkCreate = [&](bool expectSuccess, EEx exMode, int nameSuffix) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfExists);
            const TString ifNotExistsStatement = exMode == EEx::IfNotExists ? "IF NOT EXISTS" : "";
            const TString sql = fmt::format(R"sql(
                CREATE TABLE {if_not_exists} TestDdl_{name_suffix} (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
                )sql",
                "if_not_exists"_a = ifNotExistsStatement,
                "name_suffix"_a = nameSuffix
            );

            auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT(result.GetResultSets().empty());
        };

        auto checkDrop = [&](bool expectSuccess, EEx exMode, int nameSuffix) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfNotExists);
            const TString ifExistsStatement = exMode == EEx::IfExists ? "IF EXISTS" : "";
            const TString sql = fmt::format(R"sql(
                DROP TABLE {if_exists} TestDdl_{name_suffix};
                )sql",
                "if_exists"_a = ifExistsStatement,
                "name_suffix"_a = nameSuffix
            );

            auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT(result.GetResultSets().empty());
        };

        auto checkUpsert = [&](int nameSuffix) {
            const TString sql = fmt::format(R"sql(
                UPSERT INTO TestDdl_{name_suffix} (Key, Value) VALUES (1, "One");
                )sql",
                "name_suffix"_a = nameSuffix
            );

            auto result = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        };

        auto checkExists = [&](bool expectSuccess, int nameSuffix) {
            const TString sql = fmt::format(R"sql(
                SELECT * FROM TestDdl_{name_suffix};
                )sql",
                "name_suffix"_a = nameSuffix
            );
            auto result = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
                CompareYson(R"([[[1u];["One"]]])", FormatResultSetYson(result.GetResultSet(0)));
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            }
        };

        // usual create
        checkCreate(true, EEx::Empty, 0);
        checkUpsert(0);
        checkExists(true, 0);

        // create already existing table
        checkCreate(false, EEx::Empty, 0); // already exists
        checkCreate(true, EEx::IfNotExists, 0);
        checkExists(true, 0);

        // usual drop
        checkDrop(true, EEx::Empty, 0);
        checkExists(false, 0);
        checkDrop(false, EEx::Empty, 0); // no such table

        // drop if exists
        checkDrop(true, EEx::IfExists, 0);
        checkExists(false, 0);

        // failed attempt to drop nonexisting table
        checkDrop(false, EEx::Empty, 0);

        // create with if not exists
        checkCreate(true, EEx::IfNotExists, 1); // real creation
        checkUpsert(1);
        checkExists(true, 1);
        checkCreate(true, EEx::IfNotExists, 1);

        // drop if exists
        checkDrop(true, EEx::IfExists, 1); // real drop
        checkExists(false, 1);
        checkDrop(true, EEx::IfExists, 1);
    }

    Y_UNIT_TEST(DdlColumnTable) {
        const TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Key").SetType(NScheme::NTypeIds::Uint64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("Value").SetType(NScheme::NTypeIds::String)
        };

        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TTestHelper testHelper(serverSettings);
        auto& kikimr = testHelper.GetKikimr();

        auto db = kikimr.GetQueryClient();

        enum EEx {
            Empty,
            IfExists,
            IfNotExists,
        };

        auto checkCreate = [&](bool expectSuccess, EEx exMode, const TString& objPath, bool isStore) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfExists);
            const TString ifNotExistsStatement = exMode == EEx::IfNotExists ? "IF NOT EXISTS" : "";
            const TString objType = isStore ? "TABLESTORE" : "TABLE";
            const TString hash = !isStore ? " PARTITION BY HASH(Key) " : "";
            auto sql = TStringBuilder() << R"(
                --!syntax_v1
                CREATE )" << objType << " " << ifNotExistsStatement << " `" << objPath << R"(` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                ))" << hash << R"(
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
                );)";

            auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT(result.GetResultSets().empty());
        };

        auto checkAlter = [&](const TString& objPath, bool isStore) {
            const TString objType = isStore ? "TABLESTORE" : "TABLE";
            {
                auto sql = TStringBuilder() << R"(
                    --!syntax_v1
                    ALTER )" << objType << " `" << objPath << R"(`
                        ADD COLUMN NewColumn Uint64;
                    ;)";

                auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            {
                auto sql = TStringBuilder() << R"(
                    --!syntax_v1
                    ALTER )" << objType << " `" << objPath << R"(`
                        DROP COLUMN NewColumn;
                    ;)";

                auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
        };

        auto checkDrop = [&](bool expectSuccess, EEx exMode,
                const TString& objPath, bool isStore) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfNotExists);
            const TString ifExistsStatement = exMode == EEx::IfExists ? "IF EXISTS" : "";
            const TString objType = isStore ? "TABLESTORE" : "TABLE";
            auto sql = TStringBuilder() << R"(
                --!syntax_v1
                DROP )" << objType << " " << ifExistsStatement << " `" << objPath << R"(`;)";

            auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT(result.GetResultSets().empty());
        };

        auto checkAddRow = [&](const TString& objPath) {
            const size_t inserted_rows = 5;
            TTestHelper::TColumnTable testTable;
            testTable.SetName(objPath)
                .SetPrimaryKey({"Key"})
                .SetSharding({"Key"})
                .SetSchema(schema);
            {
                TTestHelper::TUpdatesBuilder tableInserter(testTable.GetArrowSchema(schema));
                for (size_t i = 0; i < inserted_rows; i++) {
                    tableInserter.AddRow().Add(i).Add("test_res_" + std::to_string(i));
                }
                testHelper.BulkUpsert(testTable, tableInserter);
            }

            Sleep(TDuration::Seconds(100));

            auto sql = TStringBuilder() << R"(
                SELECT Value FROM `)" << objPath << R"(` WHERE Key=1)";

            testHelper.ReadData(sql, "[[[\"test_res_1\"]]]");
        };

        checkCreate(true, EEx::Empty, "/Root/TableStoreTest", true);
        checkCreate(false, EEx::Empty, "/Root/TableStoreTest", true);
        checkCreate(true, EEx::IfNotExists, "/Root/TableStoreTest", true);
        checkDrop(true, EEx::Empty, "/Root/TableStoreTest", true);
        checkDrop(true, EEx::IfExists, "/Root/TableStoreTest", true);
        checkDrop(false, EEx::Empty, "/Root/TableStoreTest", true);

        checkCreate(true, EEx::IfNotExists, "/Root/TableStoreTest", true);
        checkCreate(false, EEx::Empty, "/Root/TableStoreTest", true);
        checkDrop(true, EEx::IfExists, "/Root/TableStoreTest", true);
        checkDrop(false, EEx::Empty, "/Root/TableStoreTest", true);

        checkCreate(true, EEx::IfNotExists, "/Root/ColumnTable", false);
        checkAlter("/Root/ColumnTable", false);
        checkDrop(true, EEx::IfExists, "/Root/ColumnTable", false);

        checkCreate(true, EEx::Empty, "/Root/ColumnTable", false);
        checkCreate(false, EEx::Empty, "/Root/ColumnTable", false);
        checkCreate(true, EEx::IfNotExists, "/Root/ColumnTable", false);
        checkAddRow("/Root/ColumnTable");
        checkDrop(true, EEx::IfExists, "/Root/ColumnTable", false);
        checkDrop(false, EEx::Empty, "/Root/ColumnTable", false);
        checkDrop(true, EEx::IfExists, "/Root/ColumnTable", false);
    }

    Y_UNIT_TEST(DdlUser) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            CREATE USER user1 PASSWORD 'password1';
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE USER user1 PASSWORD 'password1';
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER USER user1 WITH ENCRYPTED PASSWORD 'password3';
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            DROP USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER USER user1 WITH ENCRYPTED PASSWORD 'password3';
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            DROP USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            DROP USER IF EXISTS user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(CreateTempTable) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            const auto queryCreate = Q_(R"(
                --!syntax_v1
                CREATE TEMP TABLE Temp (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );)");

            auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

            const auto querySelect = Q_(R"(
                --!syntax_v1
                SELECT * FROM Temp;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());

            bool allDoneOk = true;
            NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

            UNIT_ASSERT(allDoneOk);
        }

        {
            const auto querySelect = Q_(R"(
                --!syntax_v1
                SELECT * FROM Temp;
            )");

            auto resultSelect = client.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
        }
    }

    Y_UNIT_TEST(TempTablesDrop) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});
        TKikimrRunner kikimr(
            serverSettings.SetWithSampleTables(false).SetEnableTempTables(true));
        auto clientConfig = NGRpcProxy::TGRpcClientConfig(kikimr.GetEndpoint());
        auto client = kikimr.GetQueryClient();

        auto session = client.GetSession().GetValueSync().GetSession();
        auto id = session.GetId();

        const auto queryCreate = Q_(R"(
            --!syntax_v1
            CREATE TEMPORARY TABLE Temp (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );)");

        auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        {
            const auto querySelect = Q_(R"(
                --!syntax_v1
                SELECT * FROM Temp;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());
        }

        const auto queryDrop = Q_(R"(
            --!syntax_v1
            DROP TABLE Temp;
        )");

        auto resultDrop = session.ExecuteQuery(
            queryDrop, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultDrop.IsSuccess(), resultDrop.GetIssues().ToString());

        {
            const auto querySelect = Q_(R"(
                --!syntax_v1
                SELECT * FROM Temp;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
        }

        bool allDoneOk = true;
        NTestHelpers::CheckDelete(clientConfig, id, Ydb::StatusIds::SUCCESS, allDoneOk);

        UNIT_ASSERT(allDoneOk);

        auto sessionAnother = client.GetSession().GetValueSync().GetSession();
        auto idAnother = sessionAnother.GetId();
        UNIT_ASSERT(id != idAnother);

        {
            const auto querySelect = Q_(R"(
                --!syntax_v1
                SELECT * FROM Temp;
            )");

            auto resultSelect = sessionAnother.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!resultSelect.IsSuccess());
        }
    }

    Y_UNIT_TEST(DdlGroup) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        // Check create and drop group
        auto result = db.ExecuteQuery(R"(
            CREATE GROUP group1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE GROUP group1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            DROP GROUP group1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE GROUP group1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            DROP GROUP group2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            DROP GROUP IF EXISTS group2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        // Check rename group
        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 RENAME TO group2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE GROUP group1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE GROUP group2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        // Check add and drop users
        result = db.ExecuteQuery(R"(
            CREATE USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE USER user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 ADD USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 ADD USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Info: Success, code: 4 subissue: { <main>: Info: Role \"user1\" is already a member of role \"group1\", code: 2 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 ADD USER user3;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 DROP USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 DROP USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Warning: Success, code: 4 subissue: { <main>: Warning: Role \"user1\" is not a member of role \"group1\", code: 3 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 DROP USER user3;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Warning: Success, code: 4 subissue: { <main>: Warning: Role \"user3\" is not a member of role \"group1\", code: 3 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 ADD USER user1, user3, user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 ADD USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Info: Success, code: 4 subissue: { <main>: Info: Role \"user1\" is already a member of role \"group1\", code: 2 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 ADD USER user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 0);

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 DROP USER user1, user3, user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Warning: Success, code: 4 subissue: { <main>: Warning: Role \"user3\" is not a member of role \"group1\", code: 3 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 DROP USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Warning: Success, code: 4 subissue: { <main>: Warning: Role \"user1\" is not a member of role \"group1\", code: 3 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group1 DROP USER user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Warning: Success, code: 4 subissue: { <main>: Warning: Role \"user2\" is not a member of role \"group1\", code: 3 } }");

        //Check create with users
        result = db.ExecuteQuery(R"(
            CREATE GROUP group3 WITH USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE GROUP group3;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            ALTER GROUP group3 ADD USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Info: Success, code: 4 subissue: { <main>: Info: Role \"user1\" is already a member of role \"group3\", code: 2 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group3 ADD USER user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE GROUP group4 WITH USER user1, user3, user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());

        result = db.ExecuteQuery(R"(
            CREATE GROUP group4;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Error: Group already exists, code: 2029 }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group4 ADD USER user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 1);
        UNIT_ASSERT(result.GetIssues().ToOneLineString() == "{ <main>: Info: Success, code: 4 subissue: { <main>: Info: Role \"user1\" is already a member of role \"group4\", code: 2 } }");

        result = db.ExecuteQuery(R"(
            ALTER GROUP group4 ADD USER user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT(result.GetIssues().Size() == 0);
    }

    struct ExpectedPermissions {
        TString Path;
        THashMap<TString, TVector<TString>> Permissions;
    };

    Y_UNIT_TEST(DdlPermission) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const auto checkPermissions = [](NYdb::NTable::TSession& session, TVector<ExpectedPermissions>&& expectedPermissionsValues) {
            for (auto& value : expectedPermissionsValues) {
                NYdb::NTable::TDescribeTableResult describe = session.DescribeTable(value.Path).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
                auto tableDesc = describe.GetTableDescription();
                const auto& permissions = tableDesc.GetPermissions();

                THashMap<TString, TVector<TString>> describePermissions;
                for (const auto& permission : permissions) {
                    auto& permissionNames = describePermissions[permission.Subject];
                    permissionNames.insert(permissionNames.end(), permission.PermissionNames.begin(), permission.PermissionNames.end());
                }

                auto& expectedPermissions = value.Permissions;
                UNIT_ASSERT_VALUES_EQUAL_C(expectedPermissions.size(), describePermissions.size(), "Number of user names does not equal on path: " + value.Path);
                for (auto& item : expectedPermissions) {
                    auto& expectedPermissionNames = item.second;
                    auto& describedPermissionNames = describePermissions[item.first];
                    UNIT_ASSERT_VALUES_EQUAL_C(expectedPermissionNames.size(), describedPermissionNames.size(), "Number of permissions for " + item.first + " does not equal on path: " + value.Path);
                    sort(expectedPermissionNames.begin(), expectedPermissionNames.end());
                    sort(describedPermissionNames.begin(), describedPermissionNames.end());
                    UNIT_ASSERT_VALUES_EQUAL_C(expectedPermissionNames, describedPermissionNames, "Permissions are not equal on path: " + value.Path);
                }
            }
        };

        auto result = db.ExecuteQuery(R"(
            GRANT ROW SELECT ON `/Root` TO user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unexpected token 'ROW'");
        checkPermissions(session, {{.Path = "/Root", .Permissions = {}}});

        result = db.ExecuteQuery(R"(
            GRANT `ydb.database.connect` ON `/Root` TO user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unexpected token '`ydb.database.connect`'");
        checkPermissions(session, {{.Path = "/Root", .Permissions = {}}});

        result = db.ExecuteQuery(R"(
            GRANT CONNECT, READ ON `/Root` TO user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unexpected token 'READ'");
        checkPermissions(session, {{.Path = "/Root", .Permissions = {}}});

        result = db.ExecuteQuery(R"(
            GRANT "" ON `/Root` TO user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Unknown permission name: ");
        checkPermissions(session, {{.Path = "/Root", .Permissions = {}}});

        result = db.ExecuteQuery(R"(
            CREATE TABLE TestDdl1 (
                Key Uint64,
                PRIMARY KEY (Key)
            );
        )", TTxControl::NoTx()).ExtractValueSync();

        result = db.ExecuteQuery(R"(
            CREATE TABLE TestDdl2 (
                Key Uint64,
                PRIMARY KEY (Key)
            );
        )", TTxControl::NoTx()).ExtractValueSync();

        result = db.ExecuteQuery(R"(
            GRANT CONNECT ON `/Root` TO user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {{"user1", {"ydb.database.connect"}}}},
            {.Path = "/Root/TestDdl1", .Permissions = {}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE "ydb.database.connect" ON `/Root` FROM user1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {}},
            {.Path = "/Root/TestDdl1", .Permissions = {}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            GRANT MODIFY TABLES, 'ydb.tables.read' ON `/Root/TestDdl1`, `/Root/TestDdl2` TO user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {}},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE SELECT TABLES, "ydb.tables.modify", ON `/Root/TestDdl2` FROM user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {}},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            GRANT "ydb.generic.read", LIST, "ydb.generic.write", USE LEGACY ON `/Root` TO user3, user4, user5;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {
                {"user3", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                {"user4", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}
            }},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE "ydb.generic.use_legacy", SELECT, "ydb.generic.list", INSERT ON `/Root` FROM user4, user3;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {{"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}}},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            GRANT ALL ON `/Root` TO user6;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {
                {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                {"user6", {"ydb.generic.full"}}
            }},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE ALL PRIVILEGES ON `/Root` FROM user6;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {{"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}}},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            GRANT "ydb.generic.use", "ydb.generic.manage" ON `/Root` TO user7 WITH GRANT OPTION;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {
                {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                {"user7", {"ydb.generic.use", "ydb.generic.manage", "ydb.access.grant"}}
            }},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE GRANT OPTION FOR USE, MANAGE ON `/Root` FROM user7;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {{"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}}},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            GRANT USE LEGACY, FULL LEGACY, FULL, CREATE, DROP, GRANT,
                  SELECT ROW, UPDATE ROW, ERASE ROW, SELECT ATTRIBUTES,
                  MODIFY ATTRIBUTES, CREATE DIRECTORY, CREATE TABLE, CREATE QUEUE,
                  REMOVE SCHEMA, DESCRIBE SCHEMA, ALTER SCHEMA ON `/Root` TO user8;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {
                {"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}},
                {"user8", {"ydb.generic.use_legacy", "ydb.generic.full_legacy", "ydb.generic.full", "ydb.database.create",
                    "ydb.database.drop", "ydb.access.grant", "ydb.granular.select_row", "ydb.granular.update_row",
                    "ydb.granular.erase_row", "ydb.granular.read_attributes", "ydb.granular.write_attributes",
                    "ydb.granular.create_directory", "ydb.granular.create_table", "ydb.granular.create_queue",
                    "ydb.granular.remove_schema", "ydb.granular.describe_schema", "ydb.granular.alter_schema"}}
            }},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE "ydb.granular.write_attributes", "ydb.granular.create_directory", "ydb.granular.create_table", "ydb.granular.create_queue",
                   "ydb.granular.select_row", "ydb.granular.update_row", "ydb.granular.erase_row", "ydb.granular.read_attributes",
                   "ydb.generic.use_legacy", "ydb.generic.full_legacy", "ydb.generic.full", "ydb.database.create", "ydb.database.drop", "ydb.access.grant",
                   "ydb.granular.remove_schema", "ydb.granular.describe_schema", "ydb.granular.alter_schema" ON `/Root` FROM user8;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {{"user5", {"ydb.generic.read", "ydb.generic.list", "ydb.generic.write", "ydb.generic.use_legacy"}}}},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE LIST, INSERT ON `/Root` FROM user9, user4, user5;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {{"user5", {"ydb.generic.read", "ydb.generic.use_legacy"}}}},
            {.Path = "/Root/TestDdl1", .Permissions = {{"user2", {"ydb.tables.read", "ydb.tables.modify"}}}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });

        result = db.ExecuteQuery(R"(
            REVOKE ALL ON `/Root`, `/Root/TestDdl1` FROM user9, user4, user5, user2;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        checkPermissions(session, {
            {.Path = "/Root", .Permissions = {}},
            {.Path = "/Root/TestDdl1", .Permissions = {}},
            {.Path = "/Root/TestDdl2", .Permissions = {}}
        });
    }

    Y_UNIT_TEST(DdlSecret) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        enum EEx {
            Empty,
            IfExists,
            IfNotExists,
        };

        auto executeSql = [&](const TString& sql, bool expectSuccess) {
            Cerr << "Execute SQL:\n" << sql << Endl;

            auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT(result.GetResultSets().empty());
        };

        auto checkCreate = [&](bool expectSuccess, EEx exMode, int nameSuffix) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfExists);
            const TString ifNotExistsStatement = exMode == EEx::IfNotExists ? "IF NOT EXISTS" : "";
            const TString sql = fmt::format(R"sql(
                CREATE OBJECT {if_not_exists} my_secret_{name_suffix} (TYPE SECRET) WITH (value="qwerty");
                )sql",
                "if_not_exists"_a = ifNotExistsStatement,
                "name_suffix"_a = nameSuffix
            );

            executeSql(sql, expectSuccess);
        };

        auto checkAlter = [&](bool expectSuccess, int nameSuffix) {
            const TString sql = fmt::format(R"sql(
                ALTER OBJECT my_secret_{name_suffix} (TYPE SECRET) SET value = "abcde";
                )sql",
                "name_suffix"_a = nameSuffix
            );

            executeSql(sql, expectSuccess);
        };

        auto checkUpsert = [&](bool expectSuccess, int nameSuffix) {
            const TString sql = fmt::format(R"sql(
                UPSERT OBJECT my_secret_{name_suffix} (TYPE SECRET) WITH value = "edcba";
                )sql",
                "name_suffix"_a = nameSuffix
            );

            executeSql(sql, expectSuccess);
        };

        auto checkDrop = [&](bool expectSuccess, EEx exMode, int nameSuffix) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfNotExists);
            const TString ifExistsStatement = exMode == EEx::IfExists ? "IF EXISTS" : "";
            const TString sql = fmt::format(R"sql(
                DROP OBJECT {if_exists} my_secret_{name_suffix} (TYPE SECRET);
                )sql",
                "if_exists"_a = ifExistsStatement,
                "name_suffix"_a = nameSuffix
            );

            executeSql(sql, expectSuccess);
        };

        checkCreate(true, EEx::Empty, 0);
        checkCreate(false, EEx::Empty, 0);
        checkAlter(true, 0);
        checkAlter(false, 2); // not exists
        checkDrop(true, EEx::Empty, 0);
        checkDrop(true, EEx::Empty, 0); // we don't check object existence

        checkCreate(true, EEx::IfNotExists, 1);
        checkCreate(true, EEx::IfNotExists, 1);
        checkDrop(true, EEx::IfExists, 1);
        checkDrop(true, EEx::IfExists, 1);

        checkUpsert(true, 2);
        checkCreate(false, EEx::Empty, 2); // already exists
        checkUpsert(true, 2);
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

    Y_UNIT_TEST(DdlExecuteScript) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting})
            .SetEnableScriptExecutionOperations(true);

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        const TString sql = R"sql(
            CREATE TABLE TestDdlExecuteScript (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )sql";

        auto scriptExecutionOperation = db.ExecuteScript(sql).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
        UNIT_ASSERT(scriptExecutionOperation.Metadata().ExecutionId);

        NYdb::NOperation::TOperationClient client(kikimr.GetDriver());
        TMaybe<NYdb::NQuery::TScriptExecutionOperation> readyOp;
        while (true) {
            auto op = client.Get<NYdb::NQuery::TScriptExecutionOperation>(scriptExecutionOperation.Id()).GetValueSync();
            if (op.Ready()) {
                readyOp = std::move(op);
                break;
            }
            UNIT_ASSERT_C(op.Status().IsSuccess(), TStringBuilder() << op.Status().GetStatus() << ":" << op.Status().GetIssues().ToString());
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT_C(readyOp->Status().IsSuccess(), readyOp->Status().GetIssues().ToString());
        UNIT_ASSERT_EQUAL_C(readyOp->Metadata().ExecStatus, EExecStatus::Completed, readyOp->Status().GetIssues().ToString());
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
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    Y_UNIT_TEST(Tcl) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto result = db.ExecuteQuery(R"(
            SELECT 1;
            COMMIT;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));

        result = db.ExecuteQuery(R"(
            SELECT 1;
            ROLLBACK;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
        UNIT_ASSERT(HasIssue(result.GetIssues(), NYql::TIssuesIds::KIKIMR_BAD_OPERATION));
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
            
    Y_UNIT_TEST(ReplaceIntoWithDefaultValue) {
        NKikimrConfig::TAppConfig appConfig;
	appConfig.MutableTableServiceConfig()->SetEnableColumnsWithDefault(true);
	auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        // auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto client = kikimr.GetQueryClient();

        {
            auto createTable = client.ExecuteQuery(R"sql(
                CREATE TABLE `/Root/test/tb` (
                    id UInt32,
                    val UInt32 NOT NULL DEFAULT(100),
                    PRIMARY KEY(id)
                );
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(createTable.IsSuccess(), createTable.GetIssues().ToString());
        }

        {
            auto replaceValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/test/tb` (id) VALUES
                    ( 1 )
                ;
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(replaceValues.IsSuccess(), replaceValues.GetIssues().ToString());
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
