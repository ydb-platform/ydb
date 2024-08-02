#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/common/columnshard.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
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

    Y_UNIT_TEST(ExecuteQueryWithWorkloadManager) {
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableResourcePools(true);

        auto kikimr = TKikimrRunner(TKikimrSettings()
            .SetAppConfig(config)
            .SetEnableResourcePools(true));
        auto db = kikimr.GetQueryClient();

        TExecuteQuerySettings settings;

        {  // Existing pool
            settings.PoolId("default");

            const TString query = "SELECT Key, Value2 FROM TwoShard WHERE Value2 > 0 ORDER BY Key";
            auto result = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            CheckQueryResult(result);
        }

        {  // Not existing pool (check workload manager enabled)
            settings.PoolId("another_pool_id");

            const TString query = "SELECT Key, Value2 FROM TwoShard WHERE Value2 > 0 ORDER BY Key";
            auto result = db.ExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::NOT_FOUND, result.GetIssues().ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Resource pool another_pool_id not found");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Failed to resolve pool id another_pool");
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Query failed during adding/waiting in workload pool");
        }
    }

    std::pair<ui32, ui32> CalcRowsAndBatches(TExecuteQueryIterator& it) {
        ui32 totalRows = 0;
        ui32 totalBatches = 0;
        for (;;) {
            auto streamPart = it.ReadNext().GetValueSync();
            if (!streamPart.IsSuccess()) {
                UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
                break;
            }

            if (streamPart.HasResultSet()) {
                auto result = streamPart.ExtractResultSet();
                UNIT_ASSERT(!result.Truncated());
                totalRows += result.RowsCount();
                totalBatches++;
            }
        }
        return {totalRows, totalBatches};
    }

    Y_UNIT_TEST(FlowControllOnHugeLiteralAsTable) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        const TString query = "SELECT * FROM AS_TABLE(ListReplicate(AsStruct(\"12345678\" AS Key), 100000))";

        {
            // Check range for chunk size settings
            auto settings = TExecuteQuerySettings().OutputChunkMaxSize(48_MB);
            auto it = db.StreamExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            auto streamPart = it.ReadNext().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(streamPart.GetStatus(), EStatus::BAD_REQUEST, streamPart.GetIssues().ToString());
        }

        auto settings = TExecuteQuerySettings().OutputChunkMaxSize(10000);
        auto it = db.StreamExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto [totalRows, totalBatches] = CalcRowsAndBatches(it);

        UNIT_ASSERT_VALUES_EQUAL(totalRows, 100000);
        // 100000 rows * 9 (?) byte per row / 10000 chunk size limit -> expect 90 batches
        UNIT_ASSERT(totalBatches >= 90); // but got 91 in our case
        UNIT_ASSERT(totalBatches < 100);
    }

    TString GetQueryToFillTable(bool longRow) {
        TString s = "12345678";
        int rows = 100000;
        if (longRow) {
            rows /= 1000;
            s.resize(1000, 'x');
        }
        return Sprintf("UPSERT INTO test SELECT * FROM AS_TABLE (ListMap(ListEnumerate(ListReplicate(\"%s\", %d)), "
                       "($x) -> {RETURN AsStruct($x.0 AS Key, $x.1 as Value)}))",
                       s.c_str(), rows);
    }

    void DoFlowControllOnHugeRealTable(bool longRow) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        {
            const TString q = "CREATE TABLE test (Key Uint64, Value String, PRIMARY KEY (Key))";
            auto r = db.ExecuteQuery(q, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(r.GetStatus(), EStatus::SUCCESS, r.GetIssues().ToString());
        }

        {
            auto q = GetQueryToFillTable(longRow);
            auto r = db.ExecuteQuery(q, TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(r.GetStatus(), EStatus::SUCCESS, r.GetIssues().ToString());
        }

        const TString query = "SELECT * FROM test";
        if (longRow) {
            // Check the case of limit less than one row size - expect one batch for each row
            auto settings = TExecuteQuerySettings().OutputChunkMaxSize(100);
            auto it = db.StreamExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

            auto [totalRows, totalBatches] = CalcRowsAndBatches(it);

            UNIT_ASSERT_VALUES_EQUAL(totalRows, 100);
            UNIT_ASSERT_VALUES_EQUAL(totalBatches, 100);
        }

        auto settings = TExecuteQuerySettings().OutputChunkMaxSize(10000);
        auto it = db.StreamExecuteQuery(query, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());

        auto [totalRows, totalBatches] = CalcRowsAndBatches(it);

        Cerr << totalBatches << Endl;
        if (longRow) {
            UNIT_ASSERT_VALUES_EQUAL(totalRows, 100);
            // 100 rows * 1000 byte per row / 10000 chunk size limit -> expect 10 batches
            UNIT_ASSERT(9 <= totalBatches);
            UNIT_ASSERT_LT_C(totalBatches, 13, totalBatches);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(totalRows, 100000);
            // 100000 rows * 12 byte per row / 10000 chunk size limit -> expect 120 batches
            UNIT_ASSERT(119 <= totalBatches);
            UNIT_ASSERT_LT_C(totalBatches, 123, totalBatches);
        }
    }

    Y_UNIT_TEST_TWIN(FlowControllOnHugeRealTable, LongRow) {
        DoFlowControllOnHugeRealTable(LongRow);
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

    Y_UNIT_TEST(IssuesInCaseOfSuccess) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        CreateSampleTablesWithIndex(session, true);
        auto selectRes = db.ExecuteQuery(
            "SELECT Value FROM `/Root/SecondaryKeys` VIEW Index WHERE Key = 2",
            TTxControl::BeginTx().CommitTx()
        ).ExtractValueSync();

        UNIT_ASSERT_C(selectRes.IsSuccess(), selectRes.GetIssues().ToString());
        const TString expected = R"([[["Payload2"]]])";
        CompareYson(expected, FormatResultSetYson(selectRes.GetResultSet(0)));
        UNIT_ASSERT_C(HasIssue(selectRes.GetIssues(), NYql::TIssuesIds::KIKIMR_WRONG_INDEX_USAGE,
            [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("Given predicate is not suitable for used index: Index");
            }), selectRes.GetIssues().ToString());
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
        UNIT_ASSERT(!result.GetTransaction());

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
        int attempt = 10;
        while (attempt-- && db.GetActiveSessionCount() > 0) {
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_ASSERT_VALUES_EQUAL(db.GetActiveSessionCount(), 0);
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

    Y_UNIT_TEST(ExecStatsAst) {
        auto kikimr = DefaultKikimrRunner();
        auto db = kikimr.GetQueryClient();

        auto settings = TExecuteQuerySettings()
            .StatsMode(EStatsMode::Full);

        std::vector<std::pair<TString, EStatus>> cases = {
            { "SELECT 42 AS test_ast_column", EStatus::SUCCESS },
            { "SELECT test_ast_column FROM TwoShard", EStatus::GENERIC_ERROR },
            { "SELECT UNWRAP(42 / 0) AS test_ast_column", EStatus::PRECONDITION_FAILED },
        };

        for (const auto& [sql, status] : cases) {
            auto result = db.ExecuteQuery(sql, TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), status, result.GetIssues().ToString());

            UNIT_ASSERT(result.GetStats().Defined());
            UNIT_ASSERT(result.GetStats()->GetAst().Defined());
            UNIT_ASSERT_STRING_CONTAINS(*result.GetStats()->GetAst(), "test_ast_column");
        }
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

        auto checkRename = [&](bool expectSuccess, int nameSuffix, int nameSuffixTo) {
            const TString sql = fmt::format(R"sql(
                ALTER TABLE TestDdl_{name_suffix} RENAME TO TestDdl_{name_suffix_to}
                )sql",
                "name_suffix"_a = nameSuffix,
                "name_suffix_to"_a = nameSuffixTo
            );

            auto result = db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
            if (expectSuccess) {
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            } else {
                UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            }
            UNIT_ASSERT(result.GetResultSets().empty());
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

        // rename
        Y_UNUSED(checkRename);
        /*
        checkCreate(true, EEx::Empty, 2);
        checkRename(true, 2, 3);
        checkRename(false, 2, 3); // already renamed, no such table
        checkDrop(false, EEx::Empty, 2); // no such table
        checkDrop(true, EEx::Empty, 3);
        */
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

            const TVector<TString> sessionIdSplitted = StringSplitter(id).SplitByString("&id=");
            const auto queryCreateRestricted = Q_(fmt::format(R"(
                --!syntax_v1
                CREATE TABLE `/Root/.tmp/sessions/{}/Test` (
                    Key Uint64 NOT NULL,
                    Value String,
                    PRIMARY KEY (Key)
                );)", sessionIdSplitted.back()));

            auto resultCreateRestricted = session.ExecuteQuery(queryCreateRestricted, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT(!resultCreateRestricted.IsSuccess());
            UNIT_ASSERT_C(resultCreateRestricted.GetIssues().ToString().Contains("error: path is temporary"), resultCreateRestricted.GetIssues().ToString());

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

        Sleep(TDuration::Seconds(1));

        {
            auto schemeClient = kikimr.GetSchemeClient();
            auto listResult = schemeClient.ListDirectory("/Root/.tmp/sessions").GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(listResult.GetStatus(), NYdb::EStatus::SUCCESS, listResult.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(listResult.GetChildren().size(), 0);
        }
    }

    Y_UNIT_TEST(AlterTempTable) {
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
        auto settings = NYdb::NQuery::TExecuteQuerySettings()
            .StatsMode(NYdb::NQuery::EStatsMode::Basic);
        {
            auto session = client.GetSession().GetValueSync().GetSession();
            auto id = session.GetId();

            {
                const auto query = Q_(R"(
                    --!syntax_v1
                    CREATE TEMP TABLE Temp (
                        Key Int32 NOT NULL,
                        Value Int32,
                        PRIMARY KEY (Key)
                    );)");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_v1
                    ALTER TABLE Temp DROP COLUMN Value;
                )");
                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_v1
                    DROP TABLE Temp;
                )");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_v1
                    CREATE TEMP TABLE Temp (
                        Key Int32 NOT NULL,
                        Value Int32,
                        PRIMARY KEY (Key)
                    );)");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

                auto resultInsert = session.ExecuteQuery(R"(
                    UPSERT INTO Temp (Key, Value) VALUES (1, 1);
                )", NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(
                    resultInsert.GetStatus(), EStatus::SUCCESS, resultInsert.GetIssues().ToString());
            }

            {
                const auto query = Q_(R"(
                    --!syntax_v1
                    SELECT * FROM Temp;
                )");

                auto result = session.ExecuteQuery(
                    query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

                UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
                CompareYson(R"([[1;[1]]])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                auto result = session.ExecuteQuery(R"(
                    ALTER TABLE Temp DROP COLUMN Value;
                )", NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
            }

            {
                const auto query = Q_(R"(
                    --!syntax_v1
                    SELECT * FROM Temp;
                )");

                auto result = session.ExecuteQuery(
                    query, NYdb::NQuery::TTxControl::BeginTx().CommitTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);

                UNIT_ASSERT_C(!result.GetResultSets().empty(), "results are empty");
                CompareYson(R"([[1]])", FormatResultSetYson(result.GetResultSet(0)));
            }

            {
                const auto query = Q_(R"(
                    --!syntax_v1
                    DROP TABLE Temp;
                )");

                auto result =
                    session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), settings).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto stats = NYdb::TProtoAccessor::GetProto(*result.GetStats());
                UNIT_ASSERT_VALUES_EQUAL(stats.compilation().from_cache(), false);
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
            CREATE TEMPORARY TABLE `/Root/test/Temp` (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );)");

        auto resultCreate = session.ExecuteQuery(queryCreate, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultCreate.IsSuccess(), resultCreate.GetIssues().ToString());

        {
            const auto querySelect = Q_(R"(
                --!syntax_v1
                SELECT * FROM `/Root/test/Temp`;
            )");

            auto resultSelect = session.ExecuteQuery(
                querySelect, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(resultSelect.IsSuccess(), resultSelect.GetIssues().ToString());
        }

        const auto queryDrop = Q_(R"(
            --!syntax_v1
            DROP TABLE `/Root/test/Temp`;
        )");

        auto resultDrop = session.ExecuteQuery(
            queryDrop, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(resultDrop.IsSuccess(), resultDrop.GetIssues().ToString());

        {
            const auto querySelect = Q_(R"(
                --!syntax_v1
                SELECT * FROM `/Root/test/Temp`;
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
                SELECT * FROM `/Root/test/Temp`;
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
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(false);
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

    Y_UNIT_TEST(DdlWithExplicitTransaction) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        {
            // DDl with explicit transaction
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdlDml1 (
                    Key Uint64,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Scheme operations cannot be executed inside transaction"));
        }

        {
            // DDl with explicit transaction
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdlDml1 (
                    Key Uint64,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE TestDdlDml2 (
                    Key Uint64,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Scheme operations cannot be executed inside transaction"));
        }

        {
            // DDl with implicit transaction
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdlDml1 (
                    Key Uint64,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            // DDl + DML with explicit transaction
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdlDml1;
                CREATE TABLE TestDdlDml2 (
                    Key Uint64,
                    PRIMARY KEY (Key)
                );
                SELECT * FROM TestDdlDml2;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Queries with mixed data and scheme operations are not supported."));
        }
    }

    Y_UNIT_TEST(Ddl_Dml) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        {
            // DDl + DML with implicit transaction
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdlDml2 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
                UPSERT INTO TestDdlDml2 (Key, Value1, Value2) VALUES (1, "1", "1");
                SELECT * FROM TestDdlDml2;
                ALTER TABLE TestDdlDml2 DROP COLUMN Value2;
                UPSERT INTO TestDdlDml2 (Key, Value1) VALUES (2, "2");
                SELECT * FROM TestDdlDml2;
                CREATE TABLE TestDdlDml33 (
                    Key Uint64,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
            CompareYson(R"([[[1u];["1"];["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([[[1u];["1"]];[[2u];["2"]]])", FormatResultSetYson(result.GetResultSet(1)));
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdlDml2;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([[[1u];["1"]];[[2u];["2"]]])", FormatResultSetYson(result.GetResultSet(0)));

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdlDml33;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));

            result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdlDml4 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
                UPSERT INTO TestDdlDml4 (Key, Value1, Value2) VALUES (1, "1", "2");
                SELECT * FROM TestDdlDml4;
                ALTER TABLE TestDdlDml4 DROP COLUMN Value2;
                UPSERT INTO TestDdlDml4 (Key, Value1) VALUES (2, "2");
                SELECT * FROM TestDdlDml5;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0);

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdlDml4;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            // Base test with ddl and dml statements
            auto result = db.ExecuteQuery(R"(
                DECLARE $name AS Text;
                $a = (SELECT * FROM TestDdl1);
                CREATE TABLE TestDdl1 (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
                UPSERT INTO TestDdl1 (Key, Value) VALUES (1, "One");
                CREATE TABLE TestDdl2 (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
                UPSERT INTO TestDdl1 (Key, Value) VALUES (2, "Two");
                SELECT * FROM $a;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]]])", FormatResultSetYson(result.GetResultSet(0)));
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                UPSERT INTO TestDdl1 (Key, Value) VALUES (3, "Three");
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl1;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]];[[3u];["Three"]]])", FormatResultSetYson(result.GetResultSet(0)));
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdl1 (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Check failed: path: '/Root/TestDdl1', error: path exist"));

            result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdl2 (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Check failed: path: '/Root/TestDdl2', error: path exist"));

            result = db.ExecuteQuery(R"(
                UPSERT INTO TestDdl2 SELECT * FROM TestDdl1;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
        }

        {
            // Test with query with error
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl2;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]];[[3u];["Three"]]])", FormatResultSetYson(result.GetResultSet(0)));
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                UPSERT INTO TestDdl2 (Key, Value) VALUES (4, "Four");
                CREATE TABLE TestDdl3 (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
                UPSERT INTO TestDdl2 (Key, Value) VALUES (5, "Five");
                CREATE TABLE TestDdl2 (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE TestDdl4 (
                    Key Uint64,
                    Value String,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0);
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Check failed: path: '/Root/TestDdl2', error: path exist"));

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl2;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]];[[3u];["Three"]]])", FormatResultSetYson(result.GetResultSet(0)));
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl3;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl4;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Cannot find table 'db.[/Root/TestDdl4]'"));
        }

        {
            // Check result sets
            auto result = db.ExecuteQuery(R"(
                $a = (SELECT * FROM TestDdl1);
                SELECT * FROM $a;
                UPSERT INTO TestDdl1 (Key, Value) VALUES (4, "Four");
                SELECT * FROM $a;
                CREATE TABLE TestDdl4 (
                    Key Uint64,
                    Value Uint64,
                    PRIMARY KEY (Key)
                );
                UPSERT INTO TestDdl4 (Key, Value) VALUES (1, 1);
                SELECT * FROM TestDdl4;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]];[[3u];["Three"]]])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([[[1u];["One"]];[[2u];["Two"]];[[3u];["Three"]];[[4u];["Four"]]])", FormatResultSetYson(result.GetResultSet(1)));
            CompareYson(R"([[[1u];[1u]]])", FormatResultSetYson(result.GetResultSet(2)));
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            result = db.ExecuteQuery(R"(
                UPSERT INTO TestDdl2 SELECT * FROM TestDdl1;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
        }

        {
            // Check EVALUATE FOR
            auto result = db.ExecuteQuery(R"(
                EVALUATE FOR $i IN AsList(1, 2, 3) DO BEGIN
                    SELECT $i;
                    SELECT $i;
                END DO;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            // Check parser errors
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO TestDdl4 (Key, Value) VALUES (2, 2);
                SELECT * FROM $a;
                UPSERT INTO TestDdl4 (Key, Value) VALUES (3, 3);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Unknown name: $a"));

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl4;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([[[1u];[1u]]])", FormatResultSetYson(result.GetResultSet(0)));

            result = db.ExecuteQuery(R"(
                UPSERT INTO TestDdl4 (Key, Value) VALUES (2, 2);
                UPSERT INTO TestDdl4 (Key, Value) VALUES (3, "3");
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Error: Failed to convert 'Value': String to Optional<Uint64>"));

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl4;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([[[1u];[1u]]])", FormatResultSetYson(result.GetResultSet(0)));

            result = db.ExecuteQuery(R"(
                CREATE TABLE TestDdl5 (
                    Key Uint64,
                    Value Uint64,
                    PRIMARY KEY (Key)
                );
                UPSERT INTO TestDdl5 (Key, Value) VALUES (1, 1);
                UPSERT INTO TestDdl5 (Key, Value) VALUES (3, "3");
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT(result.GetIssues().ToOneLineString().Contains("Error: Failed to convert 'Value': String to Optional<Uint64>"));

            result = db.ExecuteQuery(R"(
                SELECT * FROM TestDdl5;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(SeveralCTAS) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableCreateTableAs(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting})
            .SetWithSampleTables(false)
            .SetEnableTempTables(true);

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE Table1 (
                    PRIMARY KEY (Key)
                ) AS SELECT 1u AS Key, "1" AS Value1, "1" AS Value2;
                CREATE TABLE Table2 (
                    PRIMARY KEY (Key)
                ) AS SELECT 2u AS Key, "2" AS Value1, "2" AS Value2;
                CREATE TABLE Table3 (
                    PRIMARY KEY (Key)
                ) AS SELECT * FROM Table2 UNION ALL SELECT * FROM Table1;
                SELECT * FROM Table1 ORDER BY Key;
                SELECT * FROM Table2 ORDER BY Key;
                SELECT * FROM Table3 ORDER BY Key;
            )", TTxControl::NoTx(), TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);
            // Results are empty. Snapshot was taken before tables were created, so we don't see changes after snapshot.
            // This will be fixed in future, for example, by implicit commit before/after each ddl statement.
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(1)));
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(2)));

            result = db.ExecuteQuery(R"(
                SELECT * FROM Table1 ORDER BY Key;
                SELECT * FROM Table2 ORDER BY Key;
                SELECT * FROM Table3 ORDER BY Key;
            )", TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 3);
            CompareYson(R"([[[1u];["1"];["1"]]])", FormatResultSetYson(result.GetResultSet(0)));
            CompareYson(R"([[[2u];["2"];["2"]]])", FormatResultSetYson(result.GetResultSet(1)));
            // Also empty now(
            CompareYson(R"([])", FormatResultSetYson(result.GetResultSet(2)));
        }
    }

    Y_UNIT_TEST(CheckIsolationLevelFroPerStatementMode) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnablePreparedDdl(true);
        appConfig.MutableTableServiceConfig()->SetEnableAstCache(true);
        appConfig.MutableTableServiceConfig()->SetEnablePerStatementQueryExecution(true);
        auto setting = NKikimrKqp::TKqpSetting();
        auto serverSettings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetKqpSettings({setting});

        TKikimrRunner kikimr(serverSettings);
        auto db = kikimr.GetQueryClient();
        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        {
            // 1 ddl statement
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE Test1 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            NYdb::NTable::TDescribeTableResult describe = session.DescribeTable("/Root/Test1").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
        }

        {
            // 2 ddl statements
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE Test2 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
                CREATE TABLE Test3 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 0);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());

            NYdb::NTable::TDescribeTableResult describe1 = session.DescribeTable("/Root/Test2").GetValueSync();
            UNIT_ASSERT_EQUAL(describe1.GetStatus(), EStatus::SUCCESS);
            NYdb::NTable::TDescribeTableResult describe2 = session.DescribeTable("/Root/Test3").GetValueSync();
            UNIT_ASSERT_EQUAL(describe2.GetStatus(), EStatus::SUCCESS);
        }

        {
            // 1 dml statement
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM Test1;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
        }

        {
            // 2 dml statements
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM Test2;
                SELECT * FROM Test3;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
        }

        {
            // 1 ddl 1 dml statements
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE Test4 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
                SELECT * FROM Test4;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
            NYdb::NTable::TDescribeTableResult describe = session.DescribeTable("/Root/Test4").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
        }

        {
            // 1 dml 1 ddl statements
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM Test4;
                CREATE TABLE Test5 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
            NYdb::NTable::TDescribeTableResult describe = session.DescribeTable("/Root/Test5").GetValueSync();
            UNIT_ASSERT_EQUAL(describe.GetStatus(), EStatus::SUCCESS);
        }

        {
            // 1 ddl 1 dml 1 ddl 1 dml statements
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE Test6 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
                SELECT * FROM Test6;
                CREATE TABLE Test7 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
                SELECT * FROM Test7;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
            NYdb::NTable::TDescribeTableResult describe1 = session.DescribeTable("/Root/Test6").GetValueSync();
            UNIT_ASSERT_EQUAL(describe1.GetStatus(), EStatus::SUCCESS);
            NYdb::NTable::TDescribeTableResult describe2 = session.DescribeTable("/Root/Test7").GetValueSync();
            UNIT_ASSERT_EQUAL(describe2.GetStatus(), EStatus::SUCCESS);
        }

        {
            // 1 dml 1 ddl 1 dml 1 ddl statements
            auto result = db.ExecuteQuery(R"(
                SELECT * FROM Test7;
                CREATE TABLE Test8 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
                SELECT * FROM Test8;
                CREATE TABLE Test9 (
                    Key Uint64,
                    Value1 String,
                    Value2 String,
                    PRIMARY KEY (Key)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 2);
            UNIT_ASSERT_EQUAL_C(result.GetIssues().Size(), 0, result.GetIssues().ToString());
            NYdb::NTable::TDescribeTableResult describe1 = session.DescribeTable("/Root/Test8").GetValueSync();
            UNIT_ASSERT_EQUAL(describe1.GetStatus(), EStatus::SUCCESS);
            NYdb::NTable::TDescribeTableResult describe2 = session.DescribeTable("/Root/Test9").GetValueSync();
            UNIT_ASSERT_EQUAL(describe2.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(TableSink_ReplaceFromSelectOlap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnSource` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);

            CREATE TABLE `/Root/ColumnShard1` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3);

            CREATE TABLE `/Root/ColumnShard2` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnSource` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            // empty
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard1`
                SELECT * FROM `/Root/ColumnSource` WHERE Col3 == 0
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard1` ORDER BY Col1, Col2, Col3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([])");
        }

        {
            // Missing Nullable column
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard1`
                SELECT 10u + Col1 AS Col1, 100 + Col3 AS Col3 FROM `/Root/ColumnSource`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard1` ORDER BY Col1, Col2, Col3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[11u;#;110];[12u;#;111];[13u;#;112];[14u;#;113]])");
        }

        {
            // column -> column
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard2`
                SELECT * FROM `/Root/ColumnSource`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard2` ORDER BY Col1, Col2, Col3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[1u;["test1"];10];[2u;["test2"];11];[3u;["test3"];12];[4u;#;13]])");
        }
    }

    Y_UNIT_TEST(TableSink_BadTransactions) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);

            CREATE TABLE `/Root/DataShard` (
                Col1 Uint64 NOT NULL,
                Col2 String,
                Col3 Int32 NOT NULL,
                PRIMARY KEY (Col1)
            )
            WITH (UNIFORM_PARTITIONS = 2, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (10u, "test1", 10), (20u, "test2", 11), (30u, "test3", 12), (40u, NULL, 13);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            // column -> row
            const TString sql = R"(
                REPLACE INTO `/Root/DataShard`
                SELECT * FROM `/Root/ColumnShard`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Write transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // row -> column
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard`
                SELECT * FROM `/Root/DataShard`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Write transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // column & row write
            const TString sql = R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Write transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // column read & row write
            const TString sql = R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
                SELECT * FROM `/Root/ColumnShard`;
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Write transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }

        {
            // column write & row read
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (1u, "test1", 10), (2u, "test2", 11), (3u, "test3", 12), (4u, NULL, 13);
                SELECT * FROM `/Root/DataShard`;
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT(!insertResult.IsSuccess());
            UNIT_ASSERT_C(
                insertResult.GetIssues().ToString().Contains("Write transactions between column and row tables are disabled at current time"),
                insertResult.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(TableSink_ReplaceFromSelectLargeOlap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TTestHelper testHelper(settings);

        TKikimrRunner& kikimr = testHelper.GetKikimr();
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        TVector<TTestHelper::TColumnSchema> schema = {
            TTestHelper::TColumnSchema().SetName("Col1").SetType(NScheme::NTypeIds::Int64).SetNullable(false),
            TTestHelper::TColumnSchema().SetName("Col2").SetType(NScheme::NTypeIds::Int32).SetNullable(false),
        };

        TTestHelper::TColumnTable testTable1;
        testTable1.SetName("/Root/ColumnShard1").SetPrimaryKey({ "Col1" }).SetSharding({ "Col1" }).SetSchema(schema).SetMinPartitionsCount(1000);
        testHelper.CreateTable(testTable1);

        TTestHelper::TColumnTable testTable2;
        testTable2.SetName("/Root/ColumnShard2").SetPrimaryKey({ "Col1" }).SetSharding({ "Col1" }).SetSchema(schema).SetMinPartitionsCount(1000);
        testHelper.CreateTable(testTable2);

        {
            TTestHelper::TUpdatesBuilder tableInserter(testTable1.GetArrowSchema(schema));
            for (size_t index = 0; index < 10000; ++index) {
                tableInserter.AddRow().Add(index).Add(index * 10);
            }
            testHelper.BulkUpsert(testTable1, tableInserter);
        }

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto client = kikimr.GetQueryClient();

        {
            const TString sql = R"(
                REPLACE INTO `/Root/ColumnShard2`
                SELECT * FROM `/Root/ColumnShard1`
            )";
            auto insertResult = client.ExecuteQuery(sql, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/ColumnShard2`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[10000u]])");
        }
    }

    Y_UNIT_TEST(TableSink_ReplaceColumnShard) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                Col4 String,
                Col3 String NOT NULL,
                PRIMARY KEY (Col1, Col3)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        // Shuffled
        auto client = kikimr.GetQueryClient();
        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col3, Col4, Col2, Col1) VALUES
                    ("test100", "100", 1000, 100u);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col3, Col2, Col4) VALUES
                    (1u, "test1", 10, "1"), (2u, "test2", NULL, "2"), (3u, "test3", 12, NULL), (4u, "test4", NULL, NULL);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        auto it = client.StreamExecuteQuery(R"(
            SELECT Col1, Col3, Col2, Col4 FROM `/Root/ColumnShard` ORDER BY Col1, Col3, Col2, Col4;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        CompareYson(output, R"([[1u;"test1";[10];["1"]];[2u;"test2";#;["2"]];[3u;"test3";[12];#];[4u;"test4";#;#];[100u;"test100";[1000];["100"]]])");
    }

    Y_UNIT_TEST(TableSink_OltpReplace) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/DataShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                Col3 String,
                PRIMARY KEY (Col1)
            );
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        
        {
            auto it = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0);
                REPLACE INTO `/Root/DataShard` (Col1, Col3) VALUES (1u, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]]])");
        }

        { 
            auto it = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 'null');
                REPLACE INTO `/Root/DataShard` (Col1) VALUES (1u);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
        }


        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([[0u;#;["null"]];[1u;#;#]])");
        }
    }

    class TTableDataModificationTester {
    protected:
        NKikimrConfig::TAppConfig AppConfig;
        std::unique_ptr<TKikimrRunner> Kikimr;
        YDB_ACCESSOR(bool, IsOlap, false);
        virtual void DoExecute() = 0;
    public:
        void Execute() {
            AppConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
            AppConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
            AppConfig.MutableTableServiceConfig()->SetEnableKqpDataQueryStreamLookup(true);
            auto settings = TKikimrSettings().SetAppConfig(AppConfig).SetWithSampleTables(false);

            Kikimr = std::make_unique<TKikimrRunner>(settings);
            Tests::NCommon::TLoggerInit(*Kikimr).Initialize();

            auto session = Kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();

            auto csController = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
            csController->SetPeriodicWakeupActivationPeriod(TDuration::Seconds(1));
            csController->SetLagForCompactionBeforeTierings(TDuration::Seconds(1));
            csController->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);

            const TString query = Sprintf(R"(
                CREATE TABLE `/Root/DataShard` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    Col3 String,
                    PRIMARY KEY (Col1)
                ) WITH (
                    STORE = %s,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
                );
            )", IsOlap ? "COLUMN" : "ROW");

            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            DoExecute();
            csController->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Indexation);
            csController->WaitIndexation(TDuration::Seconds(5));
        }

    };

    class TUpsertFromTableTester: public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            {
                auto it = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0);
                UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (1u, 'test');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]]])");
            }

            {
                auto it = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 'null');
                UPSERT INTO `/Root/DataShard` (Col1) VALUES (1u);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.ExecuteQuery(R"(
                UPSERT INTO `/Root/DataShard` (Col3) VALUES ('null');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT(!it.IsSuccess());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];["null"]];[1u;#;["test"]]])");
            }
        }
    };

    Y_UNIT_TEST(TableSink_OltpUpsert) {
        TUpsertFromTableTester tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TableSink_OlapUpsert) {
        TUpsertFromTableTester tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TInsertFromTableTester: public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            {
                auto it = client.ExecuteQuery(R"(
                INSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0);
                INSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (1u, 'test');
                INSERT INTO `/Root/DataShard` (Col1, Col3, Col2) VALUES (2u, 't', 3);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]];[2u;[3];["t"]]])");
            }

            {
                auto it = client.ExecuteQuery(R"(
                INSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (0u, 'null');
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(!it.IsSuccess(), it.GetIssues().ToString());
                UNIT_ASSERT_C(
                    it.GetIssues().ToString().Contains("Operation is aborting because an duplicate key")
                    || it.GetIssues().ToString().Contains("Conflict with existing key."),
                    it.GetIssues().ToString());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]];[2u;[3];["t"]]])");
            }
        }
    };

    Y_UNIT_TEST(TableSink_OltpInsert) {
        TInsertFromTableTester tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TableSink_OlapInsert) {
        TInsertFromTableTester tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TDeleteFromTableTester: public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            {
                auto it = client.ExecuteQuery(R"(
                INSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0);
                INSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (1u, 'test');
                INSERT INTO `/Root/DataShard` (Col1, Col3, Col2) VALUES (2u, 't', 3);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]];[2u;[3];["t"]]])");
            }

            {
                auto it = client.ExecuteQuery(R"(
                DELETE FROM `/Root/DataShard` WHERE Col3 == 't';
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }
            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]]])");
            }

            {
                auto it = client.ExecuteQuery(R"(
                DELETE FROM `/Root/DataShard` WHERE Col3 == 'not found';
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.ExecuteQuery(R"(
                DELETE FROM `/Root/DataShard` ON SELECT 0 AS Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[1u;#;["test"]]])");
            }
        }
    };

    Y_UNIT_TEST(TableSink_OltpDelete) {
        TDeleteFromTableTester tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TableSink_OlapDelete) {
        TDeleteFromTableTester tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    class TUpdateFromTableTester: public TTableDataModificationTester {
    protected:
        void DoExecute() override {
            auto client = Kikimr->GetQueryClient();

            {
                auto it = client.ExecuteQuery(R"(
                INSERT INTO `/Root/DataShard` (Col1, Col2) VALUES (0u, 0);
                INSERT INTO `/Root/DataShard` (Col1, Col3) VALUES (1u, 'test');
                INSERT INTO `/Root/DataShard` (Col1, Col3, Col2) VALUES (2u, 't', 3);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]];[2u;[3];["t"]]])");
            }

            {
                auto it = client.ExecuteQuery(R"(
                UPDATE `/Root/DataShard` SET Col2 = 42 WHERE Col3 == 'not found';
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.ExecuteQuery(R"(
                UPDATE `/Root/DataShard` SET Col2 = 42 WHERE Col3 == 't';
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[0];#];[1u;#;["test"]];[2u;[42];["t"]]])");
            }

            {
                auto it = client.ExecuteQuery(R"(
                UPDATE `/Root/DataShard` ON SELECT 0u AS Col1, 1 AS Col2, 'text' AS Col3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            {
                auto it = client.ExecuteQuery(R"(
                UPDATE `/Root/DataShard` ON SELECT 10u AS Col1, 1 AS Col2, 'text' AS Col3;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_C(it.IsSuccess(), it.GetIssues().ToString());
            }

            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/DataShard` ORDER BY Col1;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
                TString output = StreamResultToYson(it);
                CompareYson(output, R"([[0u;[1];["text"]];[1u;#;["test"]];[2u;[42];["t"]]])");
        }
    };

    Y_UNIT_TEST(TableSink_OltpUpdate) {
        TUpdateFromTableTester tester;
        tester.SetIsOlap(false);
        tester.Execute();
    }

    Y_UNIT_TEST(TableSink_OlapUpdate) {
        TUpdateFromTableTester tester;
        tester.SetIsOlap(true);
        tester.Execute();
    }

    Y_UNIT_TEST(TableSink_ReplaceDuplicatesOlap) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2) VALUES
                    (100u, 1000), (100u, 1000);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2) VALUES
                    (100u, 1000), (100u, 1000);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        auto it = client.StreamExecuteQuery(R"(
            SELECT * FROM `/Root/ColumnShard` ORDER BY Col1, Col2;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
        TString output = StreamResultToYson(it);
        CompareYson(output, R"([[100u;[1000]]])");
    }

    Y_UNIT_TEST(TableSink_DisableSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(false);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(false);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();

        const TString query = R"(
            CREATE TABLE `/Root/ColumnShard` (
                Col1 Uint64 NOT NULL,
                Col2 Int32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 16);
        )";

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        auto client = kikimr.GetQueryClient();
        { 
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2) VALUES (1u, 1)
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT(!prepareResult.IsSuccess());
            UNIT_ASSERT_C(
                prepareResult.GetIssues().ToString().Contains("Data manipulation queries do not support column shard tables."),
                prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT * FROM `/Root/ColumnShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(output, R"([])");
        }
    }

    Y_UNIT_TEST_TWIN(TableSink_ReplaceDataShard, UseSink) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(UseSink);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(UseSink);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);
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

        auto client = kikimr.GetQueryClient();
        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (10u, "test1", 10), (20u, "test2", 11), (2147483647u, "test3", 12), (2147483640u, NULL, 13);
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/DataShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[4u]])");
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/DataShard2` SELECT * FROM `/Root/DataShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/DataShard2`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[4u]])");
        }

        {
            auto prepareResult = client.ExecuteQuery(R"(
                REPLACE INTO `/Root/DataShard2` (Col1, Col2, Col3) VALUES
                    (11u, "test1", 10), (21u, "test2", 11), (2147483646u, "test3", 12), (2147483641u, NULL, 13);
                SELECT COUNT(*) FROM `/Root/DataShard`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(prepareResult.IsSuccess(), prepareResult.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"(
                SELECT COUNT(*) FROM `/Root/DataShard2`;
            )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[8u]])");
        }
    }

    Y_UNIT_TEST(ReadDatashardAndColumnshard) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(true);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(true);
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto client = kikimr.GetQueryClient();

        {
            auto createTable = client.ExecuteQuery(R"sql(
                CREATE TABLE `/Root/DataShard` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    Col3 String,
                    PRIMARY KEY (Col1)
                ) WITH (
                    STORE = ROW,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
                );
                CREATE TABLE `/Root/ColumnShard` (
                    Col1 Uint64 NOT NULL,
                    Col2 Int32,
                    Col3 String,
                    PRIMARY KEY (Col1)
                ) WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
                );
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(createTable.IsSuccess(), createTable.GetIssues().ToString());
        }

        {
            auto replaceValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/DataShard` (Col1, Col2, Col3) VALUES
                    (1u, 1, "row");
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(replaceValues.IsSuccess(), replaceValues.GetIssues().ToString());
        }

        {
            auto replaceValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/ColumnShard` (Col1, Col2, Col3) VALUES
                    (2u, 2, "column");
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(replaceValues.IsSuccess(), replaceValues.GetIssues().ToString());
        }

        {
            auto it = client.StreamExecuteQuery(R"sql(
                SELECT * FROM `/Root/ColumnShard`;
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[2u;[2];["column"]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"sql(
                SELECT * FROM `/Root/DataShard`
                UNION ALL
                SELECT * FROM `/Root/ColumnShard`;
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[1u;[1];["row"]];[2u;[2];["column"]]])");
        }

        {
            auto it = client.StreamExecuteQuery(R"sql(
                SELECT r.Col3, c.Col3 FROM `/Root/DataShard` AS r
                JOIN `/Root/ColumnShard` AS c ON r.Col1 + 1 = c.Col1;
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(it.GetStatus(), EStatus::SUCCESS, it.GetIssues().ToString());
            TString output = StreamResultToYson(it);
            CompareYson(
                output,
                R"([[["row"];["column"]]])");
        }
    }

    Y_UNIT_TEST(ReplaceIntoWithDefaultValue) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableTableServiceConfig()->SetEnableOlapSink(false);
        appConfig.MutableTableServiceConfig()->SetEnableOltpSink(false);
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

    Y_UNIT_TEST(AlterTable_DropNotNull_Valid) {
        NKikimrConfig::TAppConfig appConfig;
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto client = kikimr.GetQueryClient();

        {
            auto createTable = client.ExecuteQuery(R"sql(
                CREATE TABLE `/Root/test/alterDropNotNull` (
                    id Int32 NOT NULL,
                    val Int32 NOT NULL,
                    PRIMARY KEY (id)
                );
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(createTable.IsSuccess(), createTable.GetIssues().ToString());
        }

        {
            auto initValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/test/alterDropNotNull` (id, val)
                VALUES
                ( 1, 1 ),
                ( 2, 10 ),
                ( 3, 100 ),
                ( 4, 1000 ),
                ( 5, 10000 ),
                ( 6, 100000 ),
                ( 7, 1000000 );
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(initValues.IsSuccess(), initValues.GetIssues().ToString());
        }

        {
            auto initNullValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/test/alterDropNotNull` (id, val)
                VALUES
                ( 1, NULL ),
                ( 2, NULL );
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!initNullValues.IsSuccess(), initNullValues.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(initNullValues.GetIssues().ToString(), "Failed to convert type: Struct<'id':Int32,'val':Null> to Struct<'id':Int32,'val':Int32>");
        }

        {
            auto setNull = client.ExecuteQuery(R"sql(
                ALTER TABLE `/Root/test/alterDropNotNull`
                ALTER COLUMN val DROP NOT NULL;
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(setNull.IsSuccess(), setNull.GetIssues().ToString());
        }

        {
            auto initNullValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/test/alterDropNotNull` (id, val)
                VALUES
                ( 1, NULL ),
                ( 2, NULL );
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(initNullValues.IsSuccess(), initNullValues.GetIssues().ToString());
        }

        {
            auto getValues = client.StreamExecuteQuery(R"sql(
                SELECT *
                FROM `/Root/test/alterDropNotNull`;
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(getValues.GetStatus(), EStatus::SUCCESS, getValues.GetIssues().ToString());
            CompareYson(
                StreamResultToYson(getValues),
                R"([[1;#];[2;#];[3;[100]];[4;[1000]];[5;[10000]];[6;[100000]];[7;[1000000]]])"
            );
        }
    }

    Y_UNIT_TEST(AlterTable_DropNotNull_WithSetFamily_Valid) {
        NKikimrConfig::TAppConfig appConfig;
        auto settings = TKikimrSettings()
            .SetAppConfig(appConfig)
            .SetWithSampleTables(false);

        TKikimrRunner kikimr(settings);
        Tests::NCommon::TLoggerInit(kikimr).Initialize();

        auto client = kikimr.GetQueryClient();

        {
            auto createTable = client.ExecuteQuery(R"sql(
                CREATE TABLE `/Root/test/alterDropNotNullWithSetFamily` (
                    id Int32 NOT NULL,
                    val1 Int32 FAMILY Family1 NOT NULL,
                    val2 Int32,
                    PRIMARY KEY (id),

                    FAMILY default (
                        DATA = "test",
                        COMPRESSION = "lz4"
                    ),
                    FAMILY Family1 (
                        DATA = "test",
                        COMPRESSION = "off"
                    )
                );
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(createTable.IsSuccess(), createTable.GetIssues().ToString());
        }

        {
            auto initValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/test/alterDropNotNullWithSetFamily` (id, val1, val2)
                VALUES
                ( 1, 1, 1 ),
                ( 2, 10, 10 ),
                ( 3, 100, 100 ),
                ( 4, 1000, 1000 ),
                ( 5, 10000, 10000 ),
                ( 6, 100000, 100000 ),
                ( 7, 1000000, 1000000 );
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(initValues.IsSuccess(), initValues.GetIssues().ToString());
        }

        {
            auto initNullValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/test/alterDropNotNullWithSetFamily` (id, val1, val2)
                VALUES
                ( 1, NULL, 1 ),
                ( 2, NULL, 2 );
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(!initNullValues.IsSuccess(), initNullValues.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(initNullValues.GetIssues().ToString(), "Failed to convert type: Struct<'id':Int32,'val1':Null,'val2':Int32> to Struct<'id':Int32,'val1':Int32,'val2':Int32?>");
        }

        {
            auto setNull = client.ExecuteQuery(R"sql(
                ALTER TABLE `/Root/test/alterDropNotNullWithSetFamily`
                ALTER COLUMN val1 DROP NOT NULL;
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(setNull.IsSuccess(), setNull.GetIssues().ToString());
        }

        {
            auto setNull = client.ExecuteQuery(R"sql(
                ALTER TABLE `/Root/test/alterDropNotNullWithSetFamily`
                ALTER COLUMN val1 SET FAMILY Family1;
            )sql", NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(setNull.IsSuccess(), setNull.GetIssues().ToString());
        }

        {
            auto initNullValues = client.ExecuteQuery(R"sql(
                REPLACE INTO `/Root/test/alterDropNotNullWithSetFamily` (id, val1, val2)
                VALUES
                ( 1, NULL, 1 ),
                ( 2, NULL, 2 );
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();
            UNIT_ASSERT_C(initNullValues.IsSuccess(), initNullValues.GetIssues().ToString());
        }

        {
            auto getValues = client.StreamExecuteQuery(R"sql(
                SELECT *
                FROM `/Root/test/alterDropNotNullWithSetFamily`;
            )sql", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(getValues.GetStatus(), EStatus::SUCCESS, getValues.GetIssues().ToString());
            CompareYson(
                StreamResultToYson(getValues),
                R"([[1;#;[1]];[2;#;[2]];[3;[100];[100]];[4;[1000];[1000]];[5;[10000];[10000]];[6;[100000];[100000]];[7;[1000000];[1000000]]])"
            );
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
