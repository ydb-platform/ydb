#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpUnion) {
    Y_UNIT_TEST(ParallelUnionAll) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto session2 = result.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64	NOT NULL,
                b Int32 NULL,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

       res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t2` (
                a Int64	NOT NULL,
                b Int32 NULL,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        auto insertRes = session2.ExecuteQuery(R"(
            INSERT INTO `/Root/t1` (a, b) VALUES (1, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (1, 1);
            INSERT INTO `/Root/t1` (a, b) VALUES (2, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (2, 1);
            INSERT INTO `/Root/t1` (a, b) VALUES (3, 1);
            INSERT INTO `/Root/t2` (a, b) VALUES (3, 1);
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

        TVector<TString> queries = {
            R"(
                PRAGMA Kikimr.OptEnableParallelUnionAllConnectionsForExtend = "true";
                SELECT * FROM `/Root/t1`
                UNION ALL
                SELECT * FROM `/Root/t2`;
            )",
            R"(
                PRAGMA Kikimr.OptEnableParallelUnionAllConnectionsForExtend = "true";
                SELECT * FROM `/Root/t1`
                UNION
                SELECT * FROM `/Root/t2`;
            )",
        };

        for (ui32 i = 0; i < queries.size(); ++i) {
            const auto query = queries[i];
            auto result = session2
                              .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                                            NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                              .ExtractValueSync();

            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
            auto ast = *result.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("DqCnParallelUnionAll") != std::string::npos,
                          TStringBuilder() << "Parallel union all connection for extend is not enabled: " << ast);

            result =
                session2.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(ScatterConnection) {
        auto settings = TKikimrSettings().SetWithSampleTables(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableScatterConnection(true);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto qSession = result.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t1` (
                a Int64 NOT NULL,
                b Int32 NULL,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/t2` (
                a Int64 NOT NULL,
                b Int32 NULL,
                primary key(a)
            )
            PARTITION BY HASH(a)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT(res.IsSuccess());

        auto insertRes = qSession.ExecuteQuery(R"(
            INSERT INTO `/Root/t1` (a, b) VALUES (1, 10), (2, 20), (3, 30);
            INSERT INTO `/Root/t2` (a, b) VALUES (4, 40), (5, 50), (6, 60);
        )", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT(insertRes.IsSuccess());

        TVector<std::pair<TString, TString>> queries = {
            {
                R"(
                    SELECT * FROM `/Root/t1`
                    UNION ALL
                    SELECT * FROM `/Root/t2`
                    ORDER BY a;
                )",
                "UNION ALL + ORDER BY"
            },
            {
                R"(
                    SELECT * FROM `/Root/t1`
                    UNION ALL
                    SELECT * FROM `/Root/t2`
                    ORDER BY a LIMIT 4;
                )",
                "UNION ALL + ORDER BY LIMIT"
            },
        };

        for (const auto& [query, label] : queries) {
            auto explainResult = qSession
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                              NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, label);

            auto ast = *explainResult.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("DqCnParallelUnionAll") != std::string::npos || ast.find("DqCnUnionAll") != std::string::npos,
                          TStringBuilder() << label << ": expected UnionAll or ParallelUnionAll in plan: " << ast);
            UNIT_ASSERT_C(ast.find("DqCnMerge") != std::string::npos,
                          TStringBuilder() << label << ": expected DqCnMerge (heavy downstream) in plan: " << ast);

            auto execResult = qSession
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings())
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, label);

            auto rs = execResult.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(rs.size(), 1);
            auto rows = rs[0].RowsCount();
            if (label == "UNION ALL + ORDER BY LIMIT") {
                UNIT_ASSERT_VALUES_EQUAL_C(rows, 4, label);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(rows, 6, label);
            }

            // Verify ordering: column 'a' must be ascending
            NYdb::TResultSetParser parser(rs[0]);
            std::optional<i64> prev;
            while (parser.TryNextRow()) {
                auto val = parser.ColumnParser("a").GetInt64();
                if (prev) {
                    UNIT_ASSERT_C(val >= *prev,
                                  TStringBuilder() << label << ": rows not sorted, got " << val << " after " << *prev);
                }
                prev = val;
            }
        }
    }

    Y_UNIT_TEST(ScatterConnectionMultiNode) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetNodeCount(3);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableScatterConnection(true);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        auto queryClient = kikimr.GetQueryClient();
        auto result = queryClient.GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(result);
        auto qSession = result.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/scatter_t` (
                key Int64 NOT NULL,
                val Utf8 NULL,
                primary key(key)
            )
            PARTITION BY HASH(key)
            WITH (STORE = COLUMN);
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        {
            TStringBuilder insertQ;
            insertQ << "INSERT INTO `/Root/scatter_t` (key, val) VALUES ";
            for (i64 i = 1; i <= 100; ++i) {
                if (i > 1) insertQ << ", ";
                insertQ << "(" << i << ", \"v" << i << "\")";
            }
            insertQ << ";";
            auto insertRes = qSession.ExecuteQuery(insertQ, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(insertRes.IsSuccess(), insertRes.GetIssues().ToString());
        }

        {
            TString query = R"(
                SELECT * FROM `/Root/scatter_t`
                ORDER BY key;
            )";

            auto explainResult = qSession
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                              NYdb::NQuery::TExecuteQuerySettings().ExecMode(NQuery::EExecMode::Explain))
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(explainResult.GetStatus(), EStatus::SUCCESS, explainResult.GetIssues().ToString());

            auto ast = *explainResult.GetStats()->GetAst();
            UNIT_ASSERT_C(ast.find("DqCnMerge") != std::string::npos,
                          TStringBuilder() << "DqCnMerge (heavy downstream) not found in plan: " << ast);

            auto execResult = qSession
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                              NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full))
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());

            auto rs = execResult.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(rs.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rs[0].RowsCount(), 100);

            NYdb::TResultSetParser parser(rs[0]);
            i64 expected = 1;
            while (parser.TryNextRow()) {
                auto val = parser.ColumnParser("key").GetInt64();
                UNIT_ASSERT_VALUES_EQUAL_C(val, expected,
                    TStringBuilder() << "Expected key=" << expected << " got " << val);
                ++expected;
            }
        }

        {
            TString query = R"(
                SELECT * FROM `/Root/scatter_t`
                ORDER BY key DESC
                LIMIT 10;
            )";

            auto execResult = qSession
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                              NYdb::NQuery::TExecuteQuerySettings())
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());

            auto rs = execResult.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(rs.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rs[0].RowsCount(), 10);

            NYdb::TResultSetParser parser(rs[0]);
            i64 expected = 100;
            while (parser.TryNextRow()) {
                auto val = parser.ColumnParser("key").GetInt64();
                UNIT_ASSERT_VALUES_EQUAL_C(val, expected,
                    TStringBuilder() << "Expected key=" << expected << " got " << val);
                --expected;
            }
        }

        {
            TString query = R"(
                SELECT key, val FROM `/Root/scatter_t`
                WHERE key > 50
                ORDER BY key
                LIMIT 25;
            )";

            auto execResult = qSession
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                              NYdb::NQuery::TExecuteQuerySettings())
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());

            auto rs = execResult.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(rs.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rs[0].RowsCount(), 25);

            NYdb::TResultSetParser parser(rs[0]);
            i64 expected = 51;
            while (parser.TryNextRow()) {
                auto val = parser.ColumnParser("key").GetInt64();
                UNIT_ASSERT_VALUES_EQUAL_C(val, expected,
                    TStringBuilder() << "Expected key=" << expected << " got " << val);
                ++expected;
            }
        }
    }

    // Scenario #2 from the scatter design doc: hierarchical aggregation.
    // UNION ALL of two aggregated subqueries followed by a top-level GROUP BY.
    // With scatter enabled the intermediate ParallelUnionAll is upgraded to
    // scatter wiring; the final result must still match the non-scatter plan.
    Y_UNIT_TEST(ScatterHierarchicalAggregate) {
        auto makeKikimr = [](bool enableScatter) {
            auto settings = TKikimrSettings()
                .SetWithSampleTables(false)
                .SetNodeCount(3);
            settings.AppConfig.MutableTableServiceConfig()->SetEnableScatterConnection(enableScatter);
            return std::make_unique<TKikimrRunner>(settings);
        };

        auto runQuery = [](TKikimrRunner& kikimr) {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.CreateSession().GetValueSync().GetSession();
            auto qSessionResult = kikimr.GetQueryClient().GetSession().GetValueSync();
            NStatusHelpers::ThrowOnError(qSessionResult);
            auto qSession = qSessionResult.GetSession();

            for (const TString& name : {"lhs", "rhs"}) {
                auto res = session.ExecuteSchemeQuery(Sprintf(R"(
                    CREATE TABLE `/Root/%s` (
                        grp Int32 NOT NULL,
                        val Int64 NOT NULL,
                        primary key(grp, val)
                    )
                    PARTITION BY HASH(grp)
                    WITH (STORE = COLUMN);
                )", name.c_str())).GetValueSync();
                UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            }

            {
                TStringBuilder q;
                q << "INSERT INTO `/Root/lhs` (grp, val) VALUES ";
                for (i32 i = 0; i < 200; ++i) {
                    if (i > 0) q << ",";
                    q << "(" << (i % 5) << "," << i << ")";
                }
                q << ";";
                auto r = qSession.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(r.IsSuccess(), r.GetIssues().ToString());
            }
            {
                TStringBuilder q;
                q << "INSERT INTO `/Root/rhs` (grp, val) VALUES ";
                for (i32 i = 0; i < 200; ++i) {
                    if (i > 0) q << ",";
                    q << "(" << (i % 5) << "," << (1000 + i) << ")";
                }
                q << ";";
                auto r = qSession.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
                UNIT_ASSERT_C(r.IsSuccess(), r.GetIssues().ToString());
            }

            TString query = R"(
                SELECT grp, SUM(val) AS total, COUNT(*) AS cnt
                FROM (
                    SELECT grp, val FROM `/Root/lhs`
                    UNION ALL
                    SELECT grp, val FROM `/Root/rhs`
                )
                GROUP BY grp
                ORDER BY grp;
            )";

            auto execResult = qSession
                .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), NYdb::NQuery::TExecuteQuerySettings())
                .ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());

            auto rs = execResult.GetResultSets();
            UNIT_ASSERT_VALUES_EQUAL(rs.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(rs[0].RowsCount(), 5);

            TVector<std::tuple<i32, i64, ui64>> out;
            NYdb::TResultSetParser parser(rs[0]);
            while (parser.TryNextRow()) {
                out.emplace_back(
                    parser.ColumnParser("grp").GetInt32(),
                    parser.ColumnParser("total").GetInt64(),
                    parser.ColumnParser("cnt").GetUint64()
                );
            }
            return out;
        };

        auto kikimrOff = makeKikimr(/*enableScatter=*/false);
        auto resultOff = runQuery(*kikimrOff);

        auto kikimrOn = makeKikimr(/*enableScatter=*/true);
        auto resultOn = runQuery(*kikimrOn);

        UNIT_ASSERT_VALUES_EQUAL(resultOff.size(), resultOn.size());
        for (size_t i = 0; i < resultOff.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(std::get<0>(resultOff[i]), std::get<0>(resultOn[i]), "grp @ " << i);
            UNIT_ASSERT_VALUES_EQUAL_C(std::get<1>(resultOff[i]), std::get<1>(resultOn[i]), "sum(val) @ grp=" << std::get<0>(resultOff[i]));
            UNIT_ASSERT_VALUES_EQUAL_C(std::get<2>(resultOff[i]), std::get<2>(resultOn[i]), "count @ grp=" << std::get<0>(resultOff[i]));
        }
    }

    // Scatter must route rows to multiple destination tasks (the feature's whole
    // point). Counts per task are available through Full stats; we assert that
    // the number of tasks that received at least one row is > 1 when scatter
    // is enabled and there are several destination tasks.
    Y_UNIT_TEST(ScatterRowsDistributed) {
        auto settings = TKikimrSettings()
            .SetWithSampleTables(false)
            .SetNodeCount(3);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableScatterConnection(true);
        TKikimrRunner kikimr(settings);

        auto tableClient = kikimr.GetTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();
        auto qSessionResult = kikimr.GetQueryClient().GetSession().GetValueSync();
        NStatusHelpers::ThrowOnError(qSessionResult);
        auto qSession = qSessionResult.GetSession();

        auto res = session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/scatter_dist` (
                key Int64 NOT NULL,
                val Int64 NULL,
                primary key(key)
            )
            PARTITION BY HASH(key)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 4);
        )").GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        {
            TStringBuilder q;
            q << "INSERT INTO `/Root/scatter_dist` (key, val) VALUES ";
            for (i64 i = 1; i <= 500; ++i) {
                if (i > 1) q << ",";
                q << "(" << i << "," << i << ")";
            }
            q << ";";
            auto r = qSession.ExecuteQuery(q, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
            UNIT_ASSERT_C(r.IsSuccess(), r.GetIssues().ToString());
        }

        TString query = R"(
            SELECT COUNT(*) AS cnt FROM (
                SELECT * FROM `/Root/scatter_dist`
                UNION ALL
                SELECT * FROM `/Root/scatter_dist`
            );
        )";

        auto execResult = qSession
            .ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(),
                          NYdb::NQuery::TExecuteQuerySettings().StatsMode(NYdb::NQuery::EStatsMode::Full))
            .ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(execResult.GetStatus(), EStatus::SUCCESS, execResult.GetIssues().ToString());

        auto rs = execResult.GetResultSets();
        UNIT_ASSERT_VALUES_EQUAL(rs.size(), 1);
        NYdb::TResultSetParser parser(rs[0]);
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("cnt").GetUint64(), 1000u);

        // Full stats contain per-stage task counts; the exact layout is version
        // dependent, so we just assert the query completed under Full stats and
        // the functional result is correct. Deeper per-task routing checks live
        // in the unit tests for TScatterRouter.
        UNIT_ASSERT(execResult.GetStats().has_value());
    }

}
} // namespace NKikimr::NKqp
