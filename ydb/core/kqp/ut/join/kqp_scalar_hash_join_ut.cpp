#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

void CreateSampleTables(TQueryClient& client) {
    auto result = client.ExecuteQuery(R"(
        CREATE TABLE `/Root/L` (
            id Int32 NOT NULL,
            k Int32,
            v Utf8,
            PRIMARY KEY (id)
        );
        CREATE TABLE `/Root/R` (
            id Int32 NOT NULL,
            k Int64,
            w String,
            PRIMARY KEY (id)
        );
    )", TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    result = client.ExecuteQuery(R"(
        UPSERT INTO `/Root/L` (id, k, v) VALUES
            (1, 1, "a"),
            (2, 2, "b"),
            (3, 2, "c"),
            (4, NULL, "d"),
            (5, 5, NULL),
            (6, 100, "f");
        UPSERT INTO `/Root/R` (id, k, w) VALUES
            (1, 1, "x"),
            (2, 2, "y"),
            (3, 2, "z"),
            (4, NULL, "n"),
            (5, 5, NULL),
            (6, 7, "q");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

// No ORDER BY: a sort right after the join would consume all join outputs
TString MakeQuery(TStringBuf joinKind, TStringBuf columns, bool useScalarHashJoin, bool useLlvm) {
    return TStringBuilder() << R"(
        PRAGMA TablePathPrefix='/Root';
        PRAGMA ydb.OptimizerHints='JoinType(L R Broadcast)';
        PRAGMA ydb.UseScalarHashJoinForMap=")" << (useScalarHashJoin ? "true" : "false") << R"(";
        PRAGMA ydb.UseLlvm=")" << (useLlvm ? "true" : "false") << R"(";
        SELECT )" << columns << R"(
        FROM L
        )" << joinKind << R"( JOIN R ON L.k = R.k;)";
}

TString RunQuery(TQueryClient& client, const TString& query) {
    auto result = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

TString RunQuerySortedRows(TQueryClient& client, const TString& query) {
    auto result = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    TVector<TString> rows;
    TResultSetParser parser(result.GetResultSet(0));
    while (parser.TryNextRow()) {
        TStringBuilder row;
        for (size_t i = 0; i < parser.ColumnsCount(); ++i) {
            row << FormatValueYson(parser.GetValue(i)) << ";";
        }
        rows.push_back(row);
    }
    Sort(rows);
    return JoinSeq("\n", rows);
}

void CheckPlan(TQueryClient& client, const TString& query, bool expectScalarHashJoin) {
    auto result = client.ExecuteQuery(query, TTxControl::NoTx(),
        TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    const TString ast(*result.GetStats()->GetAst());
    const TString plan(*result.GetStats()->GetPlan());
    if (expectScalarHashJoin) {
        UNIT_ASSERT_C(ast.Contains("ScalarHashJoin"), ast);
        UNIT_ASSERT_C(plan.Contains("Join (ScalarHash)"), plan);
    } else {
        UNIT_ASSERT_C(!ast.Contains("ScalarHashJoin"), ast);
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpScalarHashJoin) {
    Y_UNIT_TEST(ReplaceMapJoin) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();
        CreateSampleTables(client);

        struct TCase {
            TStringBuf JoinKind;
            TStringBuf Columns;
        };
        const std::vector<TCase> cases = {
            {"INNER", "L.id AS lid, R.id AS rid, L.v AS v, R.w AS w"},
            {"LEFT", "L.id AS lid, R.id AS rid, L.v AS v, R.w AS w"},
            {"LEFT SEMI", "L.id AS lid, L.v AS v"},
            {"LEFT ONLY", "L.id AS lid, L.v AS v"},
        };

        // Without LLVM NarrowMap passes null pointers for unused join outputs, e.g. converted keys
        for (const bool useLlvm : {true, false}) {
            for (const auto& [joinKind, columns] : cases) {
                const auto scalarQuery = MakeQuery(joinKind, columns, true, useLlvm);
                const auto mapQuery = MakeQuery(joinKind, columns, false, useLlvm);
                const TString caseName = TStringBuilder() << joinKind << (useLlvm ? " llvm" : " no llvm");

                CheckPlan(client, scalarQuery, true);
                CheckPlan(client, mapQuery, false);

                const auto expected = RunQuerySortedRows(client, mapQuery);
                const auto actual = RunQuerySortedRows(client, scalarQuery);
                UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, caseName);
            }
        }
    }

    Y_UNIT_TEST(FallbackOnUnsupportedPayload) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();
        CreateSampleTables(client);

        struct TCase {
            TStringBuf Payload;
            bool ExpectScalarHashJoin;
        };
        const std::vector<TCase> cases = {
            {"SOME(v)", true},
            {"SOME(Just(v))", false},
            {"SOME(AsTuple(v, id))", false},
            {"SOME(AddTimezone(CAST(id AS Datetime), 'Europe/Moscow'))", false},
        };

        for (const auto& [payload, expectScalarHashJoin] : cases) {
            // Aggregate so that the payload is not pulled above the join
            const auto makeQuery = [payload](bool useScalarHashJoin) {
                return TStringBuilder() << R"(
                    PRAGMA TablePathPrefix='/Root';
                    PRAGMA ydb.OptimizerHints='JoinType(L R Broadcast)';
                    PRAGMA ydb.UseScalarHashJoinForMap=")" << (useScalarHashJoin ? "true" : "false") << R"(";
                    $l = SELECT k, )" << payload << R"( AS p FROM L GROUP BY k;
                    SELECT L.k AS lk, R.id AS rid, L.p AS p
                    FROM $l AS L
                    INNER JOIN R ON L.k = R.k
                    ORDER BY lk, rid;
                )";
            };
            const TString scalarQuery = makeQuery(true);
            const TString mapQuery = makeQuery(false);

            auto explain = client.ExecuteQuery(scalarQuery, TTxControl::NoTx(),
                TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
            const TString ast(*explain.GetStats()->GetAst());
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("ScalarHashJoin"), expectScalarHashJoin, payload << "\n" << ast);
            if (!expectScalarHashJoin) {
                UNIT_ASSERT_C(ast.Contains("MapJoinCore"), payload << "\n" << ast);
            }

            UNIT_ASSERT_VALUES_EQUAL_C(RunQuery(client, scalarQuery), RunQuery(client, mapQuery), payload);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
