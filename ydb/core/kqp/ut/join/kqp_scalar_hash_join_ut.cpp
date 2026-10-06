#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

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

TString MakeQuery(TStringBuf joinKind, TStringBuf columns, TStringBuf orderBy, bool useScalarHashJoin) {
    return TStringBuilder() << R"(
        PRAGMA TablePathPrefix='/Root';
        PRAGMA ydb.OptimizerHints='JoinType(L R Broadcast)';
        PRAGMA ydb.UseScalarHashJoinForMap=")" << (useScalarHashJoin ? "true" : "false") << R"(";
        SELECT )" << columns << R"(
        FROM L
        )" << joinKind << R"( JOIN R ON L.k = R.k
        ORDER BY )" << orderBy << ";";
}

TString RunQuery(TQueryClient& client, const TString& query) {
    auto result = client.ExecuteQuery(query, TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
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
            TStringBuf OrderBy;
        };
        const std::vector<TCase> cases = {
            {"INNER", "L.id AS lid, R.id AS rid, L.v AS v, R.w AS w", "lid, rid"},
            {"LEFT", "L.id AS lid, R.id AS rid, L.v AS v, R.w AS w", "lid, rid"},
            {"LEFT SEMI", "L.id AS lid, L.v AS v", "lid"},
            {"LEFT ONLY", "L.id AS lid, L.v AS v", "lid"},
        };

        for (const auto& [joinKind, columns, orderBy] : cases) {
            const auto scalarQuery = MakeQuery(joinKind, columns, orderBy, true);
            const auto mapQuery = MakeQuery(joinKind, columns, orderBy, false);

            CheckPlan(client, scalarQuery, true);
            CheckPlan(client, mapQuery, false);

            const auto expected = RunQuery(client, mapQuery);
            const auto actual = RunQuery(client, scalarQuery);
            UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, joinKind);
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
