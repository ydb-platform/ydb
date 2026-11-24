#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

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

} // namespace

Y_UNIT_TEST_SUITE(KqpScalarHashJoin) {

    // The join ut binary links pg_dummy, which turns every Pg type id into 0.
    // This suite links the real PostgreSQL runtime, so the tables can be created.
    Y_UNIT_TEST(PgTypesKeepMapJoin) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false).SetDisablePgCompatibility(false);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableNewRBO(false);
        settings.AppConfig.MutableTableServiceConfig()->SetUseScalarHashJoinForMap(true);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();

        auto result = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/PgL` (
                id pgint4 NOT NULL,
                name pgtext,
                PRIMARY KEY (id)
            );
            CREATE TABLE `/Root/PgR` (
                id pgint4 NOT NULL,
                PRIMARY KEY (id)
            );
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/PgL` (id, name) VALUES
                (pgint4('42'), pgtext('hello')),
                (pgint4('0'), pgtext('goodbye'));
            UPSERT INTO `/Root/PgR` (id) VALUES
                (pgint4('42'));
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const TString query = R"(
            PRAGMA TablePathPrefix='/Root';
            PRAGMA ydb.OptimizerHints='JoinType(PgL PgR Broadcast)';
            PRAGMA ydb.UseScalarHashJoinForMap='true';
            SELECT PgL.name AS name
            FROM PgL
            INNER JOIN PgR ON PgL.id = PgR.id;
        )";

        auto explain = client.ExecuteQuery(query, TTxControl::NoTx(),
            TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();
        UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
        const TString ast(*explain.GetStats()->GetAst());
        UNIT_ASSERT_C(!ast.Contains("ScalarHashJoin"), ast);
        UNIT_ASSERT_C(ast.Contains("MapJoin"), ast);

        const TString rows = RunQuerySortedRows(client, query);
        UNIT_ASSERT_VALUES_EQUAL(rows, "[\"hello\"];");

        const TString inQuery = R"(
            PRAGMA TablePathPrefix='/Root';
            PRAGMA ydb.OptimizerHints='JoinType(PgL PgR Broadcast)';
            PRAGMA ydb.UseScalarHashJoinForMap='true';
            SELECT name
            FROM PgL
            WHERE id IN (
                SELECT id
                FROM PgR
            );
        )";
        auto inExplain = client.ExecuteQuery(inQuery, TTxControl::NoTx(),
            TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();
        UNIT_ASSERT_C(inExplain.IsSuccess(), inExplain.GetIssues().ToString());
        const TString inAst(*inExplain.GetStats()->GetAst());
        UNIT_ASSERT_C(!inAst.Contains("ScalarHashJoin"), inAst);
        UNIT_ASSERT_C(inAst.Contains("MapJoin"), inAst);
        UNIT_ASSERT_VALUES_EQUAL(RunQuerySortedRows(client, inQuery), "[\"hello\"];");

        result = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/NestedL` (
                id Int32 NOT NULL,
                name pgtext,
                PRIMARY KEY (id)
            );
            CREATE TABLE `/Root/NestedR` (
                id Int32 NOT NULL,
                PRIMARY KEY (id)
            );
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = client.ExecuteQuery(R"(
            UPSERT INTO `/Root/NestedL` (id, name) VALUES
                (1, pgtext('hello')),
                (2, pgtext('goodbye'));
            UPSERT INTO `/Root/NestedR` (id) VALUES
                (1);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        // The key is a native type. Pg is only inside the tuple payload, so skipping
        // the walk into Tuple/Optional would plan a scalar hash join again.
        const TString nestedQuery = R"(
            PRAGMA TablePathPrefix='/Root';
            PRAGMA ydb.OptimizerHints='JoinType(L R Broadcast)';
            PRAGMA ydb.UseScalarHashJoinForMap='true';
            $l = SELECT id, AsTuple(name) AS payload FROM NestedL;
            $r = SELECT id FROM NestedR;
            SELECT L.payload AS payload
            FROM $l AS L
            INNER JOIN $r AS R ON L.id = R.id;
        )";
        auto nestedExplain = client.ExecuteQuery(nestedQuery, TTxControl::NoTx(),
            TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();
        UNIT_ASSERT_C(nestedExplain.IsSuccess(), nestedExplain.GetIssues().ToString());
        const TString nestedAst(*nestedExplain.GetStats()->GetAst());
        UNIT_ASSERT_C(!nestedAst.Contains("ScalarHashJoin"), nestedAst);
        UNIT_ASSERT_C(nestedAst.Contains("MapJoin"), nestedAst);

        const TString nestedRows = RunQuerySortedRows(client, nestedQuery);
        UNIT_ASSERT_C(nestedRows.Contains("hello"), nestedRows);
    }
}

} // namespace NKqp
} // namespace NKikimr
