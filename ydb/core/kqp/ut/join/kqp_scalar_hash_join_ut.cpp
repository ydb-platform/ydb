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
    Y_UNIT_TEST_TWIN(SelectImplementationByInputLayout, ForceBlockChannels) {
        TKikimrSettings settings = TKikimrSettings().SetWithSampleTables(false);
        auto* tableService = settings.AppConfig.MutableTableServiceConfig();
        tableService->SetEnableNewRBO(false);
        tableService->SetEnableOlapSink(true);
        tableService->SetBlockChannelsMode(ForceBlockChannels
            ? NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_FORCE
            : NKikimrConfig::TTableServiceConfig_EBlockChannelsMode_BLOCK_CHANNELS_AUTO);
        TKikimrRunner kikimr(settings);
        auto client = kikimr.GetQueryClient();
        CreateSampleTables(client);

        auto result = client.ExecuteQuery(R"(
            CREATE TABLE `/Root/CL` (
                id Int32 NOT NULL,
                k Int32,
                PRIMARY KEY (id)
            ) WITH (STORE = COLUMN);
            CREATE TABLE `/Root/CR` (
                id Int32 NOT NULL,
                k Int64,
                PRIMARY KEY (id)
            ) WITH (STORE = COLUMN);
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = client.ExecuteQuery(R"(
            INSERT INTO `/Root/CL` (id, k) VALUES (1, 1), (2, 2), (3, NULL);
            INSERT INTO `/Root/CR` (id, k) VALUES (1, 1), (2, 2), (3, NULL);
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        const auto makeQuery = [](TStringBuf left, TStringBuf right, bool useScalarHashJoin) {
            return TStringBuilder() << R"(
                PRAGMA TablePathPrefix='/Root';
                PRAGMA ydb.UseBlockHashJoin='true';
                PRAGMA ydb.UseScalarHashJoin=')" << (useScalarHashJoin ? "true" : "false") << R"(';
                PRAGMA ydb.OptShuffleElimination='true';
                PRAGMA ydb.OptShuffleEliminationWithMap='true';
                PRAGMA ydb.OptimizerHints='JoinType(L R Shuffle)';
                SELECT L.id AS lid, R.id AS rid
                FROM )" << left << R"( AS L
                INNER JOIN )" << right << R"( AS R ON L.k = R.k;
            )";
        };
        const auto explain = [&](const TString& query) {
            auto explainResult = client.ExecuteQuery(query, TTxControl::NoTx(),
                TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();
            UNIT_ASSERT_C(explainResult.IsSuccess(), explainResult.GetIssues().ToString());
            return TString(*explainResult.GetStats()->GetAst());
        };

        struct TCase {
            TStringBuf Left;
            TStringBuf Right;
            bool UseScalarHashJoin;
            bool ExpectScalarHashJoin;
        };
        const std::vector<TCase> cases = {
            {"L", "R", true, !ForceBlockChannels},
            {"L", "R", false, false},
            {"CL", "CR", true, false},
            {"L", "CR", true, !ForceBlockChannels},
            {"CL", "R", true, !ForceBlockChannels},
        };
        for (const auto& [left, right, useScalarHashJoin, expectScalarHashJoin] : cases) {
            const TString ast = explain(makeQuery(left, right, useScalarHashJoin));
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("ScalarHashJoin"), expectScalarHashJoin, ast);
            UNIT_ASSERT_VALUES_EQUAL_C(ast.Contains("BlockHashJoin"), !expectScalarHashJoin, ast);
        }
    }

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

    Y_UNIT_TEST(ComplexKeys) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();
        CreateSampleTables(client);

        const std::vector<std::pair<TStringBuf, TStringBuf>> cases = {
            {"AsList(k, k)", "AsList(k, k)"},
            {"AsTuple(k, CAST(k AS String))", "AsTuple(k, CAST(k AS String))"},
            {"AsStruct(k AS a, CAST(k AS String) AS b)", "AsStruct(k AS a, CAST(k AS String) AS b)"},
            {"AsDict(AsTuple(1, k), AsTuple(2, k))", "AsDict(AsTuple(2, k), AsTuple(1, k))"},
            {"Variant(k, 'key', Variant<key: Int32?, other: String>)",
             "Variant(k, 'key', Variant<key: Int32?, other: String>)"},
            {"Just(k)", "Just(k)"},
            {"AddTimezone(CAST(k AS Datetime), 'Europe/Moscow')",
             "AddTimezone(CAST(k AS Datetime), 'Europe/Moscow')"},
            {"AsList(IF(id = 1, -0.0, CAST(k AS Double)))",
             "AsList(IF(id = 1, 0.0, CAST(k AS Double)))"},
        };
        for (const auto& [leftKey, rightKey] : cases) {
            for (const auto joinKind : {"INNER", "LEFT", "LEFT SEMI", "LEFT ONLY"}) {
                const auto makeQuery = [&](bool scalar) {
                    const bool leftOnly = TStringBuf(joinKind) == "LEFT SEMI" || TStringBuf(joinKind) == "LEFT ONLY";
                    return TStringBuilder() << R"(
                        PRAGMA TablePathPrefix='/Root';
                        PRAGMA ydb.OptimizerHints='JoinType(L R Broadcast)';
                        PRAGMA ydb.UseScalarHashJoinForMap=")" << (scalar ? "true" : "false") << R"(";
                        $l = SELECT id, SOME()" << leftKey << R"() AS key FROM L GROUP BY id;
                        $r = SELECT id, SOME()" << rightKey << R"() AS key
                            FROM (SELECT id, CAST(k AS Int32) AS k FROM R) GROUP BY id;
                        SELECT L.id AS lid )" << (leftOnly ? "" : ", R.id AS rid") << R"(
                        FROM $l AS L )" << joinKind << R"( JOIN $r AS R ON L.key = R.key;
                    )";
                };
                const TString scalarQuery = makeQuery(true);
                const TString mapQuery = makeQuery(false);
                CheckPlan(client, scalarQuery, true);
                const auto expected = RunQuerySortedRows(client, mapQuery);
                UNIT_ASSERT_C(!expected.empty(), leftKey << " " << joinKind);
                UNIT_ASSERT_VALUES_EQUAL_C(RunQuerySortedRows(client, scalarQuery), expected, leftKey << " " << joinKind);
            }
        }
    }

    Y_UNIT_TEST(ComplexPayload) {
        TKikimrRunner kikimr(TKikimrSettings().SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();
        CreateSampleTables(client);

        const std::vector<TStringBuf> cases = {
            "SOME(v)",
            "SOME(Just(v))",
            "SOME(AsTuple(v, id))",
            "SOME(AsStruct(v AS value, id AS id))",
            "SOME(AsList(v, v))",
            "SOME(AsDict(AsTuple(id, v)))",
            "SOME(AddTimezone(CAST(id AS Datetime), 'Europe/Moscow'))",
        };

        for (const auto payload : cases) {
            for (const auto joinKind : {"INNER", "LEFT", "LEFT SEMI", "LEFT ONLY"}) {
                // Aggregate so that the payload is not pulled above the join
                const auto makeQuery = [payload, joinKind](bool useScalarHashJoin) {
                    const bool leftOnly = TStringBuf(joinKind) == "LEFT SEMI" || TStringBuf(joinKind) == "LEFT ONLY";
                    return TStringBuilder() << R"(
                        PRAGMA TablePathPrefix='/Root';
                        PRAGMA ydb.OptimizerHints='JoinType(L R Broadcast)';
                        PRAGMA ydb.UseScalarHashJoinForMap=")" << (useScalarHashJoin ? "true" : "false") << R"(";
                        $l = SELECT k, )" << payload << R"( AS p FROM L GROUP BY k;
                        $r = SELECT k, )" << payload << R"( AS p
                            FROM (SELECT k, id, CAST(w AS Utf8) AS v FROM R) GROUP BY k;
                        SELECT L.k AS lk, L.p AS lp )" << (leftOnly ? "" : ", R.p AS rp") << R"(
                        FROM $l AS L
                        )" << joinKind << R"( JOIN $r AS R ON L.k = R.k
                        ORDER BY lk;
                    )";
                };
                const TString scalarQuery = makeQuery(true);
                const TString mapQuery = makeQuery(false);

                auto explain = client.ExecuteQuery(scalarQuery, TTxControl::NoTx(),
                    TExecuteQuerySettings().ExecMode(EExecMode::Explain)).GetValueSync();
                UNIT_ASSERT_C(explain.IsSuccess(), explain.GetIssues().ToString());
                const TString ast(*explain.GetStats()->GetAst());
                UNIT_ASSERT_C(ast.Contains("ScalarHashJoin"), payload << " " << joinKind << "\n" << ast);

                UNIT_ASSERT_VALUES_EQUAL_C(RunQuery(client, scalarQuery), RunQuery(client, mapQuery), payload);
            }
        }
    }
}

} // namespace NKqp
} // namespace NKikimr
