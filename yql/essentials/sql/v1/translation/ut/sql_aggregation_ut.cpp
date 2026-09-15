#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(SessionWindowNegative) {

Y_UNIT_TEST(SessionWindowWithoutSource) {
    ExpectFailWithError("SELECT 1 + SessionWindow(ts, 32);",
                        "<main>:1:12: Error: SessionWindow requires data source\n");
}

Y_UNIT_TEST(SessionWindowInProjection) {
    ExpectFailWithError("SELECT 1 + SessionWindow(ts, 32) from plato.Input;",
                        "<main>:1:12: Error: SessionWindow can only be used as a top-level GROUP BY / PARTITION BY expression\n");
}

Y_UNIT_TEST(SessionWindowWithNonConstSecondArg) {
    ExpectFailWithError(
        "SELECT key, session_start FROM plato.Input\n"
        "GROUP BY SessionWindow(ts, 32 + subkey) as session_start, key;",

        "<main>:2:10: Error: Source does not allow column references\n"
        "<main>:2:33: Error: Column reference 'subkey'\n");
}

Y_UNIT_TEST(SessionWindowWithWrongNumberOfArgs) {
    ExpectFailWithError("SELECT * FROM plato.Input GROUP BY SessionWindow()",
                        "<main>:1:36: Error: SessionWindow requires either two or four arguments\n");
    ExpectFailWithError("SELECT * FROM plato.Input GROUP BY SessionWindow(key, subkey, 100)",
                        "<main>:1:36: Error: SessionWindow requires either two or four arguments\n");
}

Y_UNIT_TEST(DuplicateSessionWindow) {
    ExpectFailWithError(
        "SELECT\n"
        "    *\n"
        "FROM plato.Input\n"
        "GROUP BY\n"
        "    SessionWindow(ts, 10),\n"
        "    user,\n"
        "    SessionWindow(ts, 20)\n"
        ";",

        "<main>:7:5: Error: Duplicate session window specification:\n"
        "<main>:5:5: Error: Previous session window is declared here\n");

    ExpectFailWithError(
        "SELECT\n"
        "    MIN(key) over w\n"
        "FROM plato.Input\n"
        "WINDOW w AS (\n"
        "    PARTITION BY SessionWindow(ts, 10), user,\n"
        "    SessionWindow(ts, 20)\n"
        ");",

        "<main>:6:5: Error: Duplicate session window specification:\n"
        "<main>:5:18: Error: Previous session window is declared here\n");
}

Y_UNIT_TEST(SessionStartStateWithoutSource) {
    ExpectFailWithError("SELECT 1 + SessionStart();",
                        "<main>:1:12: Error: SessionStart requires data source\n");
    ExpectFailWithError("SELECT 1 + SessionState();",
                        "<main>:1:12: Error: SessionState requires data source\n");
}

Y_UNIT_TEST(SessionStartStateWithoutGroupByOrWindow) {
    ExpectFailWithError("SELECT 1 + SessionStart() from plato.Input;",
                        "<main>:1:12: Error: SessionStart can not be used without aggregation by SessionWindow\n");
    ExpectFailWithError("SELECT 1 + SessionState() from plato.Input;",
                        "<main>:1:12: Error: SessionState can not be used without aggregation by SessionWindow\n");
}

Y_UNIT_TEST(SessionStartStateWithGroupByWithoutSession) {
    ExpectFailWithError("SELECT 1 + SessionStart() from plato.Input group by user;",
                        "<main>:1:12: Error: SessionStart can not be used here: SessionWindow specification is missing in GROUP BY\n");
    ExpectFailWithError("SELECT 1 + SessionState() from plato.Input group by user;",
                        "<main>:1:12: Error: SessionState can not be used here: SessionWindow specification is missing in GROUP BY\n");
}

Y_UNIT_TEST(SessionStartStateWithoutOverWithWindowWithoutSession) {
    ExpectFailWithError("SELECT 1 + SessionStart(), MIN(key) over w from plato.Input window w as ()",
                        "<main>:1:12: Error: SessionStart can not be used without aggregation by SessionWindow. Maybe you forgot to add OVER `window_name`?\n");
    ExpectFailWithError("SELECT 1 + SessionState(), MIN(key) over w from plato.Input window w as ()",
                        "<main>:1:12: Error: SessionState can not be used without aggregation by SessionWindow. Maybe you forgot to add OVER `window_name`?\n");
}

Y_UNIT_TEST(SessionStartStateWithWindowWithoutSession) {
    ExpectFailWithError("SELECT 1 + SessionStart() over w, MIN(key) over w from plato.Input window w as ()",
                        "<main>:1:12: Error: SessionStart can not be used with window w: SessionWindow specification is missing in PARTITION BY\n");
    ExpectFailWithError("SELECT 1 + SessionState() over w, MIN(key) over w from plato.Input window w as ()",
                        "<main>:1:12: Error: SessionState can not be used with window w: SessionWindow specification is missing in PARTITION BY\n");
}

Y_UNIT_TEST(SessionStartStateWithSessionedWindow) {
    ExpectFailWithError("SELECT 1 + SessionStart(), MIN(key) over w from plato.Input group by key window w as (partition by SessionWindow(ts, 1)) ",
                        "<main>:1:12: Error: SessionStart can not be used here: SessionWindow specification is missing in GROUP BY. Maybe you forgot to add OVER `window_name`?\n");
    ExpectFailWithError("SELECT 1 + SessionState(), MIN(key) over w from plato.Input group by key window w as (partition by SessionWindow(ts, 1)) ",
                        "<main>:1:12: Error: SessionState can not be used here: SessionWindow specification is missing in GROUP BY. Maybe you forgot to add OVER `window_name`?\n");
}

Y_UNIT_TEST(AggregationBySessionStateIsNotSupportedYet) {
    ExpectFailWithError("SELECT SOME(1 + SessionState()), key from plato.Input group by key, SessionWindow(ts, 1);",
                        "<main>:1:17: Error: SessionState with GROUP BY is not supported yet\n");
}

Y_UNIT_TEST(SessionWindowInRtmr) {
    NYql::TAstParseResult res = SqlToYql(
        "SELECT * FROM plato.Input GROUP BY SessionWindow(ts, 10);",
        10, TString(NYql::RtmrProviderName));
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:54: Error: Streaming group by query must have a hopping window specification.\n");

    res = SqlToYql(R"(
            SELECT key, SUM(value) AS value FROM plato.Input
            GROUP BY key, HOP(subkey, "PT10S", "PT30S", "PT20S"), SessionWindow(ts, 10);
    )", 10, TString(NYql::RtmrProviderName));

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:13: Error: SessionWindow is unsupported for streaming sources\n");
}

} // Y_UNIT_TEST_SUITE(SessionWindowNegative)

Y_UNIT_TEST_SUITE(MatchRecognizeMeasuresAggregation) {

Y_UNIT_TEST(InsideSelect) {
    ExpectFailWithError(R"sql(
            SELECT FIRST(0), LAST(1);
    )sql",
                        "<main>:2:20: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n"
                        "<main>:2:30: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n");
}

Y_UNIT_TEST(OutsideSelect) {
    ExpectFailWithError(R"sql(
            $lambda = ($x) -> (FIRST($x) + LAST($x));
            SELECT $lambda(x) FROM plato.Input;
    )sql",
                        "<main>:2:32: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n"
                        "<main>:2:44: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n");
}

Y_UNIT_TEST(AsAggregateFunction) {
    ExpectFailWithError(R"sql(
            SELECT FIRST(x), LAST(x) FROM plato.Input;
        )sql",
                        "<main>:2:20: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n"
                        "<main>:2:30: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n");
}

Y_UNIT_TEST(AsWindowFunction) {
    ExpectFailWithError(R"sql(
            SELECT FIRST(x) OVER(), LAST(x) OVER() FROM plato.Input;
    )sql",
                        "<main>:2:20: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n"
                        "<main>:2:37: Error: Cannot use FIRST and LAST outside the MATCH_RECOGNIZE context\n");
}
} // Y_UNIT_TEST_SUITE(MatchRecognizeMeasuresAggregation)

Y_UNIT_TEST_SUITE(Aggregation) {

Y_UNIT_TEST(DeduplicationDistinctSources) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT Percentile(a.x, 0.50), Percentile(b.x, 0.75)
                FROM plato.Input1 AS a
                JOIN plato.Input1 AS b ON a.x == b.x;
            )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive count = {{TString("percentile_traits_factory"), 0}};
    VerifyProgram(res, count);

    UNIT_ASSERT_VALUES_EQUAL(2, count["percentile_traits_factory"]);
}

Y_UNIT_TEST(DeduplicationSameSource) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT Percentile(a.x, 0.50), Percentile(a.x, 0.75)
                FROM plato.Input1 AS a
                JOIN plato.Input1 AS b ON a.x == b.x;
            )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive count = {{TString("percentile_traits_factory"), 0}};
    VerifyProgram(res, count);

    UNIT_ASSERT_VALUES_EQUAL(1, count["percentile_traits_factory"]);
}
} // Y_UNIT_TEST_SUITE(Aggregation)

Y_UNIT_TEST_SUITE(AggregationPhases) {

Y_UNIT_TEST(TwoArg) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::AggPhases.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT AvgIf(a, a % 2 == 0) FROM (SELECT 1 AS k, 2 AS a) GROUP BY k;
        SELECT AvgIf(a, a % 2 == 0) FROM (SELECT 1 AS k, 2 AS a) GROUP BY k WITH Combine;
        SELECT AvgIf(a, a % 2 == 0) FROM (SELECT 1 AS k, 2 AS a) GROUP BY k WITH Finalize;
    )sql", settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SingleArg) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::AggPhases.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT AvgIf(a) FROM (SELECT 1 AS k, 2 AS a) GROUP BY k WITH CombineState;
        SELECT AvgIf(a) FROM (SELECT 1 AS k, 2 AS a) GROUP BY k WITH MergeState;
    )sql", settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(Distinct) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::AggPhases.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Avg(DISTINCT a) FROM (SELECT 1 AS k, 2 AS a) GROUP BY k WITH CombineState;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Distinct is not supported with aggregation phases");
}

} // Y_UNIT_TEST_SUITE(AggregationPhases)

Y_UNIT_TEST_SUITE(HoppingWindow) {

Y_UNIT_TEST(HoppingWindow) {
    auto query = R"sql(
        SELECT
            *
        FROM plato.Input
        GROUP BY HoppingWindow(key, 39, 42);
    )sql";

    NYql::TAstParseResult res = SqlToYql(query);
    UNIT_ASSERT_VALUES_UNEQUAL(nullptr, res.Root);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_VALUES_EQUAL(0, res.Issues.Size());
}

Y_UNIT_TEST(HoppingWindowNamedParameters) {
    {
        auto query = R"sql(
            SELECT
                *
            FROM plato.Input
            GROUP BY HoppingWindow(key, 39, 42,
                                    "max" AS SizeLimit, "PT10S" AS TimeLimit,
                                    "close" AS EarlyPolicy, "adjust" AS LatePolicy);
        )sql";

        NYql::TAstParseResult res = SqlToYql(query);
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, res.Root);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
        UNIT_ASSERT_VALUES_EQUAL(0, res.Issues.Size());
    }
    {
        auto query = R"sql(
            SELECT
                *
            FROM plato.Input
            GROUP BY HoppingWindow(key, 39, 42,
                                    "drop" AS LatePolicy, "adjust" AS EarlyPolicy);
        )sql";

        NYql::TAstParseResult res = SqlToYql(query);
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, res.Root);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
        UNIT_ASSERT_VALUES_EQUAL(0, res.Issues.Size());
    }
}

Y_UNIT_TEST(HoppingWindowWithoutSource) {
    ExpectFailWithError(
        R"sql(SELECT 1 + HoppingWindow(key, 39, 42);)sql",
        "<main>:1:12: Error: HoppingWindow requires data source\n");
}

Y_UNIT_TEST(HoppingWindowInProjection) {
    ExpectFailWithError(
        R"sql(SELECT 1 + HoppingWindow(key, 39, 42) FROM plato.Input;)sql",
        "<main>:1:12: Error: HoppingWindow can only be used as a top-level GROUP BY expression\n");
}

Y_UNIT_TEST(HoppingWindowWithNonConstIntervals) {
    ExpectFailWithError(
        R"sql(
                SELECT
                    key,
                    hopping_start
                FROM plato.Input
                GROUP BY
                    HoppingWindow(key, 39 + subkey, 42) AS hopping_start,
                    key;
            )sql",

        "<main>:7:21: Error: Source does not allow column references\n"
        "<main>:7:45: Error: Column reference 'subkey'\n");

    ExpectFailWithError(
        R"sql(
                SELECT
                    key,
                    hopping_start
                FROM plato.Input
                GROUP BY
                    HoppingWindow(key, 39 + subkey, 42) AS hopping_start,
                    key;
            )sql",

        "<main>:7:21: Error: Source does not allow column references\n"
        "<main>:7:45: Error: Column reference 'subkey'\n");

    ExpectFailWithError(
        R"sql(
                SELECT
                    key,
                    hopping_start
                FROM plato.Input
                GROUP BY
                    HoppingWindow(key, 39, 42, (subkey + 42) AS SizeLimit) AS hopping_start,
                    key;
            )sql",

        "<main>:7:21: Error: Source does not allow column references\n"
        "<main>:7:49: Error: Column reference 'subkey'\n");

    ExpectFailWithError(
        R"sql(
                SELECT
                    key,
                    hopping_start
                FROM plato.Input
                GROUP BY
                    HoppingWindow(key, 39, 42, subkey AS LatePolicy) AS hopping_start,
                    key;
            )sql",

        "<main>:7:21: Error: Source does not allow column references\n"
        "<main>:7:48: Error: Column reference 'subkey'\n");
}

Y_UNIT_TEST(HoppingWindowWithWrongNumberOfArgs) {
    ExpectFailWithError(
        R"sql(
                SELECT
                    *
                FROM plato.Input
                GROUP BY HoppingWindow(key, 39);
        )sql",

        "<main>:5:26: Error: HoppingWindow requires three positional arguments\n");

    ExpectFailWithError(
        R"sql(
                SELECT
                    *
                FROM plato.Input
                GROUP BY HoppingWindow(key, 39, 42, 63);
        )sql",

        "<main>:5:26: Error: HoppingWindow requires three positional arguments\n");
}

Y_UNIT_TEST(HoppingWindowWithUnknownNamedArg) {
    ExpectFailWithError(
        R"sql(
                SELECT
                    *
                FROM plato.Input
                GROUP BY HoppingWindow(key, 39, 42, 13 AS Foobar);
        )sql",

        "<main>:5:53: Error: HoppingWindow: unsupported parameter: Foobar; expected: SizeLimit, TimeLimit, EarlyPolicy, LatePolicy\n");
}

Y_UNIT_TEST(HoppingWindowWithEvaluatedLimit) {
    {
        auto query = R"sql(
            SELECT
                *
            FROM plato.Input
            GROUP BY HoppingWindow(key, 39, 42, (Uint64("13") + Uint64("17")) AS SizeLimit);
        )sql";

        NYql::TAstParseResult res = SqlToYql(query);
        UNIT_ASSERT_VALUES_UNEQUAL(nullptr, res.Root);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
        UNIT_ASSERT_VALUES_EQUAL(0, res.Issues.Size());
    }
}

Y_UNIT_TEST(DuplicateHoppingWindow) {
    ExpectFailWithError(
        R"sql(
                SELECT
                    *
                FROM plato.Input
                GROUP BY
                    HoppingWindow(key, 39, 42),
                    subkey,
                    HoppingWindow(ts, 42, 39);
        )sql",

        "<main>:8:21: Error: Duplicate hopping window specification:\n"
        "<main>:6:21: Error: Previous hopping window is declared here\n");
}

Y_UNIT_TEST(HopStartEndWithoutSource) {
    ExpectFailWithError(
        R"sql(SELECT 1 + HopStart();)sql",
        "<main>:1:12: Error: HopStart requires data source\n");

    ExpectFailWithError(
        R"sql(SELECT 1 + HopEnd();)sql",
        "<main>:1:12: Error: HopEnd requires data source\n");
}

Y_UNIT_TEST(HopStartEndWithoutGroupByOrWindow) {
    ExpectFailWithError(
        R"sql(SELECT 1 + HopStart() FROM plato.Input;)sql",
        "<main>:1:12: Error: HopStart can not be used without aggregation by HoppingWindow\n");

    ExpectFailWithError(
        R"sql(SELECT 1 + HopEnd() FROM plato.Input;)sql",
        "<main>:1:12: Error: HopEnd can not be used without aggregation by HoppingWindow\n");
}

Y_UNIT_TEST(HopStartEndWithGroupByWithoutHopping) {
    ExpectFailWithError(
        R"sql(
                SELECT
                    1 + HopStart()
                FROM plato.Input
                GROUP BY user;
            )sql",

        "<main>:3:25: Error: HopStart can not be used here: HoppingWindow specification is missing in GROUP BY\n");

    ExpectFailWithError(
        R"sql(
                SELECT
                    1 + HopEnd()
                FROM plato.Input
                GROUP BY user;
            )sql",

        "<main>:3:25: Error: HopEnd can not be used here: HoppingWindow specification is missing in GROUP BY\n");
}

} // Y_UNIT_TEST_SUITE(HoppingWindow)
