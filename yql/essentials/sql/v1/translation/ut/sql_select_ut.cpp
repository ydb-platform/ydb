#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(InlineUncorrelatedSubquery) {

Y_UNIT_TEST(EmptyTuple) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT ();
                SELECT (());
                SELECT (,);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ParenthesisedExpression) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT 1;
                SELECT (1);
                SELECT ((1));
                SELECT (((1)));
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(Tuple) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT (1,);
                SELECT (1, 2);
                SELECT (1, 2, 3);
                SELECT (1, 2, 3, 4);
                SELECT ((1, 2));
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(Struct) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT (1 AS a);
                SELECT (1 AS a, 2 AS b);
                SELECT (1 AS a, 2 AS b, 3 AS c);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(Lambda) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT (($a) -> { RETURN $a; })(1);
                SELECT (($a, $b) -> { RETURN $a + $b; })(1, 2);
                SELECT (($a, $b, $c) -> { RETURN $a + $b + $c; })(1, 2, 3);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtProjection) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
                SELECT (SELECT 1);
                SELECT (SELECT (SELECT 1));
            )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtExpression) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
                SELECT 1 + (SELECT 1);
                SELECT (SELECT 1) + 1;
            )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtUnarySubexpr) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT (SELECT 1)[0];
            SELECT (SELECT 1)[(SELECT 1)];
            SELECT (SELECT <| x: 1 |>).x;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtCoalesce) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT (SELECT 1) ?? 2;
            SELECT 1 ?? (SELECT 2);

            $x = (SELECT * FROM (VALUES (1)) AS x(a)) ?? '';

            DEFINE SUBQUERY $y($x) AS
                SELECT 1;
            END DEFINE;

            SELECT * FROM $y($x);
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtBitwise) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = (SELECT 1) | 1;       SELECT $a;
            $b = (SELECT 1) & 1;       SELECT $b;
            $c = (SELECT 1) ^ 1;       SELECT $c;
            $d = (SELECT 1) << 1;      SELECT $d;
            $e = (SELECT 1) >> 1;      SELECT $e;
            $f = 1 | (SELECT 1);       SELECT $f;
            SELECT (SELECT 1) | 1;
            SELECT 1 | (SELECT 1);
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtComparison) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = (SELECT 1) = 1;       SELECT $a;
            $b = (SELECT 1) <> 1;      SELECT $b;
            $c = (SELECT 1) == 1;      SELECT $c;
            $d = (SELECT 1) < 1;       SELECT $d;
            $e = (SELECT 1) >= 1;      SELECT $e;
            $f = 1 = (SELECT 1);       SELECT $f;
            SELECT (SELECT 1) = 1;
            SELECT 1 = (SELECT 1);
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtDistinct) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = (SELECT 1) IS DISTINCT FROM 1;         SELECT $a;
            $b = (SELECT 1) IS NOT DISTINCT FROM 1;     SELECT $b;
            $c = 1 IS DISTINCT FROM (SELECT 1);         SELECT $c;
            SELECT (SELECT 1) IS DISTINCT FROM 1;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtIn) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = (SELECT 1) IN (1, 2);     SELECT $a;
            $b = (SELECT 1) NOT IN (1, 2); SELECT $b;
            SELECT (SELECT 1) IN (1, 2);
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtIsNull) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = (SELECT 1) IS NULL;       SELECT $a;
            $b = (SELECT 1) IS NOT NULL;   SELECT $b;
            SELECT (SELECT 1) IS NULL;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtBetween) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = (SELECT 1) BETWEEN 0 AND 2;                 SELECT $a;
            $b = 1 BETWEEN (SELECT 0) AND (SELECT 2);        SELECT $b;
            $c = (SELECT 1) NOT BETWEEN 0 AND 2;             SELECT $c;
            $d = (SELECT 1) BETWEEN SYMMETRIC 2 AND 0;       SELECT $d;
            SELECT (SELECT 1) BETWEEN 0 AND 2;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtLike) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = (SELECT 'a') LIKE 'a';                  SELECT $a;
            $b = 'a' LIKE (SELECT 'a');                  SELECT $b;
            $c = (SELECT 'a') REGEXP 'a';                SELECT $c;
            $d = 'a' REGEXP (SELECT 'a');                SELECT $d;
            SELECT (SELECT 'a') LIKE 'a';
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtCase) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = CASE (SELECT 1) WHEN 1 THEN 2 ELSE 3 END;              SELECT $a;
            $b = CASE WHEN (SELECT 1) = 1 THEN (SELECT 2) ELSE (SELECT 3) END;  SELECT $b;
            SELECT CASE (SELECT 1) WHEN 1 THEN 2 ELSE 3 END;
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AtNestedOperators) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $a = ((SELECT 1) | 2) = 3;                   SELECT $a;
            $b = (SELECT 1) + (SELECT 2) * (SELECT 3);   SELECT $b;
            $c = NOT ((SELECT 1) = 1);                   SELECT $c;
            $d = (SELECT 1) ?? (SELECT 2) ?? 3;          SELECT $d;
            $e = (SELECT 1) = 1 AND (SELECT 2) = 2;      SELECT $e;
            $f = -(SELECT 1) < ~(SELECT 2);              SELECT $f;
            SELECT (SELECT 1) BETWEEN (SELECT 0) AND (SELECT 2);
        )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(UnionParenthesis) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
                SELECT ( SELECT 1  UNION  SELECT 1);
                SELECT ( SELECT 1  UNION (SELECT 1));
                SELECT ((SELECT 1) UNION  SELECT 1);
                SELECT ((SELECT 1) UNION (SELECT 1));
            )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(IntersectParenthesis) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
                SELECT ( SELECT 1  INTERSECT  SELECT 1);
                SELECT ( SELECT 1  INTERSECT (SELECT 1));
                SELECT ((SELECT 1) INTERSECT  SELECT 1);
                SELECT ((SELECT 1) INTERSECT (SELECT 1));
            )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(UnionIntersect) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
                SELECT (SELECT 1 UNION     SELECT 1 UNION     SELECT 1);
                SELECT (SELECT 1 UNION     SELECT 1 INTERSECT SELECT 1);
                SELECT (SELECT 1 INTERSECT SELECT 1 UNION     SELECT 1);
                SELECT (SELECT 1 INTERSECT SELECT 1 INTERSECT SELECT 1);
            )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ScalarExpressionUnion) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT ((2 + 2) UNION (2 * 2));
        )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "2:24: Error: Expected SELECT/PROCESS/REDUCE statement");
}

Y_UNIT_TEST(OrderByIgnorance1) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
                SELECT (SELECT * FROM (SELECT * FROM (SELECT 1 AS x UNION SELECT 2 AS x) ORDER BY x));

            )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "ORDER BY without LIMIT in subquery will be ignored");

    TWordCountHive stat = {{TString("Sort"), 0}};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Sort"], 0);
}

Y_UNIT_TEST(OrderByIgnorance2) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
                SELECT (SELECT * FROM (SELECT 1 AS x UNION SELECT 2 AS x) ORDER BY x);
            )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "ORDER BY without LIMIT in subquery will be ignored");

    TWordCountHive stat = {{TString("Sort"), 0}};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Sort"], 0);
}

Y_UNIT_TEST(InSubquery) {
    NYql::TAstParseResult res;

    res = SqlToYql(R"sql(
                SELECT 1 IN (SELECT 1);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    res = SqlToYql(R"sql(
                SELECT * FROM (SELECT 1 AS x) WHERE x IN (SELECT 1);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    res = SqlToYql(R"sql(
                SELECT * FROM (SELECT 1 AS x) WHERE x IN ((SELECT 1));
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(GroupByUnit) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                SELECT * FROM (SELECT 1) GROUP BY ();
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(NamedNodeUnion) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                $a = (SELECT 1);
                $b = (SELECT 1);
                $x = ($a UNION $b);
                SELECT * FROM $x;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(NamedNodeExpr) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                $a = 1;            SELECT $a;
                $b = SELECT 1;     SELECT $b;
                $c = (SELECT 1);   SELECT $c;
                $d = ((SELECT 1)); SELECT $d;
                                   SELECT $b + 1;
                                   SELECT ($b);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(NamedNodeProcess) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                $a = SELECT 1, 2;
                $a = PROCESS $a;
                SELECT $a;
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(SubqueryDeduplication) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                DEFINE SUBQUERY $sub() AS
                    SELECT * FROM (SELECT 1);
                END DEFINE;
                SELECT * FROM $sub();
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(NamedNode) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
                $x = (SELECT 1 AS x);
                SELECT 1 < $x;
                SELECT 1 < ($x);
            )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(LangVerBefore202504) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = 202502;

    NYql::TAstParseResult res;
    res = SqlToYqlWithSettings(R"sql(SELECT 1 + (SELECT 1);)sql", s);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "is not available");

    res = SqlToYqlWithSettings(R"sql(SELECT (SELECT 1);)sql", s);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "is not available");

    res = SqlToYqlWithSettings(R"sql($x = SELECT 1 + (SELECT 1); SELECT $x;)sql", s);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "is not available");

    res = SqlToYqlWithSettings(R"sql($x = SELECT (SELECT 1); SELECT $x;)sql", s);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "is not available");

    // Historically, there was a bug, because of which a langver was not checked.
    res = SqlToYqlWithSettings(R"sql($x = (SELECT 1);)sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(InsideLambda) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res;

    res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $f = ($name) -> { RETURN (SELECT * FROM $name); };
            SELECT $f('1');
        )sql", s);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Reading a table in a pure context");

    res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $f = ($name) -> { RETURN 1 + (SELECT * FROM $name); };
            SELECT $f('1');
        )sql", s);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Reading a table in a pure context");

    res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $f = ($name) -> {
                $x = 1 + (SELECT * FROM $name);
                RETURN 1 + $x;
            };
            SELECT $f('1');
        )sql", s);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Reading a table in a pure context");

    res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $f = ($name) -> {
                $x = 1 + (SELECT $name);
                RETURN 1 + $x;
            };
            SELECT $f('1');
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    res = SqlToYqlWithSettings(R"sql(
            USE plato;
            $f = ($name) -> { RETURN (SELECT $name); };
            SELECT $f('1');
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ReadsFromJoinSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT * FROM plato.x JOIN (SELECT * FROM plato.y) AS y ON x.a = y.a;
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 2);
}

Y_UNIT_TEST(ReadsProjectionJoinSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT (SELECT a FROM plato.x JOIN (SELECT * FROM plato.y) AS y ON x.a = y.a);
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 2);
}

Y_UNIT_TEST(ReadsProjectionJoinInSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT 1 IN (SELECT a FROM plato.x JOIN (SELECT * FROM plato.y) AS y ON x.a = y.a);
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 2);
}

Y_UNIT_TEST(ReadsProjectionJoinExistsSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT EXISTS (SELECT a FROM plato.x JOIN (SELECT * FROM plato.y) AS y ON x.a = y.a);
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 2);
}

Y_UNIT_TEST(ReadsNamedNodeJoinExistsSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $x = (SELECT a FROM plato.x JOIN (SELECT * FROM plato.y) AS y ON x.a = y.a);
            SELECT $x;
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 2);
}

Y_UNIT_TEST(ReadsProjectionExpresionSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT 1 + (SELECT a FROM plato.x);
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 1);
}

Y_UNIT_TEST(ReadsNamedNodeExpresionSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            $x = 1 + (SELECT a FROM plato.x);
            $y = (SELECT a FROM plato.x) + 1;
            $z = (SELECT a FROM plato.x);
            $h = ((SELECT a FROM plato.x));
            SELECT $x, $y, $z, $h;
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 4);
}

Y_UNIT_TEST(ReadsProjectionFromSubquery) {
    NSQLTranslation::TTranslationSettings s;
    s.LangVer = NYql::NFeature::InlineSubquery.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            SELECT (SELECT a FROM plato.x) FROM (SELECT * FROM plato.y);
        )sql", s);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"Read!"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 2);
}

} // Y_UNIT_TEST_SUITE(InlineUncorrelatedSubquery)

Y_UNIT_TEST_SUITE(Watermarks) {

Y_UNIT_TEST(InsertAsDeprecated) {
    ExpectFailWithError(
        R"sql(
            USE plato;

            INSERT INTO Output
            SELECT * FROM Input
            WITH WATERMARK AS (ts - Interval("PT1S"));
        )sql",
        "<main>:6:12: Error: extraneous input 'WITH' expecting {<EOF>, ';'}\n");
}

Y_UNIT_TEST(SelectAsDeprecated) {
    ExpectFailWithError(
        R"sql(
            USE plato;

            SELECT * FROM Input
            WITH WATERMARK AS (ts - Interval("PT1S"));
        )sql",
        "<main>:5:12: Error: extraneous input 'WITH' expecting {<EOF>, ';'}\n");
}

Y_UNIT_TEST(InsertEquals) {
    auto res = SqlToYql(R"sql(
            USE plato;

            INSERT INTO Output
            SELECT * FROM Input
            WITH WATERMARK = ts - Interval("PT1S");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 0);
}

Y_UNIT_TEST(SelectEquals) {
    auto res = SqlToYql(R"sql(
            USE plato;

            SELECT * FROM Input
            WITH WATERMARK = ts - Interval("PT1S");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 0);
}

Y_UNIT_TEST(TopLevel) {
    auto res = SqlToYql(R"sql(
            USE plato;

            $input = SELECT * FROM Input;

            SELECT * FROM $input
            WITH WATERMARK = ts - Interval("PT1S");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

Y_UNIT_TEST(Multiple) {
    auto res = SqlToYql(R"sql(
            USE plato;

            $input = SELECT * FROM Input
            WITH WATERMARK = ts - Interval("PT1S");

            $input = SELECT * FROM $input
            WITH WATERMARK = ts - Interval("PT2S");

            SELECT * FROM $input
            WITH WATERMARK = ts - Interval("PT3S");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 2);
}

Y_UNIT_TEST(NoSuchColumn) {
    ExpectFailWithError(
        R"sql(
            USE plato;

            $input = SELECT xxx FROM Input;

            SELECT * FROM $input
            WITH WATERMARK = ts - Interval("PT1S");
        )sql",
        "<main>:7:30: Error: Column ts is not in source column set\n");
}

Y_UNIT_TEST(GroupBy) {
    auto res = SqlToYql(R"sql(
            USE plato;

            $input = SELECT * FROM Input
            GROUP BY key, HoppingWindow(ts, "PT1S", "PT2S");

            SELECT * FROM $input
            WITH WATERMARK = ts - Interval("PT1S");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

Y_UNIT_TEST(Join) {
    auto res = SqlToYql(R"sql(
            USE plato;

            $input = SELECT * FROM Input AS lhs JOIN Input AS rhs ON lhs.ts == rhs.ts;

            SELECT * FROM $input
            WITH WATERMARK = ts - Interval("PT1S");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

Y_UNIT_TEST(ParseProtoseq) {
    auto res = SqlToYql(R"sql(
            USE plato;

            $input = SELECT Protobuf::Parse(records) AS event FROM Input
            FLATTEN LIST BY (ChunksSplitters::Protoseq(Data).SplitRecords AS records);

            SELECT * FROM $input
            WITH WATERMARK = event.ts - Interval("PT1S");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

Y_UNIT_TEST(ComplexParseWithOptions) {
    auto res = SqlToYql(R"sql(
            USE plato;

            $input = SELECT Protobuf::Parse(records) AS event FROM Input
            FLATTEN LIST BY (ChunksSplitters::Protoseq(Data).SplitRecords AS records);

            SELECT * FROM $input
            WITH (
                WATERMARK = event.ts - Interval("PT1S"),
                WATERMARK_GRANULARITY = "PT1S",
                WATERMARK_IDLE_TIMEOUT = "PT1M"
            );
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        "WatermarkGenerator",
        "watermarkgranularity",
        "watermarkidletimeout",
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermarkgranularity"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermarkidletimeout"], 1);
}

Y_UNIT_TEST(ExprList) {
    auto res = SqlToYql(R"sql(
            USE plato;

            $input = SELECT * FROM Input;

            SELECT * FROM $input
            WITH WATERMARK = ("yin", "yang");
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"watermark", "WatermarkGenerator"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["watermark"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["WatermarkGenerator"], 1);
}

} // Y_UNIT_TEST_SUITE(Watermarks)

Y_UNIT_TEST_SUITE(HybridRankFusionLambda) {

// A custom fusion lambda cannot live inside the named-args struct (a lambda is not a struct-field value),
// so the SQL builder lifts it out into trailing positional children of HybridRank: a "rank"/"score" marker
// followed by the lambda (the marker comes first because it selects the lambda's argument type). RankLambda
// fuses per-branch ranks, so the marker is "rank".
Y_UNIT_TEST(RankLambdaIsLiftedWithRankMarker) {
    auto res = SqlToYql(R"sql(
            USE plato;
            $t = "x";
            SELECT key FROM Input
            ORDER BY HybridRank(
                FullTextScore(text, "cats"),
                Knn::CosineDistance(embedding, $t),
                ($ranks) -> { RETURN 1.0 / (60 + COALESCE($ranks[0], 100000)); } AS RankLambda)
            LIMIT 4;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"HybridRank"}};
    const auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["HybridRank"], 1, prog);
    // The named-args struct is left empty and the lambda becomes a positional child, preceded by its "rank"
    // marker.
    UNIT_ASSERT_STRING_CONTAINS(prog, "(AsStruct) '\"rank\" (lambda");
}

// ScoreLambda fuses the raw per-branch scores, so the marker is "score" (the type checker binds Double).
Y_UNIT_TEST(ScoreLambdaIsLiftedWithScoreMarker) {
    auto res = SqlToYql(R"sql(
            USE plato;
            $t = "x";
            SELECT key FROM Input
            ORDER BY HybridRank(
                FullTextScore(text, "cats"),
                Knn::CosineDistance(embedding, $t),
                ($scores) -> { RETURN COALESCE($scores[0], 0.0); } AS ScoreLambda)
            LIMIT 4;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"HybridRank"}};
    const auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["HybridRank"], 1, prog);
    UNIT_ASSERT_STRING_CONTAINS(prog, "(AsStruct) '\"score\" (lambda");
}

// Only the fusion lambda is lifted; other named options stay as members of the (now non-empty) struct.
Y_UNIT_TEST(FusionLambdaCoexistsWithNamedOptions) {
    auto res = SqlToYql(R"sql(
            USE plato;
            $t = "x";
            SELECT key FROM Input
            ORDER BY HybridRank(
                FullTextScore(text, "cats"),
                Knn::CosineDistance(embedding, $t),
                ("ft_idx", "vec_idx") AS Indexes,
                ($ranks) -> { RETURN 1.0 / (60 + COALESCE($ranks[0], 100000)); } AS RankLambda)
            LIMIT 4;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive elementStat = {{"HybridRank"}};
    const auto prog = VerifyProgram(res, elementStat);
    UNIT_ASSERT_VALUES_EQUAL_C(elementStat["HybridRank"], 1, prog);
    // Only the lambda is lifted (marker + lambda follow the struct); the other option stays a struct member.
    UNIT_ASSERT_STRING_CONTAINS(prog, "'\"rank\" (lambda");
    UNIT_ASSERT_STRING_CONTAINS(prog, "Indexes");
}

// RankLambda and ScoreLambda are mutually exclusive; giving both is rejected by the SQL builder.
Y_UNIT_TEST(RejectsBothFusionLambdas) {
    auto res = SqlToYql(R"sql(
            USE plato;
            $t = "x";
            SELECT key FROM Input
            ORDER BY HybridRank(
                FullTextScore(text, "cats"),
                Knn::CosineDistance(embedding, $t),
                ($ranks)  -> { RETURN 1.0; } AS RankLambda,
                ($scores) -> { RETURN 2.0; } AS ScoreLambda)
            LIMIT 4;
        )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "at most one of RankLambda or ScoreLambda");
}

} // Y_UNIT_TEST_SUITE(HybridRankFusionLambda)

Y_UNIT_TEST_SUITE(SelectExplicitPositions) {

Y_UNIT_TEST(OrderByEmptyAction) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Disable;
    ExpectFailWithError(
        "SELECT * FROM plato.t ORDER BY key DESC, EMPTY_ACTION;",
        "<main>:1:42: Error: Unable to ORDER BY constant expression\n",
        settings);
}

Y_UNIT_TEST(OrderByEmptyActionOnSeparateLine) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Disable;
    ExpectFailWithError(
        "SELECT * FROM plato.t\nORDER BY key DESC,\n    EMPTY_ACTION;",
        "<main>:3:5: Error: Unable to ORDER BY constant expression\n",
        settings);
}

Y_UNIT_TEST(AnyWithoutJoin) {
    ExpectFailWithError(
        "SELECT *\nFROM ANY\n    plato.Input;",
        "<main>:2:6: Error: ANY is not allowed without JOIN\n");
}

Y_UNIT_TEST(CrossJoinConstraint) {
    ExpectFailWithError(
        "SELECT * FROM plato.Input AS a\nCROSS JOIN plato.Input AS b\n    ON a.key = b.key;",
        "<main>:3:5: Error: Cross join should not have ON or USING expression\n");
}

Y_UNIT_TEST(JoinWithoutConstraint) {
    ExpectFailWithError(
        "SELECT * FROM plato.Input AS a\n    INNER JOIN\n        plato.Input AS b;",
        "<main>:2:5: Error: Expected ON or USING expression\n");
}

Y_UNIT_TEST(DuplicateQualifiedAsterisk) {
    ExpectFailWithError(
        "SELECT a.*,\n    a.* FROM plato.Input AS a;",
        "<main>:2:5: Error: Unable to use twice same qualified asterisk. Invalid source: a\n");
}

Y_UNIT_TEST(LimitBeforeUnion) {
    ExpectFailWithError(
        "SELECT 1\n    LIMIT 1\nUNION ALL SELECT 2;",
        "<main>:2:5: Error: LIMIT within UNION ALL is only allowed after last subquery\n");
}

Y_UNIT_TEST(AnonymousTableWithContextHints) {
    ExpectFailWithError(
        "USE plato;\n$input = SELECT 1 AS value;\nPRAGMA DirectRead;\nSELECT * FROM @$input;",
        "<main>:4:15: Error: Hints are not supported for anonymous tables\n");
}

Y_UNIT_TEST(EmptyQualifiedWithoutColumn) {
    ExpectFailWithError(
        "SELECT * WITHOUT a.\n    `` FROM plato.Input AS a;",
        "<main>:2:5: Error: Empty column name is not allowed\n");
}

Y_UNIT_TEST(RejectedHintAfterWatermarkFromSeveralHints) {
    ExpectFailWithError(
        "SELECT * FROM (VALUES (1)) WITH (\n    WATERMARK = 1,\n    zlock,\n    xlock);",
        "<main>:4:5: Error: Hint 'xlock' requires a table\n");
}

Y_UNIT_TEST(TableHintKeysAndValuesKeepTheirPositions) {
    const auto result = SqlToYql("SELECT * FROM plato.Input WITH (\n"
                                 "    zeta_setting = \"old\",\n"
                                 "        flag,\n"
                                 "  alpha = \"first\",\n"
                                 "    zeta_setting = \"last\");");
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));

    const auto assertPosition = [&](TStringBuf content, ui32 row, ui32 column) {
        const auto* quote = FindNodeByChildAtomContent(result.Root, 1, content);
        UNIT_ASSERT_C(quote, content);
        const auto& position = quote->GetChild(1)->GetPosition();
        UNIT_ASSERT_VALUES_EQUAL_C(position.Row, row, content);
        UNIT_ASSERT_VALUES_EQUAL_C(position.Column, column, content);
    };

    assertPosition("zetasetting", 5, 5);
    assertPosition("last", 5, 20);
    assertPosition("flag", 3, 9);
    assertPosition("alpha", 4, 3);
    assertPosition("first", 4, 11);
}

Y_UNIT_TEST(ContextOnlyTableHintUsesSourcePosition) {
    const auto result = SqlToYql("PRAGMA DirectRead;\nSELECT * FROM plato.Input;");
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));

    const auto* quote = FindNodeByChildAtomContent(result.Root, 1, "directread");
    UNIT_ASSERT(quote);
    const auto& position = quote->GetChild(1)->GetPosition();
    UNIT_ASSERT_VALUES_EQUAL(position.Row, 2);
    UNIT_ASSERT_VALUES_EQUAL(position.Column, 15);
}

Y_UNIT_TEST(WatermarkOptionRetainsOriginalKeyPosition) {
    const auto result = SqlToYql("SELECT * FROM (VALUES (1)) WITH (\n"
                                 "    WATERMARK = ts,\n"
                                 "        WATERMARK_LAG = \"5\",\n"
                                 "    watermarklag = \"10\");");
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));

    const auto* quote = FindNodeByChildAtomContent(result.Root, 1, "watermarklag");
    UNIT_ASSERT(quote);
    const auto& position = quote->GetChild(1)->GetPosition();
    UNIT_ASSERT_VALUES_EQUAL(position.Row, 3);
    UNIT_ASSERT_VALUES_EQUAL(position.Column, 9);
}

} // Y_UNIT_TEST_SUITE(SelectExplicitPositions)
