#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(YqlSelect) {

Y_UNIT_TEST(LangVer) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::MakeLangVersion(2025, 5);

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "YqlSelect is not available before language version 2026.02");
}

Y_UNIT_TEST(AutoTopLevel) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'auto';
        USE plato;
        SELECT * FROM my_table VIEW my_view;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(AutoSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'auto';
        USE plato;
        SELECT * FROM my_table
        WHERE a IN (SELECT a FROM my_table VIEW my_view);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(Minimal) {
    NSQLTranslation::TTranslationSettings settings;
    // TODO: remove YqlSelectLangVersion
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {{TString("YqlSelect"), 0}};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
}

Y_UNIT_TEST(Expr) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 2 + 2 * 2;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
        {TString("+MayWarn"), 0},
        {TString("*MayWarn"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["+MayWarn"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["*MayWarn"], 1);
}

Y_UNIT_TEST(ResultColumnLabel) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 AS x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {{TString("YqlSelect"), 0}};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
}

Y_UNIT_TEST(Asterisk) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT * FROM (VALUES (1));
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
        {TString("YqlStar"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlStar"], 1);
}

Y_UNIT_TEST(AsteriskInvalidLeft) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT *, a FROM (VALUES (1)) AS x(a);
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Unable to use plain '*' with other projection items");
}

Y_UNIT_TEST(AsteriskInvalidRight) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, * FROM (VALUES (1)) AS x(a);
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Unable to use plain '*' with other projection items");
}

Y_UNIT_TEST(FromAnonValues) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM (VALUES (1));
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
        {TString("YqlValuesList"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlValuesList"], 1);
}

Y_UNIT_TEST(FromValues) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a FROM (VALUES (1)) AS x (a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
        {TString("YqlValuesList"), 0},
        {TString("YqlColumnRef"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlValuesList"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlColumnRef"], 1);
}

Y_UNIT_TEST(FromTableWithImmediateCluster) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, b FROM plato.Input;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
        {TString("Read!"), 0},
    };
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('((Right! yql_read0) '"Input" '()))");
}

Y_UNIT_TEST(FromQuotedTableWithImmediateCluster) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, b FROM plato.`/Root/Yql/Select`;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
        {TString("Read!"), 0},
    };
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('((Right! yql_read0) '"/Root/Yql/Select" '()))");
}

Y_UNIT_TEST(FromQuotedTableWithImmediateClusterWithTablePrefixPath) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        USE plato;
        PRAGMA YqlSelect = 'force';
        PRAGMA TablePathPrefix = '/Root/Yql';
        SELECT a, b FROM Select;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "Read!"};

    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"((Key '('table (String '"/Root/Yql/Select"))) (Void) '()))");
}

Y_UNIT_TEST(FromNamedNodeWithQuotedTableWithImmediateClusterWithTablePrefixPath) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        USE plato;
        PRAGMA YqlSelect = 'force';
        PRAGMA TablePathPrefix = '/Root/Yql';
        $x = SELECT a FROM Select;
        SELECT $x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "Read!"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"((Key '('table (String '"/Root/Yql/Select"))) (Void) '()))");
}

// The test checks that the prefix is not added for temporary tables.
Y_UNIT_TEST(FromTmpTableWithImmediateClusterWithTablePrefixPath) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        USE plato;
        PRAGMA YqlSelect = 'force';
        PRAGMA TablePathPrefix = '/Root';
        INSERT INTO @tmp (a) VALUES (1);
        COMMIT;
        SELECT a FROM @tmp;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "Read!", "TempTable"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["TempTable"], 2);
    UNIT_ASSERT_STRING_CONTAINS(program, R"(TempTable '"tmp")");
}

Y_UNIT_TEST(FromTmpTableWithImmediateCluster) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        INSERT INTO plato.@tmp (a) VALUES (1);
        COMMIT;
        SELECT a FROM plato.@tmp;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "Read!", "TempTable"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["Read!"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["TempTable"], 2);
}

Y_UNIT_TEST(Where) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, b FROM plato.Input WHERE a < 10;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
        {TString("YqlWhere"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlWhere"], 1);
}

Y_UNIT_TEST(FromSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT * FROM (SELECT 1 as a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
}

Y_UNIT_TEST(Join2) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        USE plato;
        SELECT id FROM xx JOIN yy ON xx.id = yy.id;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(QualifiedAsteriskJoin) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        USE plato;
        SELECT xx.* FROM xx JOIN yy ON xx.id = yy.id;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "YqlSelect unsupported: an_id DOT ASTERISK");
}

Y_UNIT_TEST(Join2Alias) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        USE plato;
        SELECT id
        FROM xx AS x
        JOIN yy AS y ON x.id = y.id;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(Join3) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        USE plato;
        SELECT id
        FROM xx AS x
        JOIN yy AS y ON x.id = y.id
        JOIN zz AS z ON y.id = z.id;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(Join3Mix) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        USE plato;
        SELECT id
        FROM (VALUES (1)) AS x(id)
        JOIN (SELECT * FROM yy) AS y ON x.id = y.id
        JOIN zz AS z ON y.id = z.id;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ImplicitCrossIsDisabled) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT id FROM plato.xx, plato.yy;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "Cartesian product of tables is disabled");
}

Y_UNIT_TEST(ImplicitCrossJoin2) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;
        SELECT id FROM plato.xx, plato.yy;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ImplicitCrossJoin3) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;
        SELECT id FROM plato.xx, plato.yy, plato.zz;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ImplicitCrossJoinAndExplicitJoin) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;
        SELECT id
        FROM plato.xx, plato.yy
        JOIN plato.zz ON xx.id = zz.id;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ImplicitCrossJoinColumnName) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;
        USE plato;
        SELECT a FROM x, y;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlResultItem", "YqlColumnRef"};
    VerifyProgram(res, stat, [](const TString&, const TString& line) {
        UNIT_ASSERT_STRING_CONTAINS(line, R"(YqlResultItem '"a")");
        UNIT_ASSERT_STRING_CONTAINS(line, R"(YqlColumnRef '"a")");
    });
}

Y_UNIT_TEST(ImplicitCrossJoinColumnNameRename) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;
        USE plato;
        SELECT x.a AS b FROM x, y;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlResultItem", "YqlColumnRef"};
    VerifyProgram(res, stat, [](const TString&, const TString& line) {
        UNIT_ASSERT_STRING_CONTAINS(line, R"(YqlResultItem '"b")");
        UNIT_ASSERT_STRING_CONTAINS(line, R"(YqlColumnRef '"x" '"a")");
    });
}

Y_UNIT_TEST(ExplicitCrossJoin) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT id FROM plato.xx CROSS JOIN plato.yy;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(LeftRightOuterJoin) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT id FROM plato.xx LEFT        JOIN plato.yy ON TRUE;
        SELECT id FROM plato.xx RIGHT       JOIN plato.yy ON TRUE;
        SELECT id FROM plato.xx LEFT  OUTER JOIN plato.yy ON TRUE;
        SELECT id FROM plato.xx RIGHT OUTER JOIN plato.yy ON TRUE;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(OrderBy) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a
        FROM (VALUES (1, 2)) AS x(a, b)
        ORDER BY a ASC, b DESC;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(LimitOffset) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 LIMIT 2 OFFSET 3;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CorrelatedProjection) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT (SELECT a) FROM (VALUES (1)) AS x(a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(InSubqueryProjection) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT (1 IN (SELECT 1))
        FROM (VALUES (1)) AS x(a)
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(InSubqueryWhere) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a
        FROM (VALUES (1, 10)) AS x(a, b)
        WHERE b IN (
            SELECT y.b / 10 FROM (VALUES (1, 100)) AS y(a, b));
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(ExistsSubqueryWhere) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a
        FROM (VALUES (1, 10)) AS x(a, b)
        WHERE EXISTS (
            SELECT y.b / 10
            FROM (VALUES (1, 100)) AS y(a, b)
            WHERE x.a == y.a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {
        {TString("YqlSelect"), 0},
    };
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 4);
}

Y_UNIT_TEST(GroupByUnsupportsTwoArg) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT AggList(a) FROM (VALUES (1)) AS x(a);
    )sql", settings);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: Aggregation 'AggList'");
}

Y_UNIT_TEST(GroupByCountImplicit) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT Count(b) FROM (VALUES (1)) AS x(b);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlColumnRef", "YqlGroup", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlColumnRef"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 1 + 1);
}

Y_UNIT_TEST(GroupByCount) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, Count(b) FROM (VALUES (1, 2)) AS x(a, b) GROUP BY a;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlColumnRef", "YqlGroup", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlColumnRef"], 2 + 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 1 + 1);
}

Y_UNIT_TEST(GroupByCountAll) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, Count(*) FROM (VALUES (1, 2)) AS x(a, b) GROUP BY a;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlColumnRef", "YqlGroup", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlColumnRef"], 1 + 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 1 + 1);
}

Y_UNIT_TEST(GroupByMinOrMax) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, Min(b) FROM (VALUES (1, 2)) AS x(a, b) GROUP BY a;
        SELECT a, Max(b) FROM (VALUES (1, 2)) AS x(a, b) GROUP BY a;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlColumnRef", "YqlGroup", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2 * 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlColumnRef"], 2 * 3);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 2 * 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 2 * (1 + 1));
}

Y_UNIT_TEST(GroupBySum) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, Sum(b) FROM (VALUES (1, 2)) AS x(a, b) GROUP BY a;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlColumnRef", "YqlGroup", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlColumnRef"], 3);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 1 + 1);
}

Y_UNIT_TEST(GroupByAvg) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, Avg(b) FROM (VALUES (1, 2)) AS x(a, b) GROUP BY a;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlColumnRef", "YqlGroup", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlColumnRef"], 3);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 1 + 1);
}

Y_UNIT_TEST(GroupByExplicitWithHaving) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, Avg(b) FROM (VALUES (1, 2)) AS x(a, b)
        GROUP BY a HAVING 1 < a AND Avg(b) < 2;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlGroup", "YqlWhere", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlWhere"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 2 * (1 + 1));
}

Y_UNIT_TEST(GroupByImplicitWithHaving) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT Avg(b) FROM (VALUES (1, 2)) AS x(a, b) HAVING Avg(b) < 2;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlGroup", "YqlWhere", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlWhere"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 2 * (1 + 1));
}

Y_UNIT_TEST(GroupByDistinctArgument) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT Avg(DISTINCT a) FROM (VALUES (1)) AS x(a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlGroup", "YqlWhere", "YqlAgg"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAgg"], 1 + 1);
}

Y_UNIT_TEST(AutoGroupByCompactHint) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'auto';
        SELECT k, Avg(v) FROM plato.x GROUP /*+ COMPACT() */ BY k;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "Aggregate", "compact"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0);
    UNIT_ASSERT_VALUES_EQUAL(stat["Aggregate"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["compact"], 1);
}

Y_UNIT_TEST(GroupByExprAliasUnsupported) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a1 FROM plato.x GROUP BY a + 1 AS a1;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());

    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: GROUP BY aliases");
}

Y_UNIT_TEST(GroupByExprUnnamed) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a + 1 FROM plato.x GROUP BY a + 1;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());

    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Unnamed expressions are not supported here");
}

Y_UNIT_TEST(GroupByExprAllowUnnamed) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA YqlSelectAllowUnnamedGroupByExpr;
        SELECT a + 1 FROM plato.x GROUP BY a + 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(LegacyGroupByExprAllowUnnamed) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelectAllowUnnamedGroupByExpr;
        SELECT a + 1 FROM plato.x GROUP BY a + 1;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());

    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Error: Column `a` must either be a key column");
}

Y_UNIT_TEST(GroupByGroupingSetsOneColumn) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT a, Sum(z) FROM plato.x GROUP BY GROUPING SETS (a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
}

Y_UNIT_TEST(GroupByGroupingSetsTwoColumn) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY GROUPING SETS (a, b);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
}

Y_UNIT_TEST(GroupByGroupingSetsOneEmptyTuple) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY GROUPING SETS (());
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
}

Y_UNIT_TEST(GroupByGroupingSetsOneTuple1) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY GROUPING SETS ((a, b));
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
}

Y_UNIT_TEST(GroupByGroupingSetsTwoTuple1) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY GROUPING SETS ((a, b, c), (a, b));
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
}

Y_UNIT_TEST(GroupByGroupingSetsOneTuple2) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA YqlSelectAllowUnnamedGroupByExpr;
        SELECT (a, b) FROM plato.x GROUP BY GROUPING SETS (((a, b)));
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
}

Y_UNIT_TEST(GroupByGroupingSetsOneExpr) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA YqlSelectAllowUnnamedGroupByExpr;
        SELECT a + 1 FROM plato.x GROUP BY GROUPING SETS (a + 1);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
}

Y_UNIT_TEST(GroupByGroupingSetsNestedSpecUnsupported) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY GROUPING SETS (GROUPING SETS (a));
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());

    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: GROUPING SETS with nested ROLLUP/CUBE/GROUPING SETS");
}

Y_UNIT_TEST(GroupByHopUnsupported) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY HOP(a, "PT10S", "PT30S", "PT20S");
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());

    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: hopping_window_specification");
}

Y_UNIT_TEST(GroupByRollup) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY ROLLUP (a, b, c);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet", "rollup"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["rollup"], 1);
}

Y_UNIT_TEST(GroupByRollupSublists) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY ROLLUP (a, (b, c));
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());

    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: ROLLUP/CUBE sublists of elements in parenthesis");
}

Y_UNIT_TEST(GroupByCube) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY CUBE (a, b, c);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroupingSet", "cube"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["cube"], 1);
}

Y_UNIT_TEST(GroupByCubeSublists) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x GROUP BY CUBE (a, (b, c));
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());

    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: ROLLUP/CUBE sublists of elements in parenthesis");
}

Y_UNIT_TEST(GroupByMixedSpecs) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT 1 FROM plato.x
        GROUP BY
            a,
            GROUPING SETS (b, c),
            ROLLUP (a, b, c),
            CUBE (a, b, c);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlGroup", "YqlGroupingSet", "sets", "rollup", "cube"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroup"] - stat["YqlGroupingSet"], 4);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlGroupingSet"], 3);
    UNIT_ASSERT_VALUES_EQUAL(stat["sets"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["rollup"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["cube"], 1);
}

Y_UNIT_TEST(DiagnosticMandatoryAsColumn) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA DisableAnsiOptionalAs;
        SELECT 1 a;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":4:16: Error: Expecting mandatory AS here");
}

Y_UNIT_TEST(DiagnosticMandatoryAsTable) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA DisableAnsiOptionalAs;
        SELECT a FROM (SELECT 1 AS a) x;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":4:36: Error: Expecting mandatory AS here");
}

Y_UNIT_TEST(NamedNodeSubqueryScalar) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        $x = (SELECT 1);
        $y = SELECT 1;
        SELECT $x, $y;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlSubLink"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3 + 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSubLink"], 2);
}

Y_UNIT_TEST(NamedNodeSubqueryIn) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        $x = (SELECT 1);
        $y = SELECT 1;
        SELECT 1 IN $x, 1 IN $y;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlSubLink"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3 + 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSubLink"], 2);
}

Y_UNIT_TEST(NamedNodeSubqueryExists) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        $x = (SELECT 1);
        $y = SELECT 1;
        SELECT 1 FROM (SELECT 1)
        WHERE EXISTS $x AND EXISTS $y;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRINGS_EQUAL(Err2Str(res), "<main>:6:21: Error: no viable alternative at input 'EXISTS $'\n");
}

Y_UNIT_TEST(NamedNodeSubquerySource) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;
        $x = (SELECT 1 AS a);
        $y = SELECT 1 AS b;
        SELECT a, b FROM $x, $y;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlSubLink"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3 + 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSubLink"], 0);
}

Y_UNIT_TEST(NamedNodeSubqueryReuse) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiImplicitCrossJoin;
        $x = (SELECT 1 AS a);
        SELECT a FROM $x;
        SELECT a FROM $x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3 + 1);
}

Y_UNIT_TEST(SelectOpUnion) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        (SELECT 1 AS a) UNION (SELECT 2 AS a);
        (SELECT 1 AS a) UNION DISTINCT (SELECT 2 AS a);
        (SELECT 1 AS a) UNION ALL (SELECT 2 AS a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3);
}

Y_UNIT_TEST(SelectOpExcept) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        (SELECT 1 AS a) EXCEPT (SELECT 2 AS a);
        (SELECT 1 AS a) EXCEPT DISTINCT (SELECT 2 AS a);
        (SELECT 1 AS a) EXCEPT ALL (SELECT 2 AS a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3);
}

Y_UNIT_TEST(SelectOpIntersect) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        (SELECT 1 AS a) INTERSECT (SELECT 2 AS a);
        (SELECT 1 AS a) INTERSECT DISTINCT (SELECT 2 AS a);
        (SELECT 1 AS a) INTERSECT ALL (SELECT 2 AS a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3);
}

Y_UNIT_TEST(SelectOpUnionSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT (SELECT 1 AS a UNION SELECT 2 AS a);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlSetItem"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSetItem"], 3);
}

Y_UNIT_TEST(SelectOpUnionSubqueryParenthesis) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res;

    res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT ((SELECT 1 AS a) UNION SELECT 2 AS a);
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "YqlSelect unsupported: tuple_or_expr at UNION/EXCEPT/INTERSECT context");

    res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        SELECT (SELECT 1 AS a UNION (SELECT 2 AS a));
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        "YqlSelect unsupported: tuple_or_expr at UNION/EXCEPT/INTERSECT context");
}

Y_UNIT_TEST(TopLevelHintIsNotAvailable) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::MakeLangVersion(2025, 02);

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT /*+ yqlselect(auto) */ 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0);
}

Y_UNIT_TEST(TopLevelHintBadArgumentWarning) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT /*+ yqlselect(xxx) */ 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Invalid 'yqlselect' parameters");
}

Y_UNIT_TEST(TopLevelHintShadow) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT /*+ yqlselect(disable) yqlselect(force) */ 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Hint yqlselect will not be used");

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
}

Y_UNIT_TEST(TopLevelHintStatements) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT /*+ yqlselect(disable) */ 1;
        SELECT /*+ yqlselect(auto) */ 1;
        SELECT /*+ yqlselect(force) */ 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0 + 1 + 1);
}

Y_UNIT_TEST(TopLevelHintBeatsPragmaDisable) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'disable';
        SELECT /*+ yqlselect(disable) */ 1;
        SELECT /*+ yqlselect(auto) */ 1;
        SELECT /*+ yqlselect(force) */ 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0 + 1 + 1);
}

Y_UNIT_TEST(TopLevelHintBeatsPragmaAuto) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'auto';
        SELECT /*+ yqlselect(disable) */ 1;
        SELECT /*+ yqlselect(auto) */ 1;
        SELECT /*+ yqlselect(force) */ 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0 + 1 + 1);

    res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'auto';
        SELECT /*+ yqlselect(force) */ STREAM 1;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "was forced, but unsupported");
}

Y_UNIT_TEST(TopLevelHintStrangeOnUnionFirstDefining) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT /*+ yqlselect(force) */ 1 AS x
        UNION
        SELECT 2 AS x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlSetItem"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSetItem"], 2);
}

Y_UNIT_TEST(TopLevelHintStrangeOnUnionSecondIgnored) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT 1 AS x
        UNION
        SELECT /*+ yqlselect(force) */ 2 AS x;
    )sql", settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Hint yqlselect will not be used");

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0);
}

Y_UNIT_TEST(TopLevelHintStrangeOnUnionFirstInParensDefining) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        (SELECT /*+ yqlselect(force) */ 1 AS x)
        UNION
        (SELECT 2 AS x);
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlSetItem"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSetItem"], 2);
}

Y_UNIT_TEST(TopLevelHintStrangeOnUnionSecondInParensIgnored) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        (SELECT 1 AS x)
        UNION
        (SELECT /*+ yqlselect(force) */ 2 AS x);
    )sql", settings);

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Hint yqlselect will not be used");

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0);
}

Y_UNIT_TEST(QuotedAtoms) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        USE plato;
        SELECT 1 AS `a b`;
        SELECT * FROM `c d`;
        SELECT * FROM x AS `e f`;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('"a b")");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('"c d")");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('"e f")");
}

Y_UNIT_TEST(PriorityFieldOverNothing) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT x FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
}

Y_UNIT_TEST(PriorityFlagOverField) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Disable;
    settings.Flags.insert("AutoYqlSelect");

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT x FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
}

Y_UNIT_TEST(PriorityPragmaOverField) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'disable';
        SELECT x FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0);
}

Y_UNIT_TEST(WindowSmoke) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(x) OVER (
            PARTITION BY x, y
            ORDER BY x, y
            ROWS UNBOUNDED PRECEDING
        ) FROM plato.x;

        SELECT Sum(x) OVER w FROM plato.x WINDOW w AS (
            PARTITION BY x, y
            ORDER BY x, y
            ROWS UNBOUNDED PRECEDING
        );
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect", "YqlWindow", "YqlAggWin"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlWindow"], 2);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlAggWin"], 2);
}

Y_UNIT_TEST(WindowFrameRowsUnboundedPreceding) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(x) OVER (ROWS UNBOUNDED PRECEDING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'up))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))"); // 'c for pg
}

Y_UNIT_TEST(WindowFrameRangeUnboundedPreceding) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b RANGE UNBOUNDED PRECEDING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'range))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'up))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'c))");
}

Y_UNIT_TEST(WindowFrameGroupsUnboundedPreceding) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b GROUPS UNBOUNDED PRECEDING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'groups))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'up))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))"); // 'c for pg
}

Y_UNIT_TEST(WindowFrameRows1Preceding) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS 1 PRECEDING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))"); // 'c for pg
}

Y_UNIT_TEST(WindowFrameRowsBetweenUnboundedPrecedingAndUnboundedFollowing) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'up))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'uf))");
}

Y_UNIT_TEST(WindowFrameRowsBetweenUnboundedPrecedingAndCurrentRow) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'up))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))"); // 'c for pg
}

Y_UNIT_TEST(WindowFrameRowsBetween1PrecedingAnd1Following) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))");
}

Y_UNIT_TEST(WindowFrameRowsBetween2PrecedingAnd1Preceding) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from_value (EvaluateExpr (Int32 '"2"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'p))");
}

Y_UNIT_TEST(WindowFrameRowsBetweenCurrentRowAnd2Following) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to_value (EvaluateExpr (Int32 '"2"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))"); // 'c for pg
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))");
}

Y_UNIT_TEST(WindowFrameRowsBetweenCurrentRowAndCurrentRow) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'rows))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))"); // 'c for pg
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))");   // 'c for pg
}

Y_UNIT_TEST(WindowFrameRangeBetween1PrecedingAnd1Following) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'range))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))");
}

Y_UNIT_TEST(WindowFrameRangeBetweenIntervalPrecedingAndFollowing) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b RANGE BETWEEN Interval('P1D') PRECEDING AND Interval('P1D') FOLLOWING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from_value (EvaluateExpr )");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to_value (EvaluateExpr )");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'range))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))");
}

Y_UNIT_TEST(WindowFrameGroupsBetween1PrecedingAnd1Following) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    TString program = VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 1);
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to_value (EvaluateExpr (Int32 '"1"))))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('type 'groups))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('from 'p))");
    UNIT_ASSERT_STRING_CONTAINS(program, R"('('to 'f))");
}

Y_UNIT_TEST(WindowFrameRowsBetweenUnboundedPrecedingAndCurrentRowExcludeCurrentRow) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT Sum(a) OVER (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) FROM plato.x;
    )sql", settings);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Frame exclusion is not supported yet");
}

Y_UNIT_TEST(WindowBad) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        SELECT ListLength(a) OVER () FROM plato.x;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":2:27: Error: Expected a YqlSelect-compatible window function, but got Length");
}

Y_UNIT_TEST(AnsiCurrentRow) {
    const auto check = [](TString spec, TString a, TString b, TString c, THashSet<TString> flags) {
        TString query = R"sql(
            PRAGMA YqlSelect = 'force';
            $events = (SELECT * FROM (VALUES
                (1, 10,  5),
                (2, 10,  5),
                (3, 20, 10)
            ) AS events (event_id, ts, val));
            SELECT ts, val, SUM(val) OVER (SPEC) AS run_sum FROM $events ORDER BY ts, event_id;
        )sql";
        SubstGlobal(query, "SPEC", spec);

        NSQLTranslation::TTranslationSettings settings;
        settings.Flags = std::move(flags);
        settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

        NYql::TAstParseResult res = SqlToYqlWithSettings(query, settings);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TWordCountHive stat = {"YqlSelect"};
        TString program = VerifyProgram(res, stat);
        UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 4);
        UNIT_ASSERT_STRING_CONTAINS(program, "'('type '" + a + ")");
        UNIT_ASSERT_STRING_CONTAINS(program, "'('from '" + b + ")");
        UNIT_ASSERT_STRING_CONTAINS(program, "'('to '" + c + ")");
    };

    check("ORDER BY ts", "rows", "up", "f", {}); // to_value is 0
    check("", /*      */ "rows", "up", "uf", {});
    check("ORDER BY ts", "range", "up", "c", {"AnsiCurrentRow"});
    check("", /*      */ "rows", "up", "uf", {"AnsiCurrentRow"});
}

Y_UNIT_TEST(PragmaSupported) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA AnsiCurrentRow;
        SELECT 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(PragmaUnsupported) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'force';
        PRAGMA SimpleColumns;
        SELECT 1;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: Pragma 'simplecolumns'");
}

Y_UNIT_TEST(PragmaUnsupportedBefore) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA SimpleColumns;
        PRAGMA YqlSelect = 'force';
        SELECT 1;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "YqlSelect unsupported: Pragma 'simplecolumns'");
}

Y_UNIT_TEST(PragmaUnsupportedAuto) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
        PRAGMA YqlSelect = 'auto';
        PRAGMA SimpleColumns;
        SELECT 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 0);
}

} // Y_UNIT_TEST_SUITE(YqlSelect)

Y_UNIT_TEST_SUITE(YqlSelectWithCTE) {

void Parse(TString query) {
    NYql::TAstParseResult r = SqlToYql(query);
    TString e = Err2Str(r);
    UNIT_ASSERT_C(!e.contains("Fatal"), e);
    UNIT_ASSERT_C(!e.contains("ambiguity"), e);
    UNIT_ASSERT_C(!e.contains("extraneous input"), e);
    UNIT_ASSERT_C(!e.contains("mismatched input"), e);
}

Y_UNIT_TEST(NoSyntaxAmbiguity) {
    Parse(R"sql(
        WITH x AS (SELECT 1) SELECT * FROM x;
    )sql");
    Parse(R"sql(
        WITH x AS (VALUES (1)) SELECT * FROM x;
    )sql");
    Parse(R"sql(
        WITH x AS (SELECT 1), y AS (SELECT 1) SELECT * FROM x, y;
    )sql");
    Parse(R"sql(
        WITH x(a) AS (SELECT 1) SELECT * FROM x, y;
    )sql");
    Parse(R"sql(
        WITH x(a,b) AS (SELECT 1) SELECT * FROM x, y;
    )sql");
    Parse(R"sql(
        WITH
            RECURSIVE a(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM a
                WHERE n < 5
            ),
            RECURSIVE b(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM b
                WHERE n < 5
            )
        SELECT * FROM a UNION SELECT * FROM b;
    )sql");
    Parse(R"sql(
        WITH x AS (SELECT 1), SELECT * FROM x;
        WITH x AS (SELECT 1), y AS (SELECT 1), SELECT * FROM x, y;
    )sql");
    Parse(R"sql(
        $x =
            WITH RECURSIVE a(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM a
                WHERE n < 5
            )
            SELECT * FROM a UNION SELECT * FROM b
        ;
    )sql");
    Parse(R"sql(
        $x = (
            WITH RECURSIVE a(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM a
                WHERE n < 5
            )
            SELECT * FROM a UNION SELECT * FROM b
        );
    )sql");
    Parse(R"sql(
        SELECT (
            WITH RECURSIVE a(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM a
                WHERE n < 5
            )
            SELECT * FROM a UNION SELECT * FROM b
        );
    )sql");
    Parse(R"sql(
        INSERT INTO x (b)
        WITH RECURSIVE a(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM a
            WHERE n < 5
        )
        SELECT * FROM a;
    )sql");
}

Y_UNIT_TEST(LinearVisibilityOK) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        WITH
            x AS (SELECT 0 + 1 AS a       ),
            y AS (SELECT a + 1 AS a FROM x),
        SELECT * FROM y;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TWordCountHive stat = {"YqlSelect"};
    VerifyProgram(res, stat);
    UNIT_ASSERT_VALUES_EQUAL(stat["YqlSelect"], 3);
}

Y_UNIT_TEST(LinearVisibilityErr) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        WITH
            y AS (SELECT a + 1 AS a FROM x),
            x AS (SELECT 0 + 1 AS a       )
        SELECT * FROM y;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":3:42: Error: No cluster name given");
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "If 'x' is meant to be a CTE");
}

Y_UNIT_TEST(UnusedCTETrivial) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        WITH x AS (SELECT 1) SELECT 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":2:14: Warning: CTE Symbol x is not used");
}

Y_UNIT_TEST(UnusedCTEUnwinding) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        WITH x AS (
            WITH y AS (
                WITH z AS (
                    SELECT 1)
                SELECT 1)
            SELECT 1)
        SELECT 1;
    )sql", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":2:14: Warning: CTE Symbol x is not used");
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":3:18: Warning: CTE Symbol y is not used");
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":4:22: Warning: CTE Symbol z is not used");
}

Y_UNIT_TEST(ScopeErr) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        WITH
            y AS (
                WITH x AS (
                    SELECT 0 + 1 AS a
                )
                SELECT a + 1 AS a FROM x
            ),
            z AS (
                SELECT a + 2 AS a FROM x
            )
        SELECT a + 3 AS a FROM z;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":10:40: Error: No cluster name given");
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "If 'x' is meant to be a CTE");
}

Y_UNIT_TEST(OnlySelectAllowed) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        SELECT (WITH a AS (SELECT 1) 123);
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":2:17: Error: A WITH clause can only be used before a SELECT statement");
}

Y_UNIT_TEST(Redefinition) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        WITH
            x AS (SELECT 0 + 1 AS a       ),
            x AS (SELECT a + 1 AS a FROM x),
            x AS (SELECT a + 1 AS a FROM x),
        SELECT * FROM x;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), ":4:13: Error: Bad CTE: Redefinition is forbidden: x");
}

Y_UNIT_TEST(RecursiveReferenceFromRecursive) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    auto res = SqlToYqlWithSettings(R"sql(
        WITH RECURSIVE a AS (
            WITH RECURSIVE b AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1 AS n FROM a
                WHERE n < 5
            )
            SELECT * FROM b
        )
        SELECT * FROM a;
    )sql", settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(
        Err2Str(res),
        ":6:40: Error: Can't reference outer RECURSIVE CTE 'a'. "
        "Recursion only with a current CTE is allowed, which is 'b' here");
}

Y_UNIT_TEST(RecursiveReferenceFromSubquery) {
    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::YqlSelect.MinLangVer;
    settings.YqlSelect = NSQLTranslation::EYqlSelect::Force;

    const auto test = [&](TString query, TString position, TString target, TString current) {
        auto res = SqlToYqlWithSettings(query, settings);
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(
            Err2Str(res),
            TStringBuilder()
                << position << ": "
                << "Error: Can't reference outer RECURSIVE CTE '" << target << "'. "
                << "Recursion only with a current CTE is allowed, which is " << current << " here");
    };

    test(R"sql(
        WITH RECURSIVE a AS (
            SELECT 1 AS n
            UNION ALL
            SELECT n + 1 AS n FROM (SELECT * FROM a)
            WHERE n < 5
        )
        SELECT * FROM a;
    )sql", ":5:51", "a", "undefined");

    test(R"sql(
        WITH RECURSIVE a AS (
            SELECT 1 AS n
            UNION ALL
            SELECT (SELECT n + 1 FROM a) AS n FROM a
            WHERE n < 5
        )
        SELECT * FROM a;
    )sql", ":5:39", "a", "undefined");

    test(R"sql(
        WITH RECURSIVE a AS (
            SELECT 1 AS n
            UNION ALL
            SELECT EXISTS (SELECT n + 1 FROM a) AS n FROM a
            WHERE n < 5
        )
        SELECT * FROM a;
    )sql", ":5:46", "a", "undefined");

    test(R"sql(
        WITH RECURSIVE a AS (
            SELECT 1 AS n
            UNION ALL
            SELECT 1 IN (SELECT n + 1 FROM a) AS n FROM a
            WHERE n < 5
        )
        SELECT * FROM a;
    )sql", ":5:44", "a", "undefined");
}

} // Y_UNIT_TEST_SUITE(YqlSelectWithCTE)
