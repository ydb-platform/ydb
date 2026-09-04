#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(SqlToYQLErrors) {

Y_UNIT_TEST(UdfSyntaxSugarMissingCall) {
    auto req = "SELECT Udf(DateTime::FromString, \"foo\" as RunConfig);";
    auto res = SqlToYql(req);
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "<main>:1:8: Error: Abstract Udf Node can't be used as a part of expression.");
}

Y_UNIT_TEST(UdfSyntaxSugarIsNotCallable) {
    auto req = "SELECT Udf(123, \"foo\" as RunConfig);";
    auto res = SqlToYql(req);
    TString a1 = Err2Str(res);
    TString a2("<main>:1:8: Error: Udf: first argument must be a callable, like Foo::Bar\n");
    UNIT_ASSERT_NO_DIFF(a1, a2);
}

Y_UNIT_TEST(UdfSyntaxSugarNoArgs) {
    auto req = "SELECT Udf()();";
    auto res = SqlToYql(req);
    TString a1 = Err2Str(res);
    TString a2("<main>:1:8: Error: Udf: expected at least one argument\n");
    UNIT_ASSERT_NO_DIFF(a1, a2);
}

Y_UNIT_TEST(StrayUTF8) {
    /// 'c' in plato is russian here
    NYql::TAstParseResult res = SqlToYql("select * from сedar.Input");
    UNIT_ASSERT(!res.IsOk());

    TString a1 = Err2Str(res);
    TString a2(R"foo(<main>:1:14: Error: token recognition error at: 'с'
)foo");
    UNIT_ASSERT_NO_DIFF(a1, a2);
}

Y_UNIT_TEST(IvalidStringLiteralWithEscapedBackslash) {
    NYql::TAstParseResult res1 = SqlToYql(R"foo($bar = 'a\\'b';)foo");
    NYql::TAstParseResult res2 = SqlToYql(R"foo($bar = "a\\"b";)foo");
    UNIT_ASSERT(!res1.Root);
    UNIT_ASSERT(!res2.Root);

#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res1), "<main>:1:15: Error: Unexpected character : syntax error...\n\n");
    UNIT_ASSERT_NO_DIFF(Err2Str(res2), "<main>:1:15: Error: Unexpected character : syntax error...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res1), "<main>:1:13: Error: token recognition error at: '';'\n");
    UNIT_ASSERT_NO_DIFF(Err2Str(res2), "<main>:1:13: Error: token recognition error at: '\";'\n");
#endif
}

Y_UNIT_TEST(InvalidHexInStringLiteral) {
    NYql::TAstParseResult res = SqlToYql(R"(select "foo\x1\xfe")");
    UNIT_ASSERT(!res.IsOk());
    TString a1 = Err2Str(res);
    TString a2 = "<main>:1:15: Error: Failed to parse string literal: Invalid hexadecimal value near byte 7\n";

    UNIT_ASSERT_NO_DIFF(a1, a2);
}

Y_UNIT_TEST(InvalidOctalInMultilineStringLiteral) {
    NYql::TAstParseResult res = SqlToYql("select \"foo\n"
                                         "bar\n"
                                         "\\01\"");
    UNIT_ASSERT(!res.IsOk());
    TString a1 = Err2Str(res);
    TString a2 = "<main>:3:4: Error: Failed to parse string literal: Invalid octal value near byte 12\n";

    UNIT_ASSERT_NO_DIFF(a1, a2);
}

Y_UNIT_TEST(InvalidDoubleAtString) {
    NYql::TAstParseResult res = SqlToYql("select @@@@@@");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Unexpected character : syntax error...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: extraneous input '@' expecting {<EOF>, ';'}\n");
#endif
}

Y_UNIT_TEST(InvalidDoubleAtStringWhichWasAcceptedEarlier) {
    NYql::TAstParseResult res = SqlToYql("SELECT @@foo@@ @ @@bar@@");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '@@foo@@' : cannot match to any predicted input...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: mismatched input '@' expecting {<EOF>, ';'}\n");
#endif
}

Y_UNIT_TEST(InvalidStringFromTable) {
    NYql::TAstParseResult res = SqlToYql(R"(select "FOO""BAR from plato.foo)");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: Unexpected character : syntax error...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: token recognition error at: '\"BAR from plato.foo'\n");
#endif
}

Y_UNIT_TEST(InvalidDoubleAtStringFromTable) {
    NYql::TAstParseResult res = SqlToYql("select @@@@@@ from plato.foo");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unexpected character : syntax error...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: mismatched input '@' expecting {<EOF>, ';'}\n");
#endif
}

Y_UNIT_TEST(SelectInvalidSyntax) {
    NYql::TAstParseResult res = SqlToYql("select 1 form Wat");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:14: Error: Unexpected token 'Wat' : cannot match to any predicted input...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:14: Error: extraneous input 'Wat' expecting {<EOF>, ';'}\n");
#endif
}

Y_UNIT_TEST(SelectNoCluster) {
    NYql::TAstParseResult res = SqlToYql("select foo from bar");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: No cluster name given and no default cluster is selected\n");
}

Y_UNIT_TEST(SelectDuplicateColumns) {
    NYql::TAstParseResult res = SqlToYql("select a, a from plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:11: Error: Unable to use duplicate column names. Collision in name: a\n");
}

Y_UNIT_TEST(SelectDuplicateLabels) {
    NYql::TAstParseResult res = SqlToYql("select a as foo, b as foo from plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unable to use duplicate column names. Collision in name: foo\n");
}

Y_UNIT_TEST(SelectCaseWithoutThen) {
    NYql::TAstParseResult res = SqlToYql("select case when true 1;");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:1:22: Error: Unexpected token absence : Missing THEN \n\n"
                        "<main>:1:23: Error: Unexpected token absence : Missing END \n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:1:22: Error: missing THEN at \'1\'\n"
                        "<main>:1:23: Error: extraneous input \';\' expecting {ELSE, END, WHEN}\n");
#endif
}

Y_UNIT_TEST(SelectComplexCaseWithoutThen) {
    NYql::TAstParseResult res = SqlToYql(
        "SELECT *\n"
        "FROM plato.Input AS a\n"
        "WHERE CASE WHEN a.key = \"foo\" a.subkey ELSE a.value END\n");
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:30: Error: Unexpected token absence : Missing THEN \n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:30: Error: missing THEN at 'a'\n");
#endif
}

Y_UNIT_TEST(SelectCaseWithoutEnd) {
    NYql::TAstParseResult res = SqlToYql("select case a when b then c end from plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: ELSE is required\n");
}

Y_UNIT_TEST(SelectWithBadAggregationNoInput) {
    NYql::TAstParseResult res = SqlToYql("select a, Min(b), c");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:1:1: Error: Column references are not allowed without FROM\n"
                        "<main>:1:8: Error: Column reference 'a'\n"
                        "<main>:1:1: Error: Column references are not allowed without FROM\n"
                        "<main>:1:15: Error: Column reference 'b'\n"
                        "<main>:1:1: Error: Column references are not allowed without FROM\n"
                        "<main>:1:19: Error: Column reference 'c'\n");
}

Y_UNIT_TEST(SelectWithBadAggregation) {
    ExpectFailWithError("select count(*), 1 + key from plato.Input",
                        "<main>:1:22: Error: Column `key` must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(SelectWithBadAggregatedTerms) {
    ExpectFailWithError("select key, 2 * subkey from plato.Input group by key",
                        "<main>:1:17: Error: Column `subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(SelectDistinctWithBadAggregation) {
    ExpectFailWithError("select distinct count(*), 1 + key from plato.Input",
                        "<main>:1:31: Error: Column `key` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    ExpectFailWithError("select distinct key, 2 * subkey from plato.Input group by key",
                        "<main>:1:26: Error: Column `subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(SelectWithBadAggregationInHaving) {
    ExpectFailWithError("select key from plato.Input group by key\n"
                        "having \"f\" || value == \"foo\"",
                        "<main>:2:15: Error: Column `value` must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(JoinWithNonAggregatedColumnInProjection) {
    ExpectFailWithError("select a.key, 1 + b.subkey\n"
                        "from plato.Input1 as a join plato.Input2 as b using(key)\n"
                        "group by a.key;",
                        "<main>:1:19: Error: Column `b.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");

    ExpectFailWithError("select a.key, 1 + b.subkey.x\n"
                        "from plato.Input1 as a join plato.Input2 as b using(key)\n"
                        "group by a.key;",
                        "<main>:1:19: Error: Column must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(SelectWithBadAggregatedTermsWithSources) {
    ExpectFailWithError("select key, 1 + a.subkey\n"
                        "from plato.Input1 as a\n"
                        "group by a.key;",
                        "<main>:1:17: Error: Column `a.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");
    ExpectFailWithError("select key, 1 + a.subkey.x\n"
                        "from plato.Input1 as a\n"
                        "group by a.key;",
                        "<main>:1:17: Error: Column must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(WarnForAggregationBySelectAlias) {
    NYql::TAstParseResult res = SqlToYql("select c + 1 as c from plato.Input\n"
                                         "group by  c");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:2:11: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
                        "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n");

    res = SqlToYql("select c + 1 as c from plato.Input\n"
                   "group by Math::Floor(c + 2) as c;");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:2:22: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
                        "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n");
}

Y_UNIT_TEST(WarnForAggregationBySelectAliasAsErrorStrict) {
    NSQLTranslation::TTranslationSettings settings;

    NYql::TAstParseResult res = SqlToYqlWithSettings(R"sql(
            PRAGMA Warning("error", "*");
            SELECT c + 1 AS c
            FROM plato.Input
            GROUP BY c;
        )sql", settings);

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:5:22: Error: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
                        "<main>:3:22: Error: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n");
}

Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenAggrFunctionsAreUsedInAlias) {
    NYql::TAstParseResult res = SqlToYql("select\n"
                                         "    cast(avg(val) as int) as value,\n"
                                         "    value as key\n"
                                         "from\n"
                                         "    plato.Input\n"
                                         "group by value");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);

    res = SqlToYql("select\n"
                   "    cast(avg(val) over w as int) as value,\n"
                   "    value as key\n"
                   "from\n"
                   "    plato.Input\n"
                   "group by value\n"
                   "window w as ()");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenQualifiedNameIsUsed) {
    NYql::TAstParseResult res = SqlToYql("select\n"
                                         "  Unwrap(a.key) as key\n"
                                         "from plato.Input as a\n"
                                         "join plato.Input2 as b using(k)\n"
                                         "group by a.key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);

    res = SqlToYql("select Unwrap(a.key) as key\n"
                   "from plato.Input as a\n"
                   "group by a.key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(NoWarnForAggregationBySelectAliasWhenTrivialRenamingIsUsed) {
    NYql::TAstParseResult res = SqlToYql("select a.key as key\n"
                                         "from plato.Input as a\n"
                                         "group by key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);

    res = SqlToYql("select key as key\n"
                   "from plato.Input\n"
                   "group by key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(ErrorByAggregatingByExpressionWithSameExpressionInSelect) {
    ExpectFailWithError("select k * 2 from plato.Input group by k * 2",
                        "<main>:1:8: Error: Column `k` must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(ErrorForAggregationBySelectAlias) {
    ExpectFailWithError("select key, Math::Floor(1.1 + a.subkey) as foo\n"
                        "from plato.Input as a\n"
                        "group by a.key, foo;",
                        "<main>:3:17: Warning: GROUP BY will aggregate by column `foo` instead of aggregating by SELECT expression with same alias, code: 4532\n"
                        "<main>:1:19: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n"
                        "<main>:1:31: Error: Column `a.subkey` must either be a key column in GROUP BY or it should be used in aggregation function\n");

    ExpectFailWithError("select c + 1 as c from plato.Input\n"
                        "group by Math::Floor(c + 2);",
                        "<main>:2:22: Warning: GROUP BY will aggregate by column `c` instead of aggregating by SELECT expression with same alias, code: 4532\n"
                        "<main>:1:10: Warning: You should probably use alias in GROUP BY instead of using it here. Please consult documentation for more details, code: 4532\n"
                        "<main>:1:8: Error: Column `c` must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(SelectWithDuplicateGroupingColumns) {
    NYql::TAstParseResult res = SqlToYql("select c from plato.Input group by c, c");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Duplicate grouping column: c\n");
}

Y_UNIT_TEST(SelectWithBadAggregationInGrouping) {
    NYql::TAstParseResult res = SqlToYql("select a, Min(b), c group by c");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                      "<main>:1:30: Error: Column reference 'c'\n");
}

Y_UNIT_TEST(SelectWithOpOnBadAggregation) {
    ExpectFailWithError("select 1 + a + Min(b) from plato.Input",
                        "<main>:1:12: Error: Column `a` must either be a key column in GROUP BY or it should be used in aggregation function\n");
}

Y_UNIT_TEST(SelectOrderByConstantNum) {
    NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by 1");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY constant expression\n");
}

Y_UNIT_TEST(SelectOrderByConstantExpr) {
    NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by 1 * 42");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:38: Error: Unable to ORDER BY constant expression\n");
}

Y_UNIT_TEST(SelectOrderByConstantString) {
    NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by \"nest\"");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY constant expression\n");
}

Y_UNIT_TEST(SelectOrderByAggregated) {
    NYql::TAstParseResult res = SqlToYql("select a from plato.Input order by min(a)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:36: Error: Unable to ORDER BY aggregated values\n");
}

Y_UNIT_TEST(ErrorInOrderByExpresison) {
    NYql::TAstParseResult res = SqlToYql("select key, value from plato.Input order by (key as zey)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:45: Error: You should use in ORDER BY column name, qualified field, callable function or expression\n");
}

Y_UNIT_TEST(ErrorsInOrderByWhenColumnIsMissingInProjection) {
    ExpectFailWithError("select subkey from (select 1 as subkey) order by key", "<main>:1:50: Error: Column key is not in source column set\n");
    ExpectFailWithError("select subkey from plato.Input as a order by x.key", "<main>:1:46: Error: Unknown correlation name: x\n");
    ExpectFailWithError("select distinct a, b from plato.Input order by c", "<main>:1:48: Error: Column c is not in source column set. Did you mean a?\n");
    ExpectFailWithError("select count(*) as a from plato.Input order by c", "<main>:1:48: Error: Column c is not in source column set. Did you mean a?\n");
    ExpectFailWithError("select count(*) as a, b, from plato.Input group by b order by c", "<main>:1:63: Error: Column c is not in source column set. Did you mean a?\n");
    UNIT_ASSERT(SqlToYql("select a, b from plato.Input order by c").IsOk());
}

Y_UNIT_TEST(SelectAggregatedWhere) {
    NYql::TAstParseResult res = SqlToYql("select * from plato.Input where count(key)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:33: Error: Can not use aggregated values in filtering\n");
}

Y_UNIT_TEST(DoubleFrom) {
    NYql::TAstParseResult res = SqlToYql("from plato.Input select * from plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: Only one FROM clause is allowed\n");
}

Y_UNIT_TEST(SelectJoinMissingCorrName) {
    NYql::TAstParseResult res = SqlToYql("select * from plato.Input1 as a join plato.Input2 as b on a.key == key");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:65: Error: JOIN: column requires correlation name\n");
}

Y_UNIT_TEST(SelectJoinMissingCorrName1) {
    NYql::TAstParseResult res = SqlToYql(
        "use plato;\n"
        "$foo = select * from Input1;\n"
        "select * from Input2 join $foo USING(key);\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:27: Error: JOIN: missing correlation name for source\n");
}

Y_UNIT_TEST(SelectJoinMissingCorrName2) {
    NYql::TAstParseResult res = SqlToYql(
        "use plato;\n"
        "$foo = select * from Input1;\n"
        "select * from Input2 cross join $foo;\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:33: Error: JOIN: missing correlation name for source\n");
}

Y_UNIT_TEST(SelectJoinEmptyCorrNames) {
    NYql::TAstParseResult res = SqlToYql(
        "$left = (SELECT * FROM plato.Input1 LIMIT 2);\n"
        "$right = (SELECT * FROM plato.Input2 LIMIT 2);\n"
        "SELECT * FROM $left FULL JOIN $right USING (key);\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:45: Error: At least one correlation name is required in join\n");
}

Y_UNIT_TEST(SelectJoinSameTables) {
    NYql::TAstParseResult res = SqlToYql("Select a.key FROM plato.Input JOIN plato.Input ON Input.key == Input.subkey\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:61: Error: JOIN: different correlation names are required for joined tables\n");
}

Y_UNIT_TEST(SelectJoinSameCorrLabels) {
    NYql::TAstParseResult res = SqlToYql("Select a.key FROM plato.Input as a JOIN plato.Input1 as a ON a.key == a.subkey\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:68: Error: JOIN: different correlation names are required for joined tables\n");
}

Y_UNIT_TEST(SelectJoinSameCorrNames) {
    NYql::TAstParseResult res = SqlToYql("SELECT Input.key FROM plato.Input JOIN plato.Input1 ON Input.key == Input.subkey\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:66: Error: JOIN: different correlation names are required for joined tables\n");
}

Y_UNIT_TEST(SelectJoinConstPredicateArg) {
    NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input1 as A JOIN plato.Input2 as B ON A.key == B.key AND A.subkey == \"wtf\"\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:87: Error: JOIN: each equality predicate argument must depend on exactly one JOIN input\n");
}

Y_UNIT_TEST(SelectJoinNonEqualityPredicate) {
    NYql::TAstParseResult res = SqlToYql("SELECT * FROM plato.Input1 as A JOIN plato.Input2 as B ON A.key == B.key AND A.subkey > B.subkey\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:87: Error: JOIN ON expression must be a conjunction of equality predicates\n");
}

Y_UNIT_TEST(SelectEquiJoinCorrNameOutOfScope) {
    NYql::TAstParseResult res = SqlToYql(
        "PRAGMA equijoin;\n"
        "SELECT * FROM plato.A JOIN plato.B ON A.key == C.key JOIN plato.C ON A.subkey == C.subkey;\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:45: Error: JOIN: can not use source: C in equality predicate, it is out of current join scope\n");
}

Y_UNIT_TEST(SelectEquiJoinNoRightSource) {
    NYql::TAstParseResult res = SqlToYql(
        "PRAGMA equijoin;\n"
        "SELECT * FROM plato.A JOIN plato.B ON A.key == B.key JOIN plato.C ON A.subkey == B.subkey;\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:79: Error: JOIN ON equality predicate must have one of its arguments from the rightmost source\n");
}

Y_UNIT_TEST(SelectEquiJoinOuterWithoutType) {
    NYql::TAstParseResult res = SqlToYql(
        "SELECT * FROM plato.A Outer JOIN plato.B ON A.key == B.key;\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Invalid join type: OUTER JOIN. OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL\n");
}

Y_UNIT_TEST(SelectEquiJoinOuterWithWrongType) {
    NYql::TAstParseResult res = SqlToYql(
        "SELECT * FROM plato.A LEFT semi OUTER JOIN plato.B ON A.key == B.key;\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:33: Error: Invalid join type: LEFT SEMI OUTER JOIN. OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL\n");
}

Y_UNIT_TEST(InsertNoCluster) {
    NYql::TAstParseResult res = SqlToYql("insert into Output (foo) values (1)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: No cluster name given and no default cluster is selected\n");
}

Y_UNIT_TEST(InsertValuesNoLabels) {
    NYql::TAstParseResult res = SqlToYql("insert into plato.Output values (1)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: INSERT INTO ... VALUES requires specification of table columns\n");
}

Y_UNIT_TEST(UpsertValuesNoLabelsKikimr) {
    NYql::TAstParseResult res = SqlToYql("upsert into plato.Output values (1)", 10, TString(NYql::KikimrProviderName));
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: UPSERT INTO ... VALUES requires specification of table columns\n");
}

Y_UNIT_TEST(ReplaceValuesNoLabelsKikimr) {
    NYql::TAstParseResult res = SqlToYql("replace into plato.Output values (1)", 10, TString(NYql::KikimrProviderName));
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:20: Error: REPLACE INTO ... VALUES requires specification of table columns\n");
}

Y_UNIT_TEST(InsertValuesInvalidLabels) {
    NYql::TAstParseResult res = SqlToYql("insert into plato.Output (foo) values (1, 2)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: VALUES have 2 columns, INSERT INTO expects: 1\n");
}

Y_UNIT_TEST(BuiltinFileOpNoArgs) {
    NYql::TAstParseResult res = SqlToYql("select FilePath()");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: FilePath() requires exactly 1 arguments, given: 0\n");
}

Y_UNIT_TEST(ProcessWithHaving) {
    NYql::TAstParseResult res = SqlToYql("process plato.Input using some::udf(value) having value == 1");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: PROCESS does not allow HAVING yet!\n");
}

Y_UNIT_TEST(ReduceNoBy) {
    NYql::TAstParseResult res = SqlToYql("reduce plato.Input using some::udf(value)");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unexpected token absence : Missing ON \n\n<main>:1:25: Error: Unexpected token absence : Missing USING \n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: mismatched input 'using' expecting {',', ON, PRESORT}\n");
#endif
}

Y_UNIT_TEST(ReduceDistinct) {
    NYql::TAstParseResult res = SqlToYql("reduce plato.Input on key using some::udf(distinct value)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:43: Error: DISTINCT can not be used in PROCESS/REDUCE\n");
}

Y_UNIT_TEST(CreateTableWithView) {
    NYql::TAstParseResult res = SqlToYql("CREATE TABLE plato.foo:bar (key INT);");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: Unexpected token ':' : syntax error...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: mismatched input ':' expecting '('\n");
#endif
}

Y_UNIT_TEST(AsteriskWithSomethingAfter) {
    NYql::TAstParseResult res = SqlToYql("select *, LENGTH(value) from plato.Input;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).\n");
}

Y_UNIT_TEST(AsteriskWithSomethingBefore) {
    NYql::TAstParseResult res = SqlToYql("select LENGTH(value), * from plato.Input;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).\n");
}

Y_UNIT_TEST(DuplicatedQualifiedAsterisk) {
    NYql::TAstParseResult res = SqlToYql("select in.*, key, in.* from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unable to use twice same qualified asterisk. Invalid source: in\n");
}

Y_UNIT_TEST(BrokenLabel) {
    NYql::TAstParseResult res = SqlToYql("select in.*, key as `funny.label` from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:14: Error: Unable to use '.' in column name. Invalid column name: funny.label\n");
}

Y_UNIT_TEST(KeyConflictDetect0) {
    NYql::TAstParseResult res = SqlToYql("select key, in.key as key from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Unable to use duplicate column names. Collision in name: key\n");
}

Y_UNIT_TEST(KeyConflictDetect1) {
    NYql::TAstParseResult res = SqlToYql("select length(key) as key, key from plato.Input;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unable to use duplicate column names. Collision in name: key\n");
}

Y_UNIT_TEST(KeyConflictDetect2) {
    NYql::TAstParseResult res = SqlToYql("select key, in.key from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
}

Y_UNIT_TEST(AutogenerationAliasWithCollisionConflict1) {
    UNIT_ASSERT(SqlToYql("select LENGTH(Value), key as column0 from plato.Input;").IsOk());
}

Y_UNIT_TEST(AutogenerationAliasWithCollisionConflict2) {
    UNIT_ASSERT(SqlToYql("select key as column1, LENGTH(Value) from plato.Input;").IsOk());
}

Y_UNIT_TEST(MissedSourceTableForQualifiedAsteriskOnSimpleSelect) {
    NYql::TAstParseResult res = SqlToYql("use plato; select Intop.*, Input.key from plato.Input;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unknown correlation name: Intop\n");
}

Y_UNIT_TEST(MissedSourceTableForQualifiedAsteriskOnJoin) {
    NYql::TAstParseResult res = SqlToYql("use plato; select tmissed.*, t2.*, t1.key from plato.Input as t1 join plato.Input as t2 on t1.key==t2.key;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:19: Error: Unknown correlation name for asterisk: tmissed\n");
}

Y_UNIT_TEST(UnableToReferenceOnNotExistSubcolumn) {
    NYql::TAstParseResult res = SqlToYql("select b.subkey from (select key from plato.Input as a) as b;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Column subkey is not in source column set\n");
}

Y_UNIT_TEST(ConflictOnSameNameWithQualify0) {
    NYql::TAstParseResult res = SqlToYql("select in.key, in.key as key from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
}

Y_UNIT_TEST(ConflictOnSameNameWithQualify1) {
    NYql::TAstParseResult res = SqlToYql("select in.key, length(key) as key from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
}

Y_UNIT_TEST(ConflictOnSameNameWithQualify2) {
    NYql::TAstParseResult res = SqlToYql("select key, in.key from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
}

Y_UNIT_TEST(ConflictOnSameNameWithQualify3) {
    NYql::TAstParseResult res = SqlToYql("select in.key, subkey as key from plato.Input as in;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Duplicate column: key\n");
}

Y_UNIT_TEST(SelectFlattenBySameColumns) {
    NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, key as kk)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Duplicate column name found: key in FlattenBy section\n");
}

Y_UNIT_TEST(SelectFlattenBySameAliases) {
    NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, subkey as kk);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Duplicate alias found: kk in FlattenBy section\n");
}

Y_UNIT_TEST(SelectFlattenByExprSameAliases) {
    NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, ListSkip(subkey,1) as kk);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Collision between alias and column name: kk in FlattenBy section\n");
}

Y_UNIT_TEST(SelectFlattenByConflictNameAndAlias0) {
    NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, subkey as key);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Collision between alias and column name: key in FlattenBy section\n");
}

Y_UNIT_TEST(SelectFlattenByConflictNameAndAlias1) {
    NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, subkey as key);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Collision between alias and column name: key in FlattenBy section\n");
}

Y_UNIT_TEST(SelectFlattenByExprConflictNameAndAlias1) {
    NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key as kk, ListSkip(subkey,1) as key);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Error: Duplicate column name found: key in FlattenBy section\n");
}

Y_UNIT_TEST(SelectFlattenByUnnamedExpr) {
    NYql::TAstParseResult res = SqlToYql("select key from plato.Input flatten by (key, ListSkip(key, 1))");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:46: Error: Unnamed expression after FLATTEN BY is not allowed\n");
}

Y_UNIT_TEST(UseInOnStrings) {
    NYql::TAstParseResult res = SqlToYql(R"(select * from plato.Input where "foo" in "foovalue";)");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:42: Error: Unable to use IN predicate with string argument, it won't search substring - "
                                      "expecting tuple, list, dict or single column table source\n");
}

Y_UNIT_TEST(UseSubqueryInScalarContextInsideIn) {
    NYql::TAstParseResult res = SqlToYql("$q = (select key from plato.Input); select * from plato.Input where subkey in ($q);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:79: Warning: Using subrequest in scalar context after IN, "
                                      "perhaps you should remove parenthesis here, code: 4501\n");
}

Y_UNIT_TEST(InHintsWithKeywordClash) {
    NYql::TAstParseResult res = SqlToYql("SELECT COMPACT FROM plato.Input WHERE COMPACT IN COMPACT `COMPACT`(1,2,3)");
    UNIT_ASSERT(!res.IsOk());
    // should try to parse last compact as call expression
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:58: Error: Unknown builtin: COMPACT\n");
}

Y_UNIT_TEST(ErrorColumnPosition) {
    NYql::TAstParseResult res = SqlToYql(
        "USE plato;\n"
        "SELECT \n"
        "value FROM (\n"
        "select key from Input\n"
        ");\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:1: Error: Column value is not in source column set\n");
}

Y_UNIT_TEST(PrimaryViewAbortMapReduce) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input VIEW PRIMARY KEY");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: primary view is not supported for yt tables\n");
}

Y_UNIT_TEST(InsertAbortMapReduce) {
    NYql::TAstParseResult res = SqlToYql("INSERT OR ABORT INTO plato.Output SELECT key FROM plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: INSERT OR ABORT INTO is not supported for yt tables\n");
}

Y_UNIT_TEST(ReplaceIntoMapReduce) {
    NYql::TAstParseResult res = SqlToYql("REPLACE INTO plato.Output SELECT key FROM plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: REPLACE is not available before language version 2025.04\n");

    NSQLTranslation::TTranslationSettings settings;
    settings.LangVer = NYql::NFeature::ReplaceInto.MinLangVer;

    res = SqlToYqlWithSettings("REPLACE INTO plato.Output SELECT key FROM plato.Input", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(UpsertIntoMapReduce) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        UPSERT INTO plato.Output SELECT key FROM plato.Input
    )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Error: UPSERT INTO is not supported for yt tables\n");
}

Y_UNIT_TEST(UpdateMapReduce) {
    NYql::TAstParseResult res = SqlToYql("UPDATE plato.Output SET value = value + 1 WHERE key < 1");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: UPDATE is unsupported for yt\n");
}

Y_UNIT_TEST(DeleteMapReduce) {
    NYql::TAstParseResult res = SqlToYql("DELETE FROM plato.Output WHERE key < 1");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: DELETE is unsupported for yt\n");
}

Y_UNIT_TEST(ReplaceIntoWithTruncate) {
    NYql::TAstParseResult res = SqlToYql("REPLACE INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:32: Error: Unable REPLACE INTO with truncate mode\n");
}

Y_UNIT_TEST(UpsertIntoWithTruncate) {
    NYql::TAstParseResult res = SqlToYql("UPSERT INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:31: Error: Unable UPSERT INTO with truncate mode\n");
}

Y_UNIT_TEST(InsertIntoWithTruncateKikimr) {
    NYql::TAstParseResult res = SqlToYql("INSERT INTO plato.Output WITH TRUNCATE SELECT key FROM plato.Input", 10, TString(NYql::KikimrProviderName));
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:0: Error: INSERT INTO WITH TRUNCATE is not supported for kikimr tables\n");
}

Y_UNIT_TEST(InsertIntoWithWrongArgumentCount) {
    NYql::TAstParseResult res = SqlToYql("insert into plato.Output with truncate (key, value, subkey) values (5, '1', '2', '3');");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: VALUES have 4 columns, INSERT INTO ... WITH TRUNCATE expects: 3\n");
}

Y_UNIT_TEST(UpsertWithWrongArgumentCount) {
    NYql::TAstParseResult res = SqlToYql("upsert into plato.Output (key, value, subkey) values (2, '3');", 10, TString(NYql::KikimrProviderName));
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:39: Error: VALUES have 2 columns, UPSERT INTO expects: 3\n");
}

Y_UNIT_TEST(GroupingSetByExprWithoutAlias) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY GROUPING SETS (cast(key as uint32), subkey);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: Unnamed expressions are not supported in GROUPING SETS. Please use '<expr> AS <name>'.\n");
}

Y_UNIT_TEST(GroupingSetByExprWithoutAlias2) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY subkey || subkey, GROUPING SETS (\n"
                                         "cast(key as uint32), subkey);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:1: Error: Unnamed expressions are not supported in GROUPING SETS. Please use '<expr> AS <name>'.\n");
}

Y_UNIT_TEST(GroupingSetSmartParenthesisContext) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            SELECT * FROM plato.x GROUP BY
                b,
                c,
                CASE
                    WHEN ListHas(a, ('a',)) THEN [(a, 'a', 'b', )]
                    ELSE a
                END AS a
            ;
        )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(CubeByExprWithoutAlias) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey / key);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:56: Error: Unnamed expressions are not supported in CUBE. Please use '<expr> AS <name>'.\n");
}

Y_UNIT_TEST(RollupByExprWithoutAlias) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY ROLLUP (subkey / key);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:53: Error: Unnamed expressions are not supported in ROLLUP. Please use '<expr> AS <name>'.\n");
}

Y_UNIT_TEST(GroupByHugeCubeDeniedNoPragma) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub, key + val as keyval);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:119: Error: GROUP BY CUBE is allowed only for 5 columns, but you use 6\n");
}

Y_UNIT_TEST(GroupByInvalidPragma) {
    NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '-4';");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:27: Error: Expected unsigned integer literal as a single argument for: GroupByCubeLimit\n");
}

Y_UNIT_TEST(GroupByHugeCubeDeniedPragme) {
    NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByCubeLimit = '4'; SELECT key FROM plato.Input GROUP BY CUBE (key, subkey, value, key + subkey as sum, key - subkey as sub);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:132: Error: GROUP BY CUBE is allowed only for 4 columns, but you use 5\n");
}

Y_UNIT_TEST(GroupByFewBigCubes) {
    NYql::TAstParseResult res = SqlToYql("SELECT key FROM plato.Input GROUP BY CUBE(key, subkey, key + subkey as sum), CUBE(value, value + key + subkey as total);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Unable to GROUP BY more than 64 groups, you try use 80 groups\n");
}

Y_UNIT_TEST(GroupByFewBigCubesWithPragmaLimit) {
    NYql::TAstParseResult res = SqlToYql("PRAGMA GroupByLimit = '16'; SELECT key FROM plato.Input GROUP BY GROUPING SETS(key, subkey, key + subkey as sum), ROLLUP(value, value + key + subkey as total);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:29: Error: Unable to GROUP BY more than 16 groups, you try use 18 groups\n");
}

Y_UNIT_TEST(NoGroupingColumn0) {
    NYql::TAstParseResult res = SqlToYql(
        "select count(1), key_first, val_first, grouping(key_first, val_first, nomind) as group\n"
        "from plato.Input group by grouping sets (cast(key as uint32) /100 as key_first, Substring(value, 1, 1) as val_first);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:71: Error: Column 'nomind' is not a grouping column\n");
}

Y_UNIT_TEST(NoGroupingColumn1) {
    NYql::TAstParseResult res = SqlToYql("select count(1), grouping(key, value) as group_duo from plato.Input group by cube (key, subkey);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:32: Error: Column 'value' is not a grouping column\n");
}

Y_UNIT_TEST(EmptyAccess0) {
    NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList(``));");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:73: Error: Column reference \"\" is not allowed in current scope\n");
}

Y_UNIT_TEST(EmptyAccess1) {
    NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), ``);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:66: Error: Column reference \"\" is not allowed in current scope\n");
}

Y_UNIT_TEST(UseUnknownColumnInInsert) {
    NYql::TAstParseResult res = SqlToYql("insert into plato.Output (list0, list1) values (AsList(0, 1, 2), AsList(`test`));");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:73: Error: Column reference \"test\" is not allowed in current scope\n");
}

Y_UNIT_TEST(GroupByEmptyColumn) {
    NYql::TAstParseResult res = SqlToYql("select count(1) from plato.Input group by ``;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:43: Error: Column name can not be empty\n");
}

Y_UNIT_TEST(ConvertNumberOutOfBase) {
    NYql::TAstParseResult res = SqlToYql("select 0o80l;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0o80l, char: '8' is out of base: 8\n");
}

Y_UNIT_TEST(ConvertNumberOutOfRangeForInt64ButFitsInUint64) {
    NYql::TAstParseResult res = SqlToYql("select 0xc000000000000000l;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse 13835058055282163712 as integer literal of Int64 type: value out of range for Int64\n");
}

Y_UNIT_TEST(ConvertNumberOutOfRangeUint64) {
    NYql::TAstParseResult res = SqlToYql("select 0xc0000000000000000l;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 0xc0000000000000000l, number limit overflow\n");

    res = SqlToYql("select 1234234543563435151456;\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to parse number from string: 1234234543563435151456, number limit overflow\n");
}

Y_UNIT_TEST(ConvertNumberNegativeOutOfRange) {
    NYql::TAstParseResult res = SqlToYql("select -9223372036854775808;\n"
                                         "select -9223372036854775809;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Error: Failed to parse negative integer: -9223372036854775809, number limit overflow\n");
}

Y_UNIT_TEST(InvaildUsageReal0) {
    NYql::TAstParseResult res = SqlToYql("select .0;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "<main>:1:7: Error: extraneous input '.' expecting {");
}

Y_UNIT_TEST(InvaildUsageReal1) {
    NYql::TAstParseResult res = SqlToYql("select .0f;");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:7: Error: Unexpected token '.' : cannot match to any predicted input...\n\n");
#else
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "<main>:1:7: Error: extraneous input '.' expecting {");
#endif
}

Y_UNIT_TEST(InvaildUsageWinFunctionWithoutWindow) {
    NYql::TAstParseResult res = SqlToYql("select lead(key, 2) from plato.Input;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: Failed to use window function Lead without window specification\n");
}

Y_UNIT_TEST(DropTableWithIfExists) {
    NYql::TAstParseResult res = SqlToYql("DROP TABLE IF EXISTS plato.foo;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [](const TString& word, const TString& line) {
        if (word == "Write") {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("drop_if_exists"));
        }
    };

    TWordCountHive elementStat = {{TString("Write"), 0}};
    VerifyProgram(res, elementStat, verifyLine);

    UNIT_ASSERT_VALUES_EQUAL(1, elementStat["Write"]);
}

Y_UNIT_TEST(DropTableNamedNode) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
        $x = "y";
        DROP TABLE plato.$x;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(TooManyErrors) {
    const char* q = R"(
        USE plato;
        select A, B, C, D, E, F, G, H, I, J, K, L, M, N from (select b from `abc`);
    )";

    NYql::TAstParseResult res = SqlToYql(q, 10);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        R"(<main>:3:16: Error: Column A is not in source column set. Did you mean b?
<main>:3:19: Error: Column B is not in source column set. Did you mean b?
<main>:3:22: Error: Column C is not in source column set. Did you mean b?
<main>:3:25: Error: Column D is not in source column set. Did you mean b?
<main>:3:28: Error: Column E is not in source column set. Did you mean b?
<main>:3:31: Error: Column F is not in source column set. Did you mean b?
<main>:3:34: Error: Column G is not in source column set. Did you mean b?
<main>:3:37: Error: Column H is not in source column set. Did you mean b?
<main>:3:40: Error: Column I is not in source column set. Did you mean b?
<main>: Error: Too many issues, code: 1
)");
};

Y_UNIT_TEST(TooManyErrorsOnBuild) {
    TString q = R"sql(
        SELECT AsStruct(
            1 as '1',
            2 as '2',
            3 as '3',
            4 as '4',
            5 as '5',
            6 as '6',
            7 as '7',
            8 as '8',
            9 as '9',
            10 as '10',
            11 as '11',
            12 as '12'
        );
    )sql";

    NYql::TAstParseResult res = SqlToYql(q, 4);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(
        Err2Str(res),
        NYql::TrimIndent(R"(
            <main>:3:18: Error: String literal can not be used here
            <main>:4:18: Error: String literal can not be used here
            <main>:5:18: Error: String literal can not be used here
            <main>: Error: Too many issues, code: 1

        )"));
};

Y_UNIT_TEST(ShouldCloneBindingForNamedParameter) {
    NYql::TAstParseResult res = SqlToYql(R"($f = () -> {
        $value_type = TypeOf(1);
        $pair_type = StructType(
            TypeOf("2") AS key,
            $value_type AS value
        );

        RETURN TupleType(
            ListType($value_type),
            $pair_type);
    };

    select FormatType($f());
    )");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(BlockedInvalidFrameBounds) {
    auto check = [](const TString& frame, const TString& err) {
        const TString prefix = "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (PARTITION BY key ORDER BY subkey\n";
        NYql::TAstParseResult res = SqlToYql(prefix + frame + ")");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_NO_DIFF(Err2Str(res), err);
    };

    check("ROWS UNBOUNDED FOLLOWING", "<main>:2:5: Error: Frame cannot start from UNBOUNDED FOLLOWING\n");
    check("ROWS BETWEEN 5 PRECEDING AND UNBOUNDED PRECEDING", "<main>:2:29: Error: Frame cannot end with UNBOUNDED PRECEDING\n");
    check("ROWS BETWEEN CURRENT ROW AND 5 PRECEDING", "<main>:2:13: Error: Frame cannot start from CURRENT ROW and end with PRECEDING\n");
    check("ROWS BETWEEN 5 FOLLOWING AND CURRENT ROW", "<main>:2:14: Error: Frame cannot start from FOLLOWING and end with CURRENT ROW\n");
    check("ROWS BETWEEN 5 FOLLOWING AND 5 PRECEDING", "<main>:2:14: Error: Frame cannot start from FOLLOWING and end with PRECEDING\n");
}

Y_UNIT_TEST(BlockedRangeValueWithoutSingleOrderBy) {
    UNIT_ASSERT(SqlToYql("SELECT COUNT(*) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM plato.Input").IsOk());
    UNIT_ASSERT(SqlToYql("SELECT COUNT(*) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM plato.Input").IsOk());

    auto res = SqlToYql("SELECT COUNT(*) OVER (RANGE 5 PRECEDING) FROM plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:29: Error: RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause\n");

    res = SqlToYql("SELECT COUNT(*) OVER (ORDER BY key, value RANGE 5 PRECEDING) FROM plato.Input");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Error: RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause\n");
}

Y_UNIT_TEST(NoColumnsInFrameBounds) {
    NYql::TAstParseResult res = SqlToYql(
        "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (ROWS BETWEEN\n"
        " 1 + key PRECEDING AND 2 + key FOLLOWING);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:6: Error: Column reference \"key\" is not allowed in current scope\n");
}

Y_UNIT_TEST(WarnOnEmptyFrameBounds) {
    NYql::TAstParseResult res = SqlToYql(
        "SELECT SUM(x) OVER w FROM plato.Input WINDOW w AS (PARTITION BY key ORDER BY subkey\n"
        "ROWS BETWEEN 10 FOLLOWING AND 5 FOLLOWING)");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:14: Warning: Used frame specification implies empty window frame, code: 4520\n");
}

Y_UNIT_TEST(WarnOnRankWithUnorderedWindow) {
    NYql::TAstParseResult res = SqlToYql("SELECT RANK() OVER w FROM plato.Input WINDOW w AS ()");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Rank() is used with unordered window - all rows will be considered equal to each other, code: 4521\n");
}

Y_UNIT_TEST(WarnOnRankExprWithUnorderedWindow) {
    NYql::TAstParseResult res = SqlToYql("SELECT RANK(key) OVER w FROM plato.Input WINDOW w AS ()");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: Rank(<expression>) is used with unordered window - the result is likely to be undefined, code: 4521\n");
}

Y_UNIT_TEST(AnsiCurrentRow) {
    const auto check = [](TString spec, TString expected, TMaybe<TString> flag) {
        TString query = R"sql(
                $events = (SELECT * FROM (VALUES
                    (1, 10,  5),
                    (2, 10,  5),
                    (3, 20, 10)
                ) AS events (event_id, ts, val));
                SELECT ts, val, SUM(val) OVER (SPEC) AS run_sum FROM $events ORDER BY ts, event_id;
            )sql";
        SubstGlobal(query, "SPEC", spec);

        NSQLTranslation::TTranslationSettings settings;
        if (flag) {
            settings.Flags.emplace(*flag);
        }

        NYql::TAstParseResult res = SqlToYqlWithSettings(query, settings);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

        TWordCountHive stat({"WinOnRows", "WinOnRange"});
        VerifyProgram(res, stat);
        UNIT_ASSERT_VALUES_EQUAL(stat["WinOnRows"], expected == "WinOnRows" ? 1 : 0);
        UNIT_ASSERT_VALUES_EQUAL(stat["WinOnRange"], expected == "WinOnRange" ? 1 : 0);
    };

    check("ORDER BY ts", "WinOnRows", Nothing());
    check("", "WinOnRows", Nothing());
    check("ORDER BY ts", "WinOnRange", "AnsiCurrentRow");
    check("", "WinOnRows", "AnsiCurrentRow");
}

Y_UNIT_TEST(AnyAsTableName) {
    NYql::TAstParseResult res = SqlToYql("use plato; select * from any;");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: Unexpected token ';' : syntax error...\n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:28: Error: no viable alternative at input 'any;'\n");
#endif
}

Y_UNIT_TEST(IncorrectOrderOfLambdaOptionalArgs) {
    NYql::TAstParseResult res = SqlToYql("$f = ($x?, $y)->($x + $y); select $f(1);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Non-optional argument can not follow optional one\n");
}

Y_UNIT_TEST(IncorrectOrderOfActionOptionalArgs) {
    NYql::TAstParseResult res = SqlToYql("define action $f($x?, $y) as select $x,$y; end define; do $f(1);");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:23: Error: Non-optional argument can not follow optional one\n");
}

Y_UNIT_TEST(NotAllowedQuestionOnNamedNode) {
    NYql::TAstParseResult res = SqlToYql("$f = 1; select $f?;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: Unexpected token '?' at the end of expression\n");
}

Y_UNIT_TEST(AnyAndCrossJoin) {
    NYql::TAstParseResult res = SqlToYql("use plato; select * from any Input1 cross join Input2");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:26: Error: ANY should not be used with Cross JOIN\n");

    res = SqlToYql("use plato; select * from Input1 cross join any Input2");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:44: Error: ANY should not be used with Cross JOIN\n");
}

Y_UNIT_TEST(AnyWithCartesianProduct) {
    NYql::TAstParseResult res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; select * from any Input1, Input2");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:56: Error: ANY should not be used with Cross JOIN\n");

    res = SqlToYql("pragma AnsiImplicitCrossJoin; use plato; select * from Input1, any Input2");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:64: Error: ANY should not be used with Cross JOIN\n");
}

Y_UNIT_TEST(ErrorPlainEndAsInlineActionTerminator) {
    NYql::TAstParseResult res = SqlToYql(
        "do begin\n"
        "  select 1\n"
        "; end\n");
    UNIT_ASSERT(!res.IsOk());
#if ANTLR_VER == 3
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:0: Error: Unexpected token absence : Missing DO \n\n");
#else
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:0: Error: missing DO at '<EOF>'\n");
#endif
}

Y_UNIT_TEST(ErrorMultiWayJoinWithUsing) {
    NYql::TAstParseResult res = SqlToYql(
        "USE plato;\n"
        "PRAGMA DisableSimpleColumns;\n"
        "SELECT *\n"
        "FROM Input1 AS a\n"
        "JOIN Input2 AS b USING(key)\n"
        "JOIN Input3 AS c ON a.key = c.key;\n");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:5:24: Error: Multi-way JOINs should be connected with ON clause instead of USING clause\n");
}

Y_UNIT_TEST(RequireLabelInFlattenByWithDot) {
    NYql::TAstParseResult res = SqlToYql("select * from plato.Input flatten by x.y");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res),
                        "<main>:1:40: Error: Unnamed expression after FLATTEN BY is not allowed\n");
}

Y_UNIT_TEST(WarnUnnamedColumns) {
    NYql::TAstParseResult res = SqlToYql(
        "PRAGMA WarnUnnamedColumns;\n"
        "\n"
        "SELECT key, subkey, key || subkey FROM plato.Input ORDER BY subkey;\n");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:28: Warning: Autogenerated column name column2 will be used for expression, code: 4516\n");
}

Y_UNIT_TEST(WarnSourceColumnMismatch) {
    NYql::TAstParseResult res = SqlToYql(
        "insert into plato.Output (key, subkey, new_value, one_more_value) select key as Key, subkey, value, \"x\" from plato.Input;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:51: Warning: Column names in SELECT don't match column specification in parenthesis. \"key\" doesn't match \"Key\". \"new_value\" doesn't match \"value\", code: 4517\n");
}

Y_UNIT_TEST(YtCaseInsensitive) {
    NYql::TAstParseResult res = SqlToYql("select * from PlatO.foo;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    res = SqlToYql("use PlatO; select * from foo;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

Y_UNIT_TEST(KikimrCaseSensitive) {
    NYql::TAstParseResult res = SqlToYql("select * from PlatO.foo;", 10, "kikimr");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:15: Error: Unknown cluster: PlatO\n");

    res = SqlToYql("use PlatO; select * from foo;", 10, "kikimr");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:5: Error: Unknown cluster: PlatO\n");
}

Y_UNIT_TEST(DiscoveryModeForbidden) {
    NYql::TAstParseResult res = SqlToYqlWithMode("insert into plato.Output select * from plato.range(\"\", Input1, Input4)", NSQLTranslation::ESqlMode::DISCOVERY);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: range is not allowed in Discovery mode, code: 4600\n");

    res = SqlToYqlWithMode(R"(insert into plato.Output select * from plato.like("", "Input%"))", NSQLTranslation::ESqlMode::DISCOVERY);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: like is not allowed in Discovery mode, code: 4600\n");

    res = SqlToYqlWithMode(R"(insert into plato.Output select * from plato.regexp("", "Input."))", NSQLTranslation::ESqlMode::DISCOVERY);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: regexp is not allowed in Discovery mode, code: 4600\n");

    res = SqlToYqlWithMode(R"(insert into plato.Output select * from plato.filter("", ($name) -> { return find($name, "Input") is not null; }))", NSQLTranslation::ESqlMode::DISCOVERY);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: filter is not allowed in Discovery mode, code: 4600\n");

    res = SqlToYqlWithMode(R"(select Path from plato.folder("") where Type == "table")", NSQLTranslation::ESqlMode::DISCOVERY);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:18: Error: folder is not allowed in Discovery mode, code: 4600\n");
}

Y_UNIT_TEST(YsonFuncWithoutArgs) {
    UNIT_ASSERT(SqlToYql("SELECT Yson::SerializeText(Yson::From());").IsOk());
}

Y_UNIT_TEST(CanNotUseOrderByInNonLastSelectInUnionAllChain) {
    auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
               "use plato;\n"
               "\n"
               "select * from Input order by key\n"
               "union all\n"
               "select * from Input order by key limit 1;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:21: Error: ORDER BY within UNION ALL is only allowed after last subquery\n");
}

Y_UNIT_TEST(CanNotUseLimitInNonLastSelectInUnionAllChain) {
    auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
               "use plato;\n"
               "\n"
               "select * from Input limit 1\n"
               "union all\n"
               "select * from Input order by key limit 1;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:21: Error: LIMIT within UNION ALL is only allowed after last subquery\n");
}

Y_UNIT_TEST(CanNotUseDiscardInNonFirstSelectInUnionAllChain) {
    auto req = "pragma AnsiOrderByLimitInUnionAll;\n"
               "use plato;\n"
               "\n"
               "select * from Input\n"
               "union all\n"
               "discard select * from Input;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
}

Y_UNIT_TEST(CanNotUseIntoResultInNonLastSelectInUnionAllChain) {
    auto req = "use plato;\n"
               "pragma AnsiOrderByLimitInUnionAll;\n"
               "\n"
               "select * from Input\n"
               "union all\n"
               "discard select * from Input;";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
}

Y_UNIT_TEST(YsonStrictInvalidPragma) {
    auto res = SqlToYql("pragma yson.Strict = \"wrong\";");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:22: Error: Expected 'true', 'false' or no parameter for: Strict\n");
}

Y_UNIT_TEST(WarnTableNameInSomeContexts) {
    UNIT_ASSERT(SqlToYql("use plato; select TableName() from Input;").IsOk());
    UNIT_ASSERT(SqlToYql("use plato; select TableName(\"aaaa\");").IsOk());
    UNIT_ASSERT(SqlToYql("select TableName(\"aaaa\", \"yt\");").IsOk());

    auto res = SqlToYql("select TableName() from plato.Input;");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Error: TableName requires either service name as second argument or current cluster name\n");

    res = SqlToYql("use plato;\n"
                   "select TableName() from Input1 as a join Input2 as b using(key);");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:8: Warning: TableName() may produce empty result when used in ambiguous context (with JOIN), code: 4525\n");

    res = SqlToYql("use plato;\n"
                   "select SOME(TableName()), key from Input group by key;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:13: Warning: TableName() will produce empty result when used with aggregation.\n"
                                      "Please consult documentation for possible workaround, code: 4525\n");
}

Y_UNIT_TEST(WarnOnDistincWithHavingWithoutAggregations) {
    auto res = SqlToYql("select distinct key from plato.Input having key != '0';");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:49: Warning: The usage of HAVING without aggregations with SELECT DISTINCT is non-standard and will stop working soon. Please use WHERE instead., code: 4526\n");
}

Y_UNIT_TEST(FlattenByExprWithNestedNull) {
    auto res = SqlToYql("USE plato;\n"
                        "\n"
                        "SELECT * FROM (SELECT 1 AS region_id)\n"
                        "FLATTEN BY (\n"
                        "    CAST($unknown(region_id) AS List<String>) AS region\n"
                        ")");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:10: Error: Unknown name: $unknown\n");
}

Y_UNIT_TEST(EmptySymbolNameIsForbidden) {
    auto req = "    $`` = 1; select $``;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:5: Error: Empty symbol name is not allowed\n");
}

Y_UNIT_TEST(WarnOnBinaryOpWithNullArg) {
    auto req = "select * from plato.Input where cast(key as Int32) != NULL";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:52: Warning: Binary operation != will return NULL here, code: 4529\n");

    req = "select 1 or null";
    res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "");
}

Y_UNIT_TEST(ErrorIfTableSampleArgUsesColumns) {
    auto req = "SELECT key FROM plato.Input TABLESAMPLE BERNOULLI(MIN_OF(100.0, CAST(subkey as Int32)));";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:70: Error: Column reference \"subkey\" is not allowed in current scope\n");
}

Y_UNIT_TEST(DerivedColumnListForSelectIsNotSupportedYet) {
    auto req = "SELECT a,b,c FROM plato.Input as t(x,y,z);";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:35: Error: Derived column list is only supported for VALUES\n");
}

Y_UNIT_TEST(ErrorIfValuesHasDifferentCountOfColumns) {
    auto req = "VALUES (1,2,3), (4,5);";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: All VALUES items should have same size: expecting 3, got 2\n");
}

Y_UNIT_TEST(ErrorIfDerivedColumnSizeExceedValuesColumnCount) {
    auto req = "SELECT * FROM(VALUES (1,2), (3,4)) as t(x,y,z);";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:40: Error: Derived column list size exceeds column count in VALUES\n");
}

Y_UNIT_TEST(WarnoOnAutogeneratedNamesForValues) {
    auto req = "PRAGMA WarnUnnamedColumns;\n"
               "SELECT * FROM (VALUES (1,2,3,4), (5,6,7,8)) as t(x,y);";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:16: Warning: Autogenerated column names column2...column3 will be used here, code: 4516\n");
}

Y_UNIT_TEST(ErrUnionAllWithOrderByWithoutExplicitLegacyMode) {
    auto req = "use plato;\n"
               "\n"
               "select * from Input order by key\n"
               "union all\n"
               "select * from Input order by key;";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: ORDER BY within UNION ALL is only allowed after last subquery\n");
}

Y_UNIT_TEST(ErrUnionAllWithLimitWithoutExplicitLegacyMode) {
    auto req = "use plato;\n"
               "\n"
               "select * from Input limit 10\n"
               "union all\n"
               "select * from Input limit 1;";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: LIMIT within UNION ALL is only allowed after last subquery\n");
}

Y_UNIT_TEST(ErrUnionAllWithIntoResultWithoutExplicitLegacyMode) {
    auto req = "use plato;\n"
               "\n"
               "select * from Input into result aaa\n"
               "union all\n"
               "select * from Input;";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:3:21: Error: INTO RESULT within UNION ALL is only allowed after last subquery\n");
}

Y_UNIT_TEST(ErrUnionAllWithDiscardWithoutExplicitLegacyMode) {
    auto req = "use plato;\n"
               "\n"
               "select * from Input\n"
               "union all\n"
               "discard select * from Input;";

    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:1: Error: DISCARD within UNION ALL is only allowed before first subquery\n");
}

Y_UNIT_TEST(ErrUnionAllKeepsIgnoredOrderByWarning) {
    auto req = "use plato;\n"
               "\n"
               "SELECT * FROM (\n"
               "  SELECT * FROM Input\n"
               "  UNION ALL\n"
               "  SELECT t.* FROM Input AS t ORDER BY t.key\n"
               ");";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:3: Warning: ORDER BY without LIMIT in subquery will be ignored, code: 4504\n"
                                      "<main>:6:39: Error: Unknown correlation name: t\n");
}

Y_UNIT_TEST(ErrOrderByIgnoredButCheckedForMissingColumns) {
    auto req = "$src = SELECT key FROM (SELECT 1 as key, 2 as subkey) ORDER BY x; SELECT * FROM $src;";
    ExpectFailWithError(req, "<main>:1:8: Warning: ORDER BY without LIMIT in subquery will be ignored, code: 4504\n"
                             "<main>:1:64: Error: Column x is not in source column set\n");

    req = "$src = SELECT key FROM plato.Input ORDER BY x; SELECT * FROM $src;";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:8: Warning: ORDER BY without LIMIT in subquery will be ignored, code: 4504\n");
}

Y_UNIT_TEST(InvalidTtlInterval) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (Key Uint32, CreatedAt Timestamp, PRIMARY KEY (Key))
            WITH (TTL = 1 On CreatedAt);
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:25: Error: Literal of Interval type is expected for TTL\n"
                                      "<main>:4:25: Error: Invalid TTL settings\n");
}

Y_UNIT_TEST(InvalidTtlUnit) {
    auto req = R"(
        USE plato;
        CREATE TABLE tableName (Key Uint32, CreatedAt Uint32, PRIMARY KEY (Key))
        WITH (TTL = Interval("P1D") On CreatedAt AS PICOSECONDS);
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "mismatched input 'PICOSECONDS' expecting {MICROSECONDS, MILLISECONDS, NANOSECONDS, SECONDS}");
}

Y_UNIT_TEST(InvalidChangefeedSink) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (SINK_TYPE = "S3", MODE = "KEYS_ONLY", FORMAT = "json")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:55: Error: Unknown changefeed sink type: S3\n");
}

Y_UNIT_TEST(InvalidChangefeedSettings) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (SINK_TYPE = "local", FOO = "bar")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:64: Error: Unknown changefeed setting: FOO\n");
}

Y_UNIT_TEST(InvalidChangefeedInitialScan) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", INITIAL_SCAN = "foo")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:95: Error: Literal of Bool type is expected for INITIAL_SCAN\n");
}

Y_UNIT_TEST(InvalidChangefeedUserSIDs) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", USER_SIDS = "foo")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:92: Error: Literal of Bool type is expected for USER_SIDS\n");
}

Y_UNIT_TEST(InvalidChangefeedTraceIDs) {
    auto res = SqlToYql(R"sql(
        USE plato;
        CREATE TABLE tableName (
            Key Uint32, PRIMARY KEY (Key),
            CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", TRACE_IDS = "foo")
        );
    )sql");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:88: Error: Literal of Bool type is expected for TRACE_IDS\n");
}

Y_UNIT_TEST(InvalidChangefeedVirtualTimestamps) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", VIRTUAL_TIMESTAMPS = "foo")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:101: Error: Literal of Bool type is expected for VIRTUAL_TIMESTAMPS\n");
}

Y_UNIT_TEST(InvalidChangefeedResolvedTimestamps) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", BARRIERS_INTERVAL = "foo")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:100: Error: Literal of Interval type is expected for BARRIERS_INTERVAL\n");
}

Y_UNIT_TEST(InvalidChangefeedSchemaChanges) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", SCHEMA_CHANGES = "foo")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:97: Error: Literal of Bool type is expected for SCHEMA_CHANGES\n");
}

Y_UNIT_TEST(InvalidChangefeedRetentionPeriod) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", RETENTION_PERIOD = "foo")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:99: Error: Literal of Interval type is expected for RETENTION_PERIOD\n");
}

Y_UNIT_TEST(InvalidChangefeedTopicPartitions) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", TOPIC_MIN_ACTIVE_PARTITIONS = "foo")
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:110: Error: Literal of integer type is expected for TOPIC_MIN_ACTIVE_PARTITIONS\n");
}

Y_UNIT_TEST(InvalidChangefeedAwsRegion) {
    auto req = R"(
            USE plato;
            CREATE TABLE tableName (
                Key Uint32, PRIMARY KEY (Key),
                CHANGEFEED feedName WITH (MODE = "KEYS_ONLY", FORMAT = "json", AWS_REGION = true)
            );
    )";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:5:93: Error: Literal of String type is expected for AWS_REGION\n");
}

Y_UNIT_TEST(ErrJoinWithGroupingSetsWithoutCorrelationName) {
    auto req = "USE plato;\n"
               "\n"
               "SELECT k1, k2, subkey\n"
               "FROM T1 AS a JOIN T2 AS b USING (key)\n"
               "GROUP BY GROUPING SETS(\n"
               "  (a.key as k1, b.subkey as k2),\n"
               "  (k1),\n"
               "  (subkey)\n"
               ");";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:8:4: Error: Columns in grouping sets should have correlation name, error in key: subkey\n");
}

Y_UNIT_TEST(ErrJoinWithGroupByWithoutCorrelationName) {
    auto req = "USE plato;\n"
               "\n"
               "SELECT k1, k2,\n"
               "    value\n"
               "FROM T1 AS a JOIN T2 AS b USING (key)\n"
               "GROUP BY a.key as k1, b.subkey as k2,\n"
               "    value;";
    ExpectFailWithError(req,
                        "<main>:7:5: Error: Columns in GROUP BY should have correlation name, error in key: value\n");
}

Y_UNIT_TEST(ErrWithMissingFrom) {
    auto req = "select 1 as key where 1 > 1;";
    auto res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:25: Error: Filtering is not allowed without FROM\n");

    req = "select 1 + count(*);";
    res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:12: Error: Aggregation is not allowed without FROM\n");

    req = "select 1 as key, subkey + value;";
    res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                      "<main>:1:18: Error: Column reference 'subkey'\n"
                                      "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                      "<main>:1:27: Error: Column reference 'value'\n");

    req = "select count(1) group by key;";
    res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:1: Error: Column references are not allowed without FROM\n"
                                      "<main>:1:26: Error: Column reference 'key'\n");
}

Y_UNIT_TEST(ErrWithMissingFromForWindow) {
    auto req = "$c = () -> (1 + count(1) over w);\n"
               "select $c();";
    ExpectFailWithError(req,
                        "<main>:1:9: Error: Window and aggregation functions are not allowed in this context\n"
                        "<main>:1:17: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

    req = "$c = () -> (1 + lead(1) over w);\n"
          "select $c();";
    ExpectFailWithError(req,
                        "<main>:1:17: Error: Window functions are not allowed in this context\n"
                        "<main>:1:17: Error: Failed to use window function Lead without window specification or in wrong place\n");

    req = "select 1 + count(1) over w window w as ();";
    ExpectFailWithError(req,
                        "<main>:1:1: Error: Window and aggregation functions are not allowed without FROM\n"
                        "<main>:1:12: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

    req = "select 1 + lead(1) over w window w as ();";
    ExpectFailWithError(req,
                        "<main>:1:12: Error: Window functions are not allowed without FROM\n"
                        "<main>:1:12: Error: Failed to use window function Lead without window specification or in wrong place\n");
}

Y_UNIT_TEST(ErrWithMissingFromForInplaceWindow) {
    auto req = "$c = () -> (1 + count(1) over ());\n"
               "select $c();";
    ExpectFailWithError(req,
                        "<main>:1:26: Error: Window and aggregation functions are not allowed in this context\n");

    req = "$c = () -> (1 + lead(1) over (rows between unbounded preceding and current row));\n"
          "select $c();";
    ExpectFailWithError(req,
                        "<main>:1:25: Error: Window and aggregation functions are not allowed in this context\n");

    req = "select 1 + count(1) over ();";
    ExpectFailWithError(req,
                        "<main>:1:1: Error: Window and aggregation functions are not allowed without FROM\n"
                        "<main>:1:12: Error: Failed to use aggregation function Count without window specification or in wrong place\n");

    req = "select 1 + lead(1) over (rows between current row and unbounded following);";
    ExpectFailWithError(req,
                        "<main>:1:12: Error: Window functions are not allowed without FROM\n"
                        "<main>:1:12: Error: Failed to use window function Lead without window specification or in wrong place\n");
}

Y_UNIT_TEST(ErrDistinctInWrongPlace) {
    auto req = "select Some::Udf(distinct key) from plato.Input;";
    ExpectFailWithError(req,
                        "<main>:1:18: Error: DISTINCT can only be used in aggregation functions\n");
    req = "select sum(key)(distinct foo) from plato.Input;";
    ExpectFailWithError(req,
                        "<main>:1:17: Error: DISTINCT can only be used in aggregation functions\n");

    req = "select len(distinct foo) from plato.Input;";
    ExpectFailWithError(req,
                        "<main>:1:8: Error: DISTINCT can only be used in aggregation functions\n");

    req = "$foo = ($x) -> ($x); select $foo(distinct key) from plato.Input;";
    ExpectFailWithError(req,
                        "<main>:1:34: Error: DISTINCT can only be used in aggregation functions\n");
}

Y_UNIT_TEST(ErrForNotSingleChildInInlineAST) {
    ExpectFailWithError("select YQL::\"\"",
                        "<main>:1:8: Error: Failed to parse YQL: expecting AST root node with single child, but got 0\n");
    ExpectFailWithError("select YQL::@@  \t@@",
                        "<main>:1:8: Error: Failed to parse YQL: expecting AST root node with single child, but got 0\n");
    auto req = "$lambda = YQL::@@(lambda '(x)(+ x x)) (lambda '(y)(+ y y))@@;\n"
               "select ListMap([1, 2, 3], $lambda);";
    ExpectFailWithError(req,
                        "<main>:1:11: Error: Failed to parse YQL: expecting AST root node with single child, but got 2\n");
}

Y_UNIT_TEST(ErrEmptyColumnName) {
    ExpectFailWithError("select * without \"\" from plato.Input",
                        "<main>:1:18: Error: String literal can not be used here\n");

    ExpectFailWithError("select * without `` from plato.Input;",
                        "<main>:1:18: Error: Empty column name is not allowed\n");

    ExpectFailWithErrorForAnsiLexer("select * without \"\" from plato.Input",
                                    "<main>:1:18: Error: Empty column name is not allowed\n");

    ExpectFailWithErrorForAnsiLexer("select * without `` from plato.Input;",
                                    "<main>:1:18: Error: Empty column name is not allowed\n");
}

Y_UNIT_TEST(ErrOnNonZeroArgumentsForTableRows) {
    ExpectFailWithError("$udf=\"\";process plato.Input using $udf(TableRows(k))",
                        "<main>:1:40: Error: TableRows requires exactly 0 arguments\n");
}

Y_UNIT_TEST(ErrGroupByWithAggregationFunctionAndDistinctExpr) {
    ExpectFailWithError("select * from plato.Input group by count(distinct key|key)",
                        "<main>:1:36: Error: Unable to GROUP BY aggregated values\n");
}

// FIXME: check if we can get old behaviour
#if 0
        Y_UNIT_TEST(ErrWithSchemaWithColumnsWithoutType) {
            ExpectFailWithError("select * from plato.Input with COLUMNs",
                "<main>:1:32: Error: Expected type after COLUMNS\n"
                "<main>:1:32: Error: Failed to parse table hints\n");

            ExpectFailWithError("select * from plato.Input with scheMa",
                "<main>:1:32: Error: Expected type after SCHEMA\n"
                "<main>:1:32: Error: Failed to parse table hints\n");
        }
#endif

Y_UNIT_TEST(ErrCollectPreaggregatedInListLiteralWithoutFrom) {
    ExpectFailWithError("SELECT([VARIANCE(DISTINCT[])])",
                        "<main>:1:1: Error: Column references are not allowed without FROM\n"
                        "<main>:1:9: Error: Column reference '_yql_preagg_Variance0'\n");
}

Y_UNIT_TEST(ErrGroupBySmartParenAsTuple) {
    ExpectFailWithError("SELECT * FROM plato.Input GROUP BY (k, v,)",
                        "<main>:1:41: Error: Unexpected trailing comma in grouping elements list\n");
}

Y_UNIT_TEST(HandleNestedSmartParensInGroupBy) {
    ExpectFailWithError("SELECT * FROM plato.Input GROUP BY (+() as k)",
                        "<main>:1:37: Error: Unable to GROUP BY constant expression\n");
}

Y_UNIT_TEST(ErrRenameWithAddColumn) {
    ExpectFailWithError("USE ydb;   ALTER TABLE table RENAME TO moved, ADD COLUMN addc uint64",
                        "<main>:1:40: Error: RENAME TO can not be used together with another table action\n");
}

Y_UNIT_TEST(ErrAddColumnAndRename) {
    // FIXME: fix positions in ALTER TABLE
    ExpectFailWithError("USE ydb;   ALTER TABLE table ADD COLUMN addc uint64, RENAME TO moved",
                        "<main>:1:46: Error: RENAME TO can not be used together with another table action\n");
}

Y_UNIT_TEST(InvalidUuidValue) {
    ExpectFailWithError("SELECT Uuid('123e4567ae89ba12d3aa456a426614174ab0')",
                        "<main>:1:8: Error: Invalid value \"123e4567ae89ba12d3aa456a426614174ab0\" for type Uuid\n");
    ExpectFailWithError("SELECT Uuid('123e4567ae89b-12d3-a456-426614174000')",
                        "<main>:1:8: Error: Invalid value \"123e4567ae89b-12d3-a456-426614174000\" for type Uuid\n");
}

Y_UNIT_TEST(WindowFunctionWithoutOver) {
    ExpectFailWithError("SELECT LAST_VALUE(foo) FROM plato.Input",
                        "<main>:1:8: Error: Can't use window function LastValue without window specification (OVER keyword is missing)\n");
    ExpectFailWithError("SELECT LAST_VALUE(foo) FROM plato.Input GROUP BY key",
                        "<main>:1:8: Error: Can't use window function LastValue without window specification (OVER keyword is missing)\n");
}

Y_UNIT_TEST(CreateAlterUserWithoutCluster) {
    ExpectFailWithError("\n CREATE USER user ENCRYPTED PASSWORD 'foobar';", "<main>:2:2: Error: USE statement is missing - no default cluster is selected\n");
    ExpectFailWithError("ALTER USER CURRENT_USER RENAME TO $foo;", "<main>:1:1: Error: USE statement is missing - no default cluster is selected\n");
}

Y_UNIT_TEST(ModifyPermissionsWithoutCluster) {
    ExpectFailWithError("\n GRANT CONNECT ON `/Root` TO user;", "<main>:2:2: Error: USE statement is missing - no default cluster is selected\n");
    ExpectFailWithError("\n REVOKE MANAGE ON `/Root` FROM user;", "<main>:2:2: Error: USE statement is missing - no default cluster is selected\n");
}

Y_UNIT_TEST(ReservedRoleNames) {
    ExpectFailWithError("USE plato; CREATE USER current_User;", "<main>:1:24: Error: System role CURRENT_USER can not be used here\n");
    ExpectFailWithError("USE ydb;   ALTER USER current_User RENAME TO Current_role", "<main>:1:46: Error: System role CURRENT_ROLE can not be used here\n");
    UNIT_ASSERT(SqlToYql("USE plato; DROP GROUP IF EXISTS a, b, c, current_User;").IsOk());
}

Y_UNIT_TEST(DisableClassicDivisionWithError) {
    ExpectFailWithError("pragma ClassicDivision = 'false'; select $foo / 30;", "<main>:1:42: Error: Unknown name: $foo\n");
}

Y_UNIT_TEST(AggregationOfAgrregatedDistinctExpr) {
    ExpectFailWithError("select sum(sum(distinct x + 1)) from plato.Input", "<main>:1:12: Error: Aggregation of aggregated values is forbidden\n");
}

Y_UNIT_TEST(WarnForUnusedSqlHint) {
    NYql::TAstParseResult res = SqlToYql("select * from plato.Input1 as a join /*+ merge() */ plato.Input2 as b using(key);\n"
                                         "select --+            foo(bar)\n"
                                         "       1;");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:2:23: Warning: Hint foo will not be used, code: 4534\n");
}

Y_UNIT_TEST(WarnForUnusedSqlHintAsError) {
    NSQLTranslation::TTranslationSettings settings;

    TString query = R"sql(
            pragma warning("error", "*");

            select * from plato.Input1 as a
            join /*+ merge() */ plato.Input2 as b using(key);
            select --+            foo(bar)
                1;
        )sql";

    NYql::TAstParseResult res = SqlToYqlWithSettings(query, settings);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:6:35: Error: Hint foo will not be used, code: 4534\n");
}

Y_UNIT_TEST(WarnForDeprecatedSchema) {
    NSQLTranslation::TTranslationSettings settings;
    settings.ClusterMapping["s3bucket"] = NYql::S3ProviderName;
    NYql::TAstParseResult res = SqlToYqlWithSettings("select * from s3bucket.`foo` with schema (col1 Int32, String as col2, Int64 as col3);", settings);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "Warning: Deprecated syntax for positional schema: please use 'column type' instead of 'type AS column', code: 4535\n");
}

Y_UNIT_TEST(ErrorOnColumnNameInMaxByLimit) {
    ExpectFailWithError(
        "SELECT AGGREGATE_BY(AsTuple(value, key), AggregationFactory(\"MAX_BY\", subkey)) FROM plato.Input;",
        "<main>:1:42: Error: Source does not allow column references\n"
        "<main>:1:71: Error: Column reference 'subkey'\n");
}

Y_UNIT_TEST(ErrorInLibraryWithTopLevelNamedSubquery) {
    TString withUnusedSubq = "$unused = select max(key) from plato.Input;\n"
                             "\n"
                             "define subquery $foo() as\n"
                             "  $count = select count(*) from plato.Input;\n"
                             "  select * from plato.Input limit $count / 2;\n"
                             "end define;\n"
                             "export $foo;\n";
    UNIT_ASSERT(SqlToYqlWithMode(withUnusedSubq, NSQLTranslation::ESqlMode::LIBRARY).IsOk());

    TString withTopLevelSubq = "$count = select count(*) from plato.Input;\n"
                               "\n"
                               "define subquery $foo() as\n"
                               "  select * from plato.Input limit $count / 2;\n"
                               "end define;\n"
                               "export $foo;\n";
    auto res = SqlToYqlWithMode(withTopLevelSubq, NSQLTranslation::ESqlMode::LIBRARY);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Named subquery can not be used as a top level statement in libraries\n");
}

Y_UNIT_TEST(SessionStartAndSessionStateShouldSurviveSessionWindowArgsError) {
    TString query = R"(
            $init = ($_row) -> (min(1, 2)); -- error: aggregation func min() can not be used here
            $calculate = ($_row, $_state) -> (1);
            $update = ($_row, $_state) -> (2);
            SELECT
                SessionStart() over w as session_start,
                SessionState() over w as session_state,
            FROM plato.Input as t
            WINDOW w AS (
                PARTITION BY user, SessionWindow(ts + 1, $init, $update, $calculate)
            )
        )";
    ExpectFailWithError(query, "<main>:2:33: Error: Aggregation function Min requires exactly 1 argument(s), given: 2\n");
}

Y_UNIT_TEST(ScalarContextUsage1) {
    TString query = R"(
            $a = (select 1 as x, 2 as y);
            select 1 + $a;
        )";
    ExpectFailWithError(query, "<main>:2:39: Error: Source used in expression should contain one concrete column\n"
                               "<main>:3:24: Error: Source is used here\n");
}

Y_UNIT_TEST(ScalarContextUsage2) {
    TString query = R"(
            use plato;
            $a = (select 1 as x, 2 as y);
            select * from concat($a);
        )";
    ExpectFailWithError(query, "<main>:3:39: Error: Source used in expression should contain one concrete column\n"
                               "<main>:4:34: Error: Source is used here\n");
}

Y_UNIT_TEST(ScalarContextUsage3) {
    TString query = R"(
            use plato;
            $a = (select 1 as x, 2 as y);
            select * from range($a);
        )";
    ExpectFailWithError(query, "<main>:3:39: Error: Source used in expression should contain one concrete column\n"
                               "<main>:4:33: Error: Source is used here\n");
}

Y_UNIT_TEST(ScalarContextUsage4) {
    TString query = R"(
            use plato;
            $a = (select 1 as x, 2 as y);
            insert into $a select 1;
        )";
    ExpectFailWithError(query, "<main>:3:39: Error: Source used in expression should contain one concrete column\n"
                               "<main>:4:25: Error: Source is used here\n");
}
Y_UNIT_TEST(TablesFunctionDisallowedByDefault) {
    auto res = SqlToYql("USE plato; SELECT * FROM TABLES()");
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "TABLES is not allowed in this context");
}

} // Y_UNIT_TEST_SUITE(SqlToYQLErrors)

Y_UNIT_TEST_SUITE(Crashes) {

Y_UNIT_TEST(IncorrectCorrQuery) {
    NYql::TAstParseResult res = SqlToYql(R"sql(
            use plato;
            SELECT COUNT(DISTINCT EXISTS (SELECT 1 FROM t1 AS t2)) FROM Input AS t1
        )sql");

    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

} // Y_UNIT_TEST_SUITE(Crashes)
