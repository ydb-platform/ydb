#include "sql_ut.h"

#include <yql/essentials/sql/v1/translation/sql.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(AnsiMode) {

Y_UNIT_TEST(PragmaAnsi) {
    UNIT_ASSERT(SqlToYql("PRAGMA ANSI 2016;").IsOk());
}

} // Y_UNIT_TEST_SUITE(AnsiMode)

Y_UNIT_TEST_SUITE(AnsiIdentsNegative) {

Y_UNIT_TEST(EnableAnsiLexerFromRequestSpecialComments) {
    auto req = "\n"
               "\t --!ansi_lexer \n"
               "-- Some comment\n"
               "-- another comment\n"
               "pragma SimpleColumns;\n"
               "\n"
               "select 1, '''' as empty;";

    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(AnsiLexerShouldNotBeEnabledHere) {
    auto req = "$str = '\n"
               "--!ansi_lexer\n"
               "--!syntax_v1\n"
               "';\n"
               "\n"
               "select 1, $str, \"\" as empty;";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
}

Y_UNIT_TEST(DoubleQuotesInDictsTuplesOrLists) {
    auto req = "$d = { 'a': 1, \"b\": 2, 'c': 3,};";

    auto res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:16: Error: Column reference \"b\" is not allowed in current scope\n");

    req = "$t = (1, 2, \"a\");";

    res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:13: Error: Column reference \"a\" is not allowed in current scope\n");

    req = "$l = ['a', 'b', \"c\"];";

    res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:1:17: Error: Column reference \"c\" is not allowed in current scope\n");
}

Y_UNIT_TEST(MultilineComments) {
    auto req = "/*/**/ select 1;";
    auto res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
    req = "/*\n"
          "--/*\n"
          "*/ select 1;";
    res = SqlToYql(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT(res.Issues.Size() == 0);
    req = "/*\n"
          "/*\n"
          "--*/\n"
          "*/ select 1;";
    res = SqlToYql(req);
    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), "<main>:4:0: Error: mismatched input '*' expecting {<EOF>, ';', '(', '$', ALTER, ANALYZE, BACKUP, BATCH, COMBINE, COMMIT, CREATE, DECLARE, DEFINE, DELETE, DISCARD, DO, DROP, EVALUATE, EXPLAIN, EXPORT, FOR, FROM, GRANT, IF, IMPORT, INSERT, MATERIALIZE, PARALLEL, PRAGMA, PROCESS, REDUCE, REPLACE, RESTORE, REVOKE, ROLLBACK, SELECT, SHOW, TRUNCATE, UPDATE, UPSERT, USE, VALUES, WITH}\n");
    res = SqlToYqlWithAnsiLexer(req);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
}

} // Y_UNIT_TEST_SUITE(AnsiIdentsNegative)

Y_UNIT_TEST_SUITE(AnsiOptionalAs) {

Y_UNIT_TEST(OptionalAsInProjection) {
    UNIT_ASSERT(SqlToYql("PRAGMA AnsiOptionalAs; SELECT a b, c FROM plato.Input;").IsOk());
    ExpectFailWithError("PRAGMA DisableAnsiOptionalAs;\n"
                        "SELECT a b, c FROM plato.Input;",
                        "<main>:2:10: Error: Expecting mandatory AS here. Did you miss comma? Please add PRAGMA AnsiOptionalAs; for ANSI compatibility\n");
}

Y_UNIT_TEST(OptionalAsWithKeywords) {
    UNIT_ASSERT(SqlToYql("PRAGMA AnsiOptionalAs; SELECT a type, b data, c source FROM plato.Input;").IsOk());
}

} // Y_UNIT_TEST_SUITE(AnsiOptionalAs)
