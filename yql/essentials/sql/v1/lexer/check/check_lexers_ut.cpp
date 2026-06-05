#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/sql/v1/lexer/check/check_lexers.h>

Y_UNIT_TEST_SUITE(CheckLexers) {

Y_UNIT_TEST(PgSyntax) {
    TString query = R"sql(
        --!syntax_pg
        SELECT
            convert_from(a, 'UTF8')
        FROM
            plato.x
        WHERE
            convert_from(b, 'UTF8') !~ '^[0-9]+$';
    )sql";

    NYql::TIssues issues;
    bool result = NSQLTranslationV1::CheckLexers(NYql::TPosition(), query, issues);

    UNIT_ASSERT_C(result, issues.ToString());
}

} // Y_UNIT_TEST_SUITE(CheckLexers)
