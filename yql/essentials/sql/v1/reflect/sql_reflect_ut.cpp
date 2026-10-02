#include "sql_reflect.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLReflect;

namespace {
TLexerGrammar grammar = LoadLexerGrammar();
} // namespace

Y_UNIT_TEST_SUITE(SqlReflectTests) {
Y_UNIT_TEST(Keywords) {
    UNIT_ASSERT_VALUES_EQUAL(grammar.KeywordNames.contains("SELECT"), true);
    UNIT_ASSERT_VALUES_EQUAL(grammar.KeywordNames.contains("INSERT"), true);
    UNIT_ASSERT_VALUES_EQUAL(grammar.KeywordNames.contains("WHERE"), true);
    UNIT_ASSERT_VALUES_EQUAL(grammar.KeywordNames.contains("COMMIT"), true);
}

Y_UNIT_TEST(Punctuation) {
    UNIT_ASSERT_VALUES_EQUAL(grammar.PunctuationNames.contains("LPAREN"), true);
    UNIT_ASSERT_VALUES_EQUAL(grammar.BlockByName.at("LPAREN"), "(");

    UNIT_ASSERT_VALUES_EQUAL(grammar.PunctuationNames.contains("MINUS"), true);
    UNIT_ASSERT_VALUES_EQUAL(grammar.BlockByName.at("MINUS"), "-");

    UNIT_ASSERT_VALUES_EQUAL(grammar.PunctuationNames.contains("NAMESPACE"), true);
    UNIT_ASSERT_VALUES_EQUAL(grammar.BlockByName.at("NAMESPACE"), "::");
}

Y_UNIT_TEST(Other) {
    UNIT_ASSERT_VALUES_EQUAL(Count(grammar.OtherNames, "REAL"), 1);
    UNIT_ASSERT_VALUES_EQUAL(Count(grammar.OtherNames, "STRING_VALUE"), 1);
    UNIT_ASSERT_VALUES_EQUAL(Count(grammar.OtherNames, "STRING_MULTILINE"), 0);

    UNIT_ASSERT_VALUES_EQUAL(
        grammar.BlockByName.at("FLOAT_EXP"),
        "E (PLUS | MINUS)? DECDIGITS");
    UNIT_ASSERT_VALUES_EQUAL(
        grammar.BlockByName.at("STRING_MULTILINE"),
        "(DOUBLE_COMMAT .*? DOUBLE_COMMAT)+ COMMAT?");
    UNIT_ASSERT_VALUES_EQUAL(
        grammar.BlockByName.at("REAL"),
        "(DECDIGITS DOT DIGIT* FLOAT_EXP? | DECDIGITS FLOAT_EXP) (F | P (F ('4' | '8') | N)?)?");
}

} // Y_UNIT_TEST_SUITE(SqlReflectTests)
