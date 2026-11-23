#include <yql/essentials/public/sql_format/sql_format.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

Y_UNIT_TEST_SUITE(TSqlFormat) {
Y_UNIT_TEST(FormatsAndIsIdempotent) {
    for (const TString query : {"select 1;", "--!ansi_lexer\nselect \"column\" from foo;", "select 'Привет';"}) {
        TString formatted;
        TString error;
        UNIT_ASSERT_C(NSQLFormat::SqlFormatSimple(query, formatted, error), error);
        UNIT_ASSERT_STRING_CONTAINS(formatted, "SELECT");
        TString again;
        UNIT_ASSERT_C(NSQLFormat::SqlFormatSimple(formatted, again, error), error);
        UNIT_ASSERT_VALUES_EQUAL(formatted, again);
    }
}

Y_UNIT_TEST(LexerFailureReturnsDiagnostic) {
    TString formatted = "previous output";
    TString error;
    const TString query = "select 'unterminated";
    UNIT_ASSERT(!NSQLFormat::SqlFormatSimple(query, formatted, error));
    UNIT_ASSERT(!error.empty());
    UNIT_ASSERT_VALUES_EQUAL(formatted, query);
}

Y_UNIT_TEST(UnparsedStatementsArePreserved) {
    TString formatted;
    TString error = "previous error";
    UNIT_ASSERT(NSQLFormat::SqlFormatSimple("select (", formatted, error));
    UNIT_ASSERT_VALUES_EQUAL(formatted, "select (\n");
    UNIT_ASSERT_VALUES_EQUAL(error, "previous error");
}

Y_UNIT_TEST(EmptyInput) {
    TString formatted = "previous output";
    TString error = "previous error";
    UNIT_ASSERT_C(NSQLFormat::SqlFormatSimple("", formatted, error), error);
    UNIT_ASSERT_VALUES_EQUAL(formatted, "");
    UNIT_ASSERT_VALUES_EQUAL(error, "previous error");
}

Y_UNIT_TEST(EmbeddedNullIsNotTruncated) {
    constexpr std::array query = {'s', 'e', 'l', 'e', 'c', 't', ' ', '1', ';', '\0', 'x'};
    TString formatted;
    TString error;
    UNIT_ASSERT_C(NSQLFormat::SqlFormatSimple(TString(query.data()), formatted, error), error);
    const TString queryWithNull(query.data(), query.size());
    UNIT_ASSERT(!NSQLFormat::SqlFormatSimple(queryWithNull, formatted, error));
    UNIT_ASSERT(!error.empty());
    UNIT_ASSERT_VALUES_EQUAL(formatted, queryWithNull);
}

Y_UNIT_TEST(LexerFailureDoesNotAffectNextCall) {
    TString formatted;
    TString error;
    UNIT_ASSERT(!NSQLFormat::SqlFormatSimple("select 'unterminated", formatted, error));
    UNIT_ASSERT(!error.empty());
    UNIT_ASSERT_C(NSQLFormat::SqlFormatSimple("select 1;", formatted, error), error);
    UNIT_ASSERT_STRING_CONTAINS(formatted, "SELECT");
}

Y_UNIT_TEST(FormatsInPlace) {
    TString query = "select 1;";
    TString expected;
    TString error;
    UNIT_ASSERT_C(NSQLFormat::SqlFormatSimple(query, expected, error), error);
    UNIT_ASSERT_C(NSQLFormat::SqlFormatSimple(query, query, error), error);
    UNIT_ASSERT_VALUES_EQUAL(query, expected);
}
} // Y_UNIT_TEST_SUITE(TSqlFormat)
