#include "check_range.h"

#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NSQLTranslationV1 {
namespace {

void CheckQuery(const TString& query, bool ansiLexer = false) {
    google::protobuf::Arena arena;
    NYql::TIssues issues;
    NSQLTranslation::TParsedTokenList tokens;

    TParsers parsers;
    parsers.Antlr4 = MakeAntlr4ParserFactory(/*isAmbiguityError=*/false, /*isAmbiguityDebugging=*/false, Nothing());
    parsers.Antlr4Ansi = MakeAntlr4AnsiParserFactory(/*isAmbiguityError=*/false, /*isAmbiguityDebugging=*/false, Nothing());
    const auto* tree = SqlAST(parsers, query, "", issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS, ansiLexer, &arena);
    UNIT_ASSERT_C(tree, issues.ToString());

    TLexers lexers;
    lexers.Antlr4 = MakeAntlr4LexerFactory();
    lexers.Antlr4Ansi = MakeAntlr4AnsiLexerFactory();
    auto lexer = MakeLexer(lexers, ansiLexer);
    UNIT_ASSERT_C(NSQLTranslation::Tokenize(*lexer, query, "", tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS), issues.ToString());
    UNIT_ASSERT_C(CheckParserRanges(*tree, tokens, issues), issues.ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(ProtoParserCheckRangeTests) {
Y_UNIT_TEST(SimpleQuery) {
    CheckQuery("SELECT 1");
}

Y_UNIT_TEST(EmptyQuery) {
    CheckQuery("");
    CheckQuery("  ");
}

Y_UNIT_TEST(HiddenTokens) {
    CheckQuery("  SELECT /* comment */ 1;  ");
}

Y_UNIT_TEST(MultipleStatements) {
    CheckQuery("SELECT 1; SELECT 2");
}

Y_UNIT_TEST(ExpressionShapes) {
    CheckQuery("SELECT 1 + 2 * 3");
    CheckQuery("SELECT Abs(1)");
    CheckQuery("SELECT x FROM (SELECT 1 AS x)");
    CheckQuery("SELECT (SELECT 1)");
}

Y_UNIT_TEST(AnsiRoot) {
    CheckQuery("PRAGMA ANSI 2016;");
}

Y_UNIT_TEST(AnsiLexer) {
    CheckQuery("SELECT 1", /*ansiLexer=*/true);
}
} // Y_UNIT_TEST_SUITE(ProtoParserCheckRangeTests)

} // namespace NSQLTranslationV1
