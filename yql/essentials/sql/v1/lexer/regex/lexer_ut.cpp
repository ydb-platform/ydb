#include "lexer.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/random.h>

using namespace NSQLTranslationV1;
using NSQLTranslation::SQL_MAX_PARSER_ERRORS;
using NSQLTranslation::Tokenize;
using NSQLTranslation::TParsedToken;
using NSQLTranslation::TParsedTokenList;
using NYql::TIssues;

TLexers Lexers = {
    .Antlr4PureAnsi = MakeAntlr4PureAnsiLexerFactory(),
    .Regex = MakeRegexLexerFactory(/* ansi = */ false),
    .RegexAnsi = MakeRegexLexerFactory(/* ansi = */ true),
};

auto PureAnsiLexer = MakeLexer(
    Lexers, /* ansi = */ true, /* antlr4 = */ true, ELexerFlavor::Pure);

auto DefaultLexer = MakeLexer(
    Lexers, /* ansi = */ false, /* antlr4 = */ false, ELexerFlavor::Regex);

auto AnsiLexer = MakeLexer(
    Lexers, /* ansi = */ true, /* antlr4 = */ false, ELexerFlavor::Regex);

TString ToString(TParsedToken token) {
    TString& string = token.Name;
    if (token.Name != token.Content && token.Name != "EOF") {
        string += "(";
        string += token.Content;
        string += ")";
    }
    return string;
}

TString Tokenized(NSQLTranslation::ILexer& lexer, const TString& query) {
    TParsedTokenList tokens;
    TIssues issues;
    bool ok = Tokenize(lexer, query, "Test", tokens, issues, SQL_MAX_PARSER_ERRORS);

    TString out;
    if (!ok) {
        out = "[INVALID] ";
    }

    for (auto& token : tokens) {
        out += ToString(std::move(token));
        out += " ";
    }
    if (!out.empty()) {
        out.pop_back();
    }
    return out;
}

TString RandomMultilineCommentLikeText(size_t maxSize) {
    auto size = RandomNumber<size_t>(maxSize);
    TString comment;
    for (size_t i = 0; i < size; ++i) {
        if (auto /* isOpen */ _ = RandomNumber<bool>()) {
            comment += "/*";
        } else {
            comment += "*/";
        }

        for (int gap = RandomNumber<size_t>(2); gap > 0; --gap) {
            comment += " ";
        }
    }
    return comment;
}

void Check(TString input, TString expected, bool ansi) {
    auto* lexer = DefaultLexer.Get();
    if (ansi) {
        lexer = AnsiLexer.Get();
    }
    UNIT_ASSERT_VALUES_EQUAL(Tokenized(*lexer, input), expected);
}

void Check(TString input, TString expected) {
    Check(input, expected, /* ansi = */ false);
    Check(input, expected, /* ansi = */ true);
}

Y_UNIT_TEST_SUITE(RegexLexerTests) {
Y_UNIT_TEST(Whitespace) {
    Check("", "EOF");
    Check(" ", "WS( ) EOF");
    Check("  ", "WS( ) WS( ) EOF");
    Check("\n", "WS(\n) EOF");
}

Y_UNIT_TEST(SinleLineComment) {
    Check("--yql", "COMMENT(--yql) EOF");
    Check("--  yql ", "COMMENT(--  yql ) EOF");
    Check("-- yql\nSELECT", "COMMENT(-- yql\n) SELECT EOF");
    Check("-- yql --", "COMMENT(-- yql --) EOF");
}

Y_UNIT_TEST(MultiLineComment) {
    Check("/* yql */", "COMMENT(/* yql */) EOF");
    Check("/* yql */ */", "COMMENT(/* yql */) WS( ) ASTERISK(*) SLASH(/) EOF");
    Check("/* yql\n * yql\n */", "COMMENT(/* yql\n * yql\n */) EOF");
}

Y_UNIT_TEST(RecursiveMultiLineCommentDefault) {
    Check("/* /* yql */", "COMMENT(/* /* yql */) EOF", /* ansi = */ false);
    Check("/* /* yql */ */", "COMMENT(/* /* yql */) WS( ) ASTERISK(*) SLASH(/) EOF", /* ansi = */ false);
}

Y_UNIT_TEST(RecursiveMultiLineCommentAnsi) {
    Check("/* /* yql */", "COMMENT(/* /* yql */) EOF", /* ansi = */ true);
    Check("/* yql */ */", "COMMENT(/* yql */) WS( ) ASTERISK(*) SLASH(/) EOF", /* ansi = */ true);
    Check("/* /* /* yql */ */", "COMMENT(/* /* /* yql */ */) EOF", /* ansi = */ true);
    Check("/* /* yql */ */ */", "COMMENT(/* /* yql */ */) WS( ) ASTERISK(*) SLASH(/) EOF", /* ansi = */ true);
    Check("/* /* yql */ */", "COMMENT(/* /* yql */ */) EOF", /* ansi = */ true);
    Check("/*/*/*/", "COMMENT(/*/*/) ASTERISK(*) SLASH(/) EOF", /* ansi = */ true);
    Check("/*/**/*/*/*/", "COMMENT(/*/**/*/) ASTERISK(*) SLASH(/) ASTERISK(*) SLASH(/) EOF", /* ansi = */ true);
    Check("/* /* */ a /* /* */", "COMMENT(/* /* */ a /* /* */) EOF", /* ansi = */ true);
}

Y_UNIT_TEST(RecursiveMultiLineCommentAnsiReferenceComparion) {
    SetRandomSeed(100);
    for (size_t i = 0; i < 512; ++i) {
        auto input = RandomMultilineCommentLikeText(/* maxSize = */ 128);
        TString actual = Tokenized(*AnsiLexer, input);
        TString expected = Tokenized(*PureAnsiLexer, input);
        UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, "Input: " << input);
    }
}

Y_UNIT_TEST(Keyword) {
    Check("SELECT", "SELECT EOF");
    Check("INSERT", "INSERT EOF");
    Check("FROM", "FROM EOF");
}

Y_UNIT_TEST(Punctuation) {
    Check(
        "* / + - <|",
        "ASTERISK(*) WS( ) SLASH(/) WS( ) "
        "PLUS(+) WS( ) MINUS(-) WS( ) STRUCT_OPEN(<|) EOF");
    Check("SELECT*FROM", "SELECT ASTERISK(*) FROM EOF");
}

Y_UNIT_TEST(IdPlain) {
    Check("variable my_table", "ID_PLAIN(variable) WS( ) ID_PLAIN(my_table) EOF");
}

Y_UNIT_TEST(IdQuoted) {
    Check("``", "ID_QUOTED(``) EOF");
    Check("` `", "ID_QUOTED(` `) EOF");
    Check("` `", "ID_QUOTED(` `) EOF");
    Check("`local/table`", "ID_QUOTED(`local/table`) EOF");
}

Y_UNIT_TEST(SinleLineString) {
    Check("\"\"", "STRING_VALUE(\"\") EOF");
    Check("\' \'", "STRING_VALUE(\' \') EOF");
    Check("\" \"", "STRING_VALUE(\" \") EOF");
    Check("\"test\"", "STRING_VALUE(\"test\") EOF");

    Check("\"\\\"\"", "STRING_VALUE(\"\\\"\") EOF", /* ansi = */ false);
    Check("\"\\\"\"", "[INVALID] STRING_VALUE(\"\\\") EOF", /* ansi = */ true);

    Check("\"\"\"\"", "STRING_VALUE(\"\") STRING_VALUE(\"\") EOF", /* ansi = */ false);
    Check("\"\"\"\"", "STRING_VALUE(\"\"\"\") EOF", /* ansi = */ true);
}

Y_UNIT_TEST(MultiLineString) {
    Check("@@@@", "STRING_VALUE(@@@@) EOF");
    Check("@@ @@@", "STRING_VALUE(@@ @@@) EOF");
    Check("@@test@@", "STRING_VALUE(@@test@@) EOF");
    Check("@@line1\nline2@@", "STRING_VALUE(@@line1\nline2@@) EOF");
}

Y_UNIT_TEST(Query) {
    TString query =
        "SELECT\n"
        "  123467,\n"
        "  \"Hello, {name}!\",\n"
        "  (1 + (5 * 1 / 0)),\n"
        "  MIN(identifier),\n"
        "  Bool(field),\n"
        "  Math::Sin(var)\n"
        "FROM `local/test/space/table`\n"
        "JOIN test;";

    TString expected =
        "SELECT WS(\n) "
        "WS( ) WS( ) DIGITS(123467) COMMA(,) WS(\n) "
        "WS( ) WS( ) STRING_VALUE(\"Hello, {name}!\") COMMA(,) WS(\n) "
        "WS( ) WS( ) LPAREN(() DIGITS(1) WS( ) PLUS(+) WS( ) LPAREN(() DIGITS(5) WS( ) "
        "ASTERISK(*) WS( ) DIGITS(1) WS( ) SLASH(/) WS( ) DIGITS(0) RPAREN()) "
        "RPAREN()) COMMA(,) WS(\n) "
        "WS( ) WS( ) ID_PLAIN(MIN) LPAREN(() ID_PLAIN(identifier) RPAREN()) COMMA(,) WS(\n) "
        "WS( ) WS( ) ID_PLAIN(Bool) LPAREN(() ID_PLAIN(field) RPAREN()) COMMA(,) WS(\n) "
        "WS( ) WS( ) ID_PLAIN(Math) NAMESPACE(::) ID_PLAIN(Sin) LPAREN(() ID_PLAIN(var) RPAREN()) WS(\n) "
        "FROM WS( ) ID_QUOTED(`local/test/space/table`) WS(\n) "
        "JOIN WS( ) ID_PLAIN(test) SEMICOLON(;) EOF";

    Check(query, expected);
}

Y_UNIT_TEST(Invalid) {
    Check("\"", "[INVALID] EOF");
    Check("\" SELECT", "[INVALID] WS( ) SELECT EOF");
}

} // Y_UNIT_TEST_SUITE(RegexLexerTests)
