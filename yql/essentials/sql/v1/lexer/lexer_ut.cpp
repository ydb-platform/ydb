#include "lexer.h"
#include "lexer_ut.h"

#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <yql/essentials/sql/v1/lexer/antlr3/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr3_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/regex/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/ascii.h>
#include <util/random/random.h>

#define UNIT_ASSERT_TOKENIZED(LEXER, QUERY, TOKENS) \
    do {                                            \
        auto tokens = Tokenized((LEXER), (QUERY));  \
        UNIT_ASSERT_VALUES_EQUAL(tokens, (TOKENS)); \
    } while (false)

using namespace NSQLTranslation;
using namespace NSQLTranslationV1;

TLexers Lexers = {
    .Antlr3 = MakeAntlr3LexerFactory(),
    .Antlr3Ansi = MakeAntlr3AnsiLexerFactory(),
    .Antlr4 = MakeAntlr4LexerFactory(),
    .Antlr4Ansi = MakeAntlr4AnsiLexerFactory(),
    .Antlr4Pure = MakeAntlr4PureLexerFactory(),
    .Antlr4PureAnsi = MakeAntlr4PureAnsiLexerFactory(),
    .Regex = MakeRegexLexerFactory(/* ansi = */ false),
    .RegexAnsi = MakeRegexLexerFactory(/* ansi = */ true),
};

std::pair<TParsedTokenList, NYql::TIssues> Tokenize(ILexer::TPtr& lexer, const TString& query) {
    TParsedTokenList tokens;
    NYql::TIssues issues;
    Tokenize(*lexer, query, "", tokens, issues, SQL_MAX_PARSER_ERRORS);
    return {tokens, issues};
}

TVector<TString> GetIssueMessages(ILexer::TPtr& lexer, const TString& query) {
    TVector<TString> messages;
    for (const auto& issue : Tokenize(lexer, query).second) {
        messages.emplace_back(issue.ToString(/* oneLine = */ true));
    }
    return messages;
}

TVector<TString> GetTokenViews(ILexer::TPtr& lexer, const TString& query) {
    TVector<TString> names;
    for (auto& token : Tokenize(lexer, query).first) {
        TString view = std::move(token.Name);
        if (view == "ID_PLAIN" || view == "STRING_VALUE") {
            view.append(" (");
            view.append(token.Content);
            view.append(")");
        }
        names.emplace_back(std::move(view));
    }
    return names;
}

TString ToString(TParsedToken token) {
    TString& string = token.Name;
    if (token.Name != token.Content && token.Name != "EOF") {
        string += "(";
        string += token.Content;
        string += ")";
    }
    return string;
}

TString Tokenized(ILexer::TPtr& lexer, const TString& query) {
    TParsedTokenList tokens;
    NYql::TIssues issues;
    bool ok = Tokenize(*lexer, query, "Test", tokens, issues, SQL_MAX_PARSER_ERRORS);

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

Y_UNIT_TEST_SUITE(SQLv1Lexer) {
    Y_UNIT_TEST(UnsupportedIssues) {
        NSQLTranslationV1::TLexers factories;

        TVector<ILexer::TPtr> lexers;
        for (auto ansi : {false, true}) {
            for (auto antlr4 : {false, true}) {
                for (auto flavor : {ELexerFlavor::Default, ELexerFlavor::Pure, ELexerFlavor::Regex}) {
                    lexers.emplace_back(MakeLexer(factories, ansi, antlr4, flavor));
                }
            }
        }

        TVector<TString> actual;
        for (auto& lexer : lexers) {
            auto issues = GetIssueMessages(lexer, "");
            actual.emplace_back(std::move(issues.at(0)));
        }

        TVector<TString> expected = {
            "<main>: Error: Lexer antlr3 is not supported",
            "<main>: Error: Lexer antlr3_pure is not supported",
            "<main>: Error: Lexer regex is not supported",
            "<main>: Error: Lexer antlr4 is not supported",
            "<main>: Error: Lexer antlr4_pure is not supported",
            "<main>: Error: Lexer antlr4_regex is not supported",
            "<main>: Error: Lexer antlr3_ansi is not supported",
            "<main>: Error: Lexer antlr3_pure_ansi is not supported",
            "<main>: Error: Lexer regex_ansi is not supported",
            "<main>: Error: Lexer antlr4_ansi is not supported",
            "<main>: Error: Lexer antlr4_pure_ansi is not supported",
            "<main>: Error: Lexer antlr4_regex_ansi is not supported",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST_ON_EACH_LEXER(AntlrAndFlavorIndependent) {
        static const TVector<TString> queries = {
            "",
            "   ",
            "SELECT",
            "SEL", // identifier
            "SELECT FROM test",
            "SELECT * FROM",
            "   SELECT * FROM ",
            "SELECT \"\xF0\x9F\x98\x8A\" FROM ydb",
            (
                "SELECT \"\xF0\x9F\x98\x8A Hello, друзья\", count, name\n"
                "FROM table -- главная таблица 数据库 \n"
                "WHERE count < 6\n"
                "  AND name = \"可靠性\"\n"
                "  AND count > 12"),
            "\"select\"select",
        };

        static TVector<TString> expectations(queries.size());

        if (ANSI) {
            return;
        }

        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);

        for (size_t i = 0; i < queries.size(); ++i) {
            const auto& query = queries[i];
            auto& expected = expectations[i];

            if (expected.empty()) {
                expected = Tokenized(lexer, query);
                return;
            }

            UNIT_ASSERT_TOKENIZED(lexer, query, expected);
        }
    }

    TVector<TString> InvalidQueries();

    void TestInvalidTokensSkipped(bool antlr4, const TVector<TVector<TString>>& expected) {
        auto lexer = MakeLexer(Lexers, /* ansi = */ false, antlr4);

        auto input = InvalidQueries();
        UNIT_ASSERT_VALUES_EQUAL(input.size(), expected.size());

        for (size_t i = 0; i < input.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(GetTokenViews(lexer, input[i]), expected[i]);
        }
    }

    TVector<TString> InvalidQueries() {
        return {
            /* 0: */ "\xF0\x9F\x98\x8A",
            /* 1: */ "select \"aaaa",
            /* 2: */ "\"\\\"",
            /* 3: */ "\xF0\x9F\x98\x8A SELECT * FR",
            /* 4: */ "! SELECT *  from",
            /* 5: */ "\xF0\x9F\x98\x8Aselect ! from",
            /* 6: */ "\"",
            /* 7: */ "!select",
            /* 8: */ "SELECT \\\"\xF0\x9F\x98\x8A\\\" FROM test",
        };
    }

    Y_UNIT_TEST(ErrorRecoveryAntlr3) {
        TVector<TVector<TString>> actual = {
            /* 0: */ {"EOF"},
            /* 1: */ {"SELECT", "WS", "EOF"},
            /* 2: */ {"EOF"},
            /* 3: */ {"WS", "SELECT", "WS", "ASTERISK", "WS", "ID_PLAIN (FR)", "EOF"},
            /* 4: */ {"ID_PLAIN (ELECT)", "WS", "ASTERISK", "WS", "WS", "FROM", "EOF"},
            /* 5: */ {"SELECT", "WS", "ID_PLAIN (rom)", "EOF"},
            /* 6: */ {"EOF"},
            /* 7: */ {"ID_PLAIN (lect)", "EOF"},
            /* 8: */ {"SELECT", "WS", "EOF"},
        };
        TestInvalidTokensSkipped(/* antlr4 = */ false, actual);
    }

    Y_UNIT_TEST(ErrorRecoveryAntlr4) {
        TVector<TVector<TString>> actual = {
            /* 0: */ {"EOF"},
            /* 1: */ {"SELECT", "WS", "EOF"},
            /* 2: */ {"EOF"},
            /* 3: */ {"WS", "SELECT", "WS", "ASTERISK", "WS", "ID_PLAIN (FR)", "EOF"},
            /* 4: */ {"SELECT", "WS", "ASTERISK", "WS", "WS", "FROM", "EOF"},
            /* 5: */ {"SELECT", "WS", "FROM", "EOF"},
            /* 6: */ {"EOF"},
            /* 7: */ {"ID_PLAIN (elect)", "EOF"},
            /* 8: */ {"SELECT", "WS", "EOF"},
        };
        TestInvalidTokensSkipped(/* antlr4 = */ true, actual);
    }

    Y_UNIT_TEST(IssuesCollected) {
        auto lexer3 = MakeLexer(Lexers, /* ansi = */ false, /* antlr4 = */ false);
        auto lexer4 = MakeLexer(Lexers, /* ansi = */ false, /* antlr4 = */ true);
        auto lexer4p = MakeLexer(Lexers, /* ansi = */ false, /* antlr4 = */ true, ELexerFlavor::Pure);
        auto lexerR = MakeLexer(Lexers, /* ansi = */ false, /* antlr4 = */ false, ELexerFlavor::Regex);

        for (const auto& query : InvalidQueries()) {
            auto issues3 = GetIssueMessages(lexer3, query);
            auto issues4 = GetIssueMessages(lexer4, query);
            auto issues4p = GetIssueMessages(lexer4p, query);
            auto issuesR = GetIssueMessages(lexerR, query);

            UNIT_ASSERT(!issues3.empty());
            UNIT_ASSERT(!issues4.empty());
            UNIT_ASSERT(!issues4p.empty());
            UNIT_ASSERT(!issuesR.empty());
        }
    }

    Y_UNIT_TEST(IssueMessagesAntlr3) {
        auto lexer3 = MakeLexer(Lexers, /* ansi = */ false, /* antlr4 = */ false);

        auto actual = GetIssueMessages(lexer3, "\xF0\x9F\x98\x8A SELECT * FR");

        TVector<TString> expected = {
            "<main>:1:0: Error: Unexpected character '\xF0\x9F\x98\x8A' (Unicode character <128522>) : cannot match to any predicted input...",
            "<main>:1:1: Error: Unexpected character : cannot match to any predicted input...",
            "<main>:1:2: Error: Unexpected character : cannot match to any predicted input...",
            "<main>:1:3: Error: Unexpected character : cannot match to any predicted input...",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(IssueMessagesAntlr4) {
        auto lexer4 = MakeLexer(Lexers, /* ansi = */ false, /* antlr4 = */ true);

        auto actual = GetIssueMessages(lexer4, "\xF0\x9F\x98\x8A SELECT * FR");

        TVector<TString> expected = {
            "<main>:1:0: Error: token recognition error at: '\xF0\x9F\x98\x8A'",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Whitespace) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "", "EOF");
        UNIT_ASSERT_TOKENIZED(lexer, " ", "WS( ) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "  ", "WS( ) WS( ) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\n", "WS(\n) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Keyword) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "SELECT", "SELECT EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "INSERT", "INSERT EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "FROM", "FROM EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "from", "FROM(from) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, " UPSERT ", "WS( ) UPSERT WS( ) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "ERROR", "ERROR EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(KeywordSkip) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        if (ANTLR4 || FLAVOR == ELexerFlavor::Regex) {
            UNIT_ASSERT_TOKENIZED(lexer, "sKip", "TSKIP(sKip) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "SKIP", "TSKIP(SKIP) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, " SKIP ", "WS( ) TSKIP(SKIP) WS( ) EOF");
        } else {
            UNIT_ASSERT_TOKENIZED(lexer, "sKip", "SKIP(sKip) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "SKIP", "SKIP EOF");
            UNIT_ASSERT_TOKENIZED(lexer, " SKIP ", "WS( ) SKIP WS( ) EOF");
        }
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Punctuation) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(
            lexer,
            "* / + - <|",
            "ASTERISK(*) WS( ) SLASH(/) WS( ) "
            "PLUS(+) WS( ) MINUS(-) WS( ) STRUCT_OPEN(<|) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "SELECT*FROM", "SELECT ASTERISK(*) FROM EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(IdPlain) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "variable my_table", "ID_PLAIN(variable) WS( ) ID_PLAIN(my_table) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(IdQuoted) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "``", "ID_QUOTED(``) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "` `", "ID_QUOTED(` `) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "`local/table`", "ID_QUOTED(`local/table`) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Number) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "1", "DIGITS(1) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "123", "DIGITS(123) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "123u", "INTEGER_VALUE(123u) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "123ui", "INTEGER_VALUE(123ui) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "0xDEADbeef", "DIGITS(0xDEADbeef) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "123.45", "REAL(123.45) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "123.45E10", "REAL(123.45E10) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "123.45E+10", "REAL(123.45E+10) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "1E+10", "REAL(1E+10) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "123l", "INTEGER_VALUE(123l) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "0b01u", "INTEGER_VALUE(0b01u) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "0xfful", "INTEGER_VALUE(0xfful) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "0o7ut", "INTEGER_VALUE(0o7ut) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "456s", "INTEGER_VALUE(456s) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "1.2345f", "REAL(1.2345f) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(SingleLineString) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "\"\"", "STRING_VALUE(\"\") EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\' \'", "STRING_VALUE(\' \') EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\" \"", "STRING_VALUE(\" \") EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "\"test\"", "STRING_VALUE(\"test\") EOF");

        if (!ANSI) {
            UNIT_ASSERT_TOKENIZED(lexer, "\"\\\"\"", "STRING_VALUE(\"\\\"\") EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "\"\"\"\"", "STRING_VALUE(\"\") STRING_VALUE(\"\") EOF");
        } else if (ANTLR4 || FLAVOR == ELexerFlavor::Regex) {
            UNIT_ASSERT_TOKENIZED(lexer, "\"\\\"\"", "[INVALID] STRING_VALUE(\"\\\") EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "\"\"\"\"", "STRING_VALUE(\"\"\"\") EOF");
        }
    }

    Y_UNIT_TEST_ON_EACH_LEXER(MultiLineString) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "@@@@", "STRING_VALUE(@@@@) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "@@ @@@", "STRING_VALUE(@@ @@@) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "@@test@@", "STRING_VALUE(@@test@@) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "@@line1\nline2@@", "STRING_VALUE(@@line1\nline2@@) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "@@@@ @@A@@ @@@A@@", "STRING_VALUE(@@@@) WS( ) STRING_VALUE(@@A@@) WS( ) STRING_VALUE(@@@A@@) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(SingleLineComment) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "--yql", "COMMENT(--yql) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "--  yql ", "COMMENT(--  yql ) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "-- yql\nSELECT", "COMMENT(-- yql\n) SELECT EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "-- yql --", "COMMENT(-- yql --) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(MultiLineComment) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "/* yql */", "COMMENT(/* yql */) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "/* yql */ */", "COMMENT(/* yql */) WS( ) ASTERISK(*) SLASH(/) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "/* yql\n * yql\n */", "COMMENT(/* yql\n * yql\n */) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(RecursiveMultiLineComment) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        if (!ANSI) {
            UNIT_ASSERT_TOKENIZED(lexer, "/* /* yql */", "COMMENT(/* /* yql */) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/* /* yql */ */", "COMMENT(/* /* yql */) WS( ) ASTERISK(*) SLASH(/) EOF");
        } else if (ANTLR4 || FLAVOR == ELexerFlavor::Regex) {
            UNIT_ASSERT_TOKENIZED(lexer, "/* /* yql */", "COMMENT(/* /* yql */) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/* yql */ */", "COMMENT(/* yql */) WS( ) ASTERISK(*) SLASH(/) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/* /* /* yql */ */", "COMMENT(/* /* /* yql */ */) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/* /* yql */ */ */", "COMMENT(/* /* yql */ */) WS( ) ASTERISK(*) SLASH(/) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/* /* yql */ */", "COMMENT(/* /* yql */ */) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/*/*/*/", "COMMENT(/*/*/) ASTERISK(*) SLASH(/) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/*/**/*/*/*/", "COMMENT(/*/**/*/) ASTERISK(*) SLASH(/) ASTERISK(*) SLASH(/) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "/* /* */ a /* /* */", "COMMENT(/* /* */ a /* /* */) EOF");
        }
    }

    Y_UNIT_TEST_ON_EACH_LEXER(RandomRecursiveMultiLineComment) {
        if (!ANTLR4 && FLAVOR != ELexerFlavor::Regex || FLAVOR != ELexerFlavor::Pure) {
            return;
        }

        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        auto reference = MakeLexer(Lexers, ANSI, /* antlr4 = */ true, ELexerFlavor::Pure);

        SetRandomSeed(100);
        for (size_t i = 0; i < 128; ++i) {
            auto input = RandomMultilineCommentLikeText(/* maxSize = */ 16);
            TString actual = Tokenized(lexer, input);
            TString expected = Tokenized(reference, input);

            UNIT_ASSERT_VALUES_EQUAL_C(actual, expected, "Input: " << input);
        }
    }

    Y_UNIT_TEST_ON_EACH_LEXER(SimpleQuery) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "select 1", "SELECT(select) WS( ) DIGITS(1) EOF");
        UNIT_ASSERT_TOKENIZED(lexer, "SELect 1", "SELECT(SELect) WS( ) DIGITS(1) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(ComplexQuery) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);

        TString query =
            "SELECT\n"
            "  123467,\n"
            "  \"Hello, {name}!\",\n"
            "  (1 + (5U * 1 / 0)),\n"
            "  MIN(identifier),\n"
            "  Bool(field),\n"
            "  Math::Sin(var)\n"
            "FROM `local/test/space/table`\n"
            "JOIN test;";

        TString expected =
            "SELECT WS(\n) "
            "WS( ) WS( ) DIGITS(123467) COMMA(,) WS(\n) "
            "WS( ) WS( ) STRING_VALUE(\"Hello, {name}!\") COMMA(,) WS(\n) "
            "WS( ) WS( ) LPAREN(() DIGITS(1) WS( ) PLUS(+) WS( ) LPAREN(() INTEGER_VALUE(5U) WS( ) "
            "ASTERISK(*) WS( ) DIGITS(1) WS( ) SLASH(/) WS( ) DIGITS(0) RPAREN()) "
            "RPAREN()) COMMA(,) WS(\n) "
            "WS( ) WS( ) ID_PLAIN(MIN) LPAREN(() ID_PLAIN(identifier) RPAREN()) COMMA(,) WS(\n) "
            "WS( ) WS( ) ID_PLAIN(Bool) LPAREN(() ID_PLAIN(field) RPAREN()) COMMA(,) WS(\n) "
            "WS( ) WS( ) ID_PLAIN(Math) NAMESPACE(::) ID_PLAIN(Sin) LPAREN(() ID_PLAIN(var) RPAREN()) WS(\n) "
            "FROM WS( ) ID_QUOTED(`local/test/space/table`) WS(\n) "
            "JOIN WS( ) ID_PLAIN(test) SEMICOLON(;) EOF";

        UNIT_ASSERT_TOKENIZED(lexer, query, expected);
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Examples) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(
            lexer,
            R"(
SELECT
 YQL::@@(Uint32 '100500)@@,
 YQL::@@(String '[WAT])@@
;)",
            "WS(\n) "
            "SELECT WS(\n) WS( ) ID_PLAIN(YQL) NAMESPACE(::) STRING_VALUE(@@(Uint32 '100500)@@) COMMA(,) WS(\n) "
            "WS( ) ID_PLAIN(YQL) NAMESPACE(::) STRING_VALUE(@@(String '[WAT])@@) WS(\n) "
            "SEMICOLON(;) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(AsciiEscape) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        UNIT_ASSERT_TOKENIZED(lexer, "\0", "EOF");           // Null character
        UNIT_ASSERT_TOKENIZED(lexer, "\t", "WS(\t) EOF");    // Horizontal Tab
        UNIT_ASSERT_TOKENIZED(lexer, "\n", "WS(\n) EOF");    // Line Feed
        UNIT_ASSERT_TOKENIZED(lexer, "\v", "[INVALID] EOF"); // Vertical Tabulation
        UNIT_ASSERT_TOKENIZED(lexer, "\f", "WS(\x0C) EOF");  // Form Feed
        UNIT_ASSERT_TOKENIZED(lexer, "\r", "WS(\r) EOF");    // Carriage Return
        UNIT_ASSERT_TOKENIZED(lexer, "\r\n", "WS(\r) WS(\n) EOF");
    }

    Y_UNIT_TEST_ON_EACH_LEXER(AsciiEscapeCanon) {
        static THashMap<char, TString> canon;

        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);

        for (char c = 0; c < std::numeric_limits<char>::max(); ++c) {
            TString input;
            input += c;

            TString& expected = canon[c];
            if (expected.empty()) {
                expected = Tokenized(lexer, input);
            }

            UNIT_ASSERT_TOKENIZED(lexer, input, expected);
        }
    }

    Y_UNIT_TEST_ON_EACH_LEXER(Utf8BOM) {
        auto lexer = MakeLexer(Lexers, ANSI, ANTLR4, FLAVOR);
        if (ANTLR4 || FLAVOR == ELexerFlavor::Regex) {
            UNIT_ASSERT_TOKENIZED(lexer, "\xEF\xBB\xBF 1", "WS( ) DIGITS(1) EOF");
            UNIT_ASSERT_TOKENIZED(lexer, "\xEF\xBB\xBF \xEF\xBB\xBF", "[INVALID] WS( ) EOF");
        } else {
            UNIT_ASSERT_TOKENIZED(lexer, "\xEF\xBB\xBF 1", "[INVALID] WS( ) DIGITS(1) EOF");
        }
    }

} // Y_UNIT_TEST_SUITE(SQLv1Lexer)
