#include <ydb/library/yql/parser/lexer_common/lexer.h>

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/sql/v1/lexer/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslation;
using namespace NSQLTranslationV1;

std::pair<TParsedTokenList, NYql::TIssues> Tokenize(ILexer::TPtr& lexer, TString queryUtf8) {
    TParsedTokenList tokens;
    NYql::TIssues issues;
    Tokenize(*lexer, queryUtf8, "Query", tokens, issues, SQL_MAX_PARSER_ERRORS);
    return {tokens, issues};
}

TVector<TString> GetIssueMessages(ILexer::TPtr& lexer, TString queryUtf8) {
    TVector<TString> messages;
    for (const auto& issue : Tokenize(lexer, queryUtf8).second) {
        messages.emplace_back(issue.ToString(/* oneLine = */ true));
    }
    return messages;
}

void AssertEquivialent(const TParsedToken& lhs, const TParsedToken& rhs) {
    if (lhs.Name == "EOF" && rhs.Name == "EOF") {
        return;
    }

    UNIT_ASSERT_EQUAL(lhs.Name, rhs.Name);
    UNIT_ASSERT_EQUAL(lhs.Content, rhs.Content);
    UNIT_ASSERT_EQUAL(lhs.Line, rhs.Line);
    UNIT_ASSERT_EQUAL(lhs.RawPos, rhs.RawPos);
}

void AssertEquivialent(const TParsedTokenList& lhs, const TParsedTokenList& rhs) {
    UNIT_ASSERT_EQUAL(lhs.size(), rhs.size());
    for (size_t i = 0; i < lhs.size(); ++i) {
        AssertEquivialent(lhs.at(i), rhs.at(i));
    }
}

Y_UNIT_TEST_SUITE(SQLv1Lexer) {
    Y_UNIT_TEST(AntlrVersionIndependent) {
        const TVector<TString> queriesUtf8 = {
            "",
            "   ",
            "SELECT",
            "SEL", // identifier
            "SELECT FROM test",
            "SELECT * FROM",
            "   SELECT * FROM ",
            "SELECT \"😊\" FROM ydb",
            (
                "SELECT \"😊 Hello, друзья\", count, name\n"
                "FROM ydb -- главная таблица 数据库 \n"
                "WHERE count < 6\n"
                "  AND name = \"可靠性\"\n"
                "  AND count > 12"
            ),
        };

        auto lexer3 = MakeLexer(/* ansi = */ false, /* antlr4 = */ false);
        auto lexer4 = MakeLexer(/* ansi = */ false, /* antlr4 = */ true);

        for (const auto& query : queriesUtf8) {
            auto [tokens3, issues3] = Tokenize(lexer3, query);
            auto [tokens4, issues4] = Tokenize(lexer4, query);
            AssertEquivialent(tokens3, tokens4);
            UNIT_ASSERT(issues3.Empty());
            UNIT_ASSERT(issues4.Empty());
        }
    }

    Y_UNIT_TEST(IssuesCollected) {
        auto lexer3 = MakeLexer(/* ansi = */ false, /* antlr4 = */ false);
        auto lexer4 = MakeLexer(/* ansi = */ false, /* antlr4 = */ true);

        const TVector<TString> queriesUtf8 = {
            "😊",
            "select \"aaaa",
            "\"\\\"",
            "😊 SELECT * FR",
        };

        for (const auto& query : queriesUtf8) {
            auto issues3 = GetIssueMessages(lexer3, query);
            auto issues4 = GetIssueMessages(lexer4, query);

            UNIT_ASSERT(!issues3.empty());
            UNIT_ASSERT(!issues4.empty());
        }        
    }

    Y_UNIT_TEST(IssueMessagesAntlr3) {
        auto lexer3 = MakeLexer(/* ansi = */ false, /* antlr4 = */ false);

        auto actual = GetIssueMessages(lexer3, "😊 SELECT * FR");

        TVector<TString> expected = {
            "<main>:1:0: Error: Unexpected character '\xF0\x9F\x98\x8A' (Unicode character <128522>) : cannot match to any predicted input...",
            "<main>:1:1: Error: Unexpected character : cannot match to any predicted input...",
            "<main>:1:2: Error: Unexpected character : cannot match to any predicted input...", 
            "<main>:1:3: Error: Unexpected character : cannot match to any predicted input...",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    Y_UNIT_TEST(IssueMessagesAntlr4) {
        auto lexer4 = MakeLexer(/* ansi = */ false, /* antlr4 = */ true);

        auto actual = GetIssueMessages(lexer4, "😊 SELECT * FR");

        TVector<TString> expected = {
            "<main>:1:0: Error: token recognition error at: '\xF0\x9F\x98\x8A'",
        };

        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }
}
