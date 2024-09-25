#include "lexer.h"

#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslation;
using namespace NSQLTranslationV1;

std::pair<TParsedTokenList, NYql::TIssues> Tokenize(ILexer::TPtr& lexer, TString queryUtf8) {
    TParsedTokenList tokens;
    NYql::TIssues issues;
    Tokenize(*lexer, queryUtf8, "Query", tokens, issues, SQL_MAX_PARSER_ERRORS);
    return {tokens, issues};
}

NYql::TIssues GetIssues(ILexer::TPtr& lexer, TString queryUtf8) {
    return Tokenize(lexer, queryUtf8).second;
}

void AssertEquivialent(const TParsedToken& lhs, const TParsedToken& rhs) {
    if (lhs.Name == "EOF" && rhs.Name == "EOF") {
        return;
    }

    UNIT_ASSERT_EQUAL(lhs.Name, rhs.Name);
    UNIT_ASSERT_EQUAL(lhs.Content, rhs.Content);
    UNIT_ASSERT_EQUAL(lhs.Line, rhs.Line);
    UNIT_ASSERT_EQUAL(lhs.StartPos, rhs.StartPos);
    UNIT_ASSERT_EQUAL(lhs.StopPos, rhs.StopPos);
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
            UNIT_ASSERT_EQUAL(issues3.Empty(), issues4.Empty());
        }
    }

    Y_UNIT_TEST(IssuesCollected) {
        const TVector<TString> queriesUtf8 = {
            "😊",
            "SEL",
            "SELECT FROM test",
        };

        auto lexer3 = MakeLexer(/* ansi = */ false, /* antlr4 = */ false);
        auto lexer4 = MakeLexer(/* ansi = */ false, /* antlr4 = */ true);

        for (const auto& query : queriesUtf8) {
            auto issues3 = GetIssues(lexer3, query);
            auto issues4 = GetIssues(lexer4, query);

            UNIT_ASSERT_EQUAL(issues3.Empty(), issues4.Empty());
        }
    }
}
