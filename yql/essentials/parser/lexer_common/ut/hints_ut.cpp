#include <yql/essentials/parser/lexer_common/hints.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

using namespace NSQLTranslation;
using namespace NSQLTranslationV1;

TSQLHints CollectHints(const TString& query) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    bool ansi = false;
    auto lexer = MakeLexer(lexers, ansi, true);
    UNIT_ASSERT(lexer);
    TSQLHints result;
    NYql::TIssues issues;
    size_t maxErrors = 100;
    UNIT_ASSERT(CollectSqlHints(*lexer, query, "", "", result, issues, maxErrors, false));
    UNIT_ASSERT(issues.Empty());
    return result;
}

TString SerializeHints(const TVector<TSQLHint>& hints) {
    return JoinSeq(",", hints);
}

Y_UNIT_TEST_SUITE(TLexerHintsTests) {
    Y_UNIT_TEST(Basic) {
        TString query = "/*+ some() */ SELECT /*+ foo(one) */ --+ bar(two)";
        auto hintsWithPos = CollectHints(query);
        UNIT_ASSERT(hintsWithPos.size() == 1);
        NYql::TPosition pos = hintsWithPos.begin()->first;
        TVector<TSQLHint> hints = hintsWithPos.begin()->second;

        UNIT_ASSERT_EQUAL(pos.Row, 1);
        UNIT_ASSERT_EQUAL(pos.Column, 15);

        TStringBuf expected = R"raw("foo":{"one"},"bar":{"two"})raw";
        UNIT_ASSERT_NO_DIFF(SerializeHints(hints), expected);
    }
}
