#include "lexer.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/parser/proto_ast/collect_issues/collect_issues.h>
#include <yql/essentials/parser/proto_ast/antlr3/proto_ast_antlr3.h>
#include <yql/essentials/parser/proto_ast/antlr4/proto_ast_antlr4.h>
#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>

#include <util/string/ascii.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

namespace NALPDefault {
extern ANTLR_UINT8 *SQLv1ParserTokenNames[];
}

namespace NALPAnsi {
extern ANTLR_UINT8 *SQLv1ParserTokenNames[];
}


namespace NSQLTranslationV1 {

namespace {

#if defined(_tsan_enabled_)
TMutex SanitizerSQLTranslationMutex;
#endif

using NSQLTranslation::ILexer;

class TV1Lexer : public ILexer {
public:
    explicit TV1Lexer(bool ansi, bool antlr4)
        : Ansi(ansi), Antlr4(antlr4)
    {
    }

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) override {
        NYql::TIssues newIssues;
#if defined(_tsan_enabled_)
        TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
        NSQLTranslation::TErrorCollectorOverIssues collector(newIssues, maxErrors, "");
        if (Ansi && !Antlr4) {
            NProtoAST::TLexerTokensCollector3<NALPAnsi::SQLv1Lexer> tokensCollector(query, (const char**)NALPAnsi::SQLv1ParserTokenNames, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else if (!Ansi && !Antlr4) {
            NProtoAST::TLexerTokensCollector3<NALPDefault::SQLv1Lexer> tokensCollector(query, (const char**)NALPDefault::SQLv1ParserTokenNames, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else if (Ansi && Antlr4) {
            NProtoAST::TLexerTokensCollector4<NALPAnsiAntlr4::SQLv1Antlr4Lexer> tokensCollector(query, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else {
            NProtoAST::TLexerTokensCollector4<NALPDefaultAntlr4::SQLv1Antlr4Lexer> tokensCollector(query, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        }

        issues.AddIssues(newIssues);
        return !AnyOf(newIssues.begin(), newIssues.end(), [](auto issue) { return issue.GetSeverity() == NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR; });
    }

private:
    const bool Ansi;
    const bool Antlr4;
};

} // namespace

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi, bool antlr4) {
    return NSQLTranslation::ILexer::TPtr(new TV1Lexer(ansi, antlr4));
}

bool IsProbablyKeyword(const NSQLTranslation::TParsedToken& token) {
    return AsciiEqualsIgnoreCase(token.Name, token.Content);
}

} //  namespace NSQLTranslationV1
