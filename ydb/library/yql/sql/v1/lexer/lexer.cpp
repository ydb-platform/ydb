#include "lexer.h"

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/parser/proto_ast/collect_issues/collect_issues.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv4Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi/SQLv4Lexer.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif


namespace NSQLTranslationV1 {

namespace {

#if defined(_tsan_enabled_)
TMutex SanitizerSQLTranslationMutex;
#endif

using NSQLTranslation::ILexer;

class TV1Lexer : public ILexer {
public:
    explicit TV1Lexer(bool ansi)
        : Ansi(ansi)
    {
    }

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) override {
        NYql::TIssues newIssues;
#if defined(_tsan_enabled_)
        TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
        NSQLTranslation::TErrorCollectorOverIssues collector(newIssues, maxErrors, "");
        if (Ansi) {
            NProtoAST::TLexerTokensCollector<NALPAnsi::SQLv4Lexer> tokensCollector(query, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        } else {
            NProtoAST::TLexerTokensCollector<NALPDefault::SQLv4Lexer> tokensCollector(query, queryName);
            tokensCollector.CollectTokens(collector, onNextToken);
        }

        issues.AddIssues(newIssues);
        return !AnyOf(newIssues.begin(), newIssues.end(), [](auto issue) { return issue.GetSeverity() == NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR; });
    }

private:
    const bool Ansi;
};

} // namespace

NSQLTranslation::ILexer::TPtr MakeLexer(bool ansi) {
    return NSQLTranslation::ILexer::TPtr(new TV1Lexer(ansi));
}

} //  namespace NSQLTranslationV1
