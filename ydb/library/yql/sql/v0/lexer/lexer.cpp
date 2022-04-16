#include "lexer.h"

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/parser/proto_ast/collect_issues/collect_issues.h>
#include <ydb/library/yql/parser/proto_ast/gen/v0/SQLLexer.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

namespace NALP {
extern ANTLR_UINT8* SQLParserTokenNames[];
}

namespace NSQLTranslationV0 {

namespace {

#if defined(_tsan_enabled_)
TMutex SanitizerSQLTranslationMutex;
#endif

using NSQLTranslation::ILexer;

class TV0Lexer : public ILexer {
public:
    TV0Lexer() = default;

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) override {
        NYql::TIssues newIssues;
#if defined(_tsan_enabled_)
        TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
        NSQLTranslation::TErrorCollectorOverIssues collector(newIssues, maxErrors, "");
        NProtoAST::TLexerTokensCollector<NALP::SQLLexer> tokensCollector(query, (const char**)NALP::SQLParserTokenNames, queryName);
        tokensCollector.CollectTokens(collector, onNextToken);
        issues.AddIssues(newIssues);
        return !AnyOf(newIssues.begin(), newIssues.end(), [](auto issue) { return issue.GetSeverity() == NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR; });
    }
};

} // namespace

NSQLTranslation::ILexer::TPtr MakeLexer() {
    return NSQLTranslation::ILexer::TPtr(new TV0Lexer);
}

} //  namespace NSQLTranslationV1
