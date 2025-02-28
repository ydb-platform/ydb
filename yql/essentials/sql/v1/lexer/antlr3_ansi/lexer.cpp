#include "lexer.h"
#include <yql/essentials/parser/proto_ast/gen/v1_ansi/SQLv1Lexer.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/parser/proto_ast/collect_issues/collect_issues.h>
#include <yql/essentials/parser/proto_ast/antlr3/proto_ast_antlr3.h>

namespace NALPAnsi {
extern ANTLR_UINT8 *SQLv1ParserTokenNames[];
}

namespace NSQLTranslationV1 {

namespace {

class TLexer: public NSQLTranslation::ILexer {
public:
    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) final {
        NYql::TIssues newIssues;
        NSQLTranslation::TErrorCollectorOverIssues collector(newIssues, maxErrors, queryName);
        NProtoAST::TLexerTokensCollector3<NALPAnsi::SQLv1Lexer> tokensCollector(query, (const char**)NALPAnsi::SQLv1ParserTokenNames, queryName);
        tokensCollector.CollectTokens(collector, onNextToken);
        issues.AddIssues(newIssues);
        return !AnyOf(newIssues.begin(), newIssues.end(), [](auto issue) { return issue.GetSeverity() == NYql::ESeverity::TSeverityIds_ESeverityId_S_ERROR; });
    }
};

class TFactory: public NSQLTranslation::ILexerFactory {
public:
    THolder<NSQLTranslation::ILexer> MakeLexer() const final {
        return MakeHolder<TLexer>();
    }
};

}

NSQLTranslation::TLexerFactoryPtr MakeAntlr3AnsiLexerFactory() {
    return MakeIntrusive<TFactory>();
}

}
