#include "lexer.h"

#include <yql/essentials/parser/common/issue.h>
#include <yql/essentials/parser/common/antlr4/lexer_tokens_collector.h>

#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>

#include <yql/essentials/public/issue/yql_issue.h>

namespace NSQLTranslationV1 {

    namespace {

        class TLexer: public NSQLTranslation::ILexer {
        public:
            bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) final {
                NYql::TIssues newIssues;
                NSQLTranslation::TErrorCollectorOverIssues collector(newIssues, maxErrors, queryName);
                NAST::TLexerTokensCollector4<NALADefaultAntlr4::SQLv1Antlr4Lexer> tokensCollector(query, queryName);
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

    } // namespace

    NSQLTranslation::TLexerFactoryPtr MakeAntlr4PureLexerFactory() {
        return MakeIntrusive<TFactory>();
    }

} // namespace NSQLTranslationV1
