#include "kqp_new_parser.h"
#include "kqp_ast_builder.h"

#include <ydb/core/kqp/host/new_parser/grammar/default/SQLv1Antlr4Lexer.h>
#include <ydb/core/kqp/host/new_parser/grammar/default/SQLv1Antlr4Parser.h>
#include <ydb/core/kqp/host/new_parser/grammar/ansi/SQLv1Antlr4Lexer.h>
#include <ydb/core/kqp/host/new_parser/grammar/ansi/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/common/antlr4/depth_limiting_listener.h>
#include <yql/essentials/parser/common/antlr4/error_listener.h>
#include <yql/essentials/parser/common/issue.h>

#include <util/generic/scope.h>

namespace NKikimr::NKqp {

namespace {

template <typename TLexer, typename TParser, bool AnsiLexer>
class TParseTree {
public:
    explicit TParseTree(const TString& queryText)
        : Text(queryText)
        , Input(Text)
        , Lexer(&Input)
        , Tokens(&Lexer)
        , Parser(&Tokens)
    {}

    bool Parse(const NSQLTranslation::TTranslationSettings& settings, bool isAmbiguityError, NYql::TIssues& issues) {
        NSQLTranslation::TErrorCollectorOverIssues collector(issues, settings.MaxErrors, settings.File);
        bool hasErrors = false;
        antlr4::YqlErrorListener errorListener(&collector, &hasErrors, isAmbiguityError);
        NAntlrAST::TDepthLimitingListener depthLimiter(4096);
        Lexer.removeErrorListeners();
        Lexer.addErrorListener(&errorListener);
        Parser.removeErrorListeners();
        Parser.addErrorListener(&errorListener);
        Parser.addParseListener(&depthLimiter);
        Parser.setBuildParseTree(true);
        Y_DEFER {
            Lexer.removeErrorListeners();
            Parser.removeErrorListeners();
            Parser.removeParseListener(&depthLimiter);
        };

        try {
            Root = Parser.sql_query();
            return !hasErrors && Root;
        } catch (const NAST::TTooManyErrors&) {
            // The collector already holds the parser diagnostics.
            return false;
        }
    }

    antlr4::ParserRuleContext* GetRoot() const {
        return Root;
    }

private:
    TString Text;
    antlr4::ANTLRInputStream Input;
    TLexer Lexer;
    antlr4::CommonTokenStream Tokens;
    TParser Parser;
    antlr4::ParserRuleContext* Root = nullptr;
};

template <typename TLexer, typename TParser, bool AnsiLexer>
std::unique_ptr<NNewYql::TScript> ParseQuery(const TString& queryText,
    const NSQLTranslation::TTranslationSettings& settings, bool isAmbiguityError, NYql::TIssues& issues)
{
    TParseTree<TLexer, TParser, AnsiLexer> tree(queryText);
    if (!tree.Parse(settings, isAmbiguityError, issues)) {
        return nullptr;
    }
    return BuildNewYqlAst(*tree.GetRoot(), AnsiLexer, settings.File);
}

} // namespace

TNewYqlParseResult ParseQueryWithNewParser(const TString& queryText,
    NSQLTranslation::TTranslationSettings settings, bool isAmbiguityError)
{
    TNewYqlParseResult result;
    if (!NSQLTranslation::ParseTranslationSettings(queryText, settings, result.Issues)) {
        return result;
    }

    if (settings.PgParser || settings.SyntaxVersion != 1 || settings.Syntax) {
        ythrow TNewYqlParserFallback() << "The new YQL parser path only supports YQL v1 syntax";
    }

    if (settings.AnsiLexer) {
        result.Ast = ParseQuery<NKqpAnsiAntlr4::SQLv1Antlr4Lexer, NKqpAnsiAntlr4::SQLv1Antlr4Parser, true>(
            queryText, settings, isAmbiguityError, result.Issues);
    } else {
        result.Ast = ParseQuery<NKqpDefaultAntlr4::SQLv1Antlr4Lexer, NKqpDefaultAntlr4::SQLv1Antlr4Parser, false>(
            queryText, settings, isAmbiguityError, result.Issues);
    }
    return result;
}

} // namespace NKikimr::NKqp
