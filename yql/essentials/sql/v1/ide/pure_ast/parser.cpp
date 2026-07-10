#include "parser.h"

#include "parse_tree.h"

#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>

#include <util/system/yassert.h>
#include <util/charset/utf8.h>
#include <util/string/builder.h>

namespace NSQLPureAST {

namespace {

class TErrorStrategy: public antlr4::DefaultErrorStrategy {
public:
    antlr4::Token* singleTokenDeletion(antlr4::Parser* /* recognizer */) override {
        return nullptr;
    }
};

template <bool IsAnsiLexer>
class TParser: public IParser {
public:
    using TLexer = std::conditional_t<
        IsAnsiLexer,
        NALAAnsiAntlr4::SQLv1Antlr4Lexer,
        NALADefaultAntlr4::SQLv1Antlr4Lexer>;

    TParser()
        : Chars_()
        , Lexer_(&Chars_)
        , Tokens_(&Lexer_)
        , Parser_(&Tokens_)
    {
        Lexer_.removeErrorListeners();
        Parser_.removeErrorListeners();
        Parser_.setErrorHandler(std::make_shared<TErrorStrategy>());
    }

    TParseTree Parse(TStringBuf text) override {
        SQLv1::Sql_queryContext* sqlQuery = ParseText(text);
        Y_ENSURE(sqlQuery);

#ifdef YQL_DEBUG_GLOBAL_ANALYSIS
        Cerr << DebugDisplay(Tokens_) << Endl;
        Cerr << DebugDisplay(sqlQuery) << Endl;
#endif

        return {
            .Text = text,
            .Tokens = &Tokens_,
            .Parser = &Parser_,
            .SqlQuery = sqlQuery,
        };
    }

private:
    SQLv1::Sql_queryContext* ParseText(TStringBuf text) {
        Chars_.load(text.Data(), text.Size(), /* lenient = */ false);
        Lexer_.reset();
        Tokens_.setTokenSource(&Lexer_);
        Parser_.reset();
        return Parser_.sql_query();
    }

    TString DebugDisplay(antlr4::CommonTokenStream& tokens) {
        TStringBuilder sb;
        for (size_t i = 0; i < tokens.size(); ++i) {
            sb << DebugDisplay(tokens.get(i)) << '\n';
        }
        return sb;
    }

    TString DebugDisplay(const antlr4::Token* token) {
        return TStringBuilder()
               << token->getStartIndex()
               << "\t"
               << token->getStopIndex()
               << "\t"
               << Parser_.getVocabulary().getSymbolicName(token->getType());
    }

    TString DebugDisplay(antlr4::tree::ParseTree* tree) {
        return tree->toStringTree(&Parser_, /*pretty=*/true);
    }

    antlr4::ANTLRInputStream Chars_;
    TLexer Lexer_;
    antlr4::CommonTokenStream Tokens_;
    SQLv1 Parser_;
};

} // namespace

IParser::TPtr MakeParser(bool isAnsiLexer) {
    if (isAnsiLexer) {
        return MakeHolder<TParser<true>>();
    }
    return MakeHolder<TParser<false>>();
}

} // namespace NSQLPureAST
