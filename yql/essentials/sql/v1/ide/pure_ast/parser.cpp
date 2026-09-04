#include "parser.h"

#include "parse_tree.h"

#include <yql/essentials/parser/common/antlr4/depth_limiting_listener.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <util/system/yassert.h>
#include <util/charset/utf8.h>
#include <util/generic/maybe.h>
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
class TParseTree: public IParseTree {
    static constexpr size_t MaxParseTreeDepth = 4096;

    using TLexer = std::conditional_t<
        IsAnsiLexer,
        NALAAnsiAntlr4::SQLv1Antlr4Lexer,
        NALADefaultAntlr4::SQLv1Antlr4Lexer>;

public:
    explicit TParseTree(TStringBuf text)
        : Text_(text)
        , Chars_(Text_)
        , Lexer_(&Chars_)
        , Tokens_(&Lexer_)
        , DepthLimiter_(/*maxDepth=*/MaxParseTreeDepth)
        , Parser_(&Tokens_)
    {
        Lexer_.removeErrorListeners();
        Parser_.removeErrorListeners();
        Parser_.setErrorHandler(std::make_shared<TErrorStrategy>());
        Parser_.addParseListener(&DepthLimiter_);

        SqlQuery_ = Parser_.sql_query();
        Y_ENSURE(SqlQuery_);

#ifdef YQL_DEBUG_GLOBAL_ANALYSIS
        Cerr << DebugDisplay(Tokens_) << Endl;
        Cerr << DebugDisplay(SqlQuery_) << Endl;
#endif
    }

    TStringBuf Text() const override {
        return Text_;
    }

    const antlr4::CommonTokenStream& Tokens() const override {
        return Tokens_;
    }

    const SQLv1& Parser() const override {
        return Parser_;
    }

    SQLv1::Sql_queryContext* Root() override {
        return SqlQuery_;
    }

private:
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

    TStringBuf Text_;
    antlr4::ANTLRInputStream Chars_;
    TLexer Lexer_;
    antlr4::CommonTokenStream Tokens_;
    NAntlrAST::TDepthLimitingListener DepthLimiter_;
    SQLv1 Parser_;
    SQLv1::Sql_queryContext* SqlQuery_;
};

template <bool IsAnsiLexer>
class TParser: public IParser {
public:
    IParseTree::TPtr Parse(TStringBuf text) const override {
        return new TParseTree<IsAnsiLexer>(text);
    }
};

} // namespace

IParser::TPtr MakeParser(bool isAnsiLexer) {
    if (isAnsiLexer) {
        return MakeHolder<TParser<true>>();
    }
    return MakeHolder<TParser<false>>();
}

void ClearParserCache() {
    NALADefaultAntlr4::SQLv1Antlr4Lexer(nullptr)
        .getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();

    NALAAnsiAntlr4::SQLv1Antlr4Lexer(nullptr)
        .getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();

    NALADefaultAntlr4::SQLv1Antlr4Parser(nullptr)
        .getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();

    NALAAnsiAntlr4::SQLv1Antlr4Parser(nullptr)
        .getInterpreter<antlr4::atn::ATNSimulator>()
        ->clearDFA();
}

} // namespace NSQLPureAST
