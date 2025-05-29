#include "global.h"

#include "parse_tree.h"
#include "use.h"

#include <yql/essentials/sql/v1/complete/antlr4/pipeline.h>
#include <yql/essentials/sql/v1/complete/syntax/ansi.h>

namespace NSQLComplete {

    class TErrorStrategy: public antlr4::DefaultErrorStrategy {
    public:
        antlr4::Token* singleTokenDeletion(antlr4::Parser* /* recognizer */) override {
            return nullptr;
        }
    };

    template <bool IsAnsiLexer>
    class TSpecializedGlobalAnalysis: public IGlobalAnalysis {
    public:
        using TDefaultYQLGrammar = TAntlrGrammar<
            NALADefaultAntlr4::SQLv1Antlr4Lexer,
            NALADefaultAntlr4::SQLv1Antlr4Parser>;

        using TAnsiYQLGrammar = TAntlrGrammar<
            NALAAnsiAntlr4::SQLv1Antlr4Lexer,
            NALAAnsiAntlr4::SQLv1Antlr4Parser>;

        using G = std::conditional_t<
            IsAnsiLexer,
            TAnsiYQLGrammar,
            TDefaultYQLGrammar>;

        TSpecializedGlobalAnalysis()
            : Chars_()
            , Lexer_(&Chars_)
            , Tokens_(&Lexer_)
            , Parser_(&Tokens_)
        {
            Lexer_.removeErrorListeners();
            Parser_.removeErrorListeners();
            Parser_.setErrorHandler(std::make_shared<TErrorStrategy>());
        }

        TGlobalContext Analyze(TCompletionInput input) override {
            SQLv1::Sql_queryContext* sqlQuery = Parse(input.Text);
            Y_ENSURE(sqlQuery);

            TGlobalContext ctx;

            ctx.Use = FindUseStatement(sqlQuery, &Tokens_, input.CursorPosition);

            return ctx;
        }

    private:
        SQLv1::Sql_queryContext* Parse(TStringBuf input) {
            Chars_.load(input.Data(), input.Size(), /* lenient = */ false);
            Lexer_.reset();
            Tokens_.setTokenSource(&Lexer_);
            Parser_.reset();
            return Parser_.sql_query();
        }

        antlr4::ANTLRInputStream Chars_;
        G::TLexer Lexer_;
        antlr4::CommonTokenStream Tokens_;
        TDefaultYQLGrammar::TParser Parser_;
    };

    class TGlobalAnalysis: public IGlobalAnalysis {
    public:
        TGlobalContext Analyze(TCompletionInput input) override {
            const bool isAnsiLexer = IsAnsiQuery(TString(input.Text));
            return GetSpecialized(isAnsiLexer).Analyze(std::move(input));
        }

    private:
        IGlobalAnalysis& GetSpecialized(bool isAnsiLexer) {
            if (isAnsiLexer) {
                return AnsiAnalysis_;
            }
            return DefaultAnalysis_;
        }

        TSpecializedGlobalAnalysis</* IsAnsiLexer = */ false> DefaultAnalysis_;
        TSpecializedGlobalAnalysis</* IsAnsiLexer = */ true> AnsiAnalysis_;
    };

    IGlobalAnalysis::TPtr MakeGlobalAnalysis() {
        return MakeHolder<TGlobalAnalysis>();
    }

} // namespace NSQLComplete
