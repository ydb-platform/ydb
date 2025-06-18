#include "global.h"

#include "column.h"
#include "function.h"
#include "named_node.h"
#include "parse_tree.h"
#include "use.h"

#include <yql/essentials/sql/v1/complete/antlr4/pipeline.h>
#include <yql/essentials/sql/v1/complete/syntax/ansi.h>

#include <library/cpp/iterator/functools.h>

#include <util/string/join.h>

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

        TGlobalContext Analyze(TCompletionInput input, TEnvironment env) override {
            SQLv1::Sql_queryContext* sqlQuery = Parse(input.Text);
            Y_ENSURE(sqlQuery);

            TGlobalContext ctx;

            // TODO(YQL-19747): Add ~ParseContext(Tokens, ParseTree, CursorPosition)
            ctx.Use = FindUseStatement(sqlQuery, &Tokens_, input.CursorPosition, env);
            ctx.Names = CollectNamedNodes(sqlQuery, &Tokens_, input.CursorPosition);
            ctx.EnclosingFunction = EnclosingFunction(sqlQuery, &Tokens_, input.CursorPosition);
            ctx.Column = InferColumnContext(sqlQuery, &Tokens_, input.CursorPosition);

            if (ctx.Use && ctx.Column) {
                EnrichTableClusters(*ctx.Column, *ctx.Use);
            }

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

        void EnrichTableClusters(TColumnContext& column, const TUseContext& use) {
            for (auto& table : column.Tables) {
                if (table.Cluster.empty()) {
                    table.Cluster = use.Cluster;
                }
            }
        }

        antlr4::ANTLRInputStream Chars_;
        G::TLexer Lexer_;
        antlr4::CommonTokenStream Tokens_;
        TDefaultYQLGrammar::TParser Parser_;
    };

    class TGlobalAnalysis: public IGlobalAnalysis {
    public:
        TGlobalContext Analyze(TCompletionInput input, TEnvironment env) override {
            const bool isAnsiLexer = IsAnsiQuery(TString(input.Text));
            return GetSpecialized(isAnsiLexer).Analyze(std::move(input), std::move(env));
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

template <>
void Out<NSQLComplete::TColumnContext>(IOutputStream& out, const NSQLComplete::TColumnContext& value) {
    out << "TColumnContext { ";
    out << "Tables: " << JoinSeq(", ", value.Tables);
    out << " }";
}
