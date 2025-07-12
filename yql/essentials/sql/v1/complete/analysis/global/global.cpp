#include "global.h"

#include "column.h"
#include "function.h"
#include "input.h"
#include "named_node.h"
#include "parse_tree.h"
#include "use.h"

#include <yql/essentials/sql/v1/complete/antlr4/pipeline.h>
#include <yql/essentials/sql/v1/complete/syntax/ansi.h>
#include <yql/essentials/sql/v1/complete/text/word.h>

#include <library/cpp/iterator/functools.h>

#include <util/string/join.h>

namespace NSQLComplete {

    namespace {

        template <class C>
        void Move(TColumnContext& lhs, TColumnContext& rhs, C TColumnContext::*member) {
            C& lhsM = lhs.*member;
            C& rhsM = rhs.*member;

            lhsM.reserve(lhsM.size() + rhsM.size());
            std::move(rhsM.begin(), rhsM.end(), std::back_inserter(lhsM));
            SortUnique(lhsM);
        }

    } // namespace

    bool operator<(const TColumnId& lhs, const TColumnId& rhs) {
        return std::tie(lhs.TableAlias, lhs.Name) < std::tie(rhs.TableAlias, rhs.Name);
    }

    TColumnContext TColumnContext::ExtractAliased(TMaybe<TStringBuf> alias) {
        if (alias.Empty()) {
            return *this;
        }

        auto aliasedTables = std::ranges::partition(Tables, [&](const auto& table) {
            return table.Alias != alias;
        });

        auto aliasedColumns = std::ranges::partition(Columns, [&](const auto& column) {
            return column.TableAlias != alias;
        });

        TVector<TAliased<TTableId>> tables(aliasedTables.begin(), aliasedTables.end());
        TVector<TColumnId> columns(aliasedColumns.begin(), aliasedColumns.end());

        Tables.erase(aliasedTables.begin(), aliasedTables.end());
        Columns.erase(aliasedColumns.begin(), aliasedColumns.end());

        THashMap<TString, THashSet<TString>> without;
        if (auto it = WithoutByTableAlias.find(*alias); it != WithoutByTableAlias.end()) {
            without[*alias] = std::move(it->second);
            WithoutByTableAlias.erase(it);
        }

        return {
            .Tables = std::move(tables),
            .Columns = std::move(columns),
            .WithoutByTableAlias = std::move(without),
        };
    }

    bool TColumnContext::IsAsterisk() const {
        return Columns.size() == 1 &&
               Columns[0].TableAlias.empty() &&
               Columns[0].Name == "*";
    }

    TColumnContext TColumnContext::Renamed(TStringBuf alias) && {
        for (TAliased<TTableId>& table : Tables) {
            table.Alias = alias;
        }

        for (TColumnId& column : Columns) {
            column.TableAlias = alias;
        }

        THashSet<TString>& without = WithoutByTableAlias[alias];
        for (auto& [tableAlias, excluded] : WithoutByTableAlias) {
            if (tableAlias == alias) {
                continue;
            }

            without.insert(excluded.begin(), excluded.end());
        }

        if (without.empty()) {
            WithoutByTableAlias = {};
        } else {
            WithoutByTableAlias = {{TString(alias), std::move(without)}};
        }

        return *this;
    }

    TColumnContext operator|(TColumnContext lhs, TColumnContext rhs) {
        Move(lhs, rhs, &TColumnContext::Tables);

        Move(lhs, rhs, &TColumnContext::Columns);

        for (auto& [tableAlias, excluded] : rhs.WithoutByTableAlias) {
            auto& without = lhs.WithoutByTableAlias[tableAlias];
            without.insert(excluded.begin(), excluded.end());
        }

        return lhs;
    }

    TColumnContext TColumnContext::Asterisk() {
        return {.Columns = {{.Name = "*"}}};
    }

    class TErrorStrategy: public antlr4::DefaultErrorStrategy {
    public:
        antlr4::Token* singleTokenDeletion(antlr4::Parser* /* recognizer */) override {
            return nullptr;
        }
    };

    template <bool IsAnsiLexer>
    class TSpecializedGlobalAnalysis: public IGlobalAnalysis {
    public:
        using TLexer = std::conditional_t<
            IsAnsiLexer,
            NALAAnsiAntlr4::SQLv1Antlr4Lexer,
            NALADefaultAntlr4::SQLv1Antlr4Lexer>;

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
            TString recovered;
            if (IsRecoverable(input)) {
                recovered = TString(input.Text);

                // - "_" is to parse `SELECT x._ FROM table`
                //        instead of `SELECT x.FROM table`
                recovered.insert(input.CursorPosition, "_");

                input.Text = recovered;
            }

            SQLv1::Sql_queryContext* sqlQuery = Parse(input.Text);
            Y_ENSURE(sqlQuery);

            TGlobalContext ctx;

            TParsedInput parsed = {
                .Original = input,
                .Tokens = &Tokens_,
                .Parser = &Parser_,
                .SqlQuery = sqlQuery,
            };

            ctx.Use = FindUseStatement(parsed, env);
            ctx.Names = CollectNamedNodes(parsed);
            ctx.EnclosingFunction = EnclosingFunction(parsed);
            ctx.Column = InferColumnContext(parsed);

            if (ctx.Use && ctx.Column) {
                EnrichTableClusters(*ctx.Column, *ctx.Use);
            }

            return ctx;
        }

    private:
        bool IsRecoverable(TCompletionInput input) const {
            TStringBuf s = input.Text;
            size_t i = input.CursorPosition;

            return (i < s.size() && IsWordBoundary(s[i]) || i == s.size()) &&
                   (i > 0 /*  */ && IsWordBoundary(s[i - 1]));
        }

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

        void DebugPrint(TStringBuf query, antlr4::ParserRuleContext* ctx) {
            Cerr << "= = = = = = " << Endl;
            Cerr << query << Endl;
            Cerr << ctx->toStringTree(&Parser_, true) << Endl;
        }

        antlr4::ANTLRInputStream Chars_;
        TLexer Lexer_;
        antlr4::CommonTokenStream Tokens_;
        SQLv1 Parser_;
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
void Out<NSQLComplete::TFunctionContext>(IOutputStream& out, const NSQLComplete::TFunctionContext& value) {
    out << "TFunctionContext { ";
    out << "Name: " << value.Name;
    out << ", Args: " << value.ArgumentNumber;
    out << " }";
}

template <>
void Out<NSQLComplete::TColumnContext>(IOutputStream& out, const NSQLComplete::TColumnContext& value) {
    out << "TColumnContext { ";
    out << "Tables: " << JoinSeq(", ", value.Tables);
    out << ", Columns: " << JoinSeq(", ", value.Columns);

    if (!value.WithoutByTableAlias.empty()) {
        out << ", WithoutByTableAlias: ";
        for (const auto& [tableAlias, columns] : value.WithoutByTableAlias) {
            out << tableAlias << ".[" << JoinSeq(", ", columns) << "], ";
        }
    }

    out << " }";
}
