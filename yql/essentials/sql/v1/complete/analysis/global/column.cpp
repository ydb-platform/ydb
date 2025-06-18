#include "column.h"

#include "narrowing_visitor.h"

#include <yql/essentials/sql/v1/complete/syntax/format.h>

namespace NSQLComplete {

    namespace {

        // TODO: Extract it to `identifier.cpp` and reuse it also at `use.cpp`
        //       and replace `GetId` at `parse_tree.cpp`.
        class TIdentifierVisitor: public SQLv1Antlr4BaseVisitor {
        public:
            std::any visitCluster_expr(SQLv1::Cluster_exprContext* ctx) override {
                if (auto* x = ctx->pure_column_or_named()) {
                    return visit(x);
                }
                return {};
            }

            std::any visitTable_key(SQLv1::Table_keyContext* ctx) override {
                if (auto* x = ctx->id_table_or_type()) {
                    return visit(x);
                }
                return {};
            }

            std::any visitTerminal(antlr4::tree::TerminalNode* node) override {
                TString text = GetText(node);
                switch (node->getSymbol()->getType()) {
                    case SQLv1::TOKEN_ID_QUOTED: {
                        text = Unquoted(std::move(text));
                    } break;
                }
                return text;
            }

        private:
            TString GetText(antlr4::tree::ParseTree* tree) const {
                return TString(tree->getText());
            }
        };

        TMaybe<TString> GetId(antlr4::ParserRuleContext* ctx) {
            if (ctx == nullptr) {
                return Nothing();
            }

            std::any result = TIdentifierVisitor().visit(ctx);
            if (!result.has_value()) {
                return Nothing();
            }
            return std::any_cast<TString>(result);
        }

        class TInferenceVisitor: public SQLv1Antlr4BaseVisitor {
        public:
            std::any visitTable_ref(SQLv1::Table_refContext* ctx) override {
                TString cluster = GetId(ctx->cluster_expr()).GetOrElse("");

                TMaybe<TString> path = GetId(ctx->table_key());
                if (path.Empty()) {
                    return {};
                }

                return TColumnContext{
                    .Tables = {
                        {.Cluster = std::move(cluster), .Path = std::move(*path)},
                    },
                };
            }
        };

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(antlr4::TokenStream* tokens, size_t cursorPosition)
                : TSQLv1NarrowingVisitor(tokens, cursorPosition)
            {
            }

            std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
                if (IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitSelect_core(SQLv1::Select_coreContext* ctx) override {
                SQLv1::Join_sourceContext* source = ctx->join_source(0);
                if (source == nullptr) {
                    source = ctx->join_source(1);
                }
                if (source == nullptr) {
                    return {};
                }

                return TInferenceVisitor().visit(ctx);
            }
        };

    } // namespace

    TMaybe<TColumnContext> InferColumnContext(
        SQLv1::Sql_queryContext* ctx,
        antlr4::TokenStream* tokens,
        size_t cursorPosition) {
        // TODO: add utility `auto ToMaybe<T>(std::any any) -> TMaybe<T>`
        std::any result = TVisitor(tokens, cursorPosition).visit(ctx);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<TColumnContext>(result);
    }

} // namespace NSQLComplete
