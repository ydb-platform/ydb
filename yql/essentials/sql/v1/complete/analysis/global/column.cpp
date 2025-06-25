#include "column.h"

#include "base_visitor.h"
#include "narrowing_visitor.h"

#include <yql/essentials/sql/v1/complete/syntax/format.h>

namespace NSQLComplete {

    namespace {

        // TODO: Extract it to `identifier.cpp` and reuse it also at `use.cpp`
        //       and replace `GetId` at `parse_tree.cpp`.
        class TIdentifierVisitor: public TSQLv1BaseVisitor {
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

        class TInferenceVisitor: public TSQLv1BaseVisitor {
        public:
            std::any visitJoin_source(SQLv1::Join_sourceContext* ctx) override {
                return AccumulatingVisit(ctx->flatten_source());
            }

            std::any visitNamed_single_source(SQLv1::Named_single_sourceContext* ctx) override {
                SQLv1::Single_sourceContext* singleSource = ctx->single_source();
                if (singleSource == nullptr) {
                    return {};
                }

                std::any any = visit(singleSource);
                if (!any.has_value()) {
                    return {};
                }
                TColumnContext context = std::move(std::any_cast<TColumnContext>(any));

                TMaybe<TString> alias = GetAlias(ctx);
                if (alias.Empty()) {
                    return context;
                }

                return std::move(context).Renamed(*alias);
            }

            std::any visitTable_ref(SQLv1::Table_refContext* ctx) override {
                TString cluster = GetId(ctx->cluster_expr()).GetOrElse("");

                TMaybe<TString> path = GetId(ctx->table_key());
                if (path.Empty()) {
                    return {};
                }

                return TColumnContext{
                    .Tables = {
                        TTableId{std::move(cluster), std::move(*path)},
                    },
                };
            }

            std::any visitSelect_stmt(SQLv1::Select_stmtContext* ctx) override {
                return AccumulatingVisit(ctx->select_kind_parenthesis());
            }

            std::any visitSelect_core(SQLv1::Select_coreContext* ctx) override {
                TMaybe<TColumnContext> head = Head(ctx);
                if (head.Empty() || head->IsAsterisk()) {
                    return AccumulatingVisit(ctx->join_source());
                }

                return AccumulatingVisit(ctx->result_column());
            }

            std::any visitResult_column(SQLv1::Result_columnContext* ctx) override {
                if (ctx->TOKEN_ASTERISK() != nullptr) {
                    return TColumnContext::Asterisk();
                }

                TMaybe<TString> column = GetAlias(ctx);
                if (column.Defined()) {
                    return TColumnContext{
                        .Columns = {
                            {.Name = std::move(*column)},
                        }};
                }

                return {};
            }

        private:
            TMaybe<TString> GetAlias(SQLv1::Named_single_sourceContext* ctx) const {
                TMaybe<TString> alias = GetId(ctx->an_id());
                alias = alias.Defined() ? alias : GetId(ctx->an_id_as_compat());
                return alias;
            }

            TMaybe<TString> GetAlias(SQLv1::Result_columnContext* ctx) const {
                antlr4::ParserRuleContext* id = nullptr;
                if (ctx->TOKEN_AS() == nullptr) {
                    id = ctx->expr();
                } else {
                    id = ctx->an_id_or_type();
                    id = id ? id : ctx->an_id_as_compat();
                }
                return GetId(id);
            }

            TMaybe<TColumnContext> Head(SQLv1::Select_coreContext* ctx) {
                SQLv1::Result_columnContext* column = ctx->result_column(0);
                if (column == nullptr) {
                    return Nothing();
                }

                std::any any = visit(column);
                if (!any.has_value()) {
                    return Nothing();
                }

                return std::any_cast<TColumnContext>(any);
            }

            template <std::derived_from<antlr4::ParserRuleContext> T>
            TColumnContext AccumulatingVisit(std::vector<T*> contexts) {
                return Accumulate(
                    contexts,
                    TColumnContext(),
                    [this](TColumnContext&& acc, T* ctx) {
                        std::any any = visit(ctx);
                        if (!any.has_value()) {
                            return acc;
                        }

                        TColumnContext child = std::move(std::any_cast<TColumnContext>(any));
                        return std::move(acc) | std::move(child);
                    });
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

                return TInferenceVisitor().visit(source);
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
