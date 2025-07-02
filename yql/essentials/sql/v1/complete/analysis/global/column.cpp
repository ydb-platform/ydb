#include "column.h"

#include "base_visitor.h"
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

            std::any visitUnary_casual_subexpr(SQLv1::Unary_casual_subexprContext* ctx) override {
                std::any prev;
                if (auto* x = ctx->id_expr()) {
                    prev = visit(x);
                } else if (auto* x = ctx->atom_expr()) {
                    prev = visit(x);
                }

                std::any next = visit(ctx->unary_subexpr_suffix());
                if (!next.has_value()) {
                    return prev;
                }

                return {};
            }

            std::any visitTerminal(antlr4::tree::TerminalNode* node) override {
                switch (node->getSymbol()->getType()) {
                    case SQLv1::TOKEN_ID_QUOTED:
                        return Unquoted(GetText(node));
                    case SQLv1::TOKEN_ID_PLAIN:
                        return GetText(node);
                }
                return {};
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
                TColumnContext context = AccumulatingVisit(ctx->result_column());
                auto asterisks = std::ranges::partition(context.Columns, [](const TColumnId& x) {
                    return x.Name != "*";
                });

                if (std::ranges::empty(asterisks)) {
                    return context;
                }

                TColumnContext source = AccumulatingVisit(ctx->join_source());

                TColumnContext imported;
                for (const TColumnId& qualified : asterisks) {
                    TMaybe<TStringBuf> alias = qualified.TableAlias;
                    if (alias->Empty()) {
                        alias = Nothing();
                    }

                    auto aliased = source.ExtractAliased(alias);
                    imported = std::move(imported) | std::move(aliased);
                }

                context.Columns.erase(asterisks.begin(), asterisks.end());
                imported = std::move(imported).Renamed("");
                return std::move(context) | std::move(imported);
            }

            std::any visitResult_column(SQLv1::Result_columnContext* ctx) override {
                if (ctx->opt_id_prefix() == nullptr && ctx->TOKEN_ASTERISK() != nullptr) {
                    return TColumnContext::Asterisk();
                }

                if (ctx->opt_id_prefix() != nullptr && ctx->TOKEN_ASTERISK() != nullptr) {
                    TMaybe<TString> alias = GetId(ctx->opt_id_prefix()->an_id());
                    if (alias.Empty()) {
                        return TColumnContext::Asterisk();
                    }

                    return TColumnContext{
                        .Columns = {
                            {.TableAlias = std::move(*alias), .Name = "*"},
                        },
                    };
                }

                TMaybe<TString> column = GetAlias(ctx);
                if (column.Defined()) {
                    return TColumnContext{
                        .Columns = {
                            {.Name = std::move(*column)},
                        },
                    };
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
            TVisitor(const TParsedInput& input)
                : TSQLv1NarrowingVisitor(input)
            {
            }

            std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
                if (IsEnclosing(ctx)) {
                    return visitChildren(ctx);
                }
                return {};
            }

            std::any visitSelect_core(SQLv1::Select_coreContext* ctx) override {
                antlr4::ParserRuleContext* source = nullptr;
                if (IsEnclosingStrict(ctx->expr(0)) ||
                    IsEnclosingStrict(ctx->group_by_clause()) ||
                    IsEnclosingStrict(ctx->expr(1)) ||
                    IsEnclosingStrict(ctx->window_clause()) ||
                    IsEnclosingStrict(ctx->ext_order_by_clause())) {
                    source = ctx;
                } else {
                    source = ctx->join_source(0);
                    source = source == nullptr ? ctx->join_source(1) : source;
                }

                if (source == nullptr) {
                    return {};
                }

                return TInferenceVisitor().visit(source);
            }

        private:
            bool IsEnclosingStrict(antlr4::ParserRuleContext* ctx) const {
                return ctx != nullptr && IsEnclosing(ctx);
            }
        };

    } // namespace

    TMaybe<TColumnContext> InferColumnContext(TParsedInput input) {
        // TODO: add utility `auto ToMaybe<T>(std::any any) -> TMaybe<T>`
        std::any result = TVisitor(input).visit(input.SqlQuery);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<TColumnContext>(result);
    }

} // namespace NSQLComplete
