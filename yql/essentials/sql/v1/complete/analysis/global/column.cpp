#include "column.h"

#include "base_visitor.h"
#include "narrowing_visitor.h"

#include <yql/essentials/sql/v1/complete/syntax/format.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>

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
                        return TString(Unquoted(GetText(node)));
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
            explicit TInferenceVisitor(THashMap<TString, SQLv1::Subselect_stmtContext*> subqueries)
                : Subqueries_(std::move(subqueries))
            {
            }

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
                if (TMaybe<TString> path = GetId(ctx->table_key())) {
                    TString cluster = GetId(ctx->cluster_expr()).GetOrElse("");
                    return TColumnContext{
                        .Tables = {
                            TTableId{std::move(cluster), std::move(*path)},
                        },
                    };
                }

                if (TMaybe<TString> named = NSQLComplete::GetId(ctx->bind_parameter())) {
                    if (auto it = Subqueries_.find(*named); it != Subqueries_.end()) {
                        if (Resolving_.contains(*named)) {
                            return {};
                        }

                        Resolving_.emplace(*named);
                        Y_DEFER {
                            Resolving_.erase(*named);
                        };

                        return visit(it->second);
                    }
                }

                return {};
            }

            std::any visitSelect_stmt(SQLv1::Select_stmtContext* ctx) override {
                return AccumulatingVisit(ctx->select_stmt_intersect());
            }

            std::any visitSelect_core(SQLv1::Select_coreContext* ctx) override {
                TColumnContext without;
                if (std::any any = VisitNullable(ctx->without_column_list()); any.has_value()) {
                    without = std::move(std::any_cast<TColumnContext>(any));
                }

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
                return std::move(context) | std::move(imported) | std::move(without);
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

            std::any visitWithout_column_list(SQLv1::Without_column_listContext* ctx) override {
                return AccumulatingVisit(ctx->without_column_name());
            };

            std::any visitWithout_column_name(SQLv1::Without_column_nameContext* ctx) override {
                TString table = GetId(ctx->an_id(0)).GetOrElse("");
                TMaybe<TString> column = GetId(ctx->an_id(1)).Or([&] {
                    return GetId(ctx->an_id_without());
                });

                if (column.Empty()) {
                    return {};
                }

                return TColumnContext{
                    .WithoutByTableAlias = {
                        {std::move(table), {{std::move(*column)}}},
                    },
                };
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

            THashMap<TString, SQLv1::Subselect_stmtContext*> Subqueries_;
            THashSet<TString> Resolving_;
        };

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(const TParsedInput& input)
                : TSQLv1NarrowingVisitor(input)
            {
            }

            std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
                if (ctx->named_nodes_stmt() || IsEnclosing(ctx)) {
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

                return TInferenceVisitor(std::move(Subqueries_)).visit(source);
            }

        private:
            std::any visitNamed_nodes_stmt(SQLv1::Named_nodes_stmtContext* ctx) override {
                TMaybe<std::string> name = Name(ctx->bind_parameter_list());
                if (name.Empty()) {
                    return {};
                }

                SQLv1::Subselect_stmtContext* subselect = ctx->subselect_stmt();
                if (subselect == nullptr) {
                    return {};
                }

                Subqueries_[std::move(*name)] = subselect;
                return {};
            }

            TMaybe<std::string> Name(SQLv1::Bind_parameter_listContext* ctx) const {
                auto parameters = ctx->bind_parameter();
                if (parameters.size() != 1) {
                    return Nothing();
                }

                return NSQLComplete::GetId(parameters[0]);
            }

            bool IsEnclosingStrict(antlr4::ParserRuleContext* ctx) const {
                return ctx != nullptr && IsEnclosing(ctx);
            }

            THashMap<TString, SQLv1::Subselect_stmtContext*> Subqueries_;
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
