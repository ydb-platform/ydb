#include "column.h"

#include "base_visitor.h"
#include "evaluate.h"
#include "narrowing_visitor.h"

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>

namespace NSQLComplete {

    namespace {

        class TInferenceVisitor: public TSQLv1BaseVisitor {
        public:
            TInferenceVisitor(const TNamedNodes* nodes)
                : Nodes_(nodes)
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
                if (TMaybe<TString> path; (path = GetObjectId(ctx->table_key())) ||
                                          (path = GetObjectId(ctx->bind_parameter()))) {
                    TString cluster = GetObjectId(ctx->cluster_expr()).GetOrElse("");
                    return TColumnContext{
                        .Tables = {
                            TTableId{std::move(cluster), std::move(*path)},
                        },
                    };
                }

                if (TMaybe<TString> named = NSQLComplete::GetName(ctx->bind_parameter())) {
                    const TNamedNode* node = Nodes_->FindPtr(*named);
                    if (!node || !std::holds_alternative<SQLv1::Subselect_stmtContext*>(*node)) {
                        return {};
                    }

                    if (Resolving_.contains(*named)) {
                        return {};
                    }

                    Resolving_.emplace(*named);
                    Y_DEFER {
                        Resolving_.erase(*named);
                    };

                    return visit(std::get<SQLv1::Subselect_stmtContext*>(*node));
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
                    TMaybe<TString> alias = GetColumnId(ctx->opt_id_prefix()->an_id());
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
                TString table = GetObjectId(ctx->an_id(0)).GetOrElse("");
                TMaybe<TString> column = GetColumnId(ctx->an_id(1)).Or([&] {
                    return GetColumnId(ctx->an_id_without());
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
                TMaybe<TString> alias = GetColumnId(ctx->an_id());
                alias = alias.Defined() ? alias : GetColumnId(ctx->an_id_as_compat());
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
                return GetColumnId(id);
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

            TMaybe<TString> GetColumnId(antlr4::ParserRuleContext* ctx) const {
                if (!ctx) {
                    return Nothing();
                }

                TPartialValue value = PartiallyEvaluate(ctx, *Nodes_);
                if (!std::holds_alternative<TIdentifier>(value)) {
                    return Nothing();
                }

                return std::get<TIdentifier>(value);
            }

            TMaybe<TString> GetObjectId(antlr4::ParserRuleContext* ctx) const {
                if (!ctx) {
                    return Nothing();
                }

                return ToObjectRef(PartiallyEvaluate(ctx, *Nodes_));
            }

            THashSet<TString> Resolving_;
            const TNamedNodes* Nodes_;
        };

        class TVisitor: public TSQLv1NarrowingVisitor {
        public:
            TVisitor(const TParsedInput& input, const TNamedNodes* nodes)
                : TSQLv1NarrowingVisitor(input)
                , Nodes_(nodes)
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

                return TInferenceVisitor(Nodes_).visit(source);
            }

        private:
            bool IsEnclosingStrict(antlr4::ParserRuleContext* ctx) const {
                return ctx != nullptr && IsEnclosing(ctx);
            }

            const TNamedNodes* Nodes_;
        };

    } // namespace

    TMaybe<TColumnContext> InferColumnContext(TParsedInput input, const TNamedNodes& nodes) {
        // TODO: add utility `auto ToMaybe<T>(std::any any) -> TMaybe<T>`
        std::any result = TVisitor(input, &nodes).visit(input.SqlQuery);
        if (!result.has_value()) {
            return Nothing();
        }
        return std::any_cast<TColumnContext>(result);
    }

} // namespace NSQLComplete
