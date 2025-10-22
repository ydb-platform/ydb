#include "column.h"

#include "base_visitor.h"
#include "evaluate.h"
#include "function.h"
#include "narrowing_visitor.h"

#include <yql/essentials/sql/v1/complete/core/name.h>

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
            return VisitTableRefPath(ctx, std::move(*path));
        }

        if (TMaybe<TFunctionContext> function = GetFunction(ctx, *Nodes_)) {
            return VisitTableRefFunction(std::move(*function));
        }

        if (auto* bind_parameter = ctx->bind_parameter()) {
            return visit(bind_parameter);
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

            TColumnContext aliased = source.ExtractAliased(alias);
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

    std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
        TMaybe<TString> name = NSQLComplete::GetName(ctx);
        if (!name) {
            return {};
        }

        const TNamedNode* node = Nodes_->FindPtr(*name);
        if (!node) {
            return {};
        }

        if (Resolving_.contains(*name)) {
            return {};
        }

        Resolving_.emplace(*name);
        Y_DEFER {
            Resolving_.erase(*name);
        };

        auto* rule = std::visit([](auto&& arg) -> antlr4::ParserRuleContext* {
            using T = std::decay_t<decltype(arg)>;

            constexpr bool isRule = std::is_pointer_v<T> ||
                                    std::is_base_of_v<
                                        antlr4::ParserRuleContext*,
                                        std::remove_pointer_t<T>>;

            if constexpr (isRule) {
                return arg;
            }

            return nullptr;
        }, *node);

        if (!rule) {
            return {};
        }

        return visit(rule);
    }

private:
    std::any VisitTableRefPath(SQLv1::Table_refContext* ctx, TString path) {
        TString cluster = GetObjectId(ctx->cluster_expr()).GetOrElse("");
        return TColumnContext{
            .Tables = {
                TTableId{std::move(cluster), std::move(path)},
            },
        };
    }

    std::any VisitTableRefFunction(TFunctionContext function) {
        TString cluster = function.Cluster.GetOrElse({}).Name;

        TString path;
        function.Name = NormalizeName(function.Name);
        if (function.Name == "concat" && function.Arg0) {
            path = std::move(*function.Arg0);
        } else if (function.Name == "range" && function.Arg0 && function.Arg1) {
            path = std::move(*function.Arg0);
            path.append('/').append(*function.Arg1);
        } else {
            return {};
        }

        return TColumnContext{
            .Tables = {
                TTableId{std::move(cluster), std::move(path)},
            },
        };
    }

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

class TEnclosingSelectVisitor: public TSQLv1NarrowingVisitor {
public:
    explicit TEnclosingSelectVisitor(const TParsedInput& input)
        : TSQLv1NarrowingVisitor(input)
    {
    }

    std::any visitSelect_core(SQLv1::Select_coreContext* ctx) override {
        if (!IsEnclosing(ctx)) {
            return {};
        }

        Enclosing_ = ctx;
        return visitChildren(ctx);
    }

    SQLv1::Select_coreContext* GetEnclosing() && {
        return Enclosing_;
    }

private:
    SQLv1::Select_coreContext* Enclosing_ = nullptr;
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
        if (IsEnclosingStrict(ctx->window_clause()) ||
            IsEnclosingStrict(ctx->ext_order_by_clause())) {
            return TInferenceVisitor(Nodes_).visit(ctx);
        }

        auto* source = ctx->join_source(0);
        source = source == nullptr ? ctx->join_source(1) : source;

        if (!source) {
            return {};
        }

        auto sources = source->flatten_source();
        auto** flatten = FindIfPtr(sources, [&](auto* ctx) {
            return IsEnclosingStrict(ctx);
        });

        if (flatten) {
            return visitChildren(*flatten);
        }

        return TInferenceVisitor(Nodes_).visit(source);
    }

private:
    bool IsEnclosingStrict(antlr4::ParserRuleContext* ctx) const {
        return ctx != nullptr && IsEnclosing(ctx);
    }

    const TNamedNodes* Nodes_;
};

antlr4::ParserRuleContext* Enclosing(const TParsedInput& input) {
    TEnclosingSelectVisitor visitor(input);
    visitor.visit(input.SqlQuery);

    antlr4::ParserRuleContext* ctx = std::move(visitor).GetEnclosing();
    if (!ctx) {
        ctx = input.SqlQuery;
    }

    return ctx;
}

} // namespace

TMaybe<TColumnContext> InferColumnContext(TParsedInput input, const TNamedNodes& nodes) {
    // TODO: add utility `auto ToMaybe<T>(std::any any) -> TMaybe<T>`
    std::any result = TVisitor(input, &nodes).visit(Enclosing(input));
    if (!result.has_value()) {
        return Nothing();
    }
    return std::any_cast<TColumnContext>(result);
}

} // namespace NSQLComplete
