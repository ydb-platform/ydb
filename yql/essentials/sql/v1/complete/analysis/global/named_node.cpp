#include "named_node.h"

#include "narrowing_visitor.h"

#include <library/cpp/iterator/iterate_keys.h>
#include <library/cpp/iterator/iterate_values.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>

namespace NSQLComplete {

namespace {

class TVisitor: public TSQLv1NarrowingVisitor {
public:
    TVisitor(const TParsedInput& input, TNamedNodes* names, const TEnvironment* env)
        : TSQLv1NarrowingVisitor(input)
        , Names_(names)
        , Env_(env)
    {
    }

    std::any visitSql_stmt_core(SQLv1::Sql_stmt_coreContext* ctx) override {
        if (ctx->declare_stmt() ||
            ctx->import_stmt() ||
            ctx->define_action_or_subquery_stmt() ||
            ctx->named_nodes_stmt() ||
            IsEnclosing(ctx)) {
            return visitChildren(ctx);
        }
        return {};
    }

    std::any visitDeclare_stmt(SQLv1::Declare_stmtContext* ctx) override {
        auto* parameter = ctx->bind_parameter();
        if (!parameter) {
            return {};
        }

        TMaybe<std::string> id = GetName(parameter);
        if (id.Empty() || id == "_") {
            return {};
        }

        id->insert(0, "$");
        const NYT::TNode* node = Env_->Parameters.FindPtr(*id);
        id->erase(0, 1);

        if (node) {
            (*Names_)[*id] = *node;
        } else {
            (*Names_)[*id] = std::monostate();
        }

        return {};
    }

    std::any visitImport_stmt(SQLv1::Import_stmtContext* ctx) override {
        VisitNullableCollecting(ctx->named_bind_parameter_list());
        return {};
    }

    std::any visitDefine_action_or_subquery_stmt(
        SQLv1::Define_action_or_subquery_stmtContext* ctx) override {
        VisitNullableCollecting(ctx->bind_parameter());
        if (IsEnclosing(ctx)) {
            VisitNullableCollecting(ctx->action_or_subquery_args());
            return visitChildren(ctx);
        }
        return {};
    }

    std::any visitNamed_nodes_stmt(SQLv1::Named_nodes_stmtContext* ctx) override {
        VisitNullableCollecting(ctx->bind_parameter_list());
        if (IsEnclosing(ctx)) {
            visitChildren(ctx);
        }

        auto* list = ctx->bind_parameter_list();
        if (!list) {
            return {};
        }

        auto parameters = list->bind_parameter();
        if (parameters.size() != 1) {
            return {};
        }

        auto* parameter = parameters[0];
        if (!parameter) {
            return {};
        }

        TMaybe<std::string> id = GetName(parameter);
        if (id.Empty() || id == "_") {
            return {};
        }

        if (auto* expr = ctx->expr()) {
            (*Names_)[std::move(*id)] = expr;
        } else {
            (*Names_)[std::move(*id)] = std::monostate();
        }

        return {};
    }

    std::any visitFor_stmt(SQLv1::For_stmtContext* ctx) override {
        VisitNullableCollecting(ctx->bind_parameter());
        if (IsEnclosing(ctx)) {
            return visitChildren(ctx);
        }
        return {};
    }

    std::any visitLambda(SQLv1::LambdaContext* ctx) override {
        VisitNullableCollecting(ctx->smart_parenthesis());
        if (IsEnclosing(ctx)) {
            return visitChildren(ctx);
        }
        return {};
    }

    std::any visitNamed_bind_parameter(
        SQLv1::Named_bind_parameterContext* ctx) override {
        VisitNullableCollecting(ctx->bind_parameter(0));
        return {};
    }

    std::any visitBind_parameter(SQLv1::Bind_parameterContext* ctx) override {
        if (IsEnclosing(ctx) || !IsCollecting_) {
            return {};
        }

        TMaybe<std::string> id = GetName(ctx);
        if (id.Empty() || id == "_") {
            return {};
        }

        (*Names_)[std::move(*id)] = std::monostate();
        return {};
    }

private:
    void VisitNullableCollecting(antlr4::tree::ParseTree* tree) {
        if (tree == nullptr) {
            return;
        }

        const bool old = IsCollecting_;
        IsCollecting_ = true;
        visit(tree);
        IsCollecting_ = old;
    }

    TNamedNodes* Names_;
    const TEnvironment* Env_;
    bool IsCollecting_ = false;
};

} // namespace

TNamedNodes CollectNamedNodes(TParsedInput input, const TEnvironment& env) {
    TNamedNodes names;
    TVisitor(input, &names, &env).visit(input.SqlQuery);
    return names;
}

} // namespace NSQLComplete
