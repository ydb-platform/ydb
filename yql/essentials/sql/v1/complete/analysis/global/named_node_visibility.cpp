#include "named_node_visibility.h"

#include "narrowing_visitor.h"

#include <util/generic/hash_set.h>

namespace NSQLComplete {

namespace {

class TVisitor: public TSQLv1NarrowingVisitor {
public:
    TVisitor(const TParsedInput& input, THashSet<TString>* visible)
        : TSQLv1NarrowingVisitor(input)
        , Visible_(visible)
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
        VisitNullableCollecting(ctx->bind_parameter());
        return {};
    }

    std::any visitImport_stmt(SQLv1::Import_stmtContext* ctx) override {
        VisitNullableCollecting(ctx->named_bind_parameter_list());
        return {};
    }

    std::any visitDefine_action_or_subquery_stmt(SQLv1::Define_action_or_subquery_stmtContext* ctx) override {
        if (IsEnclosing(ctx)) {
            VisitNullableCollecting(ctx->action_or_subquery_args());
            return visitChildren(ctx);
        }
        VisitNullableCollecting(ctx->bind_parameter());
        return {};
    }

    std::any visitNamed_nodes_stmt(SQLv1::Named_nodes_stmtContext* ctx) override {
        if (IsEnclosing(ctx)) {
            visitChildren(ctx);
        }
        VisitNullableCollecting(ctx->bind_parameter_list());
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
        if (IsEnclosing(ctx)) {
            if (ctx->TOKEN_ARROW()) {
                VisitNullableCollecting(ctx->smart_parenthesis());
            }
            visitChildren(ctx);
        }
        return {};
    }

    std::any visitNamed_bind_parameter(SQLv1::Named_bind_parameterContext* ctx) override {
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

        Visible_->insert(std::move(*id));
        return {};
    }

private:
    void VisitNullableCollecting(antlr4::tree::ParseTree* tree) {
        if (tree == nullptr) {
            return;
        }

        const bool old = std::exchange(IsCollecting_, true);
        visit(tree);
        IsCollecting_ = old;
    }

    THashSet<TString>* Visible_;
    bool IsCollecting_ = false;
};

} // namespace

TVector<TString> VisibleNamedNodes(TParsedInput input) {
    THashSet<TString> visible;
    TVisitor(input, &visible).visit(input.SqlQuery);

    TVector<TString> result(Reserve(visible.size()));
    std::ranges::move(visible, std::back_inserter(result));
    return result;
}

} // namespace NSQLComplete
