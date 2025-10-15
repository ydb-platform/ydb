#include "function.h"

#include "evaluate.h"
#include "narrowing_visitor.h"
#include "use.h"

#include <library/cpp/iterator/enumerate.h>

namespace NSQLComplete {

namespace {

class TVisitor: public TSQLv1NarrowingVisitor {
public:
    TVisitor(const TParsedInput& input, const TNamedNodes* nodes)
        : TSQLv1NarrowingVisitor(input)
        , Nodes_(nodes)
    {
    }

    std::any visit(antlr4::tree::ParseTree* tree) override {
        if (IsEnclosing(tree)) {
            return TSQLv1NarrowingVisitor::visit(tree);
        }
        return {};
    }

    std::any visitTable_ref(SQLv1::Table_refContext* ctx) override {
        auto* function = ctx->an_id_expr();
        auto* lparen = ctx->TOKEN_LPAREN();
        if (function == nullptr || lparen == nullptr) {
            return {};
        }
        if (CursorPosition() <= TextInterval(lparen).b) {
            return {};
        }

        TMaybe<TFunctionContext> context = GetFunction(ctx, *Nodes_);
        if (!context) {
            return {};
        }

        const size_t argN = ArgumentNumber(ctx).GetOrElse(0);
        context->ArgumentNumber = argN;
        context->Arg0 = (argN != 0) ? context->Arg0 : Nothing();
        context->Arg1 = (argN != 1) ? context->Arg1 : Nothing();
        return *context;
    }

private:
    TMaybe<size_t> ArgumentNumber(SQLv1::Table_refContext* ctx) const {
        for (auto [i, arg] : Enumerate(ctx->table_arg())) {
            if (IsEnclosing(arg)) {
                return i;
            }
        }
        return Nothing();
    }

    const TNamedNodes* Nodes_;
};

TMaybe<TString> GetArgument(size_t index, SQLv1::Table_refContext* ctx, const TNamedNodes& nodes) {
    auto* table_arg = ctx->table_arg(index);
    if (!table_arg) {
        return Nothing();
    }

    auto* named_expr = table_arg->named_expr();
    if (!named_expr) {
        return Nothing();
    }

    return ToObjectRef(PartiallyEvaluate(named_expr, nodes));
}

TMaybe<TClusterContext> GetCluster(SQLv1::Table_refContext* ctx, const TNamedNodes& nodes) {
    auto* cluster_expr = ctx->cluster_expr();
    if (!cluster_expr) {
        return Nothing();
    }

    return ParseClusterContext(cluster_expr, nodes);
}

} // namespace

TMaybe<TFunctionContext> EnclosingFunction(TParsedInput input, const TNamedNodes& nodes) {
    std::any result = TVisitor(input, &nodes).visit(input.SqlQuery);
    if (!result.has_value()) {
        return Nothing();
    }
    return std::any_cast<TFunctionContext>(result);
}

TMaybe<TFunctionContext> GetFunction(SQLv1::Table_refContext* ctx, const TNamedNodes& nodes) {
    auto* function = ctx->an_id_expr();
    auto* lparen = ctx->TOKEN_LPAREN();
    if (function == nullptr || lparen == nullptr) {
        return Nothing();
    }

    return TFunctionContext{
        .Name = function->getText(),
        .ArgumentNumber = 0,
        .Arg0 = GetArgument(0, ctx, nodes),
        .Arg1 = GetArgument(1, ctx, nodes),
        .Cluster = GetCluster(ctx, nodes),
    };
}

} // namespace NSQLComplete
