#include "path_visitor.h"

namespace NSQLPureAST {

namespace {

bool IsEmpty(SQLv1::Unary_subexpr_suffixContext* ctx) {
    return !ctx || (ctx->key_expr().empty() &&
                    ctx->invoke_expr().empty() &&
                    ctx->TOKEN_DOT().empty() &&
                    !ctx->TOKEN_COLLATE());
}

} // namespace

std::any TSQLv1PathVisitor::visitUnary_casual_subexpr(SQLv1::Unary_casual_subexprContext* ctx) {
    if (!IsEmpty(ctx->unary_subexpr_suffix())) {
        return {};
    }

    if (auto* x = ctx->id_expr()) {
        return visit(x);
    }
    if (auto* x = ctx->atom_expr()) {
        return visit(x);
    }
    return {};
}

std::any TSQLv1PathVisitor::aggregateResult(std::any aggregate, std::any nextResult) {
    Y_UNUSED(aggregate);
    return nextResult;
}

} // namespace NSQLPureAST
