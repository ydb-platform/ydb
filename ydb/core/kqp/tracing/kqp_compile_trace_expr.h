#pragma once

#include <yql/essentials/core/yql_expr_optimize.h>

namespace NKikimr::NKqp {

// Number of nodes of an expression graph, for span attributes. Visits the
// whole graph. Header-only: the tracing library does not depend on yql core.
inline i64 CountExprNodes(const NYql::TExprNode& root) {
    i64 nodes = 0;
    NYql::VisitExpr(root, [&](const NYql::TExprNode&) {
        ++nodes;
        return true;
    });
    return nodes;
}

} // namespace NKikimr::NKqp
