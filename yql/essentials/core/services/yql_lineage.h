#pragma once
#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

struct TTypeAnnotationContext;
struct TExprContext;

TString CalculateLineage(const TExprNode& root, const TTypeAnnotationContext& ctx, TExprContext& exprCtx, bool standalone);

// Replace input and output table's IDs with pathes for checking lineage equality
TString NormalizeLineage(const TString& lineageStr);

} // namespace NYql
