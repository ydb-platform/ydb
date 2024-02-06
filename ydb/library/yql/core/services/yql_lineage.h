#pragma once
#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

struct TTypeAnnotationContext;
struct TExprContext;

TString CalculateLineage(const TExprNode& root, const TTypeAnnotationContext& ctx, TExprContext& exprCtx);

}
