#pragma once
#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

struct TTypeAnnotationContext;
struct TExprContext;

TString CalculateLineage(const TExprNode& root, const TTypeAnnotationContext& ctx, TExprContext& exprCtx, bool standalone);

// Check that lineage section is not empty
void ValidateLineage(const TString& lineageStr);

// Compare that two lineages are equvalent otherwise throw exception
void CheckEquvalentLineages(const TString& lineageFirst, const TString& lineageSecond);

} // namespace NYql
