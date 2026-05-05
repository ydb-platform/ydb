#pragma once
#include <yql/essentials/ast/yql_expr.h>

namespace NYql {

struct TTypeAnnotationContext;
struct TExprContext;

struct TLineageRunOptions {
    bool Standalone = false;
    ui32 Version = 1;
    bool YsonTypeFormat = false;
};

TString CalculateLineage(const TExprNode& root, TTypeAnnotationContext& ctx, TExprContext& exprCtx, const TLineageRunOptions& options);

// Check that lineage section is not empty
void ValidateLineage(const TString& lineageStr);

// Compare that two lineages are equvalent otherwise throw exception
void CheckEquvalentLineages(const TString& lineageFirst, const TString& lineageSecond);

} // namespace NYql
