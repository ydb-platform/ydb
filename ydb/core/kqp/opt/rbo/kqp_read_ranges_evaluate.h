#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <util/generic/maybe.h>

namespace NYql {
struct TTypeAnnotationContext;
}

namespace NKikimr::NMiniKQL {
class IFunctionRegistry;
}

namespace NKikimr::NKqp::NOpt {

// Independent of the display limit: adjacent input points may merge into very few ranges.
constexpr size_t MaxEvaluatedRangeInputs = 256;
constexpr size_t MaxRangeEvaluationNodes = 4096;
constexpr size_t MaxRangeEvaluationBytes = 64 * 1024;

// Best-effort evaluation for explain, in an isolated context. Returns a literal list, or null for runtime-dependent,
// unsupported, oversized or failing expressions. Does not change the original expression or the caller's issues/steps.
NYql::TExprNode::TPtr EvaluateReadRanges(const NYql::TExprNode::TPtr& ranges, TMaybe<size_t> expectedMaxRanges,
    NYql::TExprContext& ctx, const NYql::TTypeAnnotationContext& types,
    const NMiniKQL::IFunctionRegistry& functionRegistry);

} // namespace NKikimr::NKqp::NOpt
