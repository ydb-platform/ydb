#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NYql {
struct TTypeAnnotationContext;
}

namespace NKikimr::NMiniKQL {
class IFunctionRegistry;
}

namespace NKikimr::NKqp::NOpt {

// New RBO explain and optimizer traces show exact ranges, one interval over all key columns each, or the structure
// of the ranges expression. Neither is set when the expression has a form the formatter does not know.
struct TReadRangesDescription {
    std::optional<TVector<TString>> Ranges;
    std::optional<TString> Expression;
};

// The most ranges explain lists. A read with more ranges is described by the structure of its expression.
constexpr size_t MaxExplainedReadRanges = 20;

// Prefer the extractor's literal range, then a successfully evaluated list, otherwise describe the expression.
TReadRangesDescription DescribeReadRanges(const NYql::TExprNode::TPtr& literalRange, const NYql::TExprNode::TPtr& evaluatedRanges,
                                          const NYql::TExprNode::TPtr& ranges,
                                          const TVector<TString>& keyColumns);

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
