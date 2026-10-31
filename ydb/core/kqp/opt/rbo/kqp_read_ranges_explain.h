#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NKqp::NOpt {

// New RBO explain and optimizer traces show exact ranges, one interval over all key columns each, or the structure
// of the ranges expression. Neither is set when the expression has a form the formatter does not know.
struct TReadRangesDescription {
    std::optional<TVector<TString>> Ranges;
    std::optional<TString> Expression;
};

// The most ranges explain lists. A read with more ranges is described by the structure of its expression.
constexpr size_t MaxExplainedReadRanges = 20;

// Prefer the extractor's direct bounds, then bounded interpretation of a constant range expression,
// otherwise describe its structure. Unknown cardinality skips interpretation. Does not execute a program,
// mutate the expression or require a compiler context, type annotation or function registry.
TReadRangesDescription DescribeReadRanges(const NYql::TExprNode::TPtr& literalRange,
    const NYql::TExprNode::TPtr& ranges, const TVector<TString>& keyColumns,
    TMaybe<size_t> expectedMaxRanges = {});

// Independent of the display limit: adjacent input points may merge into very few ranges.
constexpr size_t MaxEvaluatedRangeInputs = 256;
constexpr size_t MaxRangeEvaluationNodes = 4096;
constexpr size_t MaxRangeEvaluationBytes = 64 * 1024;

} // namespace NKikimr::NKqp::NOpt
