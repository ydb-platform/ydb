#pragma once

#include "kqp_explain_context.h"

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NKqp::NOpt {

// Whole-key intervals or a symbolic expression, with fN(...) for unsupported parts.
struct TReadRangesDescription {
    std::optional<TVector<TString>> Ranges;
    std::optional<TString> Expression;
};

// The most ranges explain lists. A read with more ranges is described by the structure of its expression.
constexpr size_t MaxExplainedReadRanges = 20;

// Try literal bounds, bounded interpretation, then symbolic formatting. Never execute a program.
// Unknown cardinality skips interpretation. A non-null ranges node always has a description.
TReadRangesDescription DescribeReadRanges(const NYql::TExprNode::TPtr& literalRange,
    const NYql::TExprNode::TPtr& ranges, const TVector<TString>& keyColumns,
    TMaybe<size_t> expectedMaxRanges = {}, TExplainContext* explain = nullptr);

// Independent of the display limit: adjacent input points may merge into very few ranges.
constexpr size_t MaxEvaluatedRangeInputs = 256;
constexpr size_t MaxRangeEvaluationNodes = 4096;
constexpr size_t MaxRangeEvaluationBytes = 64 * 1024;

} // namespace NKikimr::NKqp::NOpt
