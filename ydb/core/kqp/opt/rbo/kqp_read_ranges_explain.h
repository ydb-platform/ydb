#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NKqp::NOpt {

// How explain describes the key ranges of a read: exact ranges, one interval over all key columns each, or else the
// structure of the ranges expression. Neither when the expression has a form explain does not know.
struct TReadRangesDescription {
    std::optional<TVector<TString>> Ranges;
    std::optional<TString> Expression;
};

// The most ranges explain lists. A read with more ranges is described by the structure of its expression.
constexpr size_t MaxExplainedReadRanges = 20;

// The ranges of a read from the predicate extractor: its literal range when there is one, and its ranges expression.
TReadRangesDescription DescribeReadRanges(const NYql::TExprNode::TPtr& literalRange, const NYql::TExprNode::TPtr& evaluatedRanges,
                                          const NYql::TExprNode::TPtr& ranges,
                                          const TVector<TString>& keyColumns, TMaybe<size_t> expectedMaxRanges);

// Describes a KqlKeyRange with literal or parameter bounds as one interval over all key columns.
TString DescribeLiteralKeyRange(const NYql::TExprNode::TPtr& keyRange, const TVector<TString>& keyColumns);

} // namespace NKikimr::NKqp::NOpt
