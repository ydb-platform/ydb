#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NKqp::NOpt::NReadRanges {

// Describes a ranges expression without computing it, as a product of per column conditions in key order, for example
// "a ∈ {1, 2} × b ∈ [10, $hi)". Values unknown at compile time are shown by name. It shows the expression and not the
// ranges read: past the RangeMultiply limit the read is coarser than shown. nullopt for an expression form it does not
// know.
std::optional<TString> DescribeRangesStructure(const NYql::TExprNode::TPtr& ranges, const TVector<TString>& keyColumns);

// The structure lists at most this many values of a set and counts the rest.
constexpr size_t MaxDescribedPoints = 10;

} // namespace NKikimr::NKqp::NOpt::NReadRanges
