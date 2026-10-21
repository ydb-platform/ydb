#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NKqp::NOpt {

// Describes the ranges a read scans, one named tuple interval per range, in primary-key order.
// The ranges expression is computed the way MiniKQL computes it at runtime, so the description
// lists the ranges that are actually read. nullopt means the ranges cannot be determined at
// compile time; an empty vector means no key matches.
std::optional<TVector<TString>> BuildReadRangeDescriptions(const NYql::TExprNode::TPtr& ranges, const TVector<TString>& keyColumns);

} // namespace NKikimr::NKqp::NOpt
