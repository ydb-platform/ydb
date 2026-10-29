#pragma once

#include "kqp_read_ranges_model.h"

namespace NKikimr::NKqp::NOpt::NReadRanges {

// Computes a ranges expression the way MiniKQL computes it at runtime, so the result lists the ranges that are
// actually read. nullopt when the ranges are not known at compile time: a value is a parameter or computed, the order
// or a conversion of the values is not known, the expression has a form explain does not know, or an intermediate
// list of ranges grows past MaxCalculatedRanges.
std::optional<TRangeList> CalculateRanges(const NYql::TExprNode::TPtr& ranges);

constexpr size_t MaxCalculatedRanges = 1000;

} // namespace NKikimr::NKqp::NOpt::NReadRanges
