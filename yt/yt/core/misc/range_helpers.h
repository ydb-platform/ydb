#pragma once

#include "common.h"

#include <library/cpp/iterator/zip.h>

#include <ranges>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An equivalent of Python's `zip()`, but resulting range consists of tuples
//! of pointers and has length equal to the length of the shortest container.
//! Implementation with mutable references depends on "lifetime extension in
//! range-based for loops" from C++23.
template <std::ranges::range... TRanges>
auto ZipMutable(TRanges&&... ranges);

//! Converts the provided range to the specified container.
//! This is a simplified equivalent of std::ranges::to from range-v3.
template <class TContainer, std::ranges::input_range TRange>
auto RangeTo(TRange&& range);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RANGE_HELPERS_INL_H_
#include "range_helpers-inl.h"
#undef RANGE_HELPERS_INL_H_
