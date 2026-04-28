#pragma once

#include "block_range.h"

#include <util/system/types.h>

#include <functional>

namespace NYdb::NBS::NBlockStore {

using Cb = std::function<bool(ui64 key, TBlockRange64 range)>;
// The function splits overlapping ranges into non-overlapping ranges
//   and calls cb for each of them on the same loop's iteration.
// Result is continious range's sequence, where original 'holes' are
//   fullfilled with key == 0.
template <typename TContainerRanges>
void SplitExecOnNonOverlappingRanges(
    ui64 start,
    ui64 end,
    const TContainerRanges& overlappigRanges,
    Cb cb);

}   // namespace NYdb::NBS::NBlockStore

#include "block_range_algorithms.tcc"
