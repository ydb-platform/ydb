#pragma once

#include <util/generic/set.h>
#include <util/system/types.h>

#include <concepts>

namespace NYdb::NBS::NBlockStore {

//////////////////////////////////////////////////////////////////////////////
// Forward declarations

class TBlockRangeField;

template <std::unsigned_integral T>
struct TBlockRange;

template <typename TBlockIndex>
class TBlockRangeBuilder;

struct TBlockRangeComparator;

struct TWeightedRange;

struct TPBufferKey;

//////////////////////////////////////////////////////////////////////////////
// Type aliases (most commonly used)

using TBlockRange16 = TBlockRange<ui16>;
using TBlockRange32 = TBlockRange<ui32>;
using TBlockRange64 = TBlockRange<ui64>;

using TBlockRange16Builder = TBlockRangeBuilder<ui16>;
using TBlockRange32Builder = TBlockRangeBuilder<ui32>;
using TBlockRange64Builder = TBlockRangeBuilder<ui64>;

using TBlockRangeSet16 = TSet<TBlockRange16, TBlockRangeComparator>;
using TBlockRangeSet32 = TSet<TBlockRange32, TBlockRangeComparator>;
using TBlockRangeSet64 = TSet<TBlockRange64, TBlockRangeComparator>;

//////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
