#pragma once

#include "public.h"

#include "block_range.h"

#include <util/generic/set.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TBlockRangeField
{
public:
    using TEnumerateFunc = std::function<void(TBlockRange64 item)>;

    void Add(TBlockRange64 range);
    void Remove(TBlockRange64 range);

    [[nodiscard]] bool Overlaps(TBlockRange64 other) const;

    void Enumerate(TEnumerateFunc func) const;

private:
    struct TBlockRangeComparator
    {
        bool operator()(TBlockRange64 a, TBlockRange64 b) const
        {
            return a.End < b.End;
        }
    };

    TSet<TBlockRange64, TBlockRangeComparator> Intervals;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
