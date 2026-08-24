#pragma once

#include "public.h"

#include "block_range.h"

#include <util/generic/set.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TBlockRangeField
{
public:
    enum class EEnumerateContinuation
    {
        Continue,
        Stop,
    };
    using TEnumerateFunc =
        std::function<EEnumerateContinuation(TBlockRange64 item)>;

    // Returns true if the intervals have actually changed.
    bool Add(TBlockRange64 range);
    // Returns true if the intervals have actually changed.
    bool Remove(TBlockRange64 range);
    // Returns true if the intervals have actually changed.
    bool Clear();

    [[nodiscard]] bool Overlaps(TBlockRange64 other) const;

    void Enumerate(TEnumerateFunc func) const;

    [[nodiscard]] bool Empty() const;
    [[nodiscard]] size_t GetBlockCount() const;
    [[nodiscard]] size_t GetSegmentCount() const;
    [[nodiscard]] TString Print() const;

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
