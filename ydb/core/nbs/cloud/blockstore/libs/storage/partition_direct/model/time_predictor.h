#pragma once

#include "host_mask.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/ring_buffer.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TTimePredictor
{
public:
    TTimePredictor(size_t capacity, size_t nthFromEnd);

    void Add(THostIndex host, TDuration time);
    void Add(THostMask hostMask, TDuration time);

    [[nodiscard]] TDuration Predict(THostIndex host) const;
    [[nodiscard]] TDuration Predict(THostMask hostMask) const;

private:
    struct THistory
    {
        TRingBuffer<TDuration> History;
        // Sorted ascending, sized to capacity. Count is the logical size —
        // must be sized (not reserved) so copies keep the capacity.
        TVector<TDuration> Durations;
        size_t Count = 0;

        explicit THistory(size_t capacity);

        void Add(TDuration time);
        [[nodiscard]] TDuration Predict(size_t nthFromEnd) const;
    };

    const size_t Capacity;
    const size_t NthFromEnd;
    TVector<THistory> History;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
