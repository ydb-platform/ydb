#pragma once

#include <util/generic/vector.h>
#include <util/system/types.h>

#include <optional>
#include <span>

namespace NKikimr::NCms {

    struct TDDiskOccupancySortKey {
        ui32 NodeId;
        ui32 PDiskId;
        ui32 DDiskSlotId;
        std::optional<double> Occupancy;
    };

    // Return indices into keys for the requested page, with absent values last.
    // Tablet lists and protobuf response records are not needed for sorting.
    TVector<ui32> SortAndPageDDiskOccupancy(std::span<const TDDiskOccupancySortKey> keys,
                                          bool descending, ui32 offset, ui32 limit);

} // namespace NKikimr::NCms
