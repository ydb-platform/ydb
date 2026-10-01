#include "ddisk_usage.h"

#include <algorithm>
#include <numeric>
#include <tuple>

namespace NKikimr::NCms {

    TVector<ui32> SortAndPageDDiskOccupancy(std::span<const TDDiskOccupancySortKey> keys,
                                          bool descending, ui32 offset, ui32 limit) {
        TVector<ui32> order(keys.size());
        std::iota(order.begin(), order.end(), 0);
        std::sort(order.begin(), order.end(), [&](ui32 a, ui32 b) {
            const auto& left = keys[a];
            const auto& right = keys[b];
            if (left.Occupancy.has_value() != right.Occupancy.has_value()) {
                return left.Occupancy.has_value();
            }
            if (left.Occupancy && left.Occupancy != right.Occupancy) {
                return descending ? *left.Occupancy > *right.Occupancy : *left.Occupancy < *right.Occupancy;
            }
            return std::tie(left.NodeId, left.PDiskId, left.DDiskSlotId)
                < std::tie(right.NodeId, right.PDiskId, right.DDiskSlotId);
        });
        const ui32 size = order.size();
        offset = std::min(offset, size);
        const ui32 end = limit ? std::min<ui64>(ui64(offset) + limit, size) : size;
        order.resize(end);
        order.erase(order.begin(), order.begin() + offset);
        return order;
    }

} // namespace NKikimr::NCms
