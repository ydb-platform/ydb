#include "ddisk_usage.h"

#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/blobstorage_ddisk.pb.h>
#include <util/generic/algorithm.h>
#include <algorithm>
#include <tuple>

namespace NKikimr::NCms {

    bool IsDDiskOccupancySort(NKikimrCms::EDDiskDiskSortBy sortBy) {
        return sortBy == NKikimrCms::DDISK_DISK_SORT_BY_DDISK_OCCUPANCY || sortBy == NKikimrCms::DDISK_DISK_SORT_BY_PERSISTENT_BUFFER_OCCUPANCY;
    }

    // Ordinary sorts arrive already paginated. Occupancy sorts need measurements
    // for all matching disks before selecting a page.
    void SortAndPageDDiskOccupancy(NKikimrCms::TDDiskDiskListResponse& response,
                                  const NKikimrCms::TDDiskDiskListRequest& request) {
        if (!IsDDiskOccupancySort(request.GetSortBy())) {
            return;
        }
        const bool ddisk = request.GetSortBy() == NKikimrCms::DDISK_DISK_SORT_BY_DDISK_OCCUPANCY;
        auto* disks = response.MutableDisks();
        std::sort(disks->pointer_begin(), disks->pointer_end(), [&](const auto* ap, const auto* bp) {
            const auto& a = *ap;
            const auto& b = *bp;
            const bool hasA = ddisk ? a.HasDDiskOccupancy() : a.HasPersistentBufferOccupancy();
            const bool hasB = ddisk ? b.HasDDiskOccupancy() : b.HasPersistentBufferOccupancy();
            // Missing measurements always go last, in either direction.
            if (hasA != hasB) {
                return hasA;
            }
            const double va = ddisk ? a.GetDDiskOccupancy() : a.GetPersistentBufferOccupancy();
            const double vb = ddisk ? b.GetDDiskOccupancy() : b.GetPersistentBufferOccupancy();
            if (hasA && va != vb) {
                return request.GetSortDescending() ? va > vb : va < vb;
            }
            const auto key = [](const auto& disk) {
                const auto& id = disk.GetDiskId();
                return std::make_tuple(id.GetNodeId(), id.GetPDiskId(), id.GetDDiskSlotId());
            };
            return key(a) < key(b);
        });
        const ui32 size = disks->size();
        const ui32 offset = Min(request.GetOffset(), size);
        const ui32 end = request.GetLimit()
                             ? Min<ui64>(ui64(offset) + request.GetLimit(), size)
                             : size;
        disks->DeleteSubrange(end, size - end);
        disks->DeleteSubrange(0, offset);
    }

} // namespace NKikimr::NCms
