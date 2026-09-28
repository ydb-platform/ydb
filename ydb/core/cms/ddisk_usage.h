#pragma once

namespace NKikimrCms {
    enum EDDiskDiskSortBy : int;
    class TDDiskDiskListResponse;
    class TDDiskDiskListRequest;
}

namespace NKikimr::NCms {
    bool IsDDiskOccupancySort(NKikimrCms::EDDiskDiskSortBy sortBy);
    void SortAndPageDDiskOccupancy(NKikimrCms::TDDiskDiskListResponse& response,
                                  const NKikimrCms::TDDiskDiskListRequest& request);
}
