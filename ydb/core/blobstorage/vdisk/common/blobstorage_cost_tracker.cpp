#include "blobstorage_cost_tracker.h"

namespace NKikimr {

class TBsCostModelMirror3dc : public TBsCostModelBase {};

class TBsCostModel4Plus2Block : public TBsCostModelBase {};

class TBsCostModelMirror3of4 : public TBsCostModelBase {};

TBsCostTracker::TBsCostTracker(const TBlobStorageGroupType& groupType, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : GroupType(groupType)
    , CostCounters(counters->GetSubgroup("subsystem", "advancedCost"))
    , UserDiskCost(CostCounters->GetCounter("UserDiskCost", true))
    , CompactionDiskCost(CostCounters->GetCounter("CompactionDiskCost", true))
    , ScrubDiskCost(CostCounters->GetCounter("ScrubDiskCost", true))
    , DefragDiskCost(CostCounters->GetCounter("DefragDiskCost", true))
    , InternalDiskCost(CostCounters->GetCounter("InternalDiskCost", true))
{
    switch (GroupType.GetErasure()) {
    case TBlobStorageGroupType::ErasureMirror3dc:
        CostModel = std::make_unique<TBsCostModelMirror3dc>();
        break;
    case TBlobStorageGroupType::Erasure4Plus2Block:
        CostModel = std::make_unique<TBsCostModelMirror3dc>();
        break;
    case TBlobStorageGroupType::ErasureMirror3of4:
        CostModel = std::make_unique<TBsCostModelMirror3of4>();
        break;
    default:
        break;
    }
}

} // NKikimr
