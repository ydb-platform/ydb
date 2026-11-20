#include "blobstorage_cost_tracker.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

const TDiskOperationCostEstimator TBsCostModelBase::HDDEstimator{
    { 80000, 1.774 },   // ReadCoefficients
    { 6500, 11.1 },     // WriteCoefficients
    { 6.089e+06, 8.1 }, // HugeWriteCoefficients
};

const TDiskOperationCostEstimator TBsCostModelBase::SSDEstimator{
    { 180000, 3.00 },   // ReadCoefficients
    { 430, 4.2 },     // WriteCoefficients
    { 110000, 3.6 },     // HugeWriteCoefficients
};

const TDiskOperationCostEstimator TBsCostModelBase::NVMEEstimator{
    { 10000, 1.3 },   // ReadCoefficients
    { 3300, 1.5 },     // WriteCoefficients
    { 50000, 1.83 }, // HugeWriteCoefficients
};

class TBsCostModelMirror3dc : public TBsCostModelBase {
public:
    TBsCostModelMirror3dc(NPDisk::EDeviceType deviceType)
        : TBsCostModelBase(deviceType)
    {}
};

class TBsCostModel4Plus2Block : public TBsCostModelBase {
public:
    TBsCostModel4Plus2Block(NPDisk::EDeviceType deviceType)
        : TBsCostModelBase(deviceType)
    {}
};

class TBsCostModelMirror3of4 : public TBsCostModelBase {
public:
    TBsCostModelMirror3of4(NPDisk::EDeviceType deviceType)
        : TBsCostModelBase(deviceType)
    {}
};

TBsCostTracker::TBsCostTracker(const TBlobStorageGroupType& groupType, NPDisk::EDeviceType diskType,
        const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        const TCostMetricsParameters& costMetricsParameters)
    : GroupType(groupType)
    , MonGroup(std::make_shared<NMonGroup::TCostTrackerGroup>(counters, "subsystem", "advancedCost"))
    , BurstThresholdNs(costMetricsParameters.BurstThresholdNs)
    , DiskTimeAvailableScale(costMetricsParameters.DiskTimeAvailableScale)
    , Bucket(GetBucketCapacity(), 1'000'000'000, costMetricsParameters.TimeProvider)
{
    switch (GroupType.GetErasure()) {
    case TBlobStorageGroupType::ErasureMirror3dc:
        CostModel = std::make_unique<TBsCostModelMirror3dc>(diskType);
        break;
    case TBlobStorageGroupType::Erasure4Plus2Block:
        CostModel = std::make_unique<TBsCostModel4Plus2Block>(diskType);
        break;
    case TBlobStorageGroupType::ErasureMirror3of4:
        CostModel = std::make_unique<TBsCostModelMirror3of4>(diskType);
        break;
    default:
        CostModel = std::make_unique<TBsCostModelErasureNone>(diskType);
        break;
    }
}

ui64 TBsCostTracker::GetBucketCapacity() const {
    return static_cast<ui64>(BurstThresholdNs) * static_cast<ui64>(DiskTimeAvailableScale) / 1'000;
}

void TBsCostTracker::CountRequest(ui64 cost) {
    Bucket.Take(cost);
}

void TBsCostTracker::UpdatePDiskParameters(ui32 numOwners, ui32 expectedSlotCount) {
    const ui64 totalTime = 1'000'000 * static_cast<ui64>(DiskTimeAvailableScale);
    const ui64 diskTimeAvailable = totalTime / numOwners;
    Bucket.SetReplenishmentRate(diskTimeAvailable);
    MonGroup->DiskTimeAvailableCtr() = static_cast<i64>(diskTimeAvailable);
    if (expectedSlotCount > 0) {
        MonGroup->DiskTimeFairShareNs() = static_cast<i64>(totalTime / expectedSlotCount);
    }
}

void TBsCostTracker::PublishBurstCounters() {
    Bucket.SetCapacity(GetBucketCapacity());
    const auto [greenNs, redNs] = Bucket.OverflowStats();
    const ui64 greenMs = greenNs / 1'000'000;
    const ui64 redMs = redNs / 1'000'000;
    MonGroup->BurstDetector_greenMs() += static_cast<i64>(greenMs - PublishedGreenMs);
    MonGroup->BurstDetector_redMs() += static_cast<i64>(redMs - PublishedRedMs);
    PublishedGreenMs = greenMs;
    PublishedRedMs = redMs;
}

ui64 TBsCostModelBase::GetCost(const NPDisk::TEvChunkRead& ev) const {
    return ReadCost(ev.Size);
}

ui64 TBsCostModelBase::GetCost(const NPDisk::TEvChunkWrite& ev) const {
    if (ev.PriorityClass == NPriPut::Log) {
        return WriteCost(ev.PartsPtr->ByteSize());
    } else {
        return HugeWriteCost(ev.PartsPtr->ByteSize());
    }
}

} // NKikimr
