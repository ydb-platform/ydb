#include "blobstorage_cost_tracker.h"

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
    , CostCounters(counters->GetSubgroup("subsystem", "advancedCost"))
    , MonGroup(std::make_shared<NMonGroup::TCostTrackerGroup>(CostCounters))
    , Bucket(&DiskTimeAvailable, &BucketCapacity, nullptr, nullptr, nullptr, nullptr, true)
    , BurstThresholdNs(costMetricsParameters.BurstThresholdNs)
    , DiskTimeAvailableScale(costMetricsParameters.DiskTimeAvailableScale)
{
    AtomicSet(BucketCapacity, GetDiskTimeAvailableScale() * BurstThresholdNs);
    BurstDetector.Initialize(CostCounters, "BurstDetector");
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

} // NKikimr
