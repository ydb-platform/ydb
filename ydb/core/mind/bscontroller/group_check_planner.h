#pragma once

#include <list>
#include <utility>

#include <util/datetime/base.h>
#include <ydb/core/base/blobstorage_common.h>
 
namespace NKikimr {

struct TNextScanPlan {
public:
    TGroupId GroupId;
    TMonotonic Timestamp;
};  

// TGroupCheckPlanner gives timestamps when BlobCheckerOrchestrator is allowed to start scan routine
class TGroupCheckPlanner {
public:
    TGroupCheckPlanner(TDuration periodicity, ui32 groupCount);

    void EnqueueGroupScan(TGroupId groupId);
    void DequeueGroupScan(TGroupId groupId);

    std::optional<TNextScanPlan> PlanNextScan();

    void SetGroupCount(ui32 groupCount);
    void SetPeriodicity(TDuration newPeriodicity);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

} // namespace NKikimr
