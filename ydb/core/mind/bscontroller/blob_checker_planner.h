#pragma once

#include <list>
#include <utility>

#include <util/datetime/base.h>
#include <ydb/core/base/blobstorage_common.h>
 
namespace NKikimr {
namespace NBsController {

// TBlobCheckerPlanner gives timestamps when BlobCheckerOrchestrator is allowed to start check routine
class TBlobCheckerPlanner {
public:
    TBlobCheckerPlanner(TDuration periodicity, ui32 groupCount);
    ~TBlobCheckerPlanner();

    template <class TInfo>
    void EnqueueCheck(const TInfo* groupInfo);
    // returns whether there was planned check for given group
    bool DequeueCheck(TGroupId groupId);

    void ResetState();

    // at first we obtain timestamp when to start the next check
    TMonotonic GetNextAllowedCheckTimestamp(TMonotonic now);
    // and when it is time, we get groupId if there are groups available to check by the moment
    std::optional<TGroupId> ObtainNextGroupToCheck();

    void SetGroupCount(ui32 groupCount);
    void SetPeriodicity(TDuration newPeriodicity);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

} // namespace NBsController
} // namespace NKikimr
