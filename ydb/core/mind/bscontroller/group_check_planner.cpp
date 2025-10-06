#include "scan_throttler.h"

#include <ydb/core/util/fast_lookup_unique_list.h>
 
namespace NKikimr {

/////////////////////////////////////////////////////////////////////////////////////
/// TGroupCheckPlanner implementation

class TGroupCheckPlanner::TImpl {
public:
    TGroupCheckPlanner::TImpl(TDuration periodicity, ui32 groupCount)
        : Periodicity(periodicity)
        , GroupCount(groupCount)
        , LastPlannedTimestamp(TMonotonic::Zero())
    {}

    void EnqueueGroupScan(TGroupId groupId);
    void DequeueGroupScan(TGroupId groupId);

    // returns timestamp of the next allowed operation
    std::optional<TNextScanPlan> PlanNextScan();

    void SetGroupCount(ui32 groupCount) {
        GroupCount = groupCount;
    }

    void SetPeriodicity(TDuration newPeriodicity) {
        Periodicity = newPeriodicity;
        TimeWasted = std::max(TimeWasted, Periodicity);
    }

private:

    TDuration GetNextAllowedScanTime() {
        TMonotonic now = TActivationContext::Monotonic();
        auto [delay, acceleratedTime] = GetAdjustedDelay();
        if (LastPlannedTimestamp + delay < now) {
            // last planned request was too long ago, allow next scan right now
            // and accelerate further requests to compensate wasted time
            TDuration wasted = now - (LastPlannedTimestamp + delay);
            TimeWasted = std::max(Periodicity, TimeWasted + wasted);
            LastPlannedTimestamp = now;
        } else {
            LastPlannedTimestamp += delay;
            if (TimeWasted > acceleratedTime) {
                TimeWasted -= acceleratedTime;
            } else {
                TimeWasted = TDuration::Zero();
            }
        }
        return LastPlannedTimestamp;
    }

    std::pair<TDuration, TDuration> GetAdjustedDelay() {
        TDuration targetDelay = GetTargetDelay();
        float accelerationRatio = TimeWasted / Periodicity + 1;
        TDuration adjustedDelay = targetDelay / accelerationRatio;
        return std::make_pair<TDuration, TDuration>(adjustedDelay, targetDelay - adjustedDelay);
    }

    TDuration GetTargetDelay() {
        return Periodicity / GroupCount;
    }

private:
    ui32 GroupCount;
    TDuration Periodicity;

    TMonotonic LastPlannedTimestamp;
    TDuration TimeWasted;

    TFastLookupUniqueList<TGroupId> GroupsAllowedToCheck;
    std::unordered_map<ui32, std::list<TGroupId>> LockedNodes;
};

TGroupCheckPlanner::TGroupCheckPlanner(TDuration periodicity, ui32 groupCount)
    

TMonotonic TGroupCheckPlanner::GetNextAllowedScanTime() 

void TGroupCheckPlanner::SetGroupCount(ui32 groupCount) 

void TGroupCheckPlanner::SetPeriodicity(TDuration newPeriodicity) 

std::pair<TDuration, TDuration> TGroupCheckPlanner::GetAdjustedDelay() 

TDuration TGroupCheckPlanner::GetTargetDelay() 


/////////////////////////////////////////////////////////////////////////////////////

TGroupCheckPlanner::TGroupCheckPlanner(TDuration periodicity, ui32 groupCount)
    : Impl(new TGroupCheckPlanner::TImpl(periodicity, groupCount))
{}

void TGroupCheckPlanner::EnqueueGroupScan(TGroupId groupId) {
    Impl->EnqueueGroupScan(groupId);
}

void TGroupCheckPlanner::DequeueGroupScan(TGroupId groupId) {
    Impl->DequeueGroupScan(groupId);
}

std::optional<TNextScanPlan> TGroupCheckPlanner::PlanNextScan() {
    return Impl->PlanNextScan();
}

void TGroupCheckPlanner::SetGroupCount(ui32 groupCount) {
    Impl->SetGroupCount(groupCount);
}
void TGroupCheckPlanner::SetPeriodicity(TDuration newPeriodicity) {
    Impl->SetPeriodicity(newPeriodicity);
}

} // namespace NKikimr
