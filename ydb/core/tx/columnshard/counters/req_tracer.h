#pragma once
#include "common/owner.h"
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NColumnShard {

class TRequestsTracerCounters: public TCommonCountersOwner {
private:
    using TBase = TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr RequestedMinSnapshotAge;
    NMonitoring::TDynamicCounters::TCounterPtr DefaultMinSnapshotAge;
    NMonitoring::TDynamicCounters::TCounterPtr SnapshotsCount;
    NMonitoring::TDynamicCounters::TCounterPtr SnapshotLock;
    NMonitoring::TDynamicCounters::TCounterPtr SnapshotUnlock;

public:

    TRequestsTracerCounters()
        : TBase("cs_requests_tracing")
        , RequestedMinSnapshotAge(TBase::GetValue("Snapshots/RequestedAge/Seconds"))
        , DefaultMinSnapshotAge(TBase::GetValue("Snapshots/DefaultAge/Seconds"))
        , SnapshotsCount(TBase::GetValue("Snapshots/Count"))
        , SnapshotLock(TBase::GetDeriviative("Snapshots/Lock"))
        , SnapshotUnlock(TBase::GetDeriviative("Snapshots/Unlock"))
    {

    }

    void OnDefaultMinSnapshotInstant(const TInstant instant) const {
        DefaultMinSnapshotAge->Set((TInstant::Now() - instant).Seconds());
    }

    void OnSnapshotsInfo(const ui32 count, const std::optional<NOlap::TSnapshot> snapshotPlanStep) const {
        if (snapshotPlanStep) {
            RequestedMinSnapshotAge->Set((TInstant::Now() - snapshotPlanStep->GetPlanInstant()).Seconds());
        } else {
            RequestedMinSnapshotAge->Set(0);
        }
        SnapshotsCount->Set(count);
        
    }

    void OnSnapshotLocked() const {
        SnapshotLock->Add(1);
    }
    void OnSnapshotUnlocked() const {
        SnapshotUnlock->Add(1);
    }
};

}
