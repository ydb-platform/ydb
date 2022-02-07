#include "runtime_data.h"

namespace NYql {

void TWorkerRuntimeData::AddRusageDelta(const TRusage& delta) {
    auto now = TInstant::Now();
    TGuard<TAdaptiveLock> guard(Lock);
    if (!LastUpdate) {
        Rusages[Index] = delta;
    } else {
        auto seconds = (now - LastUpdate).Seconds();
        // Current : Rusage[Index+seconds]
        // Prev: Rusage[Index..Index+seconds-1]
        for (ui32 i = 1; i <= seconds; i=i+1) {
            auto prev = Rusages[(Index + i - 1) % Rusages.size()];
            Rusages[(Index + i) % Rusages.size()] = prev;
        }
        Index = (Index + seconds) % Rusages.size();
        if (!RusageFull && (Index + 1) == static_cast<int>(Rusages.size())) {
            RusageFull = true;
        }
        Rusages[Index].Stime += delta.Stime;
        Rusages[Index].Utime += delta.Utime;
        Rusages[Index].MajorPageFaults += delta.MajorPageFaults;
    }
    LastUpdate = TInstant::Seconds(now.Seconds());
}

TRusage TWorkerRuntimeData::GetRusageDelta() {
    TGuard<TAdaptiveLock> guard(Lock);
    TRusage cur, prev;
    cur = Rusages[Index];
    if (RusageFull) {
        prev = Rusages[(Index + 1) % Rusages.size()];
    } else {
        prev = Rusages[0];
    }
    cur.Stime -= prev.Stime;
    cur.Utime -= prev.Utime;
    cur.MajorPageFaults -= prev.MajorPageFaults;
    return cur;
}

TRusage TWorkerRuntimeData::GetRusage() {
    TGuard<TAdaptiveLock> guard(Lock);
    return Rusages[Index];
}

TDuration TWorkerRuntimeData::GetRusageWindow() {
    if (RusageFull) {
        return TDuration::Seconds(WindowSize);
    } else {
        TGuard<TAdaptiveLock> guard(Lock);
        return Max(TDuration::Seconds(1), TDuration::Seconds(Index));
    }
}

void TWorkerRuntimeData::OnWorkerStart(const TString& operationId) {
    TGuard<TAdaptiveLock> guard(WorkersLock);
    RunningWorkers[operationId]++;
}

void TWorkerRuntimeData::OnWorkerStop(const TString& operationId) {
    TGuard<TAdaptiveLock> guard(WorkersLock);
    auto it = RunningWorkers.find(operationId);
    if (it != RunningWorkers.end() &&  ! --it->second) {
        RunningWorkers.erase(it);
    }
}

THashMap<TString,int> TWorkerRuntimeData::GetRunningWorkers() const {
    TGuard<TAdaptiveLock> guard(WorkersLock);
    return RunningWorkers;
}

void TWorkerRuntimeData::UpdateChannelInputDelay(TDuration duration) {
    Y_UNUSED(duration);
}

void TWorkerRuntimeData::UpdateChannelOutputDelay(TDuration duration) {
    Y_UNUSED(duration);
}

} // namespace NYql
