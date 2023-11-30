#include "blobstorage_pdisk_util_flightcontrol.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFlightControl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TFlightControl::TFlightControl(ui64 bits)
    : BeginIdx(1)
    , MaxInFlightIdx(0)
    , EndIdx(1)
    , MaxSize(1ull << bits)
    , Mask(~((~0ull) << bits))
    , IsCompleteLoop(1ull << bits)
{
    Y_ABORT_UNLESS(bits > 0 && bits < 16);
}

// Returns 0 in case of scheduling error
// Operation Idx otherwise
// May sometimes return 0 when it already can schedule
ui64 TFlightControl::TrySchedule() {
    ui64 newMaxInFlightIdx = 0;
    bool isDone = false;
    while (!isDone) {
        ui64 beginIdx = AtomicGet(BeginIdx);
        ui64 prevMaxInFlightIdx = AtomicGet(MaxInFlightIdx);
        if (prevMaxInFlightIdx >= beginIdx) {
            bool isFull = (prevMaxInFlightIdx - beginIdx + 1 >= MaxSize);
            if (isFull) {
                return 0;
            }
        }
        newMaxInFlightIdx = prevMaxInFlightIdx + 1;
        isDone = AtomicCas(&MaxInFlightIdx, newMaxInFlightIdx, prevMaxInFlightIdx);
    }
    return newMaxInFlightIdx;
}

// Blocking Schedule method
ui64 TFlightControl::Schedule(double& blockedMs) {
    NHPTimer::STime beginTime = 0;
    while (true) {
        ui64 idx = TrySchedule();
        if (idx) {
            return idx;
        } else {
            TGuard<TMutex> guard(ScheduleMutex);
            idx = TrySchedule();
            if (idx) {
                return idx;
            }
            if (beginTime == 0) {
                beginTime = HPNow();
            }
            ScheduleCondVar.WaitI(ScheduleMutex);
            blockedMs = HPMilliSecondsFloat(HPNow() - beginTime);
        }
    }
}

void TFlightControl::WakeUp() {
    TGuard<TMutex> guard(ScheduleMutex);
    ScheduleCondVar.Signal();
}

void TFlightControl::MarkComplete(ui64 idx) {
    ui64 beginIdx = AtomicGet(BeginIdx);
    Y_ABORT_UNLESS(idx >= beginIdx);
    Y_ABORT_UNLESS(idx < beginIdx + MaxSize);
    if (idx == beginIdx) {
        // It's the first item we are waiting for
        if (beginIdx == EndIdx) {
            // The loop was empty, just move both begin and end
            AtomicIncrement(BeginIdx);
            ++EndIdx;
            WakeUp();
            return;
        }
        // The loop was not empty, move begin forward once, then skip all the complete items
        ++beginIdx;
        while (beginIdx < EndIdx && IsCompleteLoop[beginIdx & Mask]) {
            ++beginIdx;
        }
        AtomicSet(BeginIdx, beginIdx);
        WakeUp();
        return;
    }
    // It's not the first item
    if (idx >= EndIdx) {
        for (ui64 i = EndIdx; i < idx; ++i) {
            IsCompleteLoop[i & Mask] = false;
        }
        EndIdx = idx + 1;
    }
    IsCompleteLoop[idx & Mask] = true;
}

ui64 TFlightControl::FirstIncompleteIdx() {
    return AtomicGet(BeginIdx);
}

} // NPDisk
} // NKikimr
