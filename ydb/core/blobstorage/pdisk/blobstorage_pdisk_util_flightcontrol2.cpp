#include "blobstorage_pdisk_util_flightcontrol2.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFlightControl2
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TFlightControl2::TFlightControl2(ui64 maxInFlightRequests, ui64 maxInFlightBytes)
    : MaxInFlightRequests(maxInFlightRequests)
    , MaxInFlightBytes(maxInFlightBytes)
    , CachedFirstIncompleteIdx(1)
    , NextScheduleIdx(1)
    , InFlightRequests(0)
    , InFlightBytes(0)
    , FirstIncompleteIdxValue(1)
{
    Y_VERIFY(maxInFlightRequests > 0);
    Y_VERIFY(maxInFlightBytes > 0);
}

void TFlightControl2::Initialize(const TString& logPrefix) {
    PDiskLogPrefix = logPrefix;
}

ui64 TFlightControl2::TryScheduleLocked(ui64 size) {
    if (InFlightRequests >= MaxInFlightRequests) {
        return 0;
    }
    if (InFlightBytes > MaxInFlightBytes - size) {
        return 0;
    }

    const ui64 idx = NextScheduleIdx++;
    ++InFlightRequests;
    InFlightBytes += size;
    return idx;
}

// Returns 0 in case of scheduling error
// Operation Idx otherwise
// May sometimes return 0 when it already can schedule
ui64 TFlightControl2::TrySchedule(ui64 size) {
    Y_VERIFY_S(size <= MaxInFlightBytes, PDiskLogPrefix << " size# " << size
            << " MaxInFlightBytes# " << MaxInFlightBytes);

    TGuard<TMutex> guard(ScheduleMutex);
    return TryScheduleLocked(size);
}

// Blocking Schedule method
ui64 TFlightControl2::Schedule(double& blockedMs, ui64 size) {
    Y_VERIFY_S(size <= MaxInFlightBytes, PDiskLogPrefix << " size# " << size
            << " MaxInFlightBytes# " << MaxInFlightBytes);

    NHPTimer::STime beginTime = 0;
    TGuard<TMutex> guard(ScheduleMutex);
    while (true) {
        if (ui64 idx = TryScheduleLocked(size)) {
            return idx;
        }
        if (beginTime == 0) {
            beginTime = HPNow();
        }
        ScheduleCondVar.WaitI(ScheduleMutex);
        blockedMs = HPMilliSecondsFloat(HPNow() - beginTime);
    }
}

void TFlightControl2::MarkComplete(ui64 idx, ui64 size) {
    TGuard<TMutex> guard(ScheduleMutex);

    Y_VERIFY_S(idx >= FirstIncompleteIdxValue, PDiskLogPrefix << " idx# " << idx
            << " FirstIncompleteIdxValue# " << FirstIncompleteIdxValue);
    Y_VERIFY_S(InFlightRequests > 0, PDiskLogPrefix);
    Y_VERIFY_S(InFlightBytes >= size, PDiskLogPrefix << " InFlightBytes# " << InFlightBytes
            << " size# " << size);

    --InFlightRequests;
    InFlightBytes -= size;

    if (idx == FirstIncompleteIdxValue) {
        ++FirstIncompleteIdxValue;
    } else {
        CompletedIdx.push(idx);
    }

    // Fold contiguous completed requests into the new first incomplete request index.
    while (!CompletedIdx.empty()) {
        const ui64 completed = CompletedIdx.top();
        if (completed < FirstIncompleteIdxValue) {
            CompletedIdx.pop();
            continue;
        }
        if (completed > FirstIncompleteIdxValue) {
            break;
        }
        CompletedIdx.pop();
        ++FirstIncompleteIdxValue;
    }

    AtomicSet(CachedFirstIncompleteIdx, FirstIncompleteIdxValue);
    ScheduleCondVar.Signal();
}

ui64 TFlightControl2::FirstIncompleteIdx() {
    return AtomicGet(CachedFirstIncompleteIdx);
}

} // NPDisk
} // NKikimr
