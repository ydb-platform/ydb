#include "blobstorage_pdisk_util_flightcontrol.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFlightControl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TFlightControl::TFlightControl(ui64 maxInFlightRequests, ui64 inFlightBytesLimit)
    : BeginIdx(1)
    , MaxInFlightIdx(0)
    , EndIdx(1)
    , MaxSize(maxInFlightRequests)
    , Mask(maxInFlightRequests - 1)
    , IsCompleteLoop(maxInFlightRequests)
{
    Y_UNUSED(inFlightBytesLimit);
    Y_VERIFY(maxInFlightRequests > 0 && maxInFlightRequests < (1ull << 16));
    Y_VERIFY((maxInFlightRequests & (maxInFlightRequests - 1)) == 0);
}

void TFlightControl::Initialize(const TString& logPrefix) {
    PDiskLogPrefix = logPrefix;
}

// Returns 0 in case of scheduling error
// Operation Idx otherwise
// May sometimes return 0 when it already can schedule
ui64 TFlightControl::TrySchedule(ui64 size) {
    Y_UNUSED(size);

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
ui64 TFlightControl::Schedule(double& blockedMs, ui64 size) {
    NHPTimer::STime beginTime = 0;
    while (true) {
        ui64 idx = TrySchedule(size);
        if (idx) {
            return idx;
        } else {
            TGuard<TMutex> guard(ScheduleMutex);
            idx = TrySchedule(size);
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

void TFlightControl::MarkComplete(ui64 idx, ui64 size) {
    Y_UNUSED(size);

    ui64 beginIdx = AtomicGet(BeginIdx);
    Y_VERIFY_S(idx >= beginIdx, PDiskLogPrefix);
    Y_VERIFY_S(idx < beginIdx + MaxSize, PDiskLogPrefix);
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBytesFlightControl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
TBytesFlightControl::TBytesFlightControl(ui64 inFlightRequestsLimit, ui64 inFlightBytesLimit)
    : InFlightRequestsLimit(inFlightRequestsLimit)
    , InFlightBytesLimit(inFlightBytesLimit)
    , CachedFirstIncompleteIdx(1)
    , NextScheduleIdx(1)
    , InFlightRequests(0)
    , InFlightBytes(0)
    , FirstIncompleteIdxValue(1)
{
    Y_VERIFY(inFlightRequestsLimit > 0);
    Y_VERIFY(inFlightBytesLimit > 0);
}

void TBytesFlightControl::Initialize(const TString& logPrefix) {
    PDiskLogPrefix = logPrefix;
}

ui64 TBytesFlightControl::TryScheduleLocked(ui64 size) {
    if (InFlightRequests >= InFlightRequestsLimit) {
        return 0;
    }
    size = std::min(size, InFlightBytesLimit);

    if (InFlightBytes + size > InFlightBytesLimit) {
        //Cerr << "reject because " << InFlightBytes << " >= " << InFlightBytesLimit << " size# " << size
        //    << " InFlightRequests# " << InFlightRequests << Endl;
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
ui64 TBytesFlightControl::TrySchedule(ui64 size) {
    TGuard<TMutex> guard(ScheduleMutex);
    return TryScheduleLocked(size);
}

// Blocking Schedule method
ui64 TBytesFlightControl::Schedule(double& blockedMs, ui64 size) {
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

void TBytesFlightControl::MarkComplete(ui64 idx, ui64 size) {
    TGuard<TMutex> guard(ScheduleMutex);
    size = std::min(size, InFlightBytesLimit);

    Y_VERIFY_S(idx >= FirstIncompleteIdxValue, PDiskLogPrefix << " idx# " << idx
            << " FirstIncompleteIdxValue# " << FirstIncompleteIdxValue);
    Y_VERIFY_S(InFlightRequests > 0, PDiskLogPrefix);
    Y_VERIFY_S(InFlightBytes >= size, PDiskLogPrefix << " InFlightBytes# " << InFlightBytes
            << " size# " << size);

    --InFlightRequests;
    InFlightBytes -= size;

    CompletedIdx.push(idx);
    while (!CompletedIdx.empty() && CompletedIdx.top() == FirstIncompleteIdxValue) {
        ++FirstIncompleteIdxValue;
        CompletedIdx.pop();
    }

    AtomicSet(CachedFirstIncompleteIdx, FirstIncompleteIdxValue);
    ScheduleCondVar.Signal();
}

ui64 TBytesFlightControl::FirstIncompleteIdx() {
    return AtomicGet(CachedFirstIncompleteIdx);
}

} // NPDisk
} // NKikimr
