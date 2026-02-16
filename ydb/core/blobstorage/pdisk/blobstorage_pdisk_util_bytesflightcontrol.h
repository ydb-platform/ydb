#pragma once
#include "defs.h"
#include "blobstorage_pdisk_mon.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/queue.h>
#include <util/generic/vector.h>
#include <util/system/condvar.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBytesFlightControl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBytesFlightControl {
    struct TMinHeapCompare {
        bool operator()(ui64 lhs, ui64 rhs) const {
            return lhs > rhs;
        }
    };

    const ui64 InFlightRequestsLimit;
    const ui64 InFlightBytesLimit;

    TAtomic CachedFirstIncompleteIdx;
    ui64 NextScheduleIdx;
    ui64 InFlightRequests;
    ui64 InFlightBytes;
    ui64 FirstIncompleteIdxValue;
    TPriorityQueue<ui64, TVector<ui64>, TMinHeapCompare> CompletedIdx;

    TMutex ScheduleMutex;
    TCondVar ScheduleCondVar;
    TString PDiskLogPrefix;

    ui64 TryScheduleLocked(ui64 size);

public:
    static constexpr ui64 DefaultInFlightBytesLimit = 1ull << 20; // 1 MiB

    TBytesFlightControl(ui64 inFlightRequestsLimit, ui64 inFlightBytesLimit = DefaultInFlightBytesLimit);

    void Initialize(const TString& logPrefix);

    // Returns 0 in case of scheduling error
    // Operation Idx otherwise
    // May sometimes return 0 when it already can schedule
    ui64 TrySchedule(ui64 size);

    // Blocking version of TrySchedule
    ui64 Schedule(double& blockedMs, ui64 size);

    void MarkComplete(ui64 idx, ui64 size);
    ui64 FirstIncompleteIdx();
};

} // NPDisk
} // NKikimr
