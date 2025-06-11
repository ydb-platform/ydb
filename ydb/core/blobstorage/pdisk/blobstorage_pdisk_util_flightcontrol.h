#pragma once
#include "defs.h"
#include "blobstorage_pdisk_mon.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/vector.h>
#include <util/system/condvar.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TFlightControl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TFlightControl {
    TAtomic BeginIdx;
    TAtomic MaxInFlightIdx;
    ui64 EndIdx;
    ui64 MaxSize;
    ui64 Mask;
    TVector<bool> IsCompleteLoop;
    TMutex ScheduleMutex;
    TCondVar ScheduleCondVar;
    TString PDiskLogPrefix;

    void WakeUp();

public:
    TFlightControl(ui64 bits);

    void Initialize(const TString& logPrefix);

    // Returns 0 in case of scheduling error
    // Operation Idx otherwise
    // May sometimes return 0 when it already can schedule
    ui64 TrySchedule();

    // Blocking version of TrySchedule
    ui64 Schedule(double& blockedMs);

    void MarkComplete(ui64 idx);
    ui64 FirstIncompleteIdx();
};

} // NPDisk
} // NKikimr
