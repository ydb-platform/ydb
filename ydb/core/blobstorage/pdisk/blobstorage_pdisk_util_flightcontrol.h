#pragma once
#include "defs.h"
#include <util/generic/vector.h>
#include <library/cpp/deprecated/atomic/atomic.h>
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

    void WakeUp();

public:
    TFlightControl(ui64 bits);

    // Returns 0 in case of scheduling error
    // Operation Idx otherwise
    // May sometimes return 0 when it already can schedule
    ui64 TrySchedule();

    // Blocking version of TrySchedule
    ui64 Schedule();

    void MarkComplete(ui64 idx);
    ui64 FirstIncompleteIdx();
};

} // NPDisk
} // NKikimr
