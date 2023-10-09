#include "backoff.h"
#include <util/generic/utility.h>
#include <util/system/yassert.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBackoffTimer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

TBackoffTimer::TBackoffTimer(ui64 initialMs, ui64 maxMs)
    : InitialBackoffMs(initialMs)
    , MaxBackoffMs(maxMs)
    , CurrentBackoffMs(0ull)
    , PreviousBackoffMs(0ull)
{
    Y_ABORT_UNLESS(initialMs <= maxMs);
}

ui64 TBackoffTimer::NextBackoffMs() {
    if (CurrentBackoffMs < InitialBackoffMs) {
        CurrentBackoffMs = InitialBackoffMs;
        PreviousBackoffMs = 0ull;
    } else {
        ui64 prevMs = CurrentBackoffMs;
        CurrentBackoffMs += PreviousBackoffMs;
        PreviousBackoffMs = prevMs;
    }
    CurrentBackoffMs = Min(CurrentBackoffMs, MaxBackoffMs);
    return CurrentBackoffMs;
}

void TBackoffTimer::Reset() {
    CurrentBackoffMs = 0ull;
    PreviousBackoffMs = 0ull;
}

} // NKikimr
