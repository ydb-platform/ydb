#include "backoff.h"
#include <util/generic/utility.h>
#include <util/random/random.h>
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

TBackoff::TBackoff(TDuration initialDelay, TDuration maxDelay)
    : TBackoff(-1ull, initialDelay, maxDelay)
{
}

TBackoff::TBackoff(size_t maxRetries, TDuration initialDelay, TDuration maxDelay)
    : Timer(initialDelay.MilliSeconds(), maxDelay.MilliSeconds())
    , MaxRetries(maxRetries)
    , Iteration(0)
{
}

bool TBackoff::HasMore() const {
    return Iteration < MaxRetries;
}

TDuration TBackoff::Next() {
    auto delay = Timer.NextBackoffMs();
    delay += RandomNumber<size_t>(delay / 4);
    return TDuration::MilliSeconds(delay);
}

void TBackoff::Reset() {
    Timer.Reset();
    Iteration = 0;
}

TBackoff::operator bool() const {
    return HasMore();
}


} // NKikimr
