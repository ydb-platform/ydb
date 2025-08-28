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

namespace {
constexpr TDuration MinDelay = TDuration::MilliSeconds(5);
constexpr size_t MaxShift = sizeof(TDuration::TValue) * 8 - 1;
}

TBackoff::TBackoff(TDuration initialDelay, TDuration maxDelay)
    : TBackoff(1ull << 63, initialDelay, maxDelay)
{
}

TBackoff::TBackoff(size_t maxRetries, TDuration initialDelay, TDuration maxDelay)
    : MaxRetries(maxRetries)
    , InitialDelay(std::max(MinDelay, initialDelay))
    , MaxDelay(std::max(InitialDelay, maxDelay))
    , Iteration(0)
{
}

bool TBackoff::HasMore() const {
    return Iteration < MaxRetries;
}

TDuration TBackoff::Next() {
    TDuration delay = TDuration::MilliSeconds(InitialDelay.MilliSeconds() << std::min(Iteration++, MaxShift));
    delay = delay == TDuration::Zero() ? MaxDelay : std::min(MaxDelay, delay);
    delay += TDuration::MilliSeconds(RandomNumber<size_t>(delay.MilliSeconds() / 4));
    return delay;
}

void TBackoff::Reset() {
    Iteration = 0;
}

TBackoff::operator bool() const {
    return HasMore();
}


} // NKikimr
