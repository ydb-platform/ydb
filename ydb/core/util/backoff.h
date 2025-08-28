#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBackoffTimer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBackoffTimer {
    ui64 InitialBackoffMs;
    ui64 MaxBackoffMs;
    ui64 CurrentBackoffMs;
    ui64 PreviousBackoffMs;

public:
    TBackoffTimer(ui64 initialMs, ui64 maxMs);
    ui64 NextBackoffMs();
    void Reset();
};


struct TBackoff {

    TBackoff(TDuration initialDelay = TDuration::Seconds(1), TDuration maxDelay = TDuration::Minutes(15));
    TBackoff(size_t maxRetries, TDuration initialDelay = TDuration::Seconds(1), TDuration maxDelay = TDuration::Minutes(15));

    bool HasMore() const;
    TDuration Next();
    void Reset();

    explicit operator bool() const;

    const size_t MaxRetries;
    const TDuration InitialDelay;
    const TDuration MaxDelay;
    size_t Iteration;
};


} // NKikimr
