#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TBackoffTimer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
class TBackoffTimer {
    const ui64 InitialBackoffMs;
    const ui64 MaxBackoffMs;
    ui64 CurrentBackoffMs;
    ui64 PreviousBackoffMs;

public:
    TBackoffTimer(ui64 initialMs, ui64 maxMs);
    ui64 NextBackoffMs();
    TDuration Next();
    void Reset();
};


struct TBackoff {
    static constexpr TDuration DefaultInitialDelay = TDuration::Seconds(1);
    static constexpr TDuration DefaultMaxDelay = TDuration::Minutes(15);

    TBackoff(TDuration initialDelay = DefaultInitialDelay, TDuration maxDelay = DefaultMaxDelay);
    TBackoff(size_t maxRetries, TDuration initialDelay = DefaultInitialDelay, TDuration maxDelay = DefaultMaxDelay);

    size_t GetIteration() const;
    bool HasMore() const;
    TDuration Next();
    void Reset();

    explicit operator bool() const;

private:
    TBackoffTimer Timer;
    const size_t MaxRetries;
    size_t Iteration;
};


} // NKikimr
