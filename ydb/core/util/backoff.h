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
    TDuration Next();
    void Reset();
};

// exponential backoff with jitter and the iteration counter
struct TBackoff {
    static constexpr TDuration DefaultInitialDelay = TDuration::Seconds(1);
    static constexpr TDuration DefaultMaxDelay = TDuration::Minutes(15);

    // backoff with unlimited retries
    explicit TBackoff(TDuration initialDelay = DefaultInitialDelay, TDuration maxDelay = DefaultMaxDelay);
    // backoff with limited retries
    explicit TBackoff(size_t maxRetries, TDuration initialDelay = DefaultInitialDelay, TDuration maxDelay = DefaultMaxDelay);

    // current retry iteration
    size_t GetIteration() const;
    // checks if there are more retries left
    bool HasMore() const;
    // next retry delay
    TDuration Next();
    void Reset();

    explicit operator bool() const;

private:
    TBackoffTimer Timer;
    size_t MaxRetries;
    size_t Iteration;
};

} // NKikimr
