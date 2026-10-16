#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/system/event_count.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TNotifyManager
{
public:
    TNotifyManager(
        TIntrusivePtr<TEventCount> eventCount,
        const NProfiling::TTagSet& counterTagSet,
        TDuration pollingPeriod);

    TCpuInstant ResetMinEnqueuedAt();

    TCpuInstant UpdateMinEnqueuedAt(TCpuInstant newMinEnqueuedAt);

    void NotifyFromInvoke(TCpuInstant cpuInstant, bool force);

    // Must be called after DoCancelWait.
    void NotifyAfterFetch(TCpuInstant cpuInstant, TCpuInstant newMinEnqueuedAt);

    void Wait(TEventCount::TCookie cookie, std::function<bool()> isStopping);

    void CancelWait();

    TEventCount* GetEventCount();

    void SetPollingPeriod(TDuration pollingPeriod);

private:
    static constexpr TCpuInstant UnlockedNotifyInstant = 0;
    static constexpr TCpuInstant SentinelMinEnqueuedAtInstant = std::numeric_limits<TCpuInstant>::max();

    const TIntrusivePtr<TEventCount> EventCount_;
    const NProfiling::TCounter WakeupCounter_;
    const NProfiling::TCounter WakeupByTimeoutCounter_;

    std::atomic<TDuration> PollingPeriod_;
    std::atomic<TCpuInstant> NotifyInstant_ = UnlockedNotifyInstant;
    std::atomic<bool> PollingWaiterLock_ = false;
    std::atomic<TCpuInstant> MinEnqueuedAtInstant_ = SentinelMinEnqueuedAtInstant;

    // Returns true if was locked.
    bool UnlockNotifies();

    void NotifyOne(TCpuInstant cpuInstant);

    TCpuInstant GetMinEnqueuedAt() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
