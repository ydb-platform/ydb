#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/threading/event_count.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TNotifyManager
{
public:
    TNotifyManager(
        TIntrusivePtr<NThreading::TEventCount> eventCount,
        const NProfiling::TTagSet& counterTagSet,
        TDuration pollingPeriod);

    TCpuInstant ResetMinEnqueuedAt();

    TCpuInstant UpdateMinEnqueuedAt(TCpuInstant newMinEnqueuedAt);

    void NotifyFromInvoke(TCpuInstant cpuInstant, bool force);

    // Must be called after DoCancelWait.
    void NotifyAfterFetch(TCpuInstant cpuInstant, TCpuInstant newMinEnqueuedAt);

    void Wait(NThreading::TEventCount::TCookie cookie, std::function<bool()> isStopping);

    void CancelWait();

    NThreading::TEventCount* GetEventCount();

private:
    static constexpr TCpuInstant SentinelMinEnqueuedAt = std::numeric_limits<TCpuInstant>::max();

    const TIntrusivePtr<NThreading::TEventCount> EventCount_;
    const NProfiling::TCounter WakeupCounter_;
    const NProfiling::TCounter WakeupByTimeoutCounter_;
    const TDuration PollingPeriod_;

    std::atomic<bool> NotifyLock_ = false;
    // LockedInstant is used for debug and check purpose.
    std::atomic<TCpuInstant> LockedInstant_ = 0;
    std::atomic<bool> PollingWaiterLock_ = false;
    std::atomic<TCpuInstant> MinEnqueuedAt_ = SentinelMinEnqueuedAt;

    // Returns true if was locked.
    bool UnlockNotifies();

    void NotifyOne(TCpuInstant cpuInstant);

    TCpuInstant GetMinEnqueuedAt() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
