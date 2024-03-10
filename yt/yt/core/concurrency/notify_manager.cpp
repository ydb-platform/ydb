#include "notify_manager.h"
#include "private.h"

#define PERIODIC_POLLING

namespace NYT::NConcurrency {

static const auto& Logger = ConcurrencyLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr auto WaitLimit = TDuration::MicroSeconds(64);
constexpr auto WaitTimeWarningThreshold = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

TNotifyManager::TNotifyManager(
    TIntrusivePtr<NThreading::TEventCount> eventCount,
    const NProfiling::TTagSet& tagSet,
    TDuration pollingPeriod)
    : EventCount_(std::move(eventCount))
    , WakeupCounter_(NProfiling::TProfiler("/action_queue")
        .WithTags(tagSet)
        .WithHot()
        .Counter("/wakeup"))
    , WakeupByTimeoutCounter_(NProfiling::TProfiler("/action_queue")
        .WithTags(tagSet)
        .Counter("/wakeup_by_timeout"))
    , PollingPeriod_(pollingPeriod)
{ }

TCpuInstant TNotifyManager::GetMinEnqueuedAt() const
{
    return MinEnqueuedAt_.load(std::memory_order::acquire);
}

TCpuInstant TNotifyManager::UpdateMinEnqueuedAt(TCpuInstant newMinEnqueuedAt)
{
    auto minEnqueuedAt = MinEnqueuedAt_.load();

    while (newMinEnqueuedAt < minEnqueuedAt) {
        if (MinEnqueuedAt_.compare_exchange_weak(minEnqueuedAt, newMinEnqueuedAt)) {
            minEnqueuedAt = newMinEnqueuedAt;
            YT_VERIFY(minEnqueuedAt != SentinelMinEnqueuedAt);
            break;
        }
    }

    return minEnqueuedAt;
}

TCpuInstant TNotifyManager::ResetMinEnqueuedAt()
{
    // Disables notifies of already enqueued actions from invoke and
    // allows to set MinEnqueuedAt in NotifyFromInvoke for new actions.
    return MinEnqueuedAt_.exchange(SentinelMinEnqueuedAt);
}

void TNotifyManager::NotifyFromInvoke(TCpuInstant cpuInstant, bool force)
{
    auto minEnqueuedAt = GetMinEnqueuedAt();

    if (minEnqueuedAt == SentinelMinEnqueuedAt) {
        MinEnqueuedAt_.compare_exchange_strong(minEnqueuedAt, cpuInstant);
    }

    auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);
    bool needNotify = force || waitTime > WaitLimit;

    YT_LOG_TRACE("Notify from invoke (Force: %v, Decision: %v, WaitTime: %v, MinEnqueuedAt: %v)",
        force,
        needNotify,
        waitTime,
        CpuInstantToInstant(minEnqueuedAt));

    if (needNotify) {
        NotifyOne(cpuInstant);
    }
}

void TNotifyManager::NotifyAfterFetch(TCpuInstant cpuInstant, TCpuInstant newMinEnqueuedAt)
{
    auto minEnqueuedAt = UpdateMinEnqueuedAt(newMinEnqueuedAt);

    // If there are actions and wait time is small do not wakeup other threads.
    auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

    if (waitTime > WaitLimit) {
        YT_LOG_TRACE("Notify after fetch (WaitTime: %v, MinEnqueuedAt: %v)",
            waitTime,
            CpuInstantToInstant(minEnqueuedAt));

        NotifyOne(cpuInstant);
    }

    // Reset LockedInstant to suppress action stuck warnings in case of progress.
    LockedInstant_ = cpuInstant;
}

void TNotifyManager::Wait(NThreading::TEventCount::TCookie cookie, std::function<bool()> isStopping)
{
    if (UnlockNotifies()) {
        // We must call either Wait or CancelWait.
        EventCount_->CancelWait();
        return;
    }

#ifdef PERIODIC_POLLING
    // One waiter makes periodic polling.
    bool firstWaiter = !PollingWaiterLock_.exchange(true);
    if (firstWaiter) {
        while (true) {
            bool notified = EventCount_->Wait(cookie, PollingPeriod_);
            if (notified) {
                break;
            }

            // Check wait time.
            auto minEnqueuedAt = GetMinEnqueuedAt();
            auto cpuInstant = GetCpuInstant();
            auto waitTime = CpuDurationToDuration(cpuInstant - minEnqueuedAt);

            if (waitTime > WaitLimit) {
                YT_LOG_DEBUG("Wake up by timeout (WaitTime: %v, MinEnqueuedAt: %v)",
                    waitTime,
                    CpuInstantToInstant(minEnqueuedAt));

                WakeupByTimeoutCounter_.Increment();

                break;
            }

            cookie = EventCount_->PrepareWait();

            // We have to check stopping between Prepare and Wait.
            // If we check before PrepareWait stop can occur (and notify) after check and before prepare
            // wait. In this case we can miss it and go waiting.
            if (isStopping()) {
                EventCount_->CancelWait();
                break;
            }
        }

        PollingWaiterLock_.store(false);
    } else {
        EventCount_->Wait(cookie);
    }
#else
    Y_UNUSED(isStopping);
    Y_UNUSED(PollingPeriod);
    EventCount_->Wait(cookie);
#endif

    UnlockNotifies();

    WakeupCounter_.Increment();
}

void TNotifyManager::CancelWait()
{
    EventCount_->CancelWait();

    // TODO(lukyan): This logic can be moved into NotifyAfterFetch.
#ifdef PERIODIC_POLLING
    // If we got an action and PollingWaiterLock_ is not locked (no polling waiter) wake up other thread.
    if (!PollingWaiterLock_.load()) {
        EventCount_->NotifyOne();
    }
#endif
}

NThreading::TEventCount* TNotifyManager::GetEventCount()
{
    return EventCount_.Get();
}

// Returns true if was locked.
bool TNotifyManager::UnlockNotifies()
{
    return NotifyLock_.exchange(false);
}

void TNotifyManager::NotifyOne(TCpuInstant cpuInstant)
{
    if (!NotifyLock_.exchange(true)) {
        LockedInstant_ = cpuInstant;
        YT_LOG_TRACE("Notify futex (MinEnqueuedAt: %v)",
            CpuInstantToInstant(GetMinEnqueuedAt()));
        EventCount_->NotifyOne();
    } else {
        auto lockedInstant = LockedInstant_.load();
        auto waitTime = CpuDurationToDuration(cpuInstant - lockedInstant);
        if (waitTime > WaitTimeWarningThreshold) {
            // Notifications are locked during more than 30 seconds.
            YT_LOG_WARNING("Action is probably stuck (MinEnqueuedAt: %v, LockedInstant: %v, WaitTime: %v)",
                CpuInstantToInstant(GetMinEnqueuedAt()),
                lockedInstant,
                waitTime);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
