#include "invoker_alarm.h"
#include "delayed_executor.h"

#include <yt/yt/core/actions/invoker.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TInvokerAlarm::TInvokerAlarm(IInvokerPtr invoker)
    : Invoker_(std::move(invoker))
{
    VERIFY_INVOKER_THREAD_AFFINITY(Invoker_, HomeThread);
}

void TInvokerAlarm::Arm(TClosure callback, TInstant deadline)
{
    VERIFY_THREAD_AFFINITY(HomeThread);
    YT_VERIFY(callback);

    auto epoch = ++Epoch_;
    Callback_ = std::move(callback);
    Deadline_ = deadline;

    TDelayedExecutor::Submit(
        BIND([=, this, weakThis = MakeWeak(this)] {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return;
            }

            VERIFY_THREAD_AFFINITY(HomeThread);

            if (Epoch_ != epoch || !Callback_) {
                return;
            }

            InvokeCallback();
        }),
        deadline,
        Invoker_);
}

void TInvokerAlarm::Arm(TClosure callback, TDuration delay)
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    Arm(std::move(callback), delay.ToDeadLine());
}

void TInvokerAlarm::Disarm()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    ++Epoch_;
    Callback_.Reset();
    Deadline_ = TInstant::Max();
}

bool TInvokerAlarm::IsArmed() const
{
    return static_cast<bool>(Callback_);
}

bool TInvokerAlarm::Check()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    if (!Callback_) {
        return false;
    }

    if (NProfiling::GetInstant() < Deadline_) {
        return false;
    }

    InvokeCallback();
    return true;
}

void TInvokerAlarm::InvokeCallback()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    // Beware of recursion, reset the state before invoking the callback.
    auto callback = std::move(Callback_);
    Deadline_ = TInstant::Max();

    callback();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
