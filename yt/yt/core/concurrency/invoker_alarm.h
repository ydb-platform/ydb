#pragma once

#include "public.h"
#include "thread_affinity.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Enables notifying a given invoker that a certain deadline has been reached.
/*!
 *  The invoker is passed to the ctor and must be single-threaded.
 *
 *  Alarm can be set via #Arm and dismissed via #Disarm. At most one deadline
 *  can be active at the moment; arming the alarm with a new deadline dismisses
 *  the previous one.
 *
 *  When the deadline is reached, a callback (given upon arming the alarm) is enqueued to the invoker.
 *
 *  Clients may also call #Check at any time to see if the deadline is already reached;
 *  if so, the callback is invoked synchronously (and its scheduled invocation becomes a no-op).
 *
 *  \note
 *  Thread-affininty: single-threaded (moreover, all methods must be called within the invoker).
 */
class TInvokerAlarm
    : public TRefCounted
{
public:
    explicit TInvokerAlarm(IInvokerPtr invoker);

    void Arm(TClosure callback, TInstant deadline);
    void Arm(TClosure callback, TDuration delay);
    void Disarm();

    bool IsArmed() const;

    bool Check();

private:
    const IInvokerPtr Invoker_;

    TClosure Callback_;
    TInstant Deadline_ = TInstant::Max();
    ui64 Epoch_ = 0;

    DECLARE_THREAD_AFFINITY_SLOT(HomeThread);

    void InvokeCallback();
};

DEFINE_REFCOUNTED_TYPE(TInvokerAlarm)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
