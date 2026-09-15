#pragma once

#include "public.h"
// TODO(babenko): drop these in favor of explicit includes
#include "serialized_invoker.h"
#include "prioritized_invoker.h"
#include "bounded_concurrency_invoker.h"
#include "suspendable_invoker.h"
#include "watchdog_invoker.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// XXX(sandello): Facade does not have to be ref-counted.
class TActionQueue
    : public TRefCounted
{
public:
    explicit TActionQueue(std::string threadName = "ActionQueue");
    virtual ~TActionQueue();

    void Shutdown(bool graceful = false);

    const IInvokerPtr& GetInvoker();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TActionQueue)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
