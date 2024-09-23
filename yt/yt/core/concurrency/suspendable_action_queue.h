#include "public.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

struct ISuspendableActionQueue
    : public TRefCounted
{
    virtual const IInvokerPtr& GetInvoker() = 0;

    //! Returns a future that becomes set when action queue is suspended
    //! and thread is blocked.
    //! If #immediately is true, queue is suspended just after completion
    //! of current fiber.
    //! If #immediately is false, queue is suspended when underlying queue
    //! becomes empty.
    virtual TFuture<void> Suspend(bool immediately) = 0;

    //! Resumes queue. Queue should be suspended prior to this call.
    virtual void Resume() = 0;

    virtual void Shutdown(bool graceful) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISuspendableActionQueue)

////////////////////////////////////////////////////////////////////////////////

struct TSuspendableActionQueueOptions
{
    std::function<void()> ThreadInitializer;
};

ISuspendableActionQueuePtr CreateSuspendableActionQueue(
    TString threadName,
    TSuspendableActionQueueOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
