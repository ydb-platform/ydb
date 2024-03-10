#include "system_invokers.h"
#include "action_queue.h"
#include "profiling_helpers.h"
#include "single_queue_scheduler_thread.h"

#include <yt/yt/core/misc/shutdown.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
class TSystemInvokerThread
{
public:
    TSystemInvokerThread(
        const TString& threadName,
        int shutdownPriority)
        : Queue_(New<TMpscInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(threadName)))
        , Invoker_(Queue_)
        , Thread_(New<TMpscSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            threadName,
            threadName,
            NThreading::TThreadOptions{
                .ShutdownPriority = shutdownPriority - 1,
            }))
        , ShutdownCookie_(RegisterShutdownCallback(
            Format("SystemInvokerThread:%v", threadName),
            BIND_NO_PROPAGATE(&TSystemInvokerThread::Shutdown, this),
            shutdownPriority))
    {
        Thread_->Start();
    }

    const IInvokerPtr& GetInvoker()
    {
        return Invoker_;
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TMpscInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;
    const TMpscSingleQueueSchedulerThreadPtr Thread_;
    const TShutdownCookie ShutdownCookie_;

    void Shutdown()
    {
        Thread_->Stop(/*graceful*/ true);
    }
};

IInvokerPtr GetFinalizerInvoker()
{
    struct TTag
    { };
    static const auto invoker = LeakySingleton<TSystemInvokerThread<TTag>>("Finalizer", -300)->GetInvoker();
    return invoker;
}

IInvokerPtr GetShutdownInvoker()
{
    struct TTag
    { };
    static const auto invoker = LeakySingleton<TSystemInvokerThread<TTag>>("Shutdown", -200)->GetInvoker();
    return invoker;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
