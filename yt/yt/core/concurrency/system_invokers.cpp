#include "system_invokers.h"
#include "action_queue.h"
#include "helpers.h"
#include "single_queue_scheduler_thread.h"

#include <yt/yt/core/misc/shutdown.h>

#include <library/cpp/yt/memory/leaky_singleton.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSystemInvokerThread
{
public:
    const IInvokerPtr& GetInvoker()
    {
        return Invoker_;
    }

protected:
    TSystemInvokerThread(
        std::string threadName,
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

////////////////////////////////////////////////////////////////////////////////

class TFinalizerInvokerThread
    : public TSystemInvokerThread
{
public:
    TFinalizerInvokerThread()
        : TSystemInvokerThread("Finalizer", -300)
    { }
};

class TShutdownInvokerThread
    : public TSystemInvokerThread
{
public:
    TShutdownInvokerThread()
        : TSystemInvokerThread("Shutdown", -200)
    { }
};

////////////////////////////////////////////////////////////////////////////////

IInvokerPtr GetFinalizerInvoker()
{
    return LeakySingleton<TFinalizerInvokerThread>()->GetInvoker();
}

IInvokerPtr GetShutdownInvoker()
{
    return LeakySingleton<TShutdownInvokerThread>()->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
