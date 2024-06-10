#include "suspendable_action_queue.h"

#include "profiling_helpers.h"
#include "single_queue_scheduler_thread.h"
#include "system_invokers.h"

#include <yt/yt/core/actions/invoker_util.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSuspendableActionQueue
    : public ISuspendableActionQueue
{
public:
    TSuspendableActionQueue(
        TString threadName,
        TSuspendableActionQueueOptions options)
        : Queue_(New<TMpscInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(threadName)))
        , Invoker_(Queue_)
        , Thread_(New<TMpscSuspendableSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            threadName,
            threadName,
            NThreading::TThreadOptions{
                .ThreadInitializer = options.ThreadInitializer,
            }))
        , ShutdownCookie_(RegisterShutdownCallback(
            Format("SuspendableActionQueue(%v)", threadName),
            BIND_NO_PROPAGATE(&TSuspendableActionQueue::Shutdown, MakeWeak(this), /*graceful*/ false),
            /*priority*/ 100))
    { }

    ~TSuspendableActionQueue()
    {
        Shutdown(/*graceful*/ false);
    }

    void Shutdown(bool graceful) final
    {
        // Synchronization done via Queue_->Shutdown().
        if (Stopped_.exchange(true, std::memory_order::relaxed)) {
            return;
        }

        Queue_->Shutdown(graceful);
        Thread_->Shutdown(graceful);
        Queue_->OnConsumerFinished();
    }

    const IInvokerPtr& GetInvoker() override
    {
        EnsureStarted();
        return Invoker_;
    }

    TFuture<void> Suspend(bool immediately) override
    {
        auto future = Thread_->Suspend(immediately);

        // Invoke empty callback to wake up thread.
        Queue_->Invoke(BIND([] { }));

        return future;
    }

    void Resume() override
    {
        Thread_->Resume();
    }

private:
    const TSuspendableActionQueueOptions Options_;
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TMpscInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;
    const TMpscSuspendableSingleQueueSchedulerThreadPtr Thread_;

    const TShutdownCookie ShutdownCookie_;
    const IInvokerPtr ShutdownInvoker_ = GetShutdownInvoker();

    std::atomic<bool> Stopped_ = false;

    void EnsureStarted()
    {
        // Thread::Start already has
        // its own short-circ.
        Thread_->Start();
    }
};

////////////////////////////////////////////////////////////////////////////////

ISuspendableActionQueuePtr CreateSuspendableActionQueue(
    TString threadName,
    TSuspendableActionQueueOptions options)
{
    return New<TSuspendableActionQueue>(
        std::move(threadName),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
