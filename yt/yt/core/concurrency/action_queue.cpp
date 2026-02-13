#include "action_queue.h"

#include "single_queue_scheduler_thread.h"
#include "profiling_helpers.h"
#include "invoker_queue.h"

#include <yt/yt/core/actions/invoker_detail.h>

namespace NYT::NConcurrency {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TActionQueue::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(std::string threadName)
        : Queue_(New<TMpscInvokerQueue>(
            CallbackEventCount_,
            GetThreadTags(threadName)))
        , Invoker_(Queue_)
        , Thread_(New<TMpscSingleQueueSchedulerThread>(
            Queue_,
            CallbackEventCount_,
            threadName,
            threadName))
        , ShutdownCookie_(RegisterShutdownCallback(
            Format("ActionQueue(%v)", threadName),
            BIND_NO_PROPAGATE(&TImpl::Shutdown, MakeWeak(this), /*graceful*/ false),
            /*priority*/ 100))
    { }

    ~TImpl()
    {
        Shutdown(/*graceful*/ false);
    }

    void Shutdown(bool graceful)
    {
        // Proper synchronization done via Queue_->Shutdown().
        if (Stopped_.exchange(true, std::memory_order::relaxed)) {
            return;
        }

        Queue_->Shutdown(graceful);
        Thread_->Stop(graceful);
        Queue_->OnConsumerFinished();
    }

    const IInvokerPtr& GetInvoker()
    {
        EnsureStarted();
        return Invoker_;
    }

private:
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TMpscInvokerQueuePtr Queue_;
    const IInvokerPtr Invoker_;
    const TMpscSingleQueueSchedulerThreadPtr Thread_;

    const TShutdownCookie ShutdownCookie_;

    std::atomic<bool> Stopped_ = false;


    void EnsureStarted()
    {
        // Thread::Start already has
        // its own short-circ mechanism.
        Thread_->Start();
    }
};

TActionQueue::TActionQueue(std::string threadName)
    : Impl_(New<TImpl>(std::move(threadName)))
{ }

TActionQueue::~TActionQueue() = default;

void TActionQueue::Shutdown(bool graceful)
{
    return Impl_->Shutdown(graceful);
}

const IInvokerPtr& TActionQueue::GetInvoker()
{
    return Impl_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
