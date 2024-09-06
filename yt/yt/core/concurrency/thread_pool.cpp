#include "thread_pool.h"
#include "notify_manager.h"
#include "single_queue_scheduler_thread.h"
#include "private.h"
#include "profiling_helpers.h"
#include "thread_pool_detail.h"

#include <yt/yt/core/actions/invoker_detail.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ypath/token.h>

namespace NYT::NConcurrency {

using namespace NProfiling;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// This routines could be placed in TThreadPool class but it causes a circular ref.
class TInvokerQueueAdapter
    : public TMpmcInvokerQueue
    , public TNotifyManager
{
public:
    TInvokerQueueAdapter(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TTagSet& counterTagSet,
        TDuration pollingPeriod)
        : TMpmcInvokerQueue(callbackEventCount, counterTagSet)
        , TNotifyManager(callbackEventCount, counterTagSet, pollingPeriod)
    { }

    template <class TIsStoppingPredicate>
    bool OnExecute(TEnqueuedAction* action, bool fetchNext, TIsStoppingPredicate isStopping)
    {
        while (true) {
            int activeThreadDelta = !action->Finished ? -1 : 0;
            TMpmcInvokerQueue::EndExecute(action);

            auto cookie = GetEventCount()->PrepareWait();
            auto minEnqueuedAt = ResetMinEnqueuedAt();

            bool result = false;
            if (fetchNext && TMpmcInvokerQueue::BeginExecute(action)) {
                YT_ASSERT(action->EnqueuedAt > 0);
                minEnqueuedAt = action->EnqueuedAt;
                activeThreadDelta += 1;
                result = true;
            }

            YT_VERIFY(activeThreadDelta <= 1 && activeThreadDelta >= -1);
            if (activeThreadDelta != 0) {
                auto activeThreads = ActiveThreads_.fetch_add(activeThreadDelta) + activeThreadDelta;
                YT_VERIFY(activeThreads >= 0 && activeThreads <= TThreadPoolBase::MaxThreadCount);
            }

            if (result || isStopping()) {
                CancelWait();

                NotifyAfterFetch(GetCpuInstant(), minEnqueuedAt);
                return result;
            }

            Wait(cookie, isStopping);
        }
    }

    void Invoke(TClosure callback) override
    {
        auto cpuInstant = TMpmcInvokerQueue::EnqueueCallback(
            std::move(callback),
            /*profilingTag*/ 0,
            /*profilerTag*/ nullptr);

        NotifyFromInvoke(cpuInstant, ActiveThreads_.load() == 0);
    }

    void Invoke(TMutableRange<TClosure> callbacks) override
    {
        auto cpuInstant = TMpmcInvokerQueue::EnqueueCallbacks(callbacks);
        NotifyFromInvoke(cpuInstant, ActiveThreads_.load() == 0);
    }

private:
    std::atomic<int> ActiveThreads_ = 0;
};

class TThreadPoolThread
    : public TSchedulerThread
{
public:
    TThreadPoolThread(
        TIntrusivePtr<TInvokerQueueAdapter> queue,
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        const TString& threadGroupName,
        const TString& threadName,
        const TThreadPoolOptions& options)
        : TSchedulerThread(
            callbackEventCount,
            threadGroupName,
            threadName,
            NThreading::TThreadOptions{
                .ThreadPriority = options.ThreadPriority,
                .ThreadInitializer = options.ThreadInitializer,
            })
        , Queue_(std::move(queue))
        , Options_(options)
    { }

protected:
    const TIntrusivePtr<TInvokerQueueAdapter> Queue_;
    const TThreadPoolOptions Options_;

    TEnqueuedAction CurrentAction_;

    TClosure OnExecute() override
    {
        bool fetchNext = !TSchedulerThread::IsStopping() || TSchedulerThread::GracefulStop_;

        bool dequeued = Queue_->OnExecute(&CurrentAction_, fetchNext, [&] {
            return TSchedulerThread::IsStopping();
        });
        return BeginExecuteImpl(dequeued, &CurrentAction_);
    }

    TClosure BeginExecute() override
    {
        Y_UNREACHABLE();
    }

    void EndExecute() override
    {
        Y_UNREACHABLE();
    }
};

class TThreadPool
    : public IThreadPool
    , public TThreadPoolBase
{
public:
    TThreadPool(
        int threadCount,
        const TString& threadNamePrefix,
        const TThreadPoolOptions& options)
        : TThreadPoolBase(threadNamePrefix)
        , Options_(options)
        , Queue_(New<TInvokerQueueAdapter>(
            CallbackEventCount_,
            GetThreadTags(ThreadNamePrefix_),
            options.PollingPeriod))
        , Invoker_(Queue_)
    {
        Configure(threadCount);
    }

    ~TThreadPool()
    {
        Shutdown();
    }

    const IInvokerPtr& GetInvoker() override
    {
        EnsureStarted();
        return Invoker_;
    }

    void Configure(int threadCount) override
    {
        TThreadPoolBase::Configure(threadCount);
    }

    void Configure(TDuration pollingPeriod) override
    {
        Queue_->Reconfigure(pollingPeriod);
    }

    int GetThreadCount() override
    {
        return TThreadPoolBase::GetThreadCount();
    }

    void Shutdown() override
    {
        TThreadPoolBase::Shutdown();
    }

private:
    const TThreadPoolOptions Options_;
    const TIntrusivePtr<NThreading::TEventCount> CallbackEventCount_ = New<NThreading::TEventCount>();
    const TIntrusivePtr<TInvokerQueueAdapter> Queue_;
    const IInvokerPtr Invoker_;

    void DoShutdown() override
    {
        Queue_->Shutdown(/*graceful*/ false);
        TThreadPoolBase::DoShutdown();
        Queue_->OnConsumerFinished();
    }

    TSchedulerThreadPtr SpawnThread(int index) override
    {
        return New<TThreadPoolThread>(
            Queue_,
            CallbackEventCount_,
            ThreadNamePrefix_,
            MakeThreadName(index),
            Options_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IThreadPoolPtr CreateThreadPool(
    int threadCount,
    const TString& threadNamePrefix,
    const TThreadPoolOptions& options)
{
    return New<TThreadPool>(
        threadCount,
        threadNamePrefix,
        options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
