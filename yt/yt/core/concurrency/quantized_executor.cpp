#include "quantized_executor.h"

#include "private.h"
#include "action_queue.h"
#include "delayed_executor.h"
#include "scheduler_api.h"
#include "suspendable_action_queue.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NConcurrency {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TQuantizedExecutor
    : public IQuantizedExecutor
{
public:
    TQuantizedExecutor(
        TString name,
        ICallbackProviderPtr callbackProvider,
        const TQuantizedExecutorOptions& options)
        : Name_(std::move(name))
        , CallbackProvider_(std::move(callbackProvider))
        , Options_(options)
        , Logger(ConcurrencyLogger.WithTag("Executor: %v", Name_))
        , ControlQueue_(New<TActionQueue>(Format("%vCtl", Name_)))
        , ControlInvoker_(ControlQueue_->GetInvoker())
        , DesiredWorkerCount_(options.WorkerCount)
    {
        VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);
    }

    TFuture<void> Run(TDuration timeout) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TQuantizedExecutor::StartQuantum, MakeStrong(this), timeout)
            .AsyncVia(ControlInvoker_)
            .Run();
    }

    void Reconfigure(int workerCount) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        DesiredWorkerCount_.store(workerCount);
    }

private:
    const TString Name_;
    const ICallbackProviderPtr CallbackProvider_;
    const TQuantizedExecutorOptions Options_;

    const TLogger Logger;

    const TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CallbackProviderLock_);

    std::vector<ISuspendableActionQueuePtr> Workers_;
    std::vector<IInvokerPtr> Invokers_;
    std::atomic<int> ActiveWorkerCount_ = 0;
    std::atomic<int> DesiredWorkerCount_ = 0;

    int QuantumIndex_ = 0;

    std::atomic<bool> FinishingQuantum_ = false;

    bool Running_ = false;
    TPromise<void> QuantumFinished_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void DoReconfigure()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_VERIFY(!Running_);

        int desiredWorkerCount = DesiredWorkerCount_.load();
        if (ActiveWorkerCount_ == desiredWorkerCount) {
            return;
        }

        int currentWorkerCount = std::ssize(Workers_);

        YT_LOG_DEBUG("Updating worker count (WorkerCount: %v -> %v)",
            currentWorkerCount,
            desiredWorkerCount);

        if (desiredWorkerCount > currentWorkerCount) {
            Workers_.reserve(desiredWorkerCount);
            Invokers_.reserve(desiredWorkerCount);
            for (int index = currentWorkerCount; index < desiredWorkerCount; ++index) {
                auto worker = CreateSuspendableActionQueue(
                    /*threadName*/ Format("%v:%v", Name_, index),
                    {.ThreadInitializer = Options_.ThreadInitializer});

                // NB: #GetInvoker initializes queue.
                Invokers_.push_back(worker->GetInvoker());

                worker->Suspend(/*immediately*/ true)
                    .Get()
                    .ThrowOnError();
                Workers_.push_back(std::move(worker));
            }
        }

        YT_VERIFY(std::ssize(Workers_) >= desiredWorkerCount);
        ActiveWorkerCount_ = desiredWorkerCount;
    }

    TFuture<void> StartQuantum(TDuration timeout)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        DoReconfigure();

        ++QuantumIndex_;

        YT_LOG_TRACE("Starting quantum (Index: %v, Timeout: %v)",
            QuantumIndex_,
            timeout);

        YT_VERIFY(!Running_);
        Running_ = true;

        QuantumFinished_ = NewPromise<void>();
        YT_UNUSED_FUTURE(QuantumFinished_
            .ToFuture()
            .Apply(BIND(&TQuantizedExecutor::OnQuantumFinished, MakeWeak(this), QuantumIndex_)
                .Via(ControlInvoker_)));

        TDelayedExecutor::Submit(
            BIND(&TQuantizedExecutor::OnTimeoutReached, MakeWeak(this), QuantumIndex_),
            timeout,
            ControlInvoker_);

        ResumeWorkers();

        for (int index = 0; index < std::ssize(Workers_); ++index) {
            OnWorkerReady(index, QuantumIndex_);
        }

        return QuantumFinished_.ToFuture();
    }

    void FinishQuantum(int quantumIndex, bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (!Running_ || quantumIndex != QuantumIndex_) {
            return;
        }

        if (FinishingQuantum_ && !immediately) {
            return;
        }

        YT_LOG_TRACE("Finishing quantum (Index: %v, Immediately: %v)",
            quantumIndex,
            immediately);

        FinishingQuantum_ = true;

        QuantumFinished_.TrySetFrom(SuspendWorkers(immediately));
    }

    TFuture<void> SuspendWorkers(bool immediately)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        std::vector<TFuture<void>> futures;
        futures.reserve(Workers_.size());
        for (const auto& worker : Workers_) {
            futures.push_back(worker->Suspend(immediately));
        }

        return AllSucceeded(std::move(futures));
    }

    void ResumeWorkers()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& worker : Workers_) {
            worker->Resume();
        }
    }

    void OnWorkerReady(int workerIndex, int quantumIndex)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!Running_ || quantumIndex != QuantumIndex_) {
            return;
        }

        // Worker is disabled, do not schedule new callbacks to it.
        if (workerIndex >= ActiveWorkerCount_) {
            return;
        }

        if (FinishingQuantum_) {
            return;
        }

        TCallback<void()> callback;
        {
            auto guard = Guard(CallbackProviderLock_);
            callback = CallbackProvider_->ExtractCallback();
        }

        if (!callback) {
            ControlInvoker_->Invoke(
                BIND(&TQuantizedExecutor::FinishQuantum, MakeStrong(this), quantumIndex, /*immediate*/ false));
            return;
        }

        const auto& invoker = Invokers_[workerIndex];
        invoker->Invoke(BIND([=, this, this_ = MakeStrong(this)] {
            callback();

            OnWorkerReady(workerIndex, quantumIndex);
        }));
    }

    void OnTimeoutReached(int quantumIndex)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (quantumIndex != QuantumIndex_) {
            return;
        }

        YT_LOG_TRACE("Quantum timeout reached (Index: %v)",
            quantumIndex);

        FinishQuantum(quantumIndex, /*immediate*/ true);
    }

    void OnQuantumFinished(int quantumIndex)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_TRACE("Quantum finished (Index: %v)",
            quantumIndex);

        FinishingQuantum_ = false;
        Running_ = false;
    }
};

////////////////////////////////////////////////////////////////////////////////

IQuantizedExecutorPtr CreateQuantizedExecutor(
    TString name,
    ICallbackProviderPtr callbackProvider,
    TQuantizedExecutorOptions options)
{
    return New<TQuantizedExecutor>(
        std::move(name),
        std::move(callbackProvider),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
