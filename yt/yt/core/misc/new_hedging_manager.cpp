#include "new_hedging_manager.h"
#include "config.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxSimultaneouslyProcessedRequestCount = 5;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TNewHedgingRequest)

struct TNewHedgingRequest final
    : public TRefTracked<TNewHedgingRequest>
{
    TNewHedgingRequest(
        TFuture<void> requestFuture,
        ISecondaryRequestGeneratorPtr secondaryRequestGenerator)
        : PrimaryRequestFuture(std::move(requestFuture))
        , SecondaryRequestGenerator(std::move(secondaryRequestGenerator))
    { }

    TFuture<void> PrimaryRequestFuture;

    ISecondaryRequestGeneratorPtr SecondaryRequestGenerator;
};

DEFINE_REFCOUNTED_TYPE(TNewHedgingRequest)

////////////////////////////////////////////////////////////////////////////////

//! Hedging manager limits the secondary to primary request ratio by maintaining quota on
//! invocation of secondary requests. This is done via tokens (TokenCount_):
//! each primary requests increases the token count by SecondaryRequestRatio up to MaxHedgingDelay
//! (both are parameters from config) where each secondary request essentialy decreases it by 1.
//! If there are not enough tokens to run secondary request it is put into the waiting queue
//! which is dequeued when token count is increased sufficiently (i.e. upon some subsequent primary request).
//!
//! Hedging manager maintains adaptive delay in invoking secondary requests as a best effort to not
//! exceed the abovementioned secondary to primary request ratio. Each request's contributes to this delay value.
//! If primary request is finished before the delay has passed the delay value is decreased by HedgingDelayTuneFactor.
//! Otherwise it is increased by HedgingDelayTuneFactor * SecondaryRequestRatio.
class TNewAdaptiveHedgingManager
    : public INewHedgingManager
{
public:
    TNewAdaptiveHedgingManager(TAdaptiveHedgingManagerConfigPtr config)
        : Config_(std::move(config))
    {
        YT_VERIFY(Config_->SecondaryRequestRatio);

        HedgingDelay_.store(Config_->MaxHedgingDelay, std::memory_order::release);
        TokenCount_.store(Config_->MaxTokenCount, std::memory_order::release);
    }

    void RegisterRequest(
        TFuture<void> requestFuture,
        ISecondaryRequestGeneratorPtr secondaryRequestGenerator) override
    {
        PrimaryRequestCount_.fetch_add(1, std::memory_order::relaxed);

        auto hedgingRequest = New<TNewHedgingRequest>(
            std::move(requestFuture),
            std::move(secondaryRequestGenerator));

        auto hedgingDelay = HedgingDelay_.load(std::memory_order::relaxed);

        TDelayedExecutor::Submit(
            BIND([request = std::move(hedgingRequest), hedgingManager = MakeStrong(this)] () mutable {
                if (request->PrimaryRequestFuture.IsSet()) {
                    hedgingManager->AdjustHedgingDelay(/*isRequestHedged*/ false);
                    return;
                }

                hedgingManager->AdjustHedgingDelay(/*isRequestHedged*/ true);
                hedgingManager->TryRunSecondaryRequest(std::move(request));
            }),
            hedgingDelay,
            GetCurrentInvoker());

        auto tokenCount = IncreaseToken();
        if (tokenCount < 1.) {
            return;
        }

        auto isQueueEmpty = IsQueueEmpty();

        while (!isQueueEmpty && tokenCount >= 1.) {
            std::vector<TNewHedgingRequestPtr> requestsForHedging;
            // Will be destroyed at end of cycle.
            std::vector<TNewHedgingRequestPtr> requestsForRemoval;

            DequeueRequests(
                &requestsForHedging,
                &requestsForRemoval,
                &isQueueEmpty,
                &tokenCount);

            for (const auto& request : requestsForHedging) {
                request->SecondaryRequestGenerator->GenerateSecondaryRequest();
            }

            SecondaryRequestCount_.fetch_add(std::ssize(requestsForHedging), std::memory_order::relaxed);
        }
    }

    TNewHedgingManagerStatistics CollectStatistics() override
    {
        TNewHedgingManagerStatistics statistics;

        statistics.PrimaryRequestCount = PrimaryRequestCount_.exchange(0, std::memory_order::relaxed);
        statistics.SecondaryRequestCount = SecondaryRequestCount_.exchange(0, std::memory_order::relaxed);
        statistics.QueuedRequestCount = QueuedRequestCount_.exchange(0, std::memory_order::relaxed);
        statistics.MaxQueueSize = MaxQueueSize_.exchange(0, std::memory_order::relaxed);

        statistics.HedgingDelay = HedgingDelay_.load(std::memory_order::relaxed);

        return statistics;
    }

private:
    const TAdaptiveHedgingManagerConfigPtr Config_;

    std::atomic<TDuration> HedgingDelay_;
    std::atomic<double> TokenCount_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, DequeLock_);
    std::deque<TNewHedgingRequestPtr> RequestDeque_;

    std::atomic<int> PrimaryRequestCount_ = 0;
    std::atomic<int> SecondaryRequestCount_ = 0;
    std::atomic<int> QueuedRequestCount_ = 0;
    std::atomic<int> MaxQueueSize_ = 0;


    void TryRunSecondaryRequest(TNewHedgingRequestPtr request)
    {
        auto isQueueEmpty = IsQueueEmpty();

        if (isQueueEmpty && TryDeductToken() >= 1.) {
            SecondaryRequestCount_.fetch_add(1, std::memory_order::relaxed);

            // NB: We don't need this request object anymore.
            request->SecondaryRequestGenerator->GenerateSecondaryRequest();
        } else {
            EnqueueRequest(std::move(request));
        }
    }

    void AdjustHedgingDelay(bool isRequestHedged)
    {
        auto hedgingDelay = HedgingDelay_.load(std::memory_order::relaxed);

        if (isRequestHedged) {
            hedgingDelay *= Config_->HedgingDelayTuneFactor;
            hedgingDelay /= *Config_->SecondaryRequestRatio;
        } else {
            hedgingDelay /= Config_->HedgingDelayTuneFactor;
        }
        hedgingDelay = std::max(Config_->MinHedgingDelay, std::min(Config_->MaxHedgingDelay, hedgingDelay));

        HedgingDelay_.store(hedgingDelay, std::memory_order::relaxed);
    }

    double TryDeductToken()
    {
        auto tokenCount = TokenCount_.load(std::memory_order::relaxed);
        while (tokenCount >= 1. && !TokenCount_.compare_exchange_weak(tokenCount, tokenCount - 1.))
        { }

        return tokenCount;
    }

    double IncreaseToken()
    {
        double tokenCount = TokenCount_.load(std::memory_order::acquire);
        double newTokenCount;
        do {
            newTokenCount = std::min<double>(tokenCount + *Config_->SecondaryRequestRatio, Config_->MaxTokenCount);
        } while (
            tokenCount < Config_->MaxTokenCount &&
            !TokenCount_.compare_exchange_weak(
                tokenCount,
                newTokenCount));

        return newTokenCount;
    }

    bool IsQueueEmpty()
    {
        auto readerGuard = ReaderGuard(DequeLock_);
        return RequestDeque_.empty();
    }

    void EnqueueRequest(TNewHedgingRequestPtr request)
    {
        QueuedRequestCount_.fetch_add(1, std::memory_order::relaxed);

        int queueSize;
        {
            auto guard = WriterGuard(DequeLock_);
            RequestDeque_.push_back(std::move(request));
            queueSize = RequestDeque_.size();
        }

        // Statistics can be inaccurate.
        if (MaxQueueSize_.load(std::memory_order::relaxed) < queueSize) {
            MaxQueueSize_.store(queueSize, std::memory_order::relaxed);
        }
    }

    void DequeueRequests(
        std::vector<TNewHedgingRequestPtr>* requestsForHedging,
        std::vector<TNewHedgingRequestPtr>* requestsForRemoval,
        bool* isQueueEmpty,
        double* tokenCount)
    {
        requestsForHedging->reserve(MaxSimultaneouslyProcessedRequestCount);
        requestsForRemoval->reserve(MaxSimultaneouslyProcessedRequestCount);

        auto writerGuard = WriterGuard(DequeLock_);

        while (
            !RequestDeque_.empty() &&
            requestsForRemoval->size() + requestsForHedging->size() < MaxSimultaneouslyProcessedRequestCount)
        {
            auto& currentRequest = RequestDeque_.front();

            if (currentRequest->PrimaryRequestFuture.IsSet()) {
                requestsForRemoval->push_back(std::move(currentRequest));
                RequestDeque_.pop_front();
            } else {
                *tokenCount = TryDeductToken();
                if (*tokenCount < 1.) {
                    break;
                }

                requestsForHedging->push_back(std::move(currentRequest));
                RequestDeque_.pop_front();
            }
        }

        *isQueueEmpty = RequestDeque_.empty();
    }
};

////////////////////////////////////////////////////////////////////////////////

INewHedgingManagerPtr CreateNewAdaptiveHedgingManager(
    TAdaptiveHedgingManagerConfigPtr config)
{
    return New<TNewAdaptiveHedgingManager>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
