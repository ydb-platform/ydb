#include "batching_timestamp_provider.h"
#include "timestamp_provider.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/library/tracing/batch_trace.h>

namespace NYT::NTransactionClient {

using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

class TBatchingTimestampProvider
    : public ITimestampProvider
{
public:
    TBatchingTimestampProvider(
        ITimestampProviderPtr underlying,
        TDuration batchPeriod)
        : Underlying_(std::move(underlying))
        , BatchPeriod_(batchPeriod)
    { }

    TFuture<TTimestamp> GenerateTimestamps(int count) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<TTimestamp> result;

        {
            auto guard = Guard(SpinLock_);
            PendingRequests_.emplace_back();
            PendingRequests_.back().Count = count;
            PendingRequests_.back().Promise = NewPromise<TTimestamp>();
            result = PendingRequests_.back().Promise.ToFuture().ToUncancelable();

            BatchTrace_.Join();

            TNullTraceContextGuard nullTraceContextGuard;
            MaybeScheduleSendGenerateRequest(guard);
        }

        return result;
    }

    TTimestamp GetLatestTimestamp() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Underlying_->GetLatestTimestamp();
    }

 private:
    const ITimestampProviderPtr Underlying_;
    const TDuration BatchPeriod_;

    struct TRequest
    {
        int Count;
        TPromise<TTimestamp> Promise;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    bool GenerateInProgress_ = false;
    bool FlushScheduled_ = false;
    std::vector<TRequest> PendingRequests_;
    TBatchTrace BatchTrace_;

    TInstant LastRequestTime_;

    void MaybeScheduleSendGenerateRequest(TGuard<NThreading::TSpinLock>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        if (PendingRequests_.empty() || GenerateInProgress_) {
            return;
        }

        auto now = GetInstant();

        if (LastRequestTime_ + BatchPeriod_ < now) {
            SendGenerateRequest(guard);
        } else if (!FlushScheduled_) {
            FlushScheduled_ = true;
            TDelayedExecutor::Submit(
                BIND([this, this_ = MakeStrong(this)] {
                    auto guard = Guard(SpinLock_);
                    FlushScheduled_ = false;
                    if (GenerateInProgress_) {
                        return;
                    }
                    SendGenerateRequest(guard);
                }),
                BatchPeriod_ - (now - LastRequestTime_));
        }
    }

    void SendGenerateRequest(TGuard<NThreading::TSpinLock>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(SpinLock_);

        YT_VERIFY(!GenerateInProgress_);
        GenerateInProgress_ = true;
        LastRequestTime_ = GetInstant();

        std::vector<TRequest> requests;
        requests.swap(PendingRequests_);

        auto [traceContext, sampled] = BatchTrace_.StartSpan("BatchingTimestampProvider:SendGenerateRequest");

        guard.Release();

        TTraceContextGuard traceContextGuard(traceContext);

        int count = 0;
        for (const auto& request : requests) {
            count += request.Count;
        }

        Underlying_->GenerateTimestamps(count).Subscribe(BIND(
            &TBatchingTimestampProvider::OnGenerateResponse,
            MakeStrong(this),
            Passed(std::move(requests))));
    }

    void OnGenerateResponse(
        std::vector<TRequest> requests,
        const TErrorOr<TTimestamp>& firstTimestampOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        {
            auto guard = Guard(SpinLock_);

            YT_VERIFY(GenerateInProgress_);
            GenerateInProgress_ = false;

            MaybeScheduleSendGenerateRequest(guard);
        }

        if (firstTimestampOrError.IsOK()) {
            auto timestamp = firstTimestampOrError.Value();
            for (const auto& request : requests) {
                request.Promise.Set(timestamp);
                timestamp += request.Count;
            }
        } else {
            for (const auto& request : requests) {
                request.Promise.Set(TError(firstTimestampOrError));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    TDuration batchPeriod)
{
    return New<TBatchingTimestampProvider>(std::move(underlying), batchPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
