#include "batching_timestamp_provider.h"
#include "timestamp_provider.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/library/tracing/batch_trace.h>

namespace NYT::NTransactionClient {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

class TRequestBatcher
    : public TRefCounted
{
public:
    TRequestBatcher(
        ITimestampProviderPtr underlying,
        TDuration batchPeriod,
        TCellTag clockClusterTag)
        : Underlying_(std::move(underlying))
        , BatchPeriod_(batchPeriod)
        , ClockClusterTag_(clockClusterTag)
    { }

    TFuture<TTimestamp> GenerateTimestamps(int count)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TFuture<TTimestamp> result;

        {
            auto guard = Guard(SpinLock_);

            auto& request = PendingRequests_.emplace_back();
            request.Count = count;
            request.Promise = NewPromise<TTimestamp>();
            result = request.Promise.ToFuture().ToUncancelable();

            BatchTrace_.Join();

            TNullTraceContextGuard nullTraceContextGuard;
            MaybeScheduleSendGenerateRequest(guard);
        }

        return result;

    }

private:
    const ITimestampProviderPtr Underlying_;
    const TDuration BatchPeriod_;
    const TCellTag ClockClusterTag_;

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
        LastRequestTime_ = GetInstant();

        std::vector<TRequest> requests;
        requests.swap(PendingRequests_);

        GenerateInProgress_ = true;

        auto [traceContext, _] = BatchTrace_.StartSpan("BatchingTimestampProvider:SendGenerateRequest");

        guard.Release();

        TTraceContextGuard traceContextGuard(traceContext);

        int count = 0;
        for (const auto& request : requests) {
            count += request.Count;
        }

        Underlying_->GenerateTimestamps(count, ClockClusterTag_).Subscribe(BIND(
            &TRequestBatcher::OnGenerateResponse,
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

DECLARE_REFCOUNTED_TYPE(TRequestBatcher)
DEFINE_REFCOUNTED_TYPE(TRequestBatcher)

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
        , NativeRequestBatcher_(New<TRequestBatcher>(Underlying_, BatchPeriod_, InvalidCellTag))
    { }

    TFuture<TTimestamp> GenerateTimestamps(int count, TCellTag clockClusterTag) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (clockClusterTag == InvalidCellTag) {
            return NativeRequestBatcher_->GenerateTimestamps(count);
        }

        TRequestBatcherPtr requestBatcher;
        {
            auto guard = Guard(SpinLock_);
            auto byCellBatcherIterator = ByTagRequestBatchers_.find(clockClusterTag);
            if (byCellBatcherIterator == ByTagRequestBatchers_.end()) {
                byCellBatcherIterator = EmplaceOrCrash(ByTagRequestBatchers_, clockClusterTag, New<TRequestBatcher>(
                    Underlying_,
                    BatchPeriod_,
                    clockClusterTag));
            }

            requestBatcher = byCellBatcherIterator->second;
        }

        return requestBatcher->GenerateTimestamps(count);
    }

    TTimestamp GetLatestTimestamp(TCellTag clockClusterTag) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Underlying_->GetLatestTimestamp(clockClusterTag);
    }

private:
    const ITimestampProviderPtr Underlying_;
    const TDuration BatchPeriod_;
    const TRequestBatcherPtr NativeRequestBatcher_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    THashMap<TCellTag, TRequestBatcherPtr> ByTagRequestBatchers_;
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
