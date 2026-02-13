#include "batching_timestamp_provider.h"
#include "timestamp_provider.h"
#include "config.h"

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
        , ClockClusterTag_(clockClusterTag)
        , BatchPeriod_(batchPeriod)
    { }

    TFuture<TTimestamp> GenerateTimestamps(int count)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

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

    void SetBatchPeriod(TDuration batchPeriod)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto guard = Guard(SpinLock_);
        BatchPeriod_ = batchPeriod;
    }

private:
    const ITimestampProviderPtr Underlying_;
    const TCellTag ClockClusterTag_;

    struct TRequest
    {
        int Count;
        TPromise<TTimestamp> Promise;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);

    TDuration BatchPeriod_;
    bool GenerateInProgress_ = false;
    bool FlushScheduled_ = false;
    std::vector<TRequest> PendingRequests_;
    TBatchTrace BatchTrace_;

    TInstant LastRequestTime_;

    void MaybeScheduleSendGenerateRequest(TGuard<NThreading::TSpinLock>& guard)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

        if (PendingRequests_.empty() || GenerateInProgress_) {
            return;
        }

        auto now = GetInstant();
        auto batchDeadline = LastRequestTime_ +  BatchPeriod_;

        if (batchDeadline < now) {
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
                batchDeadline - now);
        }
    }

    void SendGenerateRequest(TGuard<NThreading::TSpinLock>& guard)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);

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
        YT_ASSERT_THREAD_AFFINITY_ANY();

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
        const TRemoteTimestampProviderConfigPtr& config)
        : Underlying_(std::move(underlying))
        , NativeRequestBatcher_(New<TRequestBatcher>(Underlying_, config->BatchPeriod, InvalidCellTag))
        , BatchPeriod_(config->BatchPeriod)
    { }

    TFuture<TTimestamp> GenerateTimestamps(int count, TCellTag clockClusterTag) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (clockClusterTag == InvalidCellTag) {
            return NativeRequestBatcher_->GenerateTimestamps(count);
        }

        return GetAlienBatcher(clockClusterTag)->GenerateTimestamps(count);
    }

    TTimestamp GetLatestTimestamp(TCellTag clockClusterTag) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Underlying_->GetLatestTimestamp(clockClusterTag);
    }

    void Reconfigure(const TRemoteTimestampProviderConfigPtr& config) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        SetBatchPeriod(config->BatchPeriod);
        Underlying_->Reconfigure(config);
    }

private:
    const ITimestampProviderPtr Underlying_;
    const TRequestBatcherPtr NativeRequestBatcher_;

    std::atomic<TDuration> BatchPeriod_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TCellTag, TRequestBatcherPtr> Batchers_;

    TRequestBatcherPtr GetAlienBatcher(TCellTag clockClusterTag)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        {
            auto readerGuard = ReaderGuard(SpinLock_);

            if (auto batcherIt = Batchers_.find(clockClusterTag); batcherIt != Batchers_.end()) {
                return batcherIt->second;
            }
        }

        // Usually entered once during binary lifetime
        auto writerGuard = WriterGuard(SpinLock_);

        auto [batcherIt, inserted] = Batchers_.try_emplace(clockClusterTag, nullptr);
        if (inserted) {
            batcherIt->second = New<TRequestBatcher>(
                Underlying_,
                BatchPeriod_.load(),
                clockClusterTag);
        }

        return batcherIt->second;
    }

    void SetBatchPeriod(TDuration batchPeriod)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        BatchPeriod_.store(batchPeriod);
        NativeRequestBatcher_->SetBatchPeriod(batchPeriod);

        auto guard = ReaderGuard(SpinLock_);
        for (const auto& [_, requestBatcher] : Batchers_) {
            requestBatcher->SetBatchPeriod(batchPeriod);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    const TRemoteTimestampProviderConfigPtr& config)
{
    return New<TBatchingTimestampProvider>(std::move(underlying), config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
