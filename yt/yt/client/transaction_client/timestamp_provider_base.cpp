#include "timestamp_provider_base.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTransactionClient {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTimestampProviderBase::TTimestampProviderBase(std::optional<TDuration> latestTimestampUpdatePeriod)
    : LatestTimestampUpdatePeriod_(latestTimestampUpdatePeriod)
{ }

TFuture<TTimestamp> TTimestampProviderBase::GenerateTimestamps(int count, TCellTag clockClusterTag)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_TLOG_DEBUG("Generating fresh timestamps")
        .With("Count", count)
        .With("ClockClusterTag", clockClusterTag);

    return DoGenerateTimestamps(count, clockClusterTag).Apply(BIND(
        &TTimestampProviderBase::OnGenerateTimestamps,
        MakeStrong(this),
        count,
        clockClusterTag));
}


std::atomic<TTimestamp>& TTimestampProviderBase::GetLatestTimestampReferenceByTag(TCellTag clockClusterTag)
{
    if (clockClusterTag == InvalidCellTag) {
        return LatestTimestamp_;
    } else {
        auto guard = Guard(ClockClusterTagMapSpinLock_);
        return LatestTimestampByClockCellTag_.try_emplace(clockClusterTag, MinTimestamp).first->second;
    }
}

TTimestamp TTimestampProviderBase::GetLatestTimestamp(TCellTag clockClusterTag)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto result = GetLatestTimestampReferenceByTag(clockClusterTag).load(std::memory_order::relaxed);

    if (LatestTimestampUpdatePeriod_ && ++GetLatestTimestampCallCounter_ == 1) {
        LatestTimestampExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TTimestampProviderBase::UpdateLatestTimestamp, MakeWeak(this)),
            *LatestTimestampUpdatePeriod_);
        LatestTimestampExecutor_->Start();
    }

    return result;
}

TFuture<TTimestamp> TTimestampProviderBase::OnGenerateTimestamps(
    int count,
    TCellTag clockClusterTag,
    const TErrorOr<TTimestamp>& timestampOrError)
{
    if (!timestampOrError.IsOK()) {
        auto error = TError("Error generating fresh timestamps").With(timestampOrError);
        YT_TLOG_ERROR("Error generating fresh timestamps")
            .With(timestampOrError);
        return MakeFuture<TTimestamp>(error);
    }

    auto firstTimestamp = timestampOrError.Value();
    auto lastTimestamp = TTimestamp(firstTimestamp.Underlying() + count - 1);

    YT_TLOG_DEBUG("Fresh timestamps generated")
        .WithFormat("Timestamps", "%v-%v", firstTimestamp, lastTimestamp)
        .With("ClockClusterTag", clockClusterTag);

    auto& latestTimestamp = GetLatestTimestampReferenceByTag(clockClusterTag);
    auto latestTimestampValue = latestTimestamp.load(std::memory_order::relaxed);
    while (true) {
        if (latestTimestampValue >= lastTimestamp) {
            break;
        }
        if (latestTimestamp.compare_exchange_weak(latestTimestampValue, lastTimestamp, std::memory_order::relaxed)) {
            break;
        }
    }

    return MakeFuture<TTimestamp>(firstTimestamp);
}

void TTimestampProviderBase::UpdateLatestTimestamp()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_TLOG_DEBUG("Updating latest timestamp");
    GenerateTimestamps(1).Subscribe(BIND([] (const TErrorOr<TTimestamp>& timestampOrError) {
            if (timestampOrError.IsOK()) {
                YT_TLOG_DEBUG("Latest timestamp updated")
                    .With("Timestamp", timestampOrError.Value());
            } else {
                YT_TLOG_WARNING("Error updating latest timestamp")
                    .With(timestampOrError);
            }
        }));

    std::vector<TCellTag> cellTags;

    {
        auto guard = Guard(ClockClusterTagMapSpinLock_);

        cellTags.reserve(LatestTimestampByClockCellTag_.size());
        for (const auto& [cellTag, timestamp] : LatestTimestampByClockCellTag_) {
            cellTags.push_back(cellTag);
        }
    }

    for (const auto cellTag : cellTags) {
        GenerateTimestamps(1, cellTag).Subscribe(
        BIND([cellTag] (const TErrorOr<TTimestamp>& timestampOrError) {
            if (timestampOrError.IsOK()) {
                YT_TLOG_DEBUG("Latest timestamp updated")
                    .With("Timestamp", timestampOrError.Value())
                    .With("AlienCellTag", cellTag);
            } else {
                YT_TLOG_WARNING("Error updating latest timestamp")
                    .With("AlienCellTag", cellTag)
                    .With(timestampOrError);
            }
        }));

    }
}

void TTimestampProviderBase::Reconfigure(const TRemoteTimestampProviderConfigPtr& /*config*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
