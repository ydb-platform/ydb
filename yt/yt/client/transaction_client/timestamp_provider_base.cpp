#include "timestamp_provider_base.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTransactionClient {

using namespace NConcurrency;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTimestampProviderBase::TTimestampProviderBase(std::optional<TDuration> latestTimestampUpdatePeriod)
    : LatestTimestampUpdatePeriod_(latestTimestampUpdatePeriod)
{ }

TFuture<TTimestamp> TTimestampProviderBase::GenerateTimestamps(int count, TCellTag clockClusterTag)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Generating fresh timestamps (Count: %v, ClockClusterTag: %v)",
        count,
        clockClusterTag);

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
    VERIFY_THREAD_AFFINITY_ANY();

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
        auto error = TError("Error generating fresh timestamps") << timestampOrError;
        YT_LOG_ERROR(error);
        return MakeFuture<TTimestamp>(error);
    }

    auto firstTimestamp = timestampOrError.Value();
    auto lastTimestamp = firstTimestamp + count - 1;

    YT_LOG_DEBUG("Fresh timestamps generated (Timestamps: %v-%v, ClockClusterTag: %v)",
        firstTimestamp,
        lastTimestamp,
        clockClusterTag);

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
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Updating latest timestamp");
    GenerateTimestamps(1).Subscribe(BIND([] (const TErrorOr<TTimestamp>& timestampOrError) {
            if (timestampOrError.IsOK()) {
                YT_LOG_DEBUG("Latest timestamp updated (Timestamp: %v)",
                    timestampOrError.Value());
            } else {
                YT_LOG_WARNING(timestampOrError, "Error updating latest timestamp");
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
                YT_LOG_DEBUG("Latest timestamp updated (Timestamp: %v, AlienCellTag: %v)",
                    timestampOrError.Value(),
                    cellTag);
            } else {
                YT_LOG_WARNING(timestampOrError, "Error updating latest timestamp (AlienCellTag: %v)",
                cellTag);
            }
        }));

    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
