#include "timestamp_provider_base.h"
#include "private.h"

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NTransactionClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TransactionClientLogger;

////////////////////////////////////////////////////////////////////////////////

TTimestampProviderBase::TTimestampProviderBase(std::optional<TDuration> latestTimestampUpdatePeriod)
    : LatestTimestampUpdatePeriod_(latestTimestampUpdatePeriod)
{ }

TFuture<TTimestamp> TTimestampProviderBase::GenerateTimestamps(int count)
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Generating fresh timestamps (Count: %v)", count);

    return DoGenerateTimestamps(count).Apply(BIND(
        &TTimestampProviderBase::OnGenerateTimestamps,
        MakeStrong(this),
        count));
}

TTimestamp TTimestampProviderBase::GetLatestTimestamp()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto result = LatestTimestamp_.load(std::memory_order::relaxed);

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
    const TErrorOr<TTimestamp>& timestampOrError)
{
    if (!timestampOrError.IsOK()) {
        auto error = TError("Error generating fresh timestamps") << timestampOrError;
        YT_LOG_ERROR(error);
        return MakeFuture<TTimestamp>(error);
    }

    auto firstTimestamp = timestampOrError.Value();
    auto lastTimestamp = firstTimestamp + count - 1;

    YT_LOG_DEBUG("Fresh timestamps generated (Timestamps: %v-%v)",
        firstTimestamp,
        lastTimestamp);

    auto latestTimestamp = LatestTimestamp_.load(std::memory_order::relaxed);
    while (true) {
        if (latestTimestamp >= lastTimestamp) {
            break;
        }
        if (LatestTimestamp_.compare_exchange_weak(latestTimestamp, lastTimestamp, std::memory_order::relaxed)) {
            break;
        }
    }

    return MakeFuture<TTimestamp>(firstTimestamp);
}

void TTimestampProviderBase::UpdateLatestTimestamp()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YT_LOG_DEBUG("Updating latest timestamp");
    GenerateTimestamps(1).Subscribe(
        BIND([] (const TErrorOr<TTimestamp>& timestampOrError) {
            if (timestampOrError.IsOK()) {
                YT_LOG_DEBUG("Latest timestamp updated (Timestamp: %v)",
                    timestampOrError.Value());
            } else {
                YT_LOG_WARNING(timestampOrError, "Error updating latest timestamp");
            }
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
