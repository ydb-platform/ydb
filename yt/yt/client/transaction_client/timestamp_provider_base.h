#pragma once

#include "timestamp_provider.h"

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

//! Mostly implements the tracking of the latest timestamp, thus factoring out
//! common code between the actual implementations.
class TTimestampProviderBase
    : public ITimestampProvider
{
protected:
    explicit TTimestampProviderBase(std::optional<TDuration> latestTimestampUpdatePeriod);

    virtual TFuture<TTimestamp> DoGenerateTimestamps(
        int count,
        NObjectClient::TCellTag clockClusterTag) = 0;

public:
    TFuture<TTimestamp> GenerateTimestamps(
        int count,
        NObjectClient::TCellTag clockClusterTag = NObjectClient::InvalidCellTag) override;

    TTimestamp GetLatestTimestamp(NObjectClient::TCellTag clockClusterTag = NObjectClient::InvalidCellTag) override;

private:
    const std::optional<TDuration> LatestTimestampUpdatePeriod_;

    std::atomic<i64> GetLatestTimestampCallCounter_ = 0;
    NConcurrency::TPeriodicExecutorPtr LatestTimestampExecutor_;
    std::atomic<TTimestamp> LatestTimestamp_ = MinTimestamp;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ClockClusterTagMapSpinLock_);
    THashMap<NObjectClient::TCellTag, std::atomic<TTimestamp>> LatestTimestampByClockCellTag_;

    TFuture<TTimestamp> OnGenerateTimestamps(
        int count,
        NObjectClient::TCellTag clockClusterTag,
        const TErrorOr<TTimestamp>& timestampOrError);

    void UpdateLatestTimestamp();

    std::atomic<TTimestamp>& GetLatestTimestampReferenceByTag(NObjectClient::TCellTag clockClusterTag);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

