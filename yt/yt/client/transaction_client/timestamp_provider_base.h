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

    virtual TFuture<TTimestamp> DoGenerateTimestamps(int count) = 0;

public:
    TFuture<TTimestamp> GenerateTimestamps(int count) override;
    TTimestamp GetLatestTimestamp() override;

private:
    const std::optional<TDuration> LatestTimestampUpdatePeriod_;

    std::atomic<i64> GetLatestTimestampCallCounter_ = 0;
    NConcurrency::TPeriodicExecutorPtr LatestTimestampExecutor_;
    std::atomic<TTimestamp> LatestTimestamp_ = MinTimestamp;

    TFuture<TTimestamp> OnGenerateTimestamps(
        int count,
        const TErrorOr<TTimestamp>& timestampOrError);
    void UpdateLatestTimestamp();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient

