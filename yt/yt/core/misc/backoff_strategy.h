#pragma once

#include "backoff_strategy_api.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TExponentialBackoffOptions
{
    static constexpr int DefaultRetryCount = 10;
    static constexpr auto DefaultMinBackoff = TDuration::Seconds(1);
    static constexpr auto DefaultMaxBackoff = TDuration::Seconds(5);
    static constexpr double DefaultBackoffMultiplier = 1.5;
    static constexpr double DefaultBackoffJitter = 0.1;

    int RetryCount = DefaultRetryCount;
    TDuration MinBackoff = DefaultMinBackoff;
    TDuration MaxBackoff = DefaultMaxBackoff;
    double BackoffMultiplier = DefaultBackoffMultiplier;
    double BackoffJitter = DefaultBackoffJitter;
};

////////////////////////////////////////////////////////////////////////////////

struct TConstantBackoffOptions
{
    static constexpr int DefaultRetryCount = 10;
    static constexpr auto DefaultBackoff = TDuration::Seconds(3);
    static constexpr double DefaultBackoffJitter = 0.1;

    int RetryCount = DefaultRetryCount;
    TDuration Backoff = DefaultBackoff;
    double BackoffJitter = DefaultBackoffJitter;

    operator TExponentialBackoffOptions() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Implements exponential backoffs with jitter.
class TBackoffStrategy
{
public:
    explicit TBackoffStrategy(const TExponentialBackoffOptions& options);

    void Restart();
    bool Next();

    int GetRetryIndex() const;
    int GetRetryCount() const;

    TDuration GetBackoff() const;

    void UpdateOptions(const TExponentialBackoffOptions& newOptions);

private:
    TExponentialBackoffOptions Options_;

    int RetryIndex_;
    TDuration Backoff_;
    TDuration BackoffWithJitter_;

    void ApplyJitter();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
