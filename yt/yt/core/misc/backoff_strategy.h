#pragma once

#include "backoff_strategy_api.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TExponentialBackoffOptions
{
    static constexpr int DefaultInvocationCount = 10;
    static constexpr auto DefaultMinBackoff = TDuration::Seconds(1);
    static constexpr auto DefaultMaxBackoff = TDuration::Seconds(5);
    static constexpr double DefaultBackoffMultiplier = 1.5;
    static constexpr double DefaultBackoffJitter = 0.1;

    int InvocationCount = DefaultInvocationCount;
    TDuration MinBackoff = DefaultMinBackoff;
    TDuration MaxBackoff = DefaultMaxBackoff;
    double BackoffMultiplier = DefaultBackoffMultiplier;
    double BackoffJitter = DefaultBackoffJitter;
};

////////////////////////////////////////////////////////////////////////////////

struct TConstantBackoffOptions
{
    static constexpr int DefaultInvocationCount = 10;
    static constexpr auto DefaultBackoff = TDuration::Seconds(3);
    static constexpr double DefaultBackoffJitter = 0.1;

    int InvocationCount = DefaultInvocationCount;
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

    int GetInvocationIndex() const;
    int GetInvocationCount() const;

    TDuration GetBackoff() const;

    void UpdateOptions(const TExponentialBackoffOptions& newOptions);

private:
    TExponentialBackoffOptions Options_;

    int InvocationIndex_;
    TDuration Backoff_;
    TDuration BackoffWithJitter_;

    void ApplyJitter();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
