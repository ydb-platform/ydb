#include "backoff_strategy.h"

#include <util/random/normal.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TConstantBackoffOptions::operator TExponentialBackoffOptions() const
{
    return TExponentialBackoffOptions{
        .RetryCount = RetryCount,
        .MinBackoff = Backoff,
        .MaxBackoff = Backoff,
        .BackoffMultiplier = 1.0,
        .BackoffJitter = BackoffJitter
    };
}

////////////////////////////////////////////////////////////////////////////////

TBackoffStrategy::TBackoffStrategy(const TExponentialBackoffOptions& options)
    : Options_(options)
{
    Restart();
}

void TBackoffStrategy::Restart()
{
    RetryIndex_ = 0;
    Backoff_ = Options_.MinBackoff;
    ApplyJitter();
}

bool TBackoffStrategy::NextRetry()
{
    if (RetryIndex_ > 0) {
        Backoff_ = std::min(Backoff_ * Options_.BackoffMultiplier, Options_.MaxBackoff);
        ApplyJitter();
    }
    return ++RetryIndex_ < Options_.RetryCount;
}

int TBackoffStrategy::GetRetryIndex() const
{
    return RetryIndex_;
}

int TBackoffStrategy::GetRetryCount() const
{
    return Options_.RetryCount;
}

TDuration TBackoffStrategy::GetBackoff() const
{
    return BackoffWithJitter_;
}

void TBackoffStrategy::ApplyJitter()
{
    auto rnd = StdNormalRandom<double>();
    bool isNegative = rnd < 0;
    auto jitter = std::abs(rnd) * Options_.BackoffJitter * Backoff_;
    BackoffWithJitter_ = isNegative ? Backoff_ - jitter : Backoff_ + jitter;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
