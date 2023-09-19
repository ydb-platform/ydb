#include "backoff_strategy.h"

#include <util/random/normal.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TConstantBackoffOptions::operator TExponentialBackoffOptions() const
{
    return TExponentialBackoffOptions{
        .InvocationCount = InvocationCount,
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
    InvocationIndex_ = 0;
    Backoff_ = Options_.MinBackoff;
    ApplyJitter();
}

bool TBackoffStrategy::Next()
{
    if (InvocationIndex_ > 0) {
        Backoff_ = std::min(Backoff_ * Options_.BackoffMultiplier, Options_.MaxBackoff);
        ApplyJitter();
    }
    return ++InvocationIndex_ < Options_.InvocationCount;
}

int TBackoffStrategy::GetInvocationIndex() const
{
    return InvocationIndex_;
}

int TBackoffStrategy::GetInvocationCount() const
{
    return Options_.InvocationCount;
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


void TBackoffStrategy::UpdateOptions(const TExponentialBackoffOptions& newOptions)
{
    Options_ = newOptions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
