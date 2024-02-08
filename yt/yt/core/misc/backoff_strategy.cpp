#include "backoff_strategy.h"
#include "jitter.h"

#include <util/random/normal.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

double BackoffStrategyDefaultRandomGenerator()
{
    // StdNormalRandom is unlikely to produce a value outside of [-Max, Max] range.
    constexpr double Max = 7.0;
    return std::clamp(StdNormalRandom<double>() / Max, -1.0, +1.0);
}

} // namespace NDetail

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
    return ++InvocationIndex_ <= Options_.InvocationCount;
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
    BackoffWithJitter_ = ::NYT::ApplyJitter(
        Backoff_,
        Options_.BackoffJitter,
        &NDetail::BackoffStrategyDefaultRandomGenerator);
}

void TBackoffStrategy::UpdateOptions(const TExponentialBackoffOptions& newOptions)
{
    Options_ = newOptions;
}

////////////////////////////////////////////////////////////////////////////////

TRelativeConstantBackoffStrategy::TRelativeConstantBackoffStrategy(
    TConstantBackoffOptions options)
    : Options_(options)
    , BackoffWithJitter_(TDuration::Zero())
    , LastInvocationTime_(TInstant::Zero())
{ }

bool TRelativeConstantBackoffStrategy::IsOverBackoff(TInstant now) const
{
    auto deadline = GetBackoffDeadline();

    return
        deadline == TInstant::Zero() ||
        now >= deadline;
}

void TRelativeConstantBackoffStrategy::RecordInvocation()
{
    DoRecordInvocation(TInstant::Now());
}

bool TRelativeConstantBackoffStrategy::RecordInvocationIfOverBackoff(TInstant now)
{
    if (IsOverBackoff(now)) {
        DoRecordInvocation(now);
        return true;
    }

    return false;
}

TInstant TRelativeConstantBackoffStrategy::GetLastInvocationTime() const
{
    return LastInvocationTime_;
}

TInstant TRelativeConstantBackoffStrategy::GetBackoffDeadline() const
{
    return LastInvocationTime_ != TInstant::Zero() ?
        LastInvocationTime_ + BackoffWithJitter_ :
        TInstant::Zero();
}

void TRelativeConstantBackoffStrategy::Restart()
{
    LastInvocationTime_ = TInstant::Zero();
    BackoffWithJitter_ = TDuration::Zero();
}

void TRelativeConstantBackoffStrategy::UpdateOptions(TConstantBackoffOptions newOptions)
{
    Options_ = newOptions;
}

void TRelativeConstantBackoffStrategy::DoRecordInvocation(TInstant now)
{
    LastInvocationTime_ = now;
    BackoffWithJitter_ = ::NYT::ApplyJitter(
        Options_.Backoff,
        Options_.BackoffJitter,
        &NDetail::BackoffStrategyDefaultRandomGenerator);
}

} // namespace NYT
