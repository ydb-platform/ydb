#include "adjusted_exponential_moving_average.h"

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/ymath.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TAdjustedExponentialMovingAverage::TAdjustedExponentialMovingAverage(TDuration halflife)
    : Halflife_(halflife)
{ }

double TAdjustedExponentialMovingAverage::GetAverage() const
{
    return TotalNumerator_ / TotalDenominator_;
}

void TAdjustedExponentialMovingAverage::UpdateAt(TInstant now, double value)
{
    if (now < LastUpdateTime_) {
        return;
    }

    std::tie(TotalDenominator_, TotalNumerator_) =
        ApplyUpdate(TotalDenominator_, TotalNumerator_, now, value);
    LastUpdateTime_ = now;
}

//! Simulates UpdateAt(now, value) + GetAverage() without changing the state.
double TAdjustedExponentialMovingAverage::EstimateAverageWithNewValue(TInstant now, double value) const
{
    if (now < LastUpdateTime_) {
        return GetAverage();
    }

    auto [denominator, numerator] = ApplyUpdate(TotalDenominator_, TotalNumerator_, now, value);
    return numerator / denominator;
}

void TAdjustedExponentialMovingAverage::SetHalflife(TDuration halflife, bool resetOnNewHalflife)
{
    if (Halflife_ == halflife) {
        return;
    }

    Halflife_ = halflife;

    if (resetOnNewHalflife) {
        Reset();
    }
}

void TAdjustedExponentialMovingAverage::Reset()
{
    LastUpdateTime_ = TInstant::Zero();
    TotalDenominator_ = 1.0;
    TotalNumerator_ = 0.0;
}

std::pair<double, double> TAdjustedExponentialMovingAverage::ApplyUpdate(
    double denominator,
    double numerator,
    TInstant now,
    double value) const
{
    if (LastUpdateTime_ == TInstant::Zero() || Halflife_ == TDuration::Zero()) {
        return std::pair(1.0, value);
    }

    double secondsPassed = (now - LastUpdateTime_).SecondsFloat();
    double multiplier = Exp2(-secondsPassed / Halflife_.SecondsFloat());

    denominator = 1.0 + denominator * multiplier;
    numerator = value + numerator * multiplier;

    return std::pair(denominator, numerator);
}

////////////////////////////////////////////////////////////////////////////////

TAverageAdjustedExponentialMovingAverage::TAverageAdjustedExponentialMovingAverage(
    TDuration halflife,
    TDuration period)
    : Period_(period)
    , Impl_(halflife)
{ }

void TAverageAdjustedExponentialMovingAverage::SetHalflife(TDuration halflife, bool resetOnNewHalflife)
{
    Impl_.SetHalflife(halflife, resetOnNewHalflife);
}

double TAverageAdjustedExponentialMovingAverage::GetAverage()
{
    auto now = NProfiling::GetInstant();
    MaybeFlush(now);
    return Impl_.GetAverage();
}

void TAverageAdjustedExponentialMovingAverage::UpdateAt(TInstant now, double value)
{
    MaybeFlush(now);
    CurrentUsage_ += value;
}

void TAverageAdjustedExponentialMovingAverage::MaybeFlush(TInstant now)
{
    if (!IntervalStart_ || now < IntervalStart_) {
        IntervalStart_ = now;
        return;
    }

    auto diff = now - IntervalStart_;
    if (diff < Period_) {
        return;
    }

    auto ratio = diff / Period_;
    auto usagePerPeriod = CurrentUsage_ / ratio;

    Impl_.UpdateAt(now, usagePerPeriod);

    IntervalStart_ = now;
    CurrentUsage_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
