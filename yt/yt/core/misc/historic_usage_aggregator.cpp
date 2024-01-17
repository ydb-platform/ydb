#include "historic_usage_aggregator.h"

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/ymath.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

THistoricUsageAggregationParameters::THistoricUsageAggregationParameters(
    EHistoricUsageAggregationMode mode,
    double emaAlpha,
    bool resetOnNewParameters)
    : Mode(mode)
    , EmaAlpha(emaAlpha)
    , ResetOnNewParameters(resetOnNewParameters)
{ }

THistoricUsageAggregationParameters::THistoricUsageAggregationParameters(
    const THistoricUsageConfigPtr& config)
    : Mode(config->AggregationMode)
    , EmaAlpha(config->EmaAlpha)
    , ResetOnNewParameters(config->ResetOnNewParameters)
{ }

////////////////////////////////////////////////////////////////////////////////

THistoricUsageAggregator::THistoricUsageAggregator()
{
    Reset();
}

THistoricUsageAggregator::THistoricUsageAggregator(
    const THistoricUsageAggregationParameters& parameters)
    : Parameters_(parameters)
{
    Reset();
}

void THistoricUsageAggregator::UpdateParameters(
    const THistoricUsageAggregationParameters& newParameters)
{
    if (Parameters_ == newParameters) {
        return;
    }

    Parameters_ = newParameters;
    if (Parameters_.ResetOnNewParameters) {
        Reset();
    }
}

void THistoricUsageAggregator::Reset()
{
    ExponentialMovingAverage_ = 0.0;
    LastExponentialMovingAverageUpdateTime_ = TInstant::Zero();
}

void THistoricUsageAggregator::UpdateAt(TInstant now, double value)
{
    if (now < LastExponentialMovingAverageUpdateTime_) {
        return;
    }

    ExponentialMovingAverage_ = ApplyUpdate(ExponentialMovingAverage_, now, value);
    LastExponentialMovingAverageUpdateTime_ = now;
}

double THistoricUsageAggregator::SimulateUpdate(TInstant now, double value) const
{
    auto simulatedValue = GetHistoricUsage();

    if (now < LastExponentialMovingAverageUpdateTime_) {
        return simulatedValue;
    }

    return ApplyUpdate(simulatedValue, now, value);
}

double THistoricUsageAggregator::GetHistoricUsage() const
{
    return ExponentialMovingAverage_;
}

bool THistoricUsageAggregator::ShouldFlush() const
{
    return
        Parameters_.Mode == EHistoricUsageAggregationMode::None ||
        Parameters_.EmaAlpha == 0.0 ||
        LastExponentialMovingAverageUpdateTime_ == TInstant::Zero();
}

double THistoricUsageAggregator::ApplyUpdate(double current, TInstant now, double value) const
{
    if (ShouldFlush()) {
        current = value;
    } else {
        auto sinceLast = now - LastExponentialMovingAverageUpdateTime_;
        auto w = Exp2(-1. * Parameters_.EmaAlpha * sinceLast.SecondsFloat());
        current = w * ExponentialMovingAverage_ + (1 - w) * value;
    }
    return current;
}

////////////////////////////////////////////////////////////////////////////////

TAverageHistoricUsageAggregator::TAverageHistoricUsageAggregator(TDuration period)
    : Period_(period)
{ }

void TAverageHistoricUsageAggregator::UpdateParameters(THistoricUsageAggregationParameters params)
{
    HistoricUsageAggregator_.UpdateParameters(params);
}

double TAverageHistoricUsageAggregator::GetHistoricUsage()
{
    auto now = NProfiling::GetInstant();
    MaybeFlush(now);
    return HistoricUsageAggregator_.GetHistoricUsage();
}

void TAverageHistoricUsageAggregator::UpdateAt(TInstant now, double value)
{
    MaybeFlush(now);
    CurrentUsage_ += value;
}

void TAverageHistoricUsageAggregator::MaybeFlush(TInstant now)
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

    HistoricUsageAggregator_.UpdateAt(now, usagePerPeriod);

    IntervalStart_ = now;
    CurrentUsage_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
