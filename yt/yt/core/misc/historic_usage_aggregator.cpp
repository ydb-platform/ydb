#include "historic_usage_aggregator.h"

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/ymath.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

THistoricUsageAggregationParameters::THistoricUsageAggregationParameters(
    EHistoricUsageAggregationMode mode,
    double emaAlpha)
    : Mode(mode)
    , EmaAlpha(emaAlpha)
{ }

THistoricUsageAggregationParameters::THistoricUsageAggregationParameters(
    const THistoricUsageConfigPtr& config)
    : Mode(config->AggregationMode)
    , EmaAlpha(config->EmaAlpha)
{ }

bool THistoricUsageAggregationParameters::operator==(const THistoricUsageAggregationParameters& other) const
{
    return Mode == other.Mode && EmaAlpha == other.EmaAlpha;
}

////////////////////////////////////////////////////////////////////////////////

THistoricUsageAggregator::THistoricUsageAggregator()
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
    Reset();
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

    // If LastExponentialMovingAverageUpdateTime_ is zero, this is the first update (after most
    // recent reset) and we just want to leave EMA = 0.0, as if there was no previous usage.
    if (Parameters_.Mode == EHistoricUsageAggregationMode::ExponentialMovingAverage &&
        LastExponentialMovingAverageUpdateTime_ != TInstant::Zero())
    {
        auto sinceLast = now - LastExponentialMovingAverageUpdateTime_;
        auto w = Exp2(-1. * Parameters_.EmaAlpha * sinceLast.SecondsFloat());
        ExponentialMovingAverage_ = w * ExponentialMovingAverage_ + (1 - w) * value;
    }

    LastExponentialMovingAverageUpdateTime_ = now;
}

double THistoricUsageAggregator::GetHistoricUsage() const
{
    return ExponentialMovingAverage_;
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
