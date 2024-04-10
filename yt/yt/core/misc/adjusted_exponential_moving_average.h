#pragma once

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAdjustedExponentialMovingAverage
{
public:
    static constexpr auto DefaultHalflife = TDuration::Seconds(10);

    explicit TAdjustedExponentialMovingAverage(TDuration halflife = DefaultHalflife);

    double GetAverage() const;

    void UpdateAt(TInstant now, double value);

    //! Simulates UpdateAt(now, value) + GetAverage() without changing the state.
    double EstimateAverageWithNewValue(TInstant now, double value) const;

    void SetHalflife(TDuration halflife, bool resetOnNewHalflife = true);

    void Reset();

private:
    //! Parameter of adjusted exponential moving average.
    //! It means that weight of the data, recorded Halflife_ seconds ago
    //! is roughly half of what it was initially.
    //! Adjusted EMA formula described here: https://clck.ru/37bd2i
    //! under "adjusted = True" with (1 - alpha) = 2^(-1 / Halflife_).
    TDuration Halflife_;

    TInstant LastUpdateTime_ = TInstant::Zero();

    double TotalDenominator_ = 1.0;
    double TotalNumerator_ = 0.0;

    std::pair<double, double> ApplyUpdate(
        double denominator,
        double numerator,
        TInstant now,
        double value) const;
};

////////////////////////////////////////////////////////////////////////////////

class TAverageAdjustedExponentialMovingAverage
{
public:
    explicit TAverageAdjustedExponentialMovingAverage(
        TDuration halflife = TAdjustedExponentialMovingAverage::DefaultHalflife,
        TDuration period = TDuration::Seconds(1));

    void SetHalflife(TDuration halflife, bool resetOnNewHalflife = true);

    double GetAverage();

    void UpdateAt(TInstant now, double value);

private:
    TDuration Period_;

    TInstant IntervalStart_ = TInstant::Zero();
    double CurrentUsage_ = 0;

    TAdjustedExponentialMovingAverage Impl_;

    void MaybeFlush(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
