#pragma once

#include <util/datetime/base.h>

#include <deque>
#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TDurationMovingAverage
{
public:
    static constexpr auto DefaultWindow = TDuration::Seconds(10);

    explicit TDurationMovingAverage(TDuration window = DefaultWindow);

    double GetAverage() const;

    //! Updates the moving average with a new measurement at the specified time.
    //! The monotonicity of the "now" parameter is desirable for greater accuracy, but it is not required.
    void UpdateAt(TInstant now, double value);

    //! Simulates UpdateAt(now, value) + GetAverage() without changing the state.
    double EstimateAverageWithNewValue(TInstant now, double value) const;

    void SetWindow(TDuration window, bool resetOnNewWindow = true);

    void Reset();

private:
    //! Time window for the moving average.
    //! Only values within this window from the current time are considered.
    TDuration Window_;

    //! Storage for (timestamp, value) pairs.
    std::deque<std::pair<TInstant, double>> Values_;

    void RemoveOldValues(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

class TAverageDurationMovingAverage
{
public:
    static constexpr auto DefaultPeriod = TDuration::Seconds(1);

    explicit TAverageDurationMovingAverage(
        TDuration window = TDurationMovingAverage::DefaultWindow,
        TDuration period = DefaultPeriod);

    void SetWindow(TDuration window, bool resetOnNewWindow = true);

    double GetAverage();

    //! Updates the moving average with a new measurement at the specified time.
    //! The monotonicity of the "now" parameter is desirable for greater accuracy, but it is not required.
    void UpdateAt(TInstant now, double value);

private:
    TDuration Period_;

    TInstant IntervalStart_ = TInstant::Zero();
    double CurrentUsage_ = 0;

    TDurationMovingAverage MovingAverage_;

    void MaybeFlush(TInstant now);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
