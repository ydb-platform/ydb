#include "duration_moving_average.h"

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TDurationMovingAverage::TDurationMovingAverage(TDuration window)
    : Window_(window)
{
    YT_VERIFY(Window_ > TDuration::Zero());
}

double TDurationMovingAverage::GetAverage() const
{
    if (Values_.empty()) {
        return 0.0;
    }

    double sum = 0.0;
    for (const auto& [_, value] : Values_) {
        sum += value;
    }

    return sum / Values_.size();
}

void TDurationMovingAverage::UpdateAt(TInstant now, double value)
{
    RemoveOldValues(now);
    Values_.emplace_back(now, value);
}

double TDurationMovingAverage::EstimateAverageWithNewValue(TInstant now, double value) const
{
    if (Values_.empty()) {
        return value;
    }

    // Calculate sum of values within the window
    double sum = 0.0;
    int count = 0;

    TInstant cutoffTime = now - Window_;

    for (const auto& [timestamp, val] : Values_) {
        if (timestamp >= cutoffTime) {
            sum += val;
            ++count;
        }
    }

    // Add the new value
    sum += value;
    ++count;

    return count > 0 ? sum / count : 0.0;
}

void TDurationMovingAverage::SetWindow(TDuration window, bool resetOnNewWindow)
{
    YT_VERIFY(window > TDuration::Zero());

    if (Window_ == window) {
        return;
    }

    Window_ = window;

    if (resetOnNewWindow) {
        Reset();
    }
}

void TDurationMovingAverage::Reset()
{
    Values_.clear();
}

void TDurationMovingAverage::RemoveOldValues(TInstant now)
{
    TInstant cutoffTime = now - Window_;

    while (!Values_.empty() && Values_.front().first < cutoffTime) {
        Values_.pop_front();
    }
}

////////////////////////////////////////////////////////////////////////////////

TAverageDurationMovingAverage::TAverageDurationMovingAverage(
    TDuration window,
    TDuration period)
    : Period_(period)
    , MovingAverage_(window)
{ }

void TAverageDurationMovingAverage::SetWindow(TDuration window, bool resetOnNewWindow)
{
    MovingAverage_.SetWindow(window, resetOnNewWindow);
}

double TAverageDurationMovingAverage::GetAverage()
{
    auto now = NProfiling::GetInstant();
    MaybeFlush(now);
    return MovingAverage_.GetAverage();
}

void TAverageDurationMovingAverage::UpdateAt(TInstant now, double value)
{
    MaybeFlush(now);
    CurrentUsage_ += value;
}

void TAverageDurationMovingAverage::MaybeFlush(TInstant now)
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

    MovingAverage_.UpdateAt(now, usagePerPeriod);

    IntervalStart_ = now;
    CurrentUsage_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
