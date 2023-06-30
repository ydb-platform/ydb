#include "ema_counter.h"

#include <cmath>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEmaCounter::TEmaCounter(TWindowDurations windowDurations)
    : WindowDurations(std::move(windowDurations))
    , WindowRates(WindowDurations.size())
{ }

void TEmaCounter::Update(i64 newCount, TInstant newTimestamp)
{
    if (!LastTimestamp) {
        // Just set current value, we do not know enough information to deal with rates.
        Count = newCount;
        LastTimestamp = newTimestamp;
        StartTimestamp = newTimestamp;
        return;
    }

    if (newTimestamp <= *LastTimestamp) {
        // Ignore obsolete update.
        return;
    }

    auto timeDelta = (newTimestamp - *LastTimestamp).SecondsFloat();
    i64 countDelta = std::max(Count, newCount) - Count;
    auto newRate = countDelta / timeDelta;

    Count = newCount;
    ImmediateRate = newRate;
    LastTimestamp = newTimestamp;

    for (int windowIndex = 0; windowIndex < std::ssize(WindowDurations); ++windowIndex) {
        auto exp = std::exp(-timeDelta / (WindowDurations[windowIndex].SecondsFloat() / 2.0));
        auto& rate = WindowRates[windowIndex];
        rate = newRate * (1 - exp) + rate * exp;
    }
}

std::optional<double> TEmaCounter::GetRate(int windowIndex, TInstant currentTimestamp) const
{
    if (!StartTimestamp) {
        return {};
    }

    if (*StartTimestamp + WindowDurations[windowIndex] > currentTimestamp) {
        return {};
    }

    return WindowRates[windowIndex];
}

TEmaCounter operator+(const TEmaCounter& lhs, const TEmaCounter& rhs)
{
    TEmaCounter result = lhs;
    result += rhs;
    return result;
}

TEmaCounter& operator+=(TEmaCounter& lhs, const TEmaCounter& rhs)
{
    YT_VERIFY(lhs.WindowDurations == rhs.WindowDurations);
    lhs.LastTimestamp = std::max(lhs.LastTimestamp, rhs.LastTimestamp);
    lhs.StartTimestamp = std::max(lhs.StartTimestamp, rhs.StartTimestamp);
    lhs.Count += rhs.Count;
    lhs.ImmediateRate += rhs.ImmediateRate;
    for (int windowIndex = 0; windowIndex < std::ssize(lhs.WindowDurations); ++windowIndex) {
        lhs.WindowRates[windowIndex] += rhs.WindowRates[windowIndex];
    }
    return lhs;
}

TEmaCounter& operator*=(TEmaCounter& lhs, double coefficient)
{
    lhs.Count *= coefficient;
    lhs.ImmediateRate *= coefficient;
    for (auto& rate : lhs.WindowRates) {
        rate *= coefficient;
    }
    return lhs;
}

TEmaCounter operator*(const TEmaCounter& lhs, double coefficient)
{
    TEmaCounter result = lhs;
    result *= coefficient;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
