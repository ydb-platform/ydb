#ifndef EMA_COUNTER_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "ema_counter.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount>::TEmaCounter(TEmaCounterWindowDurations<WindowCount> windowDurations)
    : WindowDurations(std::move(windowDurations))
    , WindowRates(WindowDurations.size())
{ }

template <typename T, int WindowCount>
    requires std::is_arithmetic_v<T>
void TEmaCounter<T, WindowCount>::Update(T newCount, TInstant newTimestamp)
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
    auto countDelta = std::max(Count, newCount) - Count;
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

template <typename T, int WindowCount>
    requires std::is_arithmetic_v<T>
std::optional<double> TEmaCounter<T, WindowCount>::GetRate(int windowIndex, TInstant currentTimestamp) const
{
    if (!StartTimestamp) {
        return {};
    }

    if (*StartTimestamp + WindowDurations[windowIndex] > currentTimestamp) {
        return {};
    }

    return WindowRates[windowIndex];
}

template <typename T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount> operator+(const TEmaCounter<T, WindowCount>& lhs, const TEmaCounter<T, WindowCount>& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

template <typename T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount>& operator+=(TEmaCounter<T, WindowCount>& lhs, const TEmaCounter<T, WindowCount>& rhs)
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

template <typename T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount>& operator*=(TEmaCounter<T, WindowCount>& lhs, double coefficient)
{
    lhs.Count *= coefficient;
    lhs.ImmediateRate *= coefficient;
    for (auto& rate : lhs.WindowRates) {
        rate *= coefficient;
    }
    return lhs;
}

template <typename T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount> operator*(const TEmaCounter<T, WindowCount>& lhs, double coefficient)
{
    auto result = lhs;
    result *= coefficient;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
