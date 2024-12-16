#pragma once

#include <library/cpp/yt/small_containers/compact_vector.h>

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A typical number of configured time windows.
constexpr int TypicalWindowCount = 2;

template <int WindowCount = TypicalWindowCount>
using TEmaCounterWindowDurations = TCompactVector<TDuration, WindowCount>;

template <int WindowCount = TypicalWindowCount>
using TEmaCounterWindowRates = TCompactVector<double, WindowCount>;

////////////////////////////////////////////////////////////////////////////////

//! A helper structure for maintaining a monotonic counter and
//! estimating its average rate over a set of configured time windows
//! using EMA (exponential moving average) technique.
template<typename T, int WindowCount = TypicalWindowCount>
    requires std::is_arithmetic_v<T>
struct TEmaCounter
{
    //! Current value of the counter.
    T Count = 0;
    //! Last update time.
    std::optional<TInstant> LastTimestamp;
    //! First update time.
    std::optional<TInstant> StartTimestamp;
    //! Rate (measured in units per second) calculated based on the last update,
    //! i.e. #Count delta divided by the time delta measured in seconds
    //! according to the last update.
    double ImmediateRate = 0.0;

    //! Durations of configured time windows.
    TEmaCounterWindowDurations<WindowCount> WindowDurations;
    //! Estimates of a rate over corresponding time windows.
    TEmaCounterWindowRates<WindowCount> WindowRates;

    explicit TEmaCounter(TEmaCounterWindowDurations<WindowCount> windowDurations);

    //! Set new value of counter, optionally providing a current timestamp.
    void Update(T newCount, TInstant newTimestamp = TInstant::Now());

    //! Returns the rate for the given window after enough time has passed
    //! for the values to be accurate (at least the duration of the window itself).
    //! Optionally a current timestamp can be provided.
    std::optional<double> GetRate(int windowIndex, TInstant currentTimestamp = TInstant::Now()) const;
};

// Operators for linear transformations (addition, scaling) of counters over the fixed set of windows.

template <class T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount> operator+(const TEmaCounter<T, WindowCount>& lhs, const TEmaCounter<T, WindowCount>& rhs);

template <class T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount>& operator+=(TEmaCounter<T, WindowCount>& lhs, const TEmaCounter<T, WindowCount>& rhs);

template <class T, int WindowCount>
    requires std::is_arithmetic_v<T>
TEmaCounter<T, WindowCount>& operator*=(TEmaCounter<T, WindowCount>& lhs, double coefficient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EMA_COUNTER_INL_H_
#include "ema_counter-inl.h"
#undef EMA_COUNTER_INL_H_
