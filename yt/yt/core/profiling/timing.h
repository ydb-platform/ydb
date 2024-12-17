#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <library/cpp/yt/cpu_clock/clock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

using NYT::GetCpuInstant;
using NYT::GetInstant;
using NYT::CpuDurationToDuration;
using NYT::DurationToCpuDuration;
using NYT::CpuInstantToInstant;
using NYT::InstantToCpuInstant;

//! Converts a duration to TValue suitable for profiling.
/*!
 *  The current implementation just returns microseconds.
 */
TValue DurationToValue(TDuration duration);

//! Converts a TValue to duration.
/*!
 *  The current implementation assumes that #value is given in microseconds.
 */
TDuration ValueToDuration(TValue value);

//! Converts a CPU duration into TValue suitable for profiling.
TValue CpuDurationToValue(TCpuDuration cpuDuration);

////////////////////////////////////////////////////////////////////////////////

//! Continuously tracks the wall time passed since construction.
class TWallTimer
{
public:
    TWallTimer(bool start = true);

    TInstant GetStartTime() const;
    TDuration GetElapsedTime() const;
    TValue GetElapsedValue() const;
    //! Returns the time that has elapsed since the last call to Start().
    TDuration GetCurrentDuration() const;

    TCpuInstant GetStartCpuTime() const;
    TCpuDuration GetElapsedCpuTime() const;
    TCpuDuration GetCurrentCpuDuration() const;

    void Start();
    void StartIfNotActive();
    void Stop();
    void Restart();

    void Persist(const TStreamPersistenceContext& context);

private:
    TCpuInstant StartTime_ = 0;
    TCpuDuration Duration_ = 0;
    bool Active_ = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Upon destruction, increments the value by the elapsed time (measured by the timer)
//! passed since construction.
template <class TTimer>
class TValueIncrementingTimingGuard
{
public:
    explicit TValueIncrementingTimingGuard(TDuration* value);
    ~TValueIncrementingTimingGuard();

    TValueIncrementingTimingGuard(const TValueIncrementingTimingGuard&) = delete;
    TValueIncrementingTimingGuard& operator=(const TValueIncrementingTimingGuard&) = delete;

private:
    TDuration* const Value_;
    TTimer Timer_;
};

////////////////////////////////////////////////////////////////////////////////

class TCpuDurationIncrementingGuard
{
public:
    explicit TCpuDurationIncrementingGuard(TCpuDuration* value);
    ~TCpuDurationIncrementingGuard();

private:
    TCpuDuration* Value_;
    TCpuInstant StartInstant_;
};

////////////////////////////////////////////////////////////////////////////////

//! Similar to TWallTimer but excludes the time passed while the fiber was inactive.
class TFiberWallTimer
    : public TWallTimer
    , private NConcurrency::TContextSwitchGuard
{
public:
    TFiberWallTimer();
};

////////////////////////////////////////////////////////////////////////////////

//! Calls TTimer::Start() on construction and TTimer::Stop() on destruction.
template <class TTimer>
class TTimerGuard
    : public TNonCopyable
{
public:
    explicit TTimerGuard(TTimer* timer);

    TTimerGuard(TTimerGuard&& other) noexcept;
    TTimerGuard& operator = (TTimerGuard&& other) noexcept;

    ~TTimerGuard();

private:
    TTimer* Timer_;

    void TryStopTimer() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

//! Calls #callback at the end of execution slice if it was longer than #threshold.
class TFiberSliceTimer
    : private NConcurrency::TContextSwitchGuard
{
public:
    TFiberSliceTimer(TCpuDuration threshold, std::function<void(TCpuDuration)> callback);
    ~TFiberSliceTimer();

private:
    const TCpuDuration Threshold_;
    const std::function<void(TCpuDuration)> Callback_;

    TCpuInstant LastInTime_;

    void OnIn() noexcept;
    void OnOut() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

#define TIMING_INL_H_
#include "timing-inl.h"
#undef TIMING_INL_H_
