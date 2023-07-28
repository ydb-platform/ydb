#include "timing.h"

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/system/hp_timer.h>

#include <array>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

TValue DurationToValue(TDuration duration)
{
    return duration.MicroSeconds();
}

TDuration ValueToDuration(TValue value)
{
    // TDuration is unsigned and thus does not support negative values.
    if (value < 0) {
        value = 0;
    }
    return TDuration::MicroSeconds(static_cast<ui64>(value));
}

TValue CpuDurationToValue(TCpuDuration cpuDuration)
{
    return cpuDuration > 0
        ? DurationToValue(CpuDurationToDuration(cpuDuration))
        : -DurationToValue(CpuDurationToDuration(-cpuDuration));
}

////////////////////////////////////////////////////////////////////////////////

TWallTimer::TWallTimer(bool start)
{
    if (start) {
        Start();
    }
}

TInstant TWallTimer::GetStartTime() const
{
    return CpuInstantToInstant(GetStartCpuTime());
}

TDuration TWallTimer::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

TCpuInstant TWallTimer::GetStartCpuTime() const
{
    return StartTime_;
}

TCpuDuration TWallTimer::GetElapsedCpuTime() const
{
    return Duration_ + GetCurrentCpuDuration();
}

TValue TWallTimer::GetElapsedValue() const
{
    return DurationToValue(GetElapsedTime());
}

void TWallTimer::Start()
{
    StartTime_ = GetCpuInstant();
    Active_ = true;
}

void TWallTimer::StartIfNotActive()
{
    if (!Active_) {
        Start();
    }
}

void TWallTimer::Stop()
{
    Duration_ += GetCurrentCpuDuration();
    StartTime_ = 0;
    Active_ = false;
}

void TWallTimer::Restart()
{
    Duration_ = 0;
    Start();
}

TCpuDuration TWallTimer::GetCurrentCpuDuration() const
{
    return Active_
        ? Max<TCpuDuration>(GetCpuInstant() - StartTime_, 0)
        : 0;
}

void TWallTimer::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Active_);
    if (context.IsSave()) {
        auto duration = GetElapsedCpuTime();
        Persist(context, duration);
    } else {
        Persist(context, Duration_);
        StartTime_ = Active_ ? GetCpuInstant() : 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

TFiberWallTimer::TFiberWallTimer()
    : NConcurrency::TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Start(); })
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
