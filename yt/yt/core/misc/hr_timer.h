#pragma once

#include "common.h"

#include <util/system/datetime.h>

namespace NYT::NHRTimer {

////////////////////////////////////////////////////////////////////////////////

// Returns CPU internal cycle counter.
// On modern systems, cycle counters are consistent across cores and cycle rate
// can be considered constant for practical purposes.
Y_FORCE_INLINE ui64 GetRdtsc()
{
    return GetCycleCount();
}

// Represents an offset from an arbitrary point in the past;
// it should be used only for relative measurements.
struct THRInstant
{
    i64 Seconds;
    i64 Nanoseconds;
};

// Represents a duration in nano-seconds.
using THRDuration = ui64;

#ifdef _linux_
static_assert(
    sizeof(THRInstant) == sizeof(struct timespec),
    "THRInstant should be ABI-compatible with struct timespec");
static_assert(
    offsetof(THRInstant, Seconds) == offsetof(struct timespec, tv_sec),
    "THRInstant should be ABI-compatible with struct timespec");
static_assert(
    offsetof(THRInstant, Nanoseconds) == offsetof(struct timespec, tv_nsec),
    "THRInstant should be ABI-compatible with struct timespec");
#endif

// Returns instant.
void GetHRInstant(THRInstant* instant);

// Returns time difference in nanoseconds.
THRDuration GetHRDuration(const THRInstant& begin, const THRInstant& end);

// Returns instant resolution.
THRDuration GetHRResolution();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHRTimer
