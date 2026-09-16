#include <stdio.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/I128.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Platform/Clock.h"

using namespace WAVM;
using namespace WAVM::Platform;

static I128 timespecToNS(timespec t) { return I128(U64(t.tv_sec)) * 1000000000 + U64(t.tv_nsec); }

static I128 getClockAsI128(clockid_t clockId)
{
	timespec clockTime;
	WAVM_ERROR_UNLESS(!clock_gettime(clockId, &clockTime));
	return timespecToNS(clockTime);
}

static I128 getClockResAsI128(clockid_t clockId)
{
	timespec clockResolution;
	WAVM_ERROR_UNLESS(!clock_getres(clockId, &clockResolution));
	return timespecToNS(clockResolution);
}

#if defined(__APPLE__)

#include <mach/mach_time.h>

static mach_timebase_info_data_t getTimebaseInfoData()
{
	mach_timebase_info_data_t timebaseInfoData;
	WAVM_ERROR_UNLESS(mach_timebase_info(&timebaseInfoData) == KERN_SUCCESS);
	return timebaseInfoData;
}

static mach_timebase_info_data_t getCachedTimebaseInfoData()
{
	static mach_timebase_info_data_t cachedTimebaseInfoData = getTimebaseInfoData();
	return cachedTimebaseInfoData;
}

static I128 getMachAbsoluteClock()
{
	mach_timebase_info_data_t cachedTimebaseInfoData = getCachedTimebaseInfoData();
	const I128 ticks = mach_absolute_time();
	const I128 ns = ticks * cachedTimebaseInfoData.numer / cachedTimebaseInfoData.denom;

	return ns;
}
static I128 getMachAbsoluteClockResolution()
{
	mach_timebase_info_data_t cachedTimebaseInfoData = getCachedTimebaseInfoData();
	I128 ticksPerNanosecond = I128(cachedTimebaseInfoData.numer) / cachedTimebaseInfoData.denom;
	if(ticksPerNanosecond == 0) { ticksPerNanosecond = 1; }
	return ticksPerNanosecond;
}
#endif

Time Platform::getClockTime(Clock clock)
{
	switch(clock)
	{
	case Clock::realtime: return Time{getClockAsI128(CLOCK_REALTIME)};
	case Clock::monotonic: {
#if defined(__APPLE__)
		return Time{getMachAbsoluteClock()};
#elif defined(CLOCK_MONOTONIC)
		return Time{getClockAsI128(CLOCK_MONOTONIC)};
#else
#error CLOCK_MONOTONIC not supported on this platform.
#endif
	}
	case Clock::processCPUTime: {
#ifdef CLOCK_PROCESS_CPUTIME_ID
		return Time{getClockAsI128(CLOCK_PROCESS_CPUTIME_ID)};
#else
		struct rusage ru;
		WAVM_ERROR_UNLESS(!getrusage(RUSAGE_SELF, &ru));
		return Time{timevalToNS(ru.ru_stime) + timevalToNS(ru.ru_utime)};
#endif
	}
	default: WAVM_UNREACHABLE();
	}
}

Time Platform::getClockResolution(Clock clock)
{
	switch(clock)
	{
	case Clock::realtime: return Time{getClockResAsI128(CLOCK_REALTIME)};
	case Clock::monotonic: {
#if defined(__APPLE__)
		return Time{getMachAbsoluteClockResolution()};
#elif defined(CLOCK_MONOTONIC)
		return Time{getClockResAsI128(CLOCK_MONOTONIC)};
#else
#error CLOCK_MONOTONIC not supported on this platform.
#endif
	}
	case Clock::processCPUTime: {
#ifdef CLOCK_PROCESS_CPUTIME_ID
		return Time{getClockResAsI128(CLOCK_PROCESS_CPUTIME_ID)};
#else
		return Time{1000};
#endif
	}
	default: WAVM_UNREACHABLE();
	}
}
