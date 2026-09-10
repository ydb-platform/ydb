#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/I128.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Platform/Clock.h"
#include "WindowsPrivate.h"

#define NOMINMAX
#include <Windows.h>

using namespace WAVM;
using namespace WAVM::Platform;

static I128 fileTimeToI128(FILETIME fileTime)
{
	return I128(U64(fileTime.dwLowDateTime) | (U64(fileTime.dwHighDateTime) << 32)) * 100;
}

static FILETIME i128ToFileTime(I128 i128)
{
	const I128 fileTime128 = i128 / 100;
	const U64 fileTime64 = fileTime128 >= 0 && fileTime128 <= UINT64_MAX ? U64(fileTime128) : 0;
	FILETIME result;
	result.dwLowDateTime = U32(fileTime64);
	result.dwHighDateTime = U32(fileTime64 >> 32);
	return result;
}

static I128 getRealtimeClockOrigin()
{
	SYSTEMTIME systemTime;
	systemTime.wYear = 1970;
	systemTime.wMonth = 1;
	systemTime.wDay = 1;
	systemTime.wHour = 0;
	systemTime.wMinute = 0;
	systemTime.wSecond = 0;
	systemTime.wMilliseconds = 0;
	FILETIME fileTime;
	WAVM_ERROR_UNLESS(SystemTimeToFileTime(&systemTime, &fileTime));

	return fileTimeToI128(fileTime);
}

static const I128& getCachedRealtimeClockOrigin()
{
	static const I128 cachedRealtimeClockOrigin = getRealtimeClockOrigin();
	return cachedRealtimeClockOrigin;
}

static LARGE_INTEGER getQPCFrequency()
{
	LARGE_INTEGER result;
	WAVM_ERROR_UNLESS(QueryPerformanceFrequency(&result));
	return result;
}

static LARGE_INTEGER getCachedQPCFrequency()
{
	static const LARGE_INTEGER cachedResult = getQPCFrequency();
	return cachedResult;
}

Time Platform::fileTimeToWAVMRealTime(FILETIME fileTime)
{
	return Time{fileTimeToI128(fileTime) - getCachedRealtimeClockOrigin()};
}

FILETIME Platform::wavmRealTimeToFileTime(Time realTime)
{
	return i128ToFileTime(getCachedRealtimeClockOrigin() + realTime.ns);
}

Time Platform::getClockTime(Clock clock)
{
	switch(clock)
	{
	case Clock::realtime: {
		FILETIME realtimeClock;
		GetSystemTimeAsFileTime(&realtimeClock);

		return Time{fileTimeToWAVMRealTime(realtimeClock)};
	}
	case Clock::monotonic: {
		LARGE_INTEGER ticksPerSecond = getCachedQPCFrequency();
		LARGE_INTEGER ticks;
		WAVM_ERROR_UNLESS(QueryPerformanceCounter(&ticks));
		return Time{I128(ticks.QuadPart) * 1000000000 / ticksPerSecond.QuadPart};
	}
	case Clock::processCPUTime: {
		FILETIME creationTime;
		FILETIME exitTime;
		FILETIME kernelTime;
		FILETIME userTime;
		WAVM_ERROR_UNLESS(
			GetProcessTimes(GetCurrentProcess(), &creationTime, &exitTime, &kernelTime, &userTime));

		return Time{fileTimeToI128(kernelTime) + fileTimeToI128(userTime)};
	}
	default: WAVM_UNREACHABLE();
	};
}

Time Platform::getClockResolution(Clock clock)
{
	switch(clock)
	{
	case Clock::realtime: return Time{100};
	case Clock::monotonic: {
		LARGE_INTEGER ticksPerSecond = getCachedQPCFrequency();
		U64 result = U64(1000000000) / ticksPerSecond.QuadPart;
		if(result == 0) { result = 1; }
		return Time{I128(result)};
	}
	case Clock::processCPUTime: return Time{100};
	default: WAVM_UNREACHABLE();
	};
}
