#pragma once

#include "WAVM/Inline/I128.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Logging/Logging.h"
#include "WAVM/Platform/Clock.h"

namespace WAVM { namespace Timing {
	// Encapsulates a timer that starts when constructed and stops when read.
	struct Timer
	{
		Timer() : startTime(Platform::getClockTime(Platform::Clock::monotonic)), isStopped(false) {}
		void stop()
		{
			endTime = Platform::getClockTime(Platform::Clock::monotonic);
			isStopped = true;
		}
		F64 getNanoseconds()
		{
			if(!isStopped) { stop(); }
			return F64(flushNaNToZero(endTime.ns - startTime.ns));
		}
		F64 getMicroseconds() { return getNanoseconds() / 1000.0; }
		F64 getMilliseconds() { return getNanoseconds() / 1000000.0; }
		F64 getSeconds() { return getNanoseconds() / 1000000000.0; }

	private:
		Time startTime;
		Time endTime;
		bool isStopped;
	};

	// Helpers for printing timers.
	inline void logTimer(const char* context, Timer& timer)
	{
		Log::printf(Log::metrics, "%s in %.2fms\n", context, timer.getMilliseconds());
	}
	inline void logRatePerSecond(const char* context,
								 Timer& timer,
								 F64 numerator,
								 const char* numeratorUnit)
	{
		Log::printf(Log::metrics,
					"%s in %.2fms (%f %s%s)\n",
					context,
					timer.getMilliseconds(),
					timer.getSeconds() == 0.0 ? numerator : numerator / timer.getSeconds(),
					numeratorUnit,
					timer.getSeconds() == 0.0 ? "" : "/s");
	}
}}
