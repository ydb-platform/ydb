#pragma once

#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Time.h"

namespace WAVM { namespace Platform {
	enum class Clock
	{
		// The real-time since 1970/1/1 00:00:00 UTC.
		realtime,

		// A clock with an arbitrary origin that monotonically increases, and so may be used as an
		// absolute time for wait timeouts.
		monotonic,

		// The amount of CPU time used by this process.
		processCPUTime
	};

	WAVM_API Time getClockTime(Clock clock);
	WAVM_API Time getClockResolution(Clock clock);
}}
