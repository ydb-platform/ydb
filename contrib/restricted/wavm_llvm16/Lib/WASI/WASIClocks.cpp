#include "./WASIPrivate.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/I128.h"
#include "WAVM/Platform/Clock.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/WASI/WASI.h"
#include "WAVM/WASI/WASIABI.h"

using namespace WAVM;
using namespace WAVM::WASI;
using namespace WAVM::Runtime;

namespace WAVM { namespace WASI {
	WAVM_DEFINE_INTRINSIC_MODULE(wasiClocks)
}}

static bool getPlatformClock(__wasi_clockid_t clock, Platform::Clock& outPlatformClock)
{
	switch(clock)
	{
	case __WASI_CLOCK_REALTIME: outPlatformClock = Platform::Clock::realtime; return true;
	case __WASI_CLOCK_MONOTONIC: outPlatformClock = Platform::Clock::monotonic; return true;
	case __WASI_CLOCK_PROCESS_CPUTIME_ID:
	case __WASI_CLOCK_THREAD_CPUTIME_ID:
		outPlatformClock = Platform::Clock::processCPUTime;
		return true;
	default: return false;
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wasiClocks,
							   "clock_res_get",
							   __wasi_errno_return_t,
							   __wasi_clock_res_get,
							   __wasi_clockid_t clockId,
							   WASIAddress resolutionAddress)
{
	TRACE_SYSCALL("clock_res_get", "(%u, " WASIADDRESS_FORMAT ")", clockId, resolutionAddress);

	Platform::Clock platformClock;
	if(!getPlatformClock(clockId, platformClock)) { return TRACE_SYSCALL_RETURN(__WASI_EINVAL); }

	const Time clockResolution = Platform::getClockResolution(platformClock);

	Process* process = getProcessFromContextRuntimeData(contextRuntimeData);

	__wasi_timestamp_t wasiClockResolution = __wasi_timestamp_t(clockResolution.ns);
	memoryRef<__wasi_timestamp_t>(process->memory, resolutionAddress) = wasiClockResolution;

	return TRACE_SYSCALL_RETURN(__WASI_ESUCCESS, "(%" PRIu64 ")", wasiClockResolution);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wasiClocks,
							   "clock_time_get",
							   __wasi_errno_return_t,
							   __wasi_clock_time_get,
							   __wasi_clockid_t clockId,
							   __wasi_timestamp_t precision,
							   WASIAddress timeAddress)
{
	TRACE_SYSCALL("clock_time_get",
				  "(%u, %" PRIu64 ", " WASIADDRESS_FORMAT ")",
				  clockId,
				  precision,
				  timeAddress);

	Process* process = getProcessFromContextRuntimeData(contextRuntimeData);

	Platform::Clock platformClock;
	if(!getPlatformClock(clockId, platformClock)) { return TRACE_SYSCALL_RETURN(__WASI_EINVAL); }

	Time clockTime = Platform::getClockTime(platformClock);

	if(platformClock == Platform::Clock::processCPUTime)
	{ clockTime.ns -= process->processClockOrigin.ns; }

	__wasi_timestamp_t wasiClockTime = __wasi_timestamp_t(clockTime.ns);
	memoryRef<__wasi_timestamp_t>(process->memory, timeAddress) = wasiClockTime;

	return TRACE_SYSCALL_RETURN(__WASI_ESUCCESS, "(%" PRIu64 ")", wasiClockTime);
}
