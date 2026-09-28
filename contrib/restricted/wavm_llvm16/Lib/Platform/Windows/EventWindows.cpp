#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/I128.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Platform/Clock.h"
#include "WAVM/Platform/Event.h"

#define NOMINMAX
#include <Windows.h>

using namespace WAVM;
using namespace WAVM::Platform;

Platform::Event::Event()
{
	handle = CreateEvent(nullptr, FALSE, FALSE, nullptr);
	WAVM_ERROR_UNLESS(handle);
}

Platform::Event::~Event() { WAVM_ERROR_UNLESS(CloseHandle(handle)); }

bool Platform::Event::wait(Time waitDuration)
{
	if(isInfinity(waitDuration))
	{
		const U32 waitResult = WaitForSingleObject(handle, INFINITE);
		WAVM_ERROR_UNLESS(waitResult == WAIT_OBJECT_0);
		return true;
	}
	else
	{
		Time currentTime = getClockTime(Clock::monotonic);
		const Time untilTime = Time{currentTime.ns + waitDuration.ns};
		while(true)
		{
			const I128 durationMS
				= currentTime.ns > untilTime.ns ? 0 : (untilTime.ns - currentTime.ns) / 1000000;
			const U32 durationMS32
				= durationMS <= 0 ? 0
								  : durationMS >= UINT32_MAX ? (UINT32_MAX - 1) : U32(durationMS);

			const U32 waitResult = WaitForSingleObject(handle, durationMS32);
			if(waitResult != WAIT_TIMEOUT)
			{
				WAVM_ERROR_UNLESS(waitResult == WAIT_OBJECT_0);
				return true;
			}
			else
			{
				currentTime = getClockTime(Clock::monotonic);
				if(currentTime.ns >= untilTime.ns) { return false; }
			}
		};
	}
}

void Platform::Event::signal() { WAVM_ERROR_UNLESS(SetEvent(handle)); }
