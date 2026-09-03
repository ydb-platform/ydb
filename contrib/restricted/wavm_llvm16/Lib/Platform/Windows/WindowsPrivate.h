#pragma once

#include <intrin.h>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Platform/Diagnostics.h"

#define NOMINMAX
#include <Windows.h>

namespace WAVM { namespace Platform {
	void initThread();

	CallStack unwindStack(const CONTEXT& immutableContext, Uptr numOmittedFramesFromTop);

	Time fileTimeToWAVMRealTime(FILETIME fileTime);
	FILETIME wavmRealTimeToFileTime(Time realTime);
}}
