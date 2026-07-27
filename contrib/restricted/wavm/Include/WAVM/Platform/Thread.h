#pragma once

#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM { namespace Platform {
	struct Thread;
	WAVM_API Thread* createThread(Uptr numStackBytes, I64 (*threadEntry)(void*), void* argument);
	WAVM_API void detachThread(Thread* thread);
	WAVM_API I64 joinThread(Thread* thread);

	WAVM_API Uptr getNumberOfHardwareThreads();

	WAVM_API void yieldToAnotherThread();
}}
