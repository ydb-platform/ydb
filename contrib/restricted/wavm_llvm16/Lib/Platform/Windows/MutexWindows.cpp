#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/Mutex.h"

#define NOMINMAX
#include <Windows.h>

using namespace WAVM;
using namespace WAVM::Platform;

Platform::Mutex::Mutex()
{
	static_assert(sizeof(LockData) == sizeof(SRWLOCK), "");
	static_assert(alignof(LockData) >= alignof(SRWLOCK), "");
	InitializeSRWLock((SRWLOCK*)&lockData);
}

Platform::Mutex::~Mutex()
{
	if(!TryAcquireSRWLockExclusive((SRWLOCK*)&lockData))
	{ Errors::fatal("Destroying Mutex that is locked"); }
}

void Platform::Mutex::lock()
{
	AcquireSRWLockExclusive((SRWLOCK*)&lockData);
#if WAVM_ENABLE_ASSERTS
	lockingThreadId.store(GetCurrentThreadId(), std::memory_order_relaxed);
#endif
}

void Platform::Mutex::unlock()
{
#if WAVM_ENABLE_ASSERTS
	lockingThreadId.store(0, std::memory_order_relaxed);
#endif
	ReleaseSRWLockExclusive((SRWLOCK*)&lockData);
}

#if WAVM_ENABLE_ASSERTS
bool Platform::Mutex::isLockedByCurrentThread()
{
	return lockingThreadId.load(std::memory_order_relaxed) == GetCurrentThreadId();
}
#endif
