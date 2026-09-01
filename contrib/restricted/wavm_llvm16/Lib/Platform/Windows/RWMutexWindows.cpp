#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/RWMutex.h"

#define NOMINMAX
#include <Windows.h>

using namespace WAVM;
using namespace WAVM::Platform;

Platform::RWMutex::RWMutex()
{
	static_assert(sizeof(LockData) == sizeof(SRWLOCK), "");
	static_assert(alignof(LockData) >= alignof(SRWLOCK), "");
	InitializeSRWLock((SRWLOCK*)&lockData);
}

Platform::RWMutex::~RWMutex()
{
	if(!TryAcquireSRWLockExclusive((SRWLOCK*)&lockData))
	{ Errors::fatal("Destroying RWMutex that is locked"); }
}

void Platform::RWMutex::lock(LockShareability shareability)
{
	if(shareability == LockShareability::exclusive)
	{
		AcquireSRWLockExclusive((SRWLOCK*)&lockData);
#if WAVM_ENABLE_ASSERTS
		exclusiveLockingThreadId.store(GetCurrentThreadId(), std::memory_order_relaxed);
#endif
	}
	else
	{
		AcquireSRWLockShared((SRWLOCK*)&lockData);
	}
}

void Platform::RWMutex::unlock(LockShareability shareability)
{
	if(shareability == LockShareability::exclusive)
	{
#if WAVM_ENABLE_ASSERTS
		exclusiveLockingThreadId.store(0, std::memory_order_relaxed);
#endif
		ReleaseSRWLockExclusive((SRWLOCK*)&lockData);
	}
	else
	{
		ReleaseSRWLockShared((SRWLOCK*)&lockData);
	}
}

#if WAVM_ENABLE_ASSERTS
bool Platform::RWMutex::isExclusivelyLockedByCurrentThread()
{
	return exclusiveLockingThreadId.load(std::memory_order_relaxed) == GetCurrentThreadId();
}
#endif
