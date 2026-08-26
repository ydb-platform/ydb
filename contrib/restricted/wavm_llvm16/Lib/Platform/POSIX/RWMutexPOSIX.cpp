#include <pthread.h>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/RWMutex.h"

using namespace WAVM;
using namespace WAVM::Platform;

Platform::RWMutex::RWMutex()
{
	static_assert(sizeof(LockData) >= sizeof(pthread_rwlock_t), "");
	static_assert(alignof(LockData) >= alignof(pthread_rwlock_t), "");

#if WAVM_ENABLE_ASSERTS
	static_assert(sizeof(exclusiveLockingThreadId) == sizeof(pthread_t), "");
	static_assert(alignof(Uptr) >= alignof(pthread_t), "");
#endif

	WAVM_ERROR_UNLESS(!pthread_rwlock_init((pthread_rwlock_t*)&lockData, nullptr));
}

Platform::RWMutex::~RWMutex()
{
	WAVM_ERROR_UNLESS(!pthread_rwlock_destroy((pthread_rwlock_t*)&lockData));
}

void Platform::RWMutex::lock(LockShareability shareability)
{
	if(shareability == LockShareability::exclusive)
	{
		WAVM_ERROR_UNLESS(!pthread_rwlock_wrlock((pthread_rwlock_t*)&lockData));
#if WAVM_ENABLE_ASSERTS
		exclusiveLockingThreadId.store(reinterpret_cast<Uptr>(pthread_self()),
									   std::memory_order_relaxed);
#endif
	}
	else
	{
		WAVM_ERROR_UNLESS(!pthread_rwlock_rdlock((pthread_rwlock_t*)&lockData));
	}
}

void Platform::RWMutex::unlock(LockShareability shareability)
{
	if(shareability == LockShareability::exclusive)
	{
		WAVM_ERROR_UNLESS(!pthread_rwlock_unlock((pthread_rwlock_t*)&lockData));
#if WAVM_ENABLE_ASSERTS
		exclusiveLockingThreadId.store(0, std::memory_order_relaxed);
#endif
	}
	else
	{
		WAVM_ERROR_UNLESS(!pthread_rwlock_unlock((pthread_rwlock_t*)&lockData));
	}
}

#if WAVM_ENABLE_ASSERTS
bool Platform::RWMutex::isExclusivelyLockedByCurrentThread()
{
	return reinterpret_cast<pthread_t>(exclusiveLockingThreadId.load(std::memory_order_relaxed))
		   == pthread_self();
}
#endif
