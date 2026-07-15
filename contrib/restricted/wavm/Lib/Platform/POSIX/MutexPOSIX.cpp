#include <pthread.h>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/Mutex.h"

using namespace WAVM;
using namespace WAVM::Platform;

Platform::Mutex::Mutex()
{
	static_assert(sizeof(LockData) >= sizeof(pthread_mutex_t), "");
	static_assert(alignof(LockData) >= alignof(pthread_mutex_t), "");

#if WAVM_ENABLE_ASSERTS
	// Use a recursive mutex in debug builds to support isLockedByCurrentThread.
	pthread_mutexattr_t pthreadMutexAttr;
	WAVM_ERROR_UNLESS(!pthread_mutexattr_init(&pthreadMutexAttr));
	WAVM_ERROR_UNLESS(!pthread_mutexattr_settype(&pthreadMutexAttr, PTHREAD_MUTEX_RECURSIVE));
	WAVM_ERROR_UNLESS(!pthread_mutex_init((pthread_mutex_t*)&lockData, &pthreadMutexAttr));
	WAVM_ERROR_UNLESS(!pthread_mutexattr_destroy(&pthreadMutexAttr));
	isLocked = false;
#else
	WAVM_ERROR_UNLESS(!pthread_mutex_init((pthread_mutex_t*)&lockData, nullptr));
#endif
}

Platform::Mutex::~Mutex()
{
	WAVM_ERROR_UNLESS(!pthread_mutex_destroy((pthread_mutex_t*)&lockData));
}

void Platform::Mutex::lock()
{
	WAVM_ERROR_UNLESS(!pthread_mutex_lock((pthread_mutex_t*)&lockData));
#if WAVM_ENABLE_ASSERTS
	if(isLocked) { Errors::fatal("Recursive mutex lock"); }
	isLocked = true;
#endif
}

void Platform::Mutex::unlock()
{
#if WAVM_ENABLE_ASSERTS
	isLocked = false;
#endif
	WAVM_ERROR_UNLESS(!pthread_mutex_unlock((pthread_mutex_t*)&lockData));
}

#if WAVM_ENABLE_ASSERTS
bool Platform::Mutex::isLockedByCurrentThread()
{
	// Try to lock the mutex, and if EDEADLK is returned, it means the mutex was already locked by
	// this thread.
	int tryLockResult = pthread_mutex_trylock((pthread_mutex_t*)&lockData);
	if(tryLockResult) { return false; }

	const bool result = isLocked;
	WAVM_ERROR_UNLESS(!pthread_mutex_unlock((pthread_mutex_t*)&lockData));
	return result;
}
#endif
