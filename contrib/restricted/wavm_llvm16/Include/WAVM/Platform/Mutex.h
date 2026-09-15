#pragma once

#include <atomic>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM { namespace Platform {
	// Platform-independent mutexes.
	struct Mutex
	{
		WAVM_API Mutex();
		WAVM_API ~Mutex();

		// Don't allow copying or moving a Mutex.
		Mutex(const Mutex&) = delete;
		Mutex(Mutex&&) = delete;
		void operator=(const Mutex&) = delete;
		void operator=(Mutex&&) = delete;

		WAVM_API void lock();
		WAVM_API void unlock();

#if WAVM_ENABLE_ASSERTS
		WAVM_API bool isLockedByCurrentThread();
#endif

		// Scoped lock: automatically unlocks when destructed.
		struct Lock
		{
			Lock() : mutex(nullptr) {}
			Lock(Mutex& inMutex) : mutex(&inMutex) { mutex->lock(); }
			~Lock() { unlock(); }

			void unlock()
			{
				if(mutex)
				{
					mutex->unlock();
					mutex = nullptr;
				}
			}

		private:
			Mutex* mutex;
		};

	private:
		struct LockData
		{
#if defined(WIN32)
			Uptr data;
#elif defined(__linux__) && defined(__x86_64__)
			U64 data[5];
#elif defined(__linux__) && defined(__aarch64__)
			U64 data[6];
#elif defined(__APPLE__)
			U64 data[8];
#else
			// For unidentified platforms, just use 64 bytes for the mutex.
			U64 data[8];
#endif
		} lockData;

#if WAVM_ENABLE_ASSERTS
#if defined(_WIN32)
		std::atomic<U32> lockingThreadId{0};
#else
		bool isLocked;
#endif
#endif
	};
}}

#if WAVM_ENABLE_ASSERTS
#define WAVM_ASSERT_MUTEX_IS_LOCKED_BY_CURRENT_THREAD(mutex)                                       \
	WAVM_ASSERT((mutex).isLockedByCurrentThread())
#else
#define WAVM_ASSERT_MUTEX_IS_LOCKED_BY_CURRENT_THREAD(mutex) WAVM_ASSERT(&(mutex) == &(mutex))
#endif
