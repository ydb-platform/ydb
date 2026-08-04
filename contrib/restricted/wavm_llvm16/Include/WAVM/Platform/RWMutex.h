#pragma once

#include <atomic>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM { namespace Platform {
	// Platform-independent reader-writer mutexes.
	// Allows more than 1 shared lock at a time, as long as there are no exclusive locks.
	struct RWMutex
	{
		enum LockShareability
		{
			exclusive = 0,
			shareable = 1,
		};

		WAVM_API RWMutex();
		WAVM_API ~RWMutex();

		// Don't allow copying or moving a RWMutex.
		RWMutex(const RWMutex&) = delete;
		RWMutex(RWMutex&&) = delete;
		void operator=(const RWMutex&) = delete;
		void operator=(RWMutex&&) = delete;

		WAVM_API void lock(LockShareability shareability);
		WAVM_API void unlock(LockShareability shareability);

#if WAVM_ENABLE_ASSERTS
		WAVM_API bool isExclusivelyLockedByCurrentThread();
#endif

		// Scoped lock: automatically unlocks when destructed.
		struct Lock
		{
			Lock() : mutex(nullptr) {}
			Lock(RWMutex& inMutex, LockShareability inShareability)
			: mutex(&inMutex), shareability(inShareability)
			{
				mutex->lock(shareability);
			}
			~Lock() { unlock(); }

			void unlock()
			{
				if(mutex)
				{
					mutex->unlock(shareability);
					mutex = nullptr;
				}
			}

		private:
			RWMutex* mutex;
			LockShareability shareability;
		};

		struct ExclusiveLock : Lock
		{
			ExclusiveLock() = default;
			ExclusiveLock(RWMutex& inMutex) : Lock(inMutex, exclusive) {}
		};

		struct ShareableLock : Lock
		{
			ShareableLock() = default;
			ShareableLock(RWMutex& inMutex) : Lock(inMutex, shareable) {}
		};

	private:
		struct LockData
		{
#if defined(WIN32)
			Uptr data;
#elif defined(__linux__) && defined(__x86_64__)
			U64 data[7];
#elif defined(__APPLE__)
			U64 data[25]; // !!
#else
			// For unidentified platforms, just allocate 64 bytes for the mutex.
			U64 data[8];
#endif
		} lockData;

#if WAVM_ENABLE_ASSERTS
#if defined(_WIN32)
		std::atomic<U32> exclusiveLockingThreadId{0};
#else
		std::atomic<Uptr> exclusiveLockingThreadId{0};
#endif
#endif
	};
}}

#if WAVM_ENABLE_ASSERTS
#define WAVM_ASSERT_RWMUTEX_IS_EXCLUSIVELY_LOCKED_BY_CURRENT_THREAD(mutex)                         \
	WAVM_ASSERT((mutex).isExclusivelyLockedByCurrentThread())
#else
#define WAVM_ASSERT_RWMUTEX_IS_EXCLUSIVELY_LOCKED_BY_CURRENT_THREAD(mutex)                         \
	WAVM_ASSERT(&(mutex) == &(mutex))
#endif
