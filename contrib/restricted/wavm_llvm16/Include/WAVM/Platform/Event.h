#pragma once

#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Platform/Defines.h"

namespace WAVM { namespace Platform {
	// Platform-independent events.
	struct Event
	{
		WAVM_API Event();
		WAVM_API ~Event();

		// Don't allow copying or moving an Event.
		Event(const Event&) = delete;
		Event(Event&&) = delete;
		void operator=(const Event&) = delete;
		void operator=(Event&&) = delete;

		// Wait for the event to be signaled. Cancels the wait after waitDuration has elapsed.
		// Returns true if the event was signaled, false if the wait was cancelled.
		WAVM_API bool wait(Time waitDuration);
		WAVM_API void signal();

	private:
#ifdef WIN32
		void* handle;
#elif defined(__linux__) && defined(__x86_64__)
		struct PthreadMutex
		{
			Uptr data[5];
		} pthreadMutex;
		struct PthreadCond
		{
			Uptr data[6];
		} pthreadCond;
#elif defined(__linux__) && defined(__aarch64__)
		struct PthreadMutex
		{
			Uptr data[6];
		} pthreadMutex;
		struct PthreadCond
		{
			Uptr data[6];
		} pthreadCond;
#elif defined(__APPLE__)
		struct PthreadMutex
		{
			Uptr data[8];
		} pthreadMutex;
		struct PthreadCond
		{
			Uptr data[6];
		} pthreadCond;
#elif defined(__WAVIX__)
		struct PthreadMutex
		{
			Uptr data[6];
		} pthreadMutex;
		struct PthreadCond
		{
			Uptr data[12];
		} pthreadCond;
#else
#error unsupported platform
#endif
	};
}}
