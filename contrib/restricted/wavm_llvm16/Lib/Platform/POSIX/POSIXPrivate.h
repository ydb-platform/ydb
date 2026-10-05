#pragma once

#include <setjmp.h>
#include <functional>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Signal.h"

#ifdef __WAVIX__
// libunwind dynamic frame registration
inline void __register_frame(const void* fde)
{
	WAVM::Errors::unimplemented("Wavix __register_frame");
}

inline void __deregister_frame(const void* fde)
{
	WAVM::Errors::unimplemented("Wavix __deregister_frame");
}

#else
// libunwind dynamic frame registration
extern "C" void __register_frame(const void* fde);
extern "C" void __deregister_frame(const void* fde);
#endif

namespace WAVM { namespace Platform {

	struct CallStack;

	struct SignalContext
	{
		SignalContext* outerContext;
		jmp_buf catchJump;
		bool (*filter)(void*, Signal, CallStack&&);
		void* filterArgument;
	};

	struct SigAltStack
	{
		~SigAltStack() { deinit(); }

		void init();
		void deinit();

		void getNonSignalStack(U8*& outMinGuardAddr, U8*& outMinAddr, U8*& outMaxAddr);

	private:
		U8* base = nullptr;

		U8* stackMinAddr;
		U8* stackMaxAddr;
		U8* stackMinGuardAddr;
	};

	extern thread_local SigAltStack sigAltStack;
	extern thread_local SignalContext* innermostSignalContext;

	extern bool initThreadAndGlobalSignalsOnce();
	extern bool initGlobalSignalsOnce();

	inline void initGlobalSignals()
	{
		static bool initedGlobalSignals = initGlobalSignalsOnce();
		WAVM_ASSERT(initedGlobalSignals);
	}
	inline void initThreadAndGlobalSignals()
	{
		static thread_local bool initedThread = initThreadAndGlobalSignalsOnce();
		WAVM_ASSERT(initedThread);
	}

	void dumpErrorCallStack(Uptr numOmittedFramesFromTop);
	void getCurrentThreadStack(U8*& outMinGuardAddr, U8*& outMinAddr, U8*& outMaxAddr);
}}
