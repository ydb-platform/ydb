#include <atomic>
#include <functional>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Defines.h"
#include "WAVM/Platform/Signal.h"
#include "WindowsPrivate.h"

#define NOMINMAX
#include <Windows.h>

using namespace WAVM;
using namespace WAVM::Platform;

void Platform::registerEHFrames(const U8* imageBase, const U8* ehFrames, Uptr numBytes)
{
#ifdef _WIN64
	const U32 numFunctions = (U32)(numBytes / sizeof(RUNTIME_FUNCTION));

	// Register our manually fixed up copy of the function table.
	if(!RtlAddFunctionTable(
		   (RUNTIME_FUNCTION*)ehFrames, numFunctions, reinterpret_cast<ULONG_PTR>(imageBase)))
	{ Errors::fatal("RtlAddFunctionTable failed"); }
#else
	Errors::fatal("registerEHFrames isn't implemented on 32-bit Windows");
#endif
}
void Platform::deregisterEHFrames(const U8* imageBase, const U8* ehFrames, Uptr numBytes)
{
#ifdef _WIN64
	RtlDeleteFunctionTable((RUNTIME_FUNCTION*)ehFrames);
#else
	Errors::fatal("deregisterEHFrames isn't implemented on 32-bit Windows");
#endif
}

static bool translateSEHToSignal(EXCEPTION_POINTERS* exceptionPointers, Signal& outSignal)
{
	// Decide how to handle this exception code.
	switch(exceptionPointers->ExceptionRecord->ExceptionCode)
	{
	case EXCEPTION_ACCESS_VIOLATION: {
		outSignal.type = Signal::Type::accessViolation;
		outSignal.accessViolation.address
			= exceptionPointers->ExceptionRecord->ExceptionInformation[1];
		return true;
	}
	case EXCEPTION_STACK_OVERFLOW: outSignal.type = Signal::Type::stackOverflow; return true;
	case STATUS_INTEGER_DIVIDE_BY_ZERO:
		outSignal.type = Signal::Type::intDivideByZeroOrOverflow;
		return true;
	case STATUS_INTEGER_OVERFLOW:
		outSignal.type = Signal::Type::intDivideByZeroOrOverflow;
		return true;
	default: return false;
	}
}

// __try/__except doesn't support locals with destructors in the same function, so this is just
// the body of the sehSignalFilterFunction __try pulled out into a function.
static LONG CALLBACK sehSignalFilterFunctionNonReentrant(EXCEPTION_POINTERS* exceptionPointers,
														 bool (*filter)(void*, Signal, CallStack&&),
														 void* context)
{
	Signal signal;
	if(!translateSEHToSignal(exceptionPointers, signal)) { return EXCEPTION_CONTINUE_SEARCH; }
	else
	{
		// Unwind the stack frames from the context of the exception.
		CallStack callStack = unwindStack(*exceptionPointers->ContextRecord, 0);

		if((*filter)(context, signal, std::move(callStack))) { return EXCEPTION_EXECUTE_HANDLER; }
		else
		{
			return EXCEPTION_CONTINUE_SEARCH;
		}
	}
}

static LONG CALLBACK sehSignalFilterFunction(EXCEPTION_POINTERS* exceptionPointers,
											 bool (*filter)(void*, Signal, CallStack&&),
											 void* context)
{
	__try
	{
		return sehSignalFilterFunctionNonReentrant(exceptionPointers, filter, context);
	}
	__except(Errors::fatal("reentrant exception"), true)
	{
		WAVM_UNREACHABLE();
	}
}

bool Platform::catchSignals(void (*thunk)(void*),
							bool (*filter)(void*, Signal, CallStack&&),
							void* context)
{
	initThread();

	__try
	{
		(*thunk)(context);
		return false;
	}
	__except(sehSignalFilterFunction(GetExceptionInformation(), filter, context))
	{
		// After a stack overflow, the stack will be left in a damaged state. Let the CRT repair it.
		WAVM_ERROR_UNLESS(_resetstkoflw());

		return true;
	}
}
