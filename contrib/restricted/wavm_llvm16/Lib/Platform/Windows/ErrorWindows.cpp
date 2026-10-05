#include <cstdarg>
#include <cstdio>
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Diagnostics.h"
#include "WAVM/Platform/Error.h"
#include "WAVM/Platform/Mutex.h"

#define NOMINMAX
#include <Windows.h>

using namespace WAVM;
using namespace WAVM::Platform;

static Mutex& getErrorReportingMutex()
{
	static Platform::Mutex mutex;
	return mutex;
}

static void dumpErrorCallStack(Uptr numOmittedFramesFromTop)
{
	std::fprintf(stderr, "Call stack:\n");
	CallStack callStack = captureCallStack(numOmittedFramesFromTop);
	for(Uptr frameIndex = 0; frameIndex < callStack.frames.size(); ++frameIndex)
	{
		std::string frameDescription;
		Platform::InstructionSource source;
		if(!Platform::getInstructionSourceByAddress(callStack.frames[frameIndex].ip, source))
		{ frameDescription = "<unknown function>"; }
		else
		{
			frameDescription = asString(source);
		}
		std::fprintf(stderr, "  %s\n", frameDescription.c_str());
	}
	std::fflush(stderr);
}

void Platform::handleFatalError(const char* messageFormat, bool printCallStack, va_list varArgs)
{
	Platform::Mutex::Lock lock(getErrorReportingMutex());
	std::vfprintf(stderr, messageFormat, varArgs);
	std::fprintf(stderr, "\n");
	if(printCallStack) { dumpErrorCallStack(2); }
	std::fflush(stderr);
	if(IsDebuggerPresent()) { DebugBreak(); }
	TerminateProcess(GetCurrentProcess(), 1);

	// This throw is necessary to convince clang-cl that the function doesn't return.
	throw;
}

void Platform::handleAssertionFailure(const AssertMetadata& metadata)
{
	Platform::Mutex::Lock lock(getErrorReportingMutex());
	std::fprintf(stderr,
				 "Assertion failed at %s(%u): %s\n",
				 metadata.file,
				 metadata.line,
				 metadata.condition);
	dumpErrorCallStack(2);
}
