#include <cstdarg>
#include <cstdio>
#include "POSIXPrivate.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Diagnostics.h"
#include "WAVM/Platform/Error.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Runtime/Runtime.h"

using namespace WAVM;
using namespace WAVM::Platform;

static Mutex& getErrorReportingMutex()
{
	static Platform::Mutex mutex;
	return mutex;
}

void Platform::dumpErrorCallStack(Uptr numOmittedFramesFromTop)
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
	auto toString = [] (const char* format, va_list varArgs) -> std::string {
		int length = 0;
		{
			va_list varArgsCopy;
			va_copy(varArgsCopy, varArgs);
			length = vsnprintf(nullptr, 0, format, varArgsCopy);
			va_end(varArgsCopy);
			if (length < 0) {
				return "";
			}
		}

		auto result = std::string();
		result.resize(length + 1);
		vsnprintf(result.data(), result.size(), format, varArgs);
		return result;
	};

	auto result = toString(messageFormat, varArgs);

	if (printCallStack) {
		result.back() = '\n';

		auto callStack = WAVM::Platform::captureCallStack(1);
        auto description = WAVM::Runtime::describeCallStack(callStack);
        int i = 0;
        for (auto& item : description) {
            result += std::to_string(i++) + ". ";
            result += item;
            result += '\n';
        }
	}

	throw std::runtime_error(result.c_str());
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
	std::fflush(stderr);
}
