#include <cxxabi.h>
#include <dlfcn.h>
#include <stdio.h>
#include <atomic>
#include <string>
#include "POSIXPrivate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Platform/Diagnostics.h"

#if WAVM_ENABLE_UNWIND
#define UNW_LOCAL_ONLY
#include "libunwind.h"
#endif

#if WAVM_ENABLE_ASAN                                                                               \
	&& (defined(HAS_SANITIZER_PRINT_MEMORY_PROFILE_1)                                              \
		|| defined(HAS_SANITIZER_PRINT_MEMORY_PROFILE_2))
#include <sanitizer/common_interface_defs.h>
#endif

using namespace WAVM;
using namespace WAVM::Platform;

extern "C" const char* __asan_default_options()
{
	return "handle_segv=false"
		   ":handle_sigbus=false"
		   ":handle_sigfpe=false"
		   ":replace_intrin=false";
}

CallStack Platform::captureCallStack(Uptr numOmittedFramesFromTop)
{
	CallStack result;

#if WAVM_ENABLE_UNWIND
	unw_context_t context;
	WAVM_ERROR_UNLESS(!unw_getcontext(&context));

	unw_cursor_t cursor;

	WAVM_ERROR_UNLESS(!unw_init_local(&cursor, &context));
	for(Uptr frameIndex = 0; !result.frames.isFull() && unw_step(&cursor) > 0; ++frameIndex)
	{
		if(frameIndex >= numOmittedFramesFromTop)
		{
			unw_word_t ip;
			WAVM_ERROR_UNLESS(!unw_get_reg(&cursor, UNW_REG_IP, &ip));
			result.frames.push_back(CallStack::Frame{frameIndex == 0 ? ip : (ip - 1)});
		}
	}
#endif

	return result;
}

bool Platform::getInstructionSourceByAddress(Uptr ip, InstructionSource& outSource)
{
#if defined(__linux__) || defined(__APPLE__)
	// Look up static symbol information for the address.
	Dl_info symbolInfo;
	if(dladdr((void*)ip, &symbolInfo))
	{
		WAVM_ASSERT(symbolInfo.dli_fname);
		outSource.module = symbolInfo.dli_fname;
		if(!symbolInfo.dli_sname)
		{
			outSource.function = std::string();
			outSource.instructionOffset = ip - reinterpret_cast<Uptr>(symbolInfo.dli_fbase);
		}
		else
		{
			if(symbolInfo.dli_sname[0] == '_')
			{
				int demangleStatus = 0;
				if(char* demangledBuffer
				   = abi::__cxa_demangle(symbolInfo.dli_sname, nullptr, nullptr, &demangleStatus))
				{
					outSource.function = demangledBuffer;
					free(demangledBuffer);
				}
			}
			else
			{
				outSource.function = symbolInfo.dli_sname;
			}
			outSource.instructionOffset = ip - reinterpret_cast<Uptr>(symbolInfo.dli_saddr);
		}
		return true;
	}
#endif
	return false;
}

static std::atomic<Uptr> numCommittedPageBytes{0};

void Platform::printMemoryProfile()
{
#if WAVM_ENABLE_ASAN
#if defined(HAS_SANITIZER_PRINT_MEMORY_PROFILE_1)
	__sanitizer_print_memory_profile(100);
#elif defined(HAS_SANITIZER_PRINT_MEMORY_PROFILE_2)
	__sanitizer_print_memory_profile(100, 20);
#endif
#endif
	printf("Committed virtual pages: %" PRIuPTR " KB\n",
		   uintptr_t(numCommittedPageBytes.load(std::memory_order_seq_cst) / 1024));
	fflush(stdout);
}

void Platform::registerVirtualAllocation(Uptr numBytes) { numCommittedPageBytes += numBytes; }

void Platform::deregisterVirtualAllocation(Uptr numBytes) { numCommittedPageBytes -= numBytes; }
