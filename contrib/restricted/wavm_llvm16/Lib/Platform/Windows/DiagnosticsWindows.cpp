#include <stdio.h>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Diagnostics.h"
#include "WAVM/Platform/Mutex.h"
#include "WindowsPrivate.h"

#define NOMINMAX
#include <Windows.h>

#include <DbgHelp.h>
#include <string>

using namespace WAVM;
using namespace WAVM::Platform;

// The interface to the DbgHelp DLL
struct DbgHelp
{
	typedef BOOL(WINAPI* SymFromAddr)(HANDLE, U64, U64*, SYMBOL_INFO*);
	SymFromAddr symFromAddr;

	static DbgHelp* get()
	{
		static Platform::Mutex dbgHelpMutex;
		static DbgHelp* dbgHelp = nullptr;
		if(!dbgHelp)
		{
			Platform::Mutex::Lock dbgHelpLock(dbgHelpMutex);
			if(!dbgHelp) { dbgHelp = new DbgHelp(); }
		}
		return dbgHelp;
	}

private:
	DbgHelp()
	{
		HMODULE dbgHelpModule = ::LoadLibraryA("Dbghelp.dll");
		if(dbgHelpModule)
		{
			symFromAddr = (SymFromAddr)::GetProcAddress(dbgHelpModule, "SymFromAddr");

			// Initialize the debug symbol lookup.
			typedef BOOL(WINAPI * SymInitialize)(HANDLE, PCTSTR, BOOL);
			SymInitialize symInitialize
				= (SymInitialize)::GetProcAddress(dbgHelpModule, "SymInitialize");
			if(symInitialize) { symInitialize(GetCurrentProcess(), nullptr, TRUE); }
		}
	}
};

static HMODULE getCurrentModule()
{
	HMODULE module = nullptr;
	GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS, (LPCTSTR)getCurrentModule, &module);
	return module;
}

static HMODULE getModuleFromBaseAddress(Uptr baseAddress)
{
	return reinterpret_cast<HMODULE>(baseAddress);
}

static std::string getModuleName(HMODULE module)
{
	char moduleFilename[MAX_PATH + 1];
	U32 moduleFilenameResult = GetModuleFileNameA(module, moduleFilename, MAX_PATH + 1);
	return std::string(moduleFilename, moduleFilenameResult);
}

static std::string trimModuleName(std::string moduleName)
{
	const std::string thisModuleName = getModuleName(getCurrentModule());
	Uptr lastBackslashOffset = thisModuleName.find_last_of('\\');
	if(lastBackslashOffset != UINTPTR_MAX && moduleName.size() >= lastBackslashOffset
	   && moduleName.substr(0, lastBackslashOffset)
			  == thisModuleName.substr(0, lastBackslashOffset))
	{ return moduleName.substr(lastBackslashOffset + 1); }
	else
	{
		return moduleName;
	}
}

bool Platform::getInstructionSourceByAddress(Uptr ip, InstructionSource& outSource)
{
	// Initialize DbgHelp.
	DbgHelp* dbgHelp = DbgHelp::get();

	// Allocate up a SYMBOL_INFO struct to receive information about the symbol for this
	// instruction pointer.
	const Uptr maxSymbolNameChars = 256;
	const Uptr symbolAllocationSize
		= sizeof(SYMBOL_INFO) + sizeof(TCHAR) * (maxSymbolNameChars - 1);
	SYMBOL_INFO* symbolInfo = (SYMBOL_INFO*)alloca(symbolAllocationSize);
	ZeroMemory(symbolInfo, symbolAllocationSize);
	symbolInfo->SizeOfStruct = sizeof(SYMBOL_INFO);
	symbolInfo->MaxNameLen = maxSymbolNameChars;

	// Call DbgHelp::SymFromAddr to try to find any debug symbol containing this address.
	U64 displacement;
	if(!dbgHelp->symFromAddr(GetCurrentProcess(), ip, &displacement, symbolInfo)) { return false; }
	else
	{
		outSource.module
			= trimModuleName(getModuleName(getModuleFromBaseAddress(Uptr(symbolInfo->ModBase))));
		outSource.function = std::string(symbolInfo->Name, symbolInfo->NameLen);
		outSource.instructionOffset = Uptr(displacement);
		return true;
	}
}

CallStack Platform::unwindStack(const CONTEXT& immutableContext, Uptr numOmittedFramesFromTop)
{
	// Make a mutable copy of the context.
	CONTEXT context;
	memcpy(&context, &immutableContext, sizeof(CONTEXT));

	// Unwind the stack until there isn't a valid instruction pointer, which signals we've
	// reached the base.
	CallStack callStack;
#if WAVM_ENABLE_UNWIND
	for(Uptr frameIndex = 0; !callStack.frames.isFull() && context.Rip; ++frameIndex)
	{
		if(frameIndex >= numOmittedFramesFromTop)
		{
			callStack.frames.push_back(
				CallStack::Frame{frameIndex == 0 ? context.Rip : (context.Rip - 1)});
		}

		// Look up the SEH unwind information for this function.
		U64 imageBase;
		auto runtimeFunction = RtlLookupFunctionEntry(context.Rip, &imageBase, nullptr);
		if(!runtimeFunction)
		{
			// Leaf functions that don't touch Rsp may not have unwind information.
			context.Rip = *(U64*)context.Rsp;
			context.Rsp += 8;
		}
		else
		{
			// Use the SEH information to unwind to the next stack frame.
			void* handlerData;
			U64 establisherFrame;
			RtlVirtualUnwind(UNW_FLAG_NHANDLER,
							 imageBase,
							 context.Rip,
							 runtimeFunction,
							 &context,
							 &handlerData,
							 &establisherFrame,
							 nullptr);
		}
	}
#endif

	return callStack;
}

CallStack Platform::captureCallStack(Uptr numOmittedFramesFromTop)
{
	// Capture the current processor state.
	CONTEXT context;
	RtlCaptureContext(&context);

	// Unwind the stack.
	return unwindStack(context, numOmittedFramesFromTop + 1);
}

static std::atomic<Uptr> numCommittedPageBytes{0};

void Platform::printMemoryProfile()
{
	printf("Committed virtual pages: %" PRIuPTR " KB\n",
		   numCommittedPageBytes.load(std::memory_order_seq_cst) / 1024);
	fflush(stdout);
}

void Platform::registerVirtualAllocation(Uptr numBytes) { numCommittedPageBytes += numBytes; }

void Platform::deregisterVirtualAllocation(Uptr numBytes) { numCommittedPageBytes -= numBytes; }
