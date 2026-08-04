#pragma once

#include <atomic>
#include <vector>
#include "EmscriptenABI.h"
#include "WAVM/Emscripten/Emscripten.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/IndexMap.h"
#include "WAVM/Inline/IntrusiveSharedPtr.h"
#include "WAVM/Inline/Time.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Platform/Thread.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Linker.h"
#include "WAVM/Runtime/Runtime.h"

namespace WAVM { namespace IR {
	struct Module;
}}

namespace WAVM { namespace VFS {
	struct VFD;
}}

namespace WAVM { namespace Emscripten {

	// Metadata from the emscripten_metadata user section of a module emitted by Emscripten.
	struct EmscriptenModuleMetadata
	{
		U32 metadataVersionMajor;
		U32 metadataVersionMinor;

		U32 abiVersionMajor;
		U32 abiVersionMinor;

		U32 backendID;
		U32 numMemoryPages;
		U32 numTableElems;
		U32 globalBaseAddress;
		U32 dynamicBaseAddress;
		U32 dynamicTopAddressAddress;
		U32 tempDoubleAddress;
		U32 standaloneWASM;
	};

	// Keeps track of the entry function used by a running WebAssembly-spawned thread.
	// Used to find garbage collection roots.
	struct Thread
	{
		Emscripten::Process* process;
		emabi::pthread_t id = 0;
		std::atomic<Uptr> numRefs{0};

		Platform::Thread* platformThread = nullptr;
		Runtime::GCPointer<Runtime::Context> context;
		Runtime::GCPointer<Runtime::Function> threadFunc;

		emabi::Address stackAddress;
		emabi::Address numStackBytes;

		std::atomic<U32> exitCode{0};

		I32 argument;

		HashMap<emabi::pthread_key_t, emabi::Address> pthreadSpecific;

		Thread(Emscripten::Process* inProcess,
			   Runtime::Context* inContext,
			   Runtime::Function* inEntryFunction,
			   I32 inArgument)
		: process(inProcess), context(inContext), threadFunc(inEntryFunction), argument(inArgument)
		{
		}

		void addRef(Uptr delta = 1) { numRefs += delta; }
		void removeRef()
		{
			if(--numRefs == 0) { delete this; }
		}
	};

	struct Process : Runtime::Resolver
	{
		Runtime::GCPointer<Runtime::Compartment> compartment;

		Runtime::GCPointer<Runtime::Instance> wasi_snapshot_preview1;
		Runtime::GCPointer<Runtime::Instance> env;
		Runtime::GCPointer<Runtime::Instance> asm2wasm;
		Runtime::GCPointer<Runtime::Instance> global;

		IntrusiveSharedPtr<Thread> mainThread;

		Runtime::GCPointer<Runtime::Instance> instance;
		Runtime::GCPointer<Runtime::Memory> memory;
		Runtime::GCPointer<Runtime::Table> table;

		Runtime::GCPointer<Runtime::Function> malloc;
		Runtime::GCPointer<Runtime::Function> free;
		Runtime::GCPointer<Runtime::Function> stackAlloc;
		Runtime::GCPointer<Runtime::Function> stackSave;
		Runtime::GCPointer<Runtime::Function> stackRestore;
		Runtime::GCPointer<Runtime::Function> errnoLocation;

		// A global list of running threads created by WebAssembly code.
		Platform::Mutex threadsMutex;
		IndexMap<emabi::pthread_t, IntrusiveSharedPtr<Thread>> threads{1, UINT32_MAX};

		std::atomic<emabi::pthread_key_t> pthreadSpecificNextKey{0};

		std::atomic<emabi::Address> currentLocale;

		std::vector<std::string> args;
		std::vector<std::string> envs;

		VFS::VFD* stdIn{nullptr};
		VFS::VFD* stdOut{nullptr};
		VFS::VFD* stdErr{nullptr};

		Time processClockOrigin;

		~Process();

		bool resolve(const std::string& moduleName,
					 const std::string& exportName,
					 IR::ExternType type,
					 Runtime::Object*& outObject) override;
	};

	struct ExitException
	{
		U32 exitCode;
	};

	struct ExitThreadException
	{
		U32 exitCode;
	};

	inline emabi::Address coerce32bitAddress(Runtime::Memory* memory, Uptr address)
	{
		if(address >= emabi::addressMax)
		{
			Runtime::throwException(Runtime::ExceptionTypes::outOfBoundsMemoryAccess,
									{Runtime::asObject(memory), U64(address)});
		}
		return (U32)address;
	}

	inline emabi::Result coerce32bitAddressResult(Runtime::Memory* memory, Iptr address)
	{
		if(address >= emabi::resultMax)
		{
			Runtime::throwException(Runtime::ExceptionTypes::outOfBoundsMemoryAccess,
									{Runtime::asObject(memory), U64(address)});
		}
		return (emabi::Result)address;
	}

	inline Emscripten::Process* getProcess(Runtime::ContextRuntimeData* contextRuntimeData)
	{
		auto process = (Emscripten::Process*)getUserData(
			getCompartmentFromContextRuntimeData(contextRuntimeData));
		WAVM_ASSERT(process);
		WAVM_ASSERT(process->memory);
		return process;
	}

	inline Emscripten::Thread* getEmscriptenThread(Runtime::ContextRuntimeData* contextRuntimeData)
	{
		auto thread
			= (Emscripten::Thread*)getUserData(getContextFromRuntimeData(contextRuntimeData));
		WAVM_ASSERT(thread);
		WAVM_ASSERT(thread->process);
		return thread;
	}

	emabi::Address dynamicAlloc(Emscripten::Process* process,
								Runtime::Context* context,
								emabi::Size numBytes);
	void initThreadLocals(Thread* thread);

	WAVM_DECLARE_INTRINSIC_MODULE(envThreads)
}}
