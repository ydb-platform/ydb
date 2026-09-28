#include <intrin.h>
#include <atomic>
#include <memory>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/Event.h"
#include "WAVM/Platform/Thread.h"
#include "WindowsPrivate.h"

#define NOMINMAX
#include <Windows.h>

#define POISON_FORKED_STACK_SELF_POINTERS 0

using namespace WAVM;
using namespace WAVM::Platform;

static thread_local bool isThreadInitialized = false;

struct Platform::Thread
{
	HANDLE handle = INVALID_HANDLE_VALUE;
	DWORD id = 0xffffffff;
	std::atomic<I32> numRefs{2};
	I64 result = -1;

	void releaseRef()
	{
		if(--numRefs == 0) { delete this; }
	}

private:
	~Thread()
	{
		WAVM_ERROR_UNLESS(CloseHandle(handle));
		handle = nullptr;
		id = 0;
	}
};

struct CreateThreadArgs
{
	Thread* thread{nullptr};
	I64 (*entry)(void*){nullptr};
	void* entryArgument{nullptr};

	~CreateThreadArgs()
	{
		if(thread)
		{
			thread->releaseRef();
			thread = nullptr;
		}
	}
};

void Platform::initThread()
{
	if(!isThreadInitialized)
	{
		isThreadInitialized = true;

		// Ensure that there's enough space left on the stack in the case of a stack overflow to
		// prepare the stack trace.
		ULONG stackOverflowReserveBytes = 32768;
		SetThreadStackGuarantee(&stackOverflowReserveBytes);
	}
}

static DWORD WINAPI createThreadEntry(void* argsVoid)
{
	initThread();

	std::unique_ptr<CreateThreadArgs> args((CreateThreadArgs*)argsVoid);

	args->thread->result = (*args->entry)(args->entryArgument);

	return 0;
}

struct ProcessorGroupInfo
{
	U32 numProcessors;
};

static std::vector<ProcessorGroupInfo> getProcessorGroupInfos()
{
	std::vector<ProcessorGroupInfo> results;
	const U16 numProcessorGroups = GetActiveProcessorGroupCount();
	for(U16 groupIndex = 0; groupIndex < numProcessorGroups; ++groupIndex)
	{ results.push_back({GetActiveProcessorCount(groupIndex)}); }
	return results;
}

Platform::Thread* Platform::createThread(Uptr numStackBytes,
										 I64 (*entry)(void*),
										 void* entryArgument)
{
	CreateThreadArgs* args = new CreateThreadArgs;
	auto thread = new Thread;
	args->thread = thread;
	args->entry = entry;
	args->entryArgument = entryArgument;

	thread->handle
		= CreateThread(nullptr, numStackBytes, createThreadEntry, args, 0, &args->thread->id);

	// On systems with more than 64 logical processors, Windows splits them into processor groups,
	// and a thread may only be assigned to run on a single processor group. By default, all threads
	// in a process are assigned to a processor group that is chosen when creating the process.
	// To allow running threads on all the processors in a system, assign new threads to processor
	// groups in a round robin manner.

	static std::vector<ProcessorGroupInfo> processorGroupInfos = getProcessorGroupInfos();
	static std::atomic<U16> nextProcessorGroup{0};

	GROUP_AFFINITY groupAffinity;
	memset(&groupAffinity, 0, sizeof(groupAffinity));
	groupAffinity.Group = nextProcessorGroup++ % processorGroupInfos.size();
	groupAffinity.Mask = (1ull << U64(processorGroupInfos[groupAffinity.Group].numProcessors)) - 1;
	if(!SetThreadGroupAffinity(thread->handle, &groupAffinity, nullptr))
	{ Errors::fatalf("SetThreadGroupAffinity failed: GetLastError=%x", GetLastError()); }

	return args->thread;
}

void Platform::detachThread(Thread* thread)
{
	WAVM_ASSERT(thread);
	thread->releaseRef();
}

I64 Platform::joinThread(Thread* thread)
{
	DWORD waitResult = WaitForSingleObject(thread->handle, INFINITE);
	switch(waitResult)
	{
	case WAIT_OBJECT_0: break;
	case WAIT_ABANDONED:
		Errors::fatal("WaitForSingleObject(INFINITE) on thread returned WAIT_ABANDONED");
		break;
	case WAIT_TIMEOUT:
		Errors::fatal("WaitForSingleObject(INFINITE) on thread returned WAIT_TIMEOUT");
		break;
	case WAIT_FAILED:
		Errors::fatalf(
			"WaitForSingleObject(INFINITE) on thread returned WAIT_FAILED. GetLastError()=%u",
			GetLastError());
		break;
	};

	const I64 result = thread->result;
	thread->releaseRef();
	return result;
}

static Uptr getNumberOfHardwareThreadsImpl()
{
	Uptr result = 0;
	const U16 numProcessorGroups = GetActiveProcessorGroupCount();
	for(U16 groupIndex = 0; groupIndex < numProcessorGroups; ++groupIndex)
	{ result += GetActiveProcessorCount(groupIndex); }
	return result;
}

Uptr Platform::getNumberOfHardwareThreads()
{
	static Uptr cachedNumberOfHardwareThreads = getNumberOfHardwareThreadsImpl();
	return cachedNumberOfHardwareThreads;
}

void Platform::yieldToAnotherThread() { SwitchToThread(); }
