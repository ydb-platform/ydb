#include "WAVM/Platform/Thread.h"
#include <stdint.h>
#include <atomic>
#include <memory>
#include <utility>
#include <vector>
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/IndexMap.h"
#include "WAVM/Inline/IntrusiveSharedPtr.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"
#include "WAVM/ThreadTest/ThreadTest.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

static constexpr Uptr numStackBytes = 1 * 1024 * 1024;

// Keeps track of the entry function used by a running WebAssembly-spawned thread.
// Used to find garbage collection roots.
struct Thread
{
	Uptr id = UINTPTR_MAX;
	std::atomic<Uptr> numRefs{0};

	Platform::Thread* platformThread = nullptr;
	GCPointer<Context> context;
	GCPointer<Function> entryFunction;

	Platform::Mutex resultMutex;
	bool threwException = false;
	Exception* exception = nullptr;
	I64 result = -1;

	I32 argument;

	WAVM_FORCENOINLINE Thread(Context* inContext, Function* inEntryFunction, I32 inArgument)
	: context(inContext), entryFunction(inEntryFunction), argument(inArgument)
	{
	}

	void addRef(Uptr delta = 1) { numRefs += delta; }
	void removeRef()
	{
		if(--numRefs == 0) { delete this; }
	}
};

struct ExitThreadException
{
	I64 code;
};

// A global list of running threads created by WebAssembly code.
static Platform::Mutex threadsMutex;
static IndexMap<Uptr, IntrusiveSharedPtr<Thread>> threads(1, UINTPTR_MAX);

// A shared pointer to the current thread. This is used to decrement the thread's reference count
// when the thread exits.
static thread_local IntrusiveSharedPtr<Thread> currentThread = nullptr;

// Adds the thread to the global thread array, assigning it an ID corresponding to its index in the
// array.
WAVM_FORCENOINLINE static Uptr allocateThreadId(Thread* thread)
{
	Platform::Mutex::Lock threadsLock(threadsMutex);
	thread->id = threads.add(0, thread);
	WAVM_ERROR_UNLESS(thread->id != 0);
	return thread->id;
}

// Validates that a thread ID is valid. i.e. 0 < threadId < threads.size(), and threads[threadId] !=
// null If the thread ID is invalid, throws an invalid argument exception. The caller must have
// already locked threadsMutex before calling validateThreadId.
static void validateThreadId(Uptr threadId)
{
	if(threadId == 0 || !threads.contains(threadId))
	{ throwException(ExceptionTypes::invalidArgument); }
}

WAVM_DEFINE_INTRINSIC_MODULE(threadTest);

static I64 threadEntry(void* threadVoid)
{
	// Assign the thread reference to currentThread, and discard it the local copy.
	{
		Thread* thread = (Thread*)threadVoid;
		currentThread = thread;
		thread->removeRef();
	}

	catchRuntimeExceptions(
		[]() {
			I64 result;
			try
			{
				UntaggedValue argumentValue{currentThread->argument};
				UntaggedValue resultValue;
				invokeFunction(currentThread->context,
							   currentThread->entryFunction,
							   FunctionType({ValueType::i64}, {ValueType::i32}),
							   &argumentValue,
							   &resultValue);
				result = resultValue.i64;
			}
			catch(ExitThreadException& exitThreadException)
			{
				result = exitThreadException.code;
			}

			Platform::Mutex::Lock resultLock(currentThread->resultMutex);
			currentThread->result = result;
		},
		[](Exception* exception) {
			Platform::Mutex::Lock resultLock(currentThread->resultMutex);
			if(currentThread->numRefs == 1)
			{
				// If the thread has already been detached, the exception is fatal.
				Errors::fatalf("Runtime exception in detached thread: %s",
							   describeException(exception).c_str());
			}
			else
			{
				currentThread->threwException = true;
				currentThread->exception = exception;
			}
		});

	return 0;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(threadTest,
							   "createThread",
							   U64,
							   createThread,
							   Function* entryFunction,
							   I32 entryArgument)
{
	// Validate that the entry function is non-null and has the correct type (i32)->i64
	if(!entryFunction
	   || IR::FunctionType{entryFunction->encodedType}
			  != FunctionType(TypeTuple{ValueType::i64}, TypeTuple{ValueType::i32}))
	{ throwException(Runtime::ExceptionTypes::indirectCallSignatureMismatch); }

	// Create a thread object that will expose its entry and error functions to the garbage
	// collector as roots.
	auto newContext = createContext(getCompartmentFromContextRuntimeData(contextRuntimeData));
	Thread* thread = new Thread(newContext, entryFunction, entryArgument);

	allocateThreadId(thread);

	// Increment the Thread's reference count for the pointer passed to the thread's entry function.
	// threadFunc calls the corresponding removeRef.
	thread->addRef();

	// Spawn and detach a platform thread that calls threadFunc.
	thread->platformThread = Platform::createThread(numStackBytes, threadEntry, thread);

	return thread->id;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(threadTest, "exitThread", void, exitThread, I64 code)
{
	if(!currentThread) { throwException(ExceptionTypes::calledAbort); }

	throw ExitThreadException{code};
}

// Validates a thread ID, removes the corresponding thread from the threads array, and returns it.
static IntrusiveSharedPtr<Thread> removeThreadById(Uptr threadId)
{
	IntrusiveSharedPtr<Thread> thread;

	Platform::Mutex::Lock threadsLock(threadsMutex);
	validateThreadId(threadId);
	thread = std::move(threads[threadId]);
	threads.removeOrFail(threadId);

	WAVM_ASSERT(thread->id == Uptr(threadId));
	thread->id = UINTPTR_MAX;

	return thread;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(threadTest, "joinThread", I64, joinThread, U64 threadId)
{
	IntrusiveSharedPtr<Thread> thread = removeThreadById(Uptr(threadId));
	Platform::joinThread(thread->platformThread);
	thread->platformThread = nullptr;

	Platform::Mutex::Lock resultLock(thread->resultMutex);
	if(thread->threwException) { throwException(thread->exception); }
	else
	{
		return thread->result;
	}
}

WAVM_DEFINE_INTRINSIC_FUNCTION(threadTest, "detachThread", void, detachThread, U64 threadId)
{
	IntrusiveSharedPtr<Thread> thread = removeThreadById(Uptr(threadId));

	Platform::detachThread(thread->platformThread);
	thread->platformThread = nullptr;

	// If the thread threw an exception, turn it into a fatal error.
	Platform::Mutex::Lock resultLock(thread->resultMutex);
	if(thread->threwException)
	{
		Errors::fatalf("Runtime exception in detached thread: %s",
					   describeException(thread->exception).c_str());
	}
}

Instance* ThreadTest::instantiate(Compartment* compartment)
{
	return Intrinsics::instantiateModule(
		compartment, {WAVM_INTRINSIC_MODULE_REF(threadTest)}, "threadTest");
}
