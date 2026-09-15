#include "EmscriptenABI.h"
#include "EmscriptenPrivate.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/IntrusiveSharedPtr.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Platform/Thread.h"
#include "WAVM/Runtime/Intrinsics.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;
using namespace WAVM::Emscripten;

namespace WAVM { namespace Emscripten {
	WAVM_DEFINE_INTRINSIC_MODULE(envThreads)
}}

// Adds the thread to the global thread array, assigning it an ID corresponding to its index in the
// array.
static U32 allocateThreadId(Emscripten::Process* process, Thread* thread)
{
	Platform::Mutex::Lock threadsLock(process->threadsMutex);
	thread->id = process->threads.add(0, thread);
	WAVM_ERROR_UNLESS(thread->id != 0);
	return thread->id;
}

// Validates that a thread ID is valid. i.e. 0 < threadId < threads.size(), and threads[threadId] !=
// null If the thread ID is invalid, throws an invalid argument exception. The caller must have
// already locked threadsMutex before calling validateThreadId.
static bool validateThreadId(Emscripten::Process* process, emabi::pthread_t threadId)
{
	WAVM_ASSERT_MUTEX_IS_LOCKED_BY_CURRENT_THREAD(process->threadsMutex);

	return threadId != 0 && process->threads.contains(threadId);
}

void Emscripten::joinAllThreads(Process& process)
{
	while(true)
	{
		Platform::Mutex::Lock threadsLock(process.threadsMutex);

		if(!process.threads.size()) { break; }
		auto it = process.threads.begin();
		WAVM_ASSERT(it != process.threads.end());

		emabi::pthread_t threadId = it.getIndex();
		IntrusiveSharedPtr<Thread> thread = std::move(*it);
		process.threads.removeOrFail(threadId);

		threadsLock.unlock();

		Platform::joinThread(thread->platformThread);
		thread->platformThread = nullptr;
	};
}

static I64 threadEntry(void* threadVoid)
{
	IntrusiveSharedPtr<Thread> thread
		= IntrusiveSharedPtr<Thread>::adopt(std::move((Thread*)threadVoid));

	catchRuntimeExceptions(
		[&]() {
			try
			{
				initThreadLocals(thread);

				UntaggedValue argumentValue{thread->argument};
				UntaggedValue resultValue;
				invokeFunction(thread->context,
							   thread->threadFunc,
							   FunctionType({ValueType::i32}, {ValueType::i32}),
							   &argumentValue,
							   &resultValue);
				thread->exitCode.store(resultValue.u32);
			}
			catch(Emscripten::ExitThreadException const& exitThreadException)
			{
				thread->exitCode.store(exitThreadException.exitCode);
			}
		},
		[](Exception* exception) {
			Errors::fatalf("Runtime exception: %s", describeException(exception).c_str());
		});

	return 0;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_cond_wait",
							   emabi::Result,
							   emscripten_pthread_cond_wait,
							   I32 a,
							   I32 b)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_cond_broadcast",
							   emabi::Result,
							   emscripten_pthread_cond_broadcast,
							   I32 a)
{
	return emabi::esuccess;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads, "_pthread_equal", I32, _pthread_equal, I32 a, I32 b)
{
	return a == b;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_key_create",
							   emabi::Result,
							   emscripten_pthread_key_create,
							   U32 keyAddress,
							   I32 destructorPtr)
{
	if(keyAddress == 0) { return emabi::einval; }

	Emscripten::Process* process = getProcess(contextRuntimeData);
	Emscripten::Thread* thread = getEmscriptenThread(contextRuntimeData);

	emabi::pthread_key_t key = process->pthreadSpecificNextKey++;
	memoryRef<U32>(process->memory, keyAddress) = key;
	thread->pthreadSpecific.set(key, 0);

	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_mutex_lock",
							   emabi::Result,
							   emscripten_pthread_mutex_lock,
							   I32 a)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_mutex_unlock",
							   emabi::Result,
							   emscripten_pthread_mutex_unlock,
							   I32 a)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_setspecific",
							   emabi::Result,
							   emscripten_pthread_setspecific,
							   emabi::pthread_key_t key,
							   emabi::Address value)
{
	Emscripten::Thread* thread = getEmscriptenThread(contextRuntimeData);
	if(!thread->pthreadSpecific.contains(key)) { return emabi::einval; }
	thread->pthreadSpecific.set(key, value);
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_getspecific",
							   emabi::Address,
							   emscripten_pthread_getspecific,
							   emabi::pthread_key_t key)
{
	Emscripten::Thread* thread = getEmscriptenThread(contextRuntimeData);
	const emabi::Address* value = thread->pthreadSpecific.get(key);
	return value ? *value : 0;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_once",
							   emabi::Result,
							   emscripten_pthread_once,
							   I32 a,
							   I32 b)
{
	throwException(Runtime::ExceptionTypes::calledUnimplementedIntrinsic);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_cleanup_push",
							   void,
							   emscripten_pthread_cleanup_push,
							   I32 a,
							   I32 b)
{
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_cleanup_pop",
							   void,
							   emscripten_pthread_cleanup_pop,
							   I32 a)
{
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads, "_pthread_self", emabi::pthread_t, _pthread_self)
{
	Emscripten::Thread* thread = getEmscriptenThread(contextRuntimeData);
	return thread->id;
}

WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_attr_init",
							   emabi::Result,
							   emscripten_pthread_attr_init,
							   I32 address)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_attr_destroy",
							   emabi::Result,
							   emscripten_pthread_attr_destroy,
							   I32 address)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_getattr_np",
							   emabi::Result,
							   emscripten_pthread_getattr_np,
							   I32 thread,
							   I32 address)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_attr_getstack",
							   emabi::Result,
							   emscripten_pthread_attr_getstack,
							   U32 attrAddress,
							   U32 stackBaseAddress,
							   U32 stackSizeAddress)
{
	Emscripten::Process* process = getProcess(contextRuntimeData);
	Emscripten::Thread* thread = getEmscriptenThread(contextRuntimeData);
	memoryRef<U32>(process->memory, stackBaseAddress) = thread->stackAddress;
	memoryRef<U32>(process->memory, stackSizeAddress) = thread->numStackBytes;
	return emabi::esuccess;
}
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(envThreads,
											 "_pthread_cond_destroy",
											 emabi::Result,
											 emscripten_pthread_cond_destroy,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(envThreads,
											 "_pthread_cond_init",
											 emabi::Result,
											 emscripten_pthread_cond_init,
											 U32,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(envThreads,
											 "_pthread_cond_signal",
											 emabi::Result,
											 emscripten_pthread_cond_signal,
											 U32);
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(envThreads,
											 "_pthread_cond_timedwait",
											 emabi::Result,
											 emscripten_pthread_cond_timedwait,
											 U32,
											 U32,
											 U32);

WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_create",
							   emabi::Result,
							   emscripten_pthread_create,
							   U32 threadIdAddress,
							   U32 threadAttrAddress,
							   U32 threadFuncIndex,
							   U32 threadFuncArg)
{
	auto process = getProcess(contextRuntimeData);

	Runtime::Function* threadFunc = nullptr;
	Runtime::unwindSignalsAsExceptions(
		[&] { threadFunc = asFunctionNullable(getTableElement(process->table, threadFuncIndex)); });

	// Validate that the entry function is non-null and has the correct type i32->i32
	if(!threadFunc
	   || getFunctionType(threadFunc) != FunctionType({ValueType::i32}, {ValueType::i32}))
	{ throwException(Runtime::ExceptionTypes::indirectCallSignatureMismatch); }

	// Create a thread object that will expose its entry and error functions to the garbage
	// collector as roots.
	auto newContext = createContext(getCompartmentFromContextRuntimeData(contextRuntimeData));
	Thread* thread = new Thread(process, newContext, threadFunc, threadFuncArg);
	setUserData(newContext, thread);

	allocateThreadId(process, thread);

	// Allocate the aliased stack for the thread.
	thread->numStackBytes = 2 * 1024 * 1024;
	thread->stackAddress = dynamicAlloc(
		process, getContextFromRuntimeData(contextRuntimeData), thread->numStackBytes);

	// Increment the Thread's reference count for the pointer passed to the thread's entry function.
	thread->addRef();

	// Spawn a platform thread that calls threadFunc.
	thread->platformThread = Platform::createThread(0, threadEntry, thread);

	// Write the thread ID to the address provided.
	unwindSignalsAsExceptions(
		[=] { memoryRef<emabi::pthread_t>(process->memory, threadIdAddress) = thread->id; });

	return emabi::esuccess;
}

// Validates a thread ID, removes the corresponding thread from the threads array, and returns it.
static bool removeThreadById(Process* process,
							 emabi::pthread_t threadId,
							 IntrusiveSharedPtr<Thread>& outThread)
{
	Platform::Mutex::Lock threadsLock(process->threadsMutex);
	if(!validateThreadId(process, threadId)) { return false; }
	outThread = std::move(process->threads[threadId]);
	process->threads.removeOrFail(threadId);

	WAVM_ASSERT(outThread->id == threadId);
	outThread->id = 0;

	return true;
}

// Note that we need to be careful about how to implement detach, so it's not possible to create
// threads that outlive the instance.
WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(envThreads,
											 "_pthread_detach",
											 emabi::Result,
											 emscripten_pthread_detach,
											 emabi::pthread_t);

WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_join",
							   emabi::Result,
							   emscripten_pthread_join,
							   emabi::pthread_t threadId,
							   U32 resultAddress)
{
	auto process = getProcess(contextRuntimeData);

	IntrusiveSharedPtr<Thread> thread;
	if(!removeThreadById(process, threadId, thread)) { return emabi::esrch; }
	Platform::joinThread(thread->platformThread);
	thread->platformThread = nullptr;

	const U32 exitCode = thread->exitCode.load();

	// Write the exit code to the address provided.
	unwindSignalsAsExceptions([=] { memoryRef<U32>(process->memory, resultAddress) = exitCode; });

	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_mutexattr_destroy",
							   emabi::Result,
							   emscripten_pthread_mutexattr_destroy,
							   U32 attrAddress)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_mutexattr_init",
							   emabi::Result,
							   emscripten_pthread_mutexattr_init,
							   U32 attrAddress)
{
	return emabi::esuccess;
}
WAVM_DEFINE_INTRINSIC_FUNCTION(envThreads,
							   "_pthread_mutexattr_settype",
							   emabi::Result,
							   emscripten_pthread_mutexattr_settype,
							   U32 attrAddress,
							   U32 type)
{
	return emabi::esuccess;
}
