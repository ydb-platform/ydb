#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <cmath>
#include <memory>
#include <utility>
#include <vector>
#include "RuntimePrivate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Platform/Clock.h"
#include "WAVM/Platform/Event.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/Runtime/Intrinsics.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

using namespace WAVM;
using namespace WAVM::Runtime;

namespace WAVM { namespace Runtime {
	WAVM_DEFINE_INTRINSIC_MODULE(wavmIntrinsicsAtomics)
}}

// Holds a list of threads (in the form of events that will wake them) that
// are waiting on a specific address.
struct WaitList
{
	Platform::Mutex mutex;
	std::vector<Platform::Event*> wakeEvents;
	std::atomic<Uptr> numReferences;

	WaitList() : numReferences(1) {}
};

// An event that is reused within a thread when it waits on a WaitList.
thread_local std::unique_ptr<Platform::Event> threadWakeEvent = nullptr;

// A map from address to a list of threads waiting on that address.
static Platform::Mutex addressToWaitListMapMutex;
static HashMap<Uptr, WaitList*> addressToWaitListMap;

// Opens the wait list for a given address.
// Increases the wait list's reference count, and returns a pointer to it.
// Note that it does not lock the wait list mutex.
// A call to openWaitList should be followed by a call to closeWaitList to avoid leaks.
static WaitList* openWaitList(Uptr address)
{
	Platform::Mutex::Lock mapLock(addressToWaitListMapMutex);
	auto waitListPtr = addressToWaitListMap.get(address);
	if(waitListPtr)
	{
		++(*waitListPtr)->numReferences;
		return *waitListPtr;
	}
	else
	{
		WaitList* waitList = new WaitList();
		addressToWaitListMap.set(address, waitList);
		return waitList;
	}
}

// Closes a wait list, deleting it and removing it from the global map if it was the last reference.
static void closeWaitList(Uptr address, WaitList* waitList)
{
	if(--waitList->numReferences == 0)
	{
		Platform::Mutex::Lock mapLock(addressToWaitListMapMutex);
		if(!waitList->numReferences)
		{
			WAVM_ASSERT(!waitList->wakeEvents.size());
			delete waitList;
			addressToWaitListMap.remove(address);
		}
	}
}

// Loads a value from memory with seq_cst memory order.
// The caller must ensure that the pointer is naturally aligned.
template<typename Value> static Value atomicLoad(const Value* valuePointer)
{
	static_assert(sizeof(std::atomic<Value>) == sizeof(Value), "relying on non-standard behavior");
	std::atomic<Value>* valuePointerAtomic = (std::atomic<Value>*)valuePointer;
	return valuePointerAtomic->load();
}

// Stores a value to memory with seq_cst memory order.
// The caller must ensure that the pointer is naturally aligned.
template<typename Value> static void atomicStore(Value* valuePointer, Value newValue)
{
	static_assert(sizeof(std::atomic<Value>) == sizeof(Value), "relying on non-standard behavior");
	std::atomic<Value>* valuePointerAtomic = (std::atomic<Value>*)valuePointer;
	valuePointerAtomic->store(newValue);
}

template<typename Value>
static U32 waitOnAddress(Value* valuePointer, Value expectedValue, I64 timeout)
{
	// Open the wait list for this address.
	const Uptr address = reinterpret_cast<Uptr>(valuePointer);
	WaitList* waitList = openWaitList(address);

	// Lock the wait list, and check that *valuePointer is still what the caller expected it to be.
	{
		Platform::Mutex::Lock waitListLock(waitList->mutex);

		// Use unwindSignalsAsExceptions to ensure that an access violation signal produced by the
		// load will be thrown as a Runtime::Exception and unwind the stack (e.g. the locks).
		Value value;
		Runtime::unwindSignalsAsExceptions(
			[valuePointer, &value] { value = atomicLoad(valuePointer); });

		if(value != expectedValue)
		{
			// If *valuePointer wasn't the expected value, unlock the wait list and return.
			waitListLock.unlock();
			closeWaitList(address, waitList);
			return 1;
		}
		else
		{
			// If the thread hasn't yet created a wake event, do so.
			if(!threadWakeEvent)
			{ threadWakeEvent = std::unique_ptr<Platform::Event>(new Platform::Event()); }

			// Add the wake event to the wait list, and unlock the wait list.
			waitList->wakeEvents.push_back(threadWakeEvent.get());
			waitListLock.unlock();
		}
	}

	// Wait for the thread's wake event to be signaled.
	bool timedOut = false;
	if(!threadWakeEvent->wait(timeout < 0 ? Time::infinity() : Time{I128(timeout)}))
	{
		// If the wait timed out, lock the wait list and check if the thread's wake event is still
		// in the wait list.
		Platform::Mutex::Lock waitListLock(waitList->mutex);
		auto wakeEventIt = std::find(
			waitList->wakeEvents.begin(), waitList->wakeEvents.end(), threadWakeEvent.get());
		if(wakeEventIt != waitList->wakeEvents.end())
		{
			// If the event was still on the wait list, remove it, and return the "timed out"
			// result.
			waitList->wakeEvents.erase(wakeEventIt);
			timedOut = true;
		}
		else
		{
			// In between the wait timing out and locking the wait list, some other thread tried to
			// wake this thread. The event will now be signaled, so use an immediately expiring wait
			// on it to reset it.
			WAVM_ERROR_UNLESS(
				threadWakeEvent->wait(Platform::getClockTime(Platform::Clock::monotonic)));
		}
	}

	closeWaitList(address, waitList);
	return timedOut ? 2 : 0;
}

static U32 wakeAddress(void* pointer, U32 numToWake)
{
	if(numToWake == 0) { return 0; }

	// Open the wait list for this address.
	const Uptr address = reinterpret_cast<Uptr>(pointer);
	WaitList* waitList = openWaitList(address);
	Uptr actualNumToWake = numToWake;
	{
		Platform::Mutex::Lock waitListLock(waitList->mutex);

		// Determine how many threads to wake.
		// numToWake==UINT32_MAX means wake all waiting threads.
		if(actualNumToWake == UINT32_MAX || actualNumToWake > waitList->wakeEvents.size())
		{ actualNumToWake = waitList->wakeEvents.size(); }

		// Signal the events corresponding to the oldest waiting threads.
		for(Uptr wakeIndex = 0; wakeIndex < actualNumToWake; ++wakeIndex)
		{ waitList->wakeEvents[wakeIndex]->signal(); }

		// Remove the events from the wait list.
		waitList->wakeEvents.erase(waitList->wakeEvents.begin(),
								   waitList->wakeEvents.begin() + actualNumToWake);
	}
	closeWaitList(address, waitList);

	if(actualNumToWake > UINT32_MAX)
	{ throwException(ExceptionTypes::integerDivideByZeroOrOverflow); }
	return U32(actualNumToWake);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsAtomics,
							   "misalignedAtomicTrap",
							   void,
							   misalignedAtomicTrap,
							   U64 address)
{
	throwException(ExceptionTypes::misalignedAtomicMemoryAccess, {address});
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsAtomics,
							   "memory.atomic.notify",
							   I32,
							   atomic_notify,
							   Uptr address,
							   I32 numToWake,
							   Uptr memoryId)
{
	Memory* memory = getMemoryFromRuntimeData(contextRuntimeData, memoryId);

	// Validate that the address is within the memory's bounds.
	const U64 memoryNumBytes = U64(memory->numPages) * IR::numBytesPerPage;
	if(U64(address) + 4 > memoryNumBytes)
	{ throwException(ExceptionTypes::outOfBoundsMemoryAccess, {memory, memoryNumBytes}); }

	// The alignment check is done by the caller.
	WAVM_ASSERT(!(address & 3));

	return wakeAddress(memory->getBaseAddress() + address, numToWake);
}

WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsAtomics,
							   "memory.atomic.wait32",
							   I32,
							   atomic_wait_I32,
							   Uptr address,
							   I32 expectedValue,
							   I64 timeout,
							   Uptr memoryId)
{
	Memory* memory = getMemoryFromRuntimeData(contextRuntimeData, memoryId);

	// Throw a waitOnUnsharedMemory exception if the memory is not shared.
	if(!memory->isShared) { throwException(ExceptionTypes::waitOnUnsharedMemory, {memory}); }

	// Assume that the caller has validated the alignment.
	WAVM_ASSERT(!(address & 3));

	// Validate that the address is within the memory's bounds, and convert it to a pointer.
	I32* valuePointer = &memoryRef<I32>(memory, address);

	return waitOnAddress(valuePointer, expectedValue, timeout);
}
WAVM_DEFINE_INTRINSIC_FUNCTION(wavmIntrinsicsAtomics,
							   "memory.atomic.wait64",
							   I32,
							   atomic_wait_i64,
							   Uptr address,
							   I64 expectedValue,
							   I64 timeout,
							   Uptr memoryId)
{
	Memory* memory = getMemoryFromRuntimeData(contextRuntimeData, memoryId);

	// Throw a waitOnUnsharedMemory exception if the memory is not shared.
	if(!memory->isShared) { throwException(ExceptionTypes::waitOnUnsharedMemory, {memory}); }

	// Assume that the caller has validated the alignment.
	WAVM_ASSERT(!(address & 7));

	// Validate that the address is within the memory's bounds, and convert it to a pointer.
	I64* valuePointer = &memoryRef<I64>(memory, address);

	return waitOnAddress(valuePointer, expectedValue, timeout);
}
