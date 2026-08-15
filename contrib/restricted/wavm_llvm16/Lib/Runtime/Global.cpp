#include <stdint.h>
#include <string.h>
#include <atomic>
#include <vector>
#include "RuntimePrivate.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/Runtime/Runtime.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

Global* Runtime::createGlobal(Compartment* compartment,
							  GlobalType type,
							  std::string&& debugName,
							  ResourceQuotaRefParam resourceQuota)
{
	U32 mutableGlobalIndex = UINT32_MAX;
	if(type.isMutable)
	{
		mutableGlobalIndex = compartment->globalDataAllocationMask.getSmallestNonMember();
		if(mutableGlobalIndex == maxMutableGlobals) { return nullptr; }
		compartment->globalDataAllocationMask.add(mutableGlobalIndex);

		// Zero-initialize the global's mutable value for all current and future contexts.
		compartment->initialContextMutableGlobals[mutableGlobalIndex] = IR::UntaggedValue();
		for(Context* context : compartment->contexts)
		{ context->runtimeData->mutableGlobals[mutableGlobalIndex] = IR::UntaggedValue(); }
	}

	// Create the global and add it to the compartment's list of globals.
	Global* global = new Global(compartment, type, mutableGlobalIndex, std::move(debugName));
	{
		Platform::RWMutex::ExclusiveLock compartmentLock(compartment->mutex);
		global->id = compartment->globals.add(UINTPTR_MAX, global);
		if(global->id == UINTPTR_MAX)
		{
			delete global;
			WAVM_ASSERT(global->id != UINTPTR_MAX);
			return nullptr;
		}
	}

	return global;
}

void Runtime::initializeGlobal(Global* global, Value value)
{
	Compartment* compartment = global->compartment;
	WAVM_ERROR_UNLESS(isSubtype(value.type, global->type.valueType));
	WAVM_ERROR_UNLESS(!isReferenceType(global->type.valueType) || !value.object
					  || isInCompartment(value.object, compartment));

	WAVM_ERROR_UNLESS(!global->hasBeenInitialized);
	global->hasBeenInitialized = true;

	global->initialValue = value;
	if(global->type.isMutable)
	{
		// Initialize the global's mutable value for all current and future contexts.
		compartment->initialContextMutableGlobals[global->mutableGlobalIndex] = value;
		for(Context* context : compartment->contexts)
		{ context->runtimeData->mutableGlobals[global->mutableGlobalIndex] = value; }
	}
}

Global* Runtime::cloneGlobal(Global* global, Compartment* newCompartment)
{
	IR::UntaggedValue initialValue = global->initialValue;
	if(isReferenceType(global->type.valueType))
	{
		initialValue.object = remapToClonedCompartment(initialValue.object, newCompartment);
		if(global->type.isMutable)
		{
			Object*& initialMutableRef
				= newCompartment->initialContextMutableGlobals[global->mutableGlobalIndex].object;
			initialMutableRef = remapToClonedCompartment(initialMutableRef, newCompartment);
		}
	}

	Global* newGlobal = new Global(newCompartment,
								   global->type,
								   global->mutableGlobalIndex,
								   std::string(global->debugName),
								   initialValue);
	newGlobal->id = global->id;

	Platform::RWMutex::ExclusiveLock compartmentLock(newCompartment->mutex);
	newCompartment->globals.insertOrFail(global->id, newGlobal);
	return newGlobal;
}

Runtime::Global::~Global()
{
	if(id != UINTPTR_MAX)
	{
		WAVM_ASSERT_RWMUTEX_IS_EXCLUSIVELY_LOCKED_BY_CURRENT_THREAD(compartment->mutex);
		compartment->globals.removeOrFail(id);
	}

	if(type.isMutable)
	{
		WAVM_ASSERT(mutableGlobalIndex < maxMutableGlobals);
		WAVM_ASSERT(compartment->globalDataAllocationMask.contains(mutableGlobalIndex));
		compartment->globalDataAllocationMask.remove(mutableGlobalIndex);
	}
}

Value Runtime::getGlobalValue(const Context* context, const Global* global)
{
	WAVM_ASSERT(context || !global->type.isMutable);
	return Value(global->type.valueType,
				 global->type.isMutable
					 ? context->runtimeData->mutableGlobals[global->mutableGlobalIndex]
					 : global->initialValue);
}

Value Runtime::setGlobalValue(Context* context, const Global* global, Value newValue)
{
	WAVM_ASSERT(context);
	WAVM_ASSERT(newValue.type == global->type.valueType);
	WAVM_ASSERT(global->type.isMutable);
	WAVM_ERROR_UNLESS(context->compartment == global->compartment);
	WAVM_ERROR_UNLESS(!isReferenceType(global->type.valueType) || !newValue.object
					  || isInCompartment(newValue.object, context->compartment));
	UntaggedValue& value = context->runtimeData->mutableGlobals[global->mutableGlobalIndex];
	const Value previousValue = Value(global->type.valueType, value);
	value = newValue;
	return previousValue;
}

GlobalType Runtime::getGlobalType(const Global* global) { return global->type; }
