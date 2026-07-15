#include "WAVM/Runtime/Runtime.h"
#include "RuntimePrivate.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Platform/RWMutex.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::Runtime;

#define DEFINE_OBJECT_TYPE(kindId, kindName, Type)                                                 \
	Runtime::Type* Runtime::as##kindName(Object* object)                                           \
	{                                                                                              \
		WAVM_ASSERT(!object || object->kind == kindId);                                            \
		return (Runtime::Type*)object;                                                             \
	}                                                                                              \
	Runtime::Type* Runtime::as##kindName##Nullable(Object* object)                                 \
	{                                                                                              \
		return object && object->kind == kindId ? (Runtime::Type*)object : nullptr;                \
	}                                                                                              \
	const Runtime::Type* Runtime::as##kindName(const Object* object)                               \
	{                                                                                              \
		WAVM_ASSERT(!object || object->kind == kindId);                                            \
		return (const Runtime::Type*)object;                                                       \
	}                                                                                              \
	const Runtime::Type* Runtime::as##kindName##Nullable(const Object* object)                     \
	{                                                                                              \
		return object && object->kind == kindId ? (const Runtime::Type*)object : nullptr;          \
	}                                                                                              \
	Object* Runtime::asObject(Runtime::Type* object) { return (Object*)object; }                   \
	const Object* Runtime::asObject(const Runtime::Type* object) { return (const Object*)object; }

#define DEFINE_GCOBJECT_TYPE(kindId, kindName, Type)                                               \
	DEFINE_OBJECT_TYPE(kindId, kindName, Type)                                                     \
	void Runtime::setUserData(Runtime::Type* object, void* userData, void (*finalizer)(void*))     \
	{                                                                                              \
		object->userData = userData;                                                               \
		object->finalizeUserData = finalizer;                                                      \
	}                                                                                              \
	void* Runtime::getUserData(const Runtime::Type* object) { return object->userData; }           \
	const std::string& Runtime::getDebugName(const Type* object) { return object->debugName; }

DEFINE_GCOBJECT_TYPE(ObjectKind::table, Table, Table);
DEFINE_GCOBJECT_TYPE(ObjectKind::memory, Memory, Memory);
DEFINE_GCOBJECT_TYPE(ObjectKind::global, Global, Global);
DEFINE_GCOBJECT_TYPE(ObjectKind::exceptionType, ExceptionType, ExceptionType);
DEFINE_GCOBJECT_TYPE(ObjectKind::instance, Instance, Instance);
DEFINE_GCOBJECT_TYPE(ObjectKind::context, Context, Context);
DEFINE_GCOBJECT_TYPE(ObjectKind::compartment, Compartment, Compartment);
DEFINE_GCOBJECT_TYPE(ObjectKind::foreign, Foreign, Foreign);

DEFINE_OBJECT_TYPE(ObjectKind::function, Function, Function);
void Runtime::setUserData(Runtime::Function* function, void* userData, void (*finalizer)(void*))
{
	function->mutableData->userData = userData;
	function->mutableData->finalizeUserData = finalizer;
}
void* Runtime::getUserData(const Runtime::Function* function)
{
	return function->mutableData->userData;
}
const std::string& Runtime::getDebugName(const Runtime::Function* function)
{
	return function->mutableData->debugName;
}

void Runtime::setUserData(Runtime::Object* object, void* userData, void (*finalizer)(void*))
{
	if(object->kind == ObjectKind::function)
	{ setUserData(asFunction(object), userData, finalizer); }
	else
	{
		auto gcObject = (GCObject*)object;
		gcObject->userData = userData;
		gcObject->finalizeUserData = finalizer;
	}
}

void* Runtime::getUserData(const Runtime::Object* object)
{
	if(object->kind == ObjectKind::function) { return getUserData(asFunction(object)); }
	else
	{
		auto gcObject = (GCObject*)object;
		return gcObject->userData;
	}
}

const std::string& Runtime::getDebugName(const Runtime::Object* object)
{
	if(object->kind == ObjectKind::function) { return getDebugName(asFunction(object)); }
	else
	{
		auto gcObject = (GCObject*)object;
		return gcObject->debugName;
	}
}

Runtime::GCObject::~GCObject()
{
	WAVM_ASSERT(numRootReferences.load(std::memory_order_acquire) == 0);
	if(finalizeUserData) { (*finalizeUserData)(userData); }
}

bool Runtime::isA(const Object* object, const ExternType& type)
{
	if(ObjectKind(type.kind) != object->kind) { return false; }

	switch(type.kind)
	{
	case ExternKind::function: return asFunction(object)->encodedType == asFunctionType(type);
	case ExternKind::global: return isSubtype(asGlobal(object)->type, asGlobalType(type));
	case ExternKind::table: return isSubtype(getTableType(asTable(object)), asTableType(type));
	case ExternKind::memory: return isSubtype(getMemoryType(asMemory(object)), asMemoryType(type));
	case ExternKind::exceptionType:
		return isSubtype(asExceptionType(type).params, asExceptionType(object)->sig.params);

	case ExternKind::invalid:
	default: WAVM_UNREACHABLE();
	}
}

ExternType Runtime::getExternType(const Object* object)
{
	switch(object->kind)
	{
	case ObjectKind::function: return FunctionType(asFunction(object)->encodedType);
	case ObjectKind::global: return asGlobal(object)->type;
	case ObjectKind::table: return getTableType(asTable(object));
	case ObjectKind::memory: return getMemoryType(asMemory(object));
	case ObjectKind::exceptionType: return asExceptionType(object)->sig;

	case ObjectKind::instance:
	case ObjectKind::context:
	case ObjectKind::compartment:
	case ObjectKind::foreign:
	case ObjectKind::invalid:
	default: WAVM_UNREACHABLE();
	};
}

FunctionType Runtime::getFunctionType(const Function* function) { return function->encodedType; }

Context* Runtime::getContextFromRuntimeData(ContextRuntimeData* contextRuntimeData)
{
	return contextRuntimeData->context;
}

Compartment* Runtime::getCompartmentFromContextRuntimeData(
	struct ContextRuntimeData* contextRuntimeData)
{
	const CompartmentRuntimeData* compartmentRuntimeData
		= getCompartmentRuntimeData(contextRuntimeData);
	return compartmentRuntimeData->compartment;
}

ContextRuntimeData* Runtime::getContextRuntimeData(const Context* context)
{
	return context->runtimeData;
}

Instance* Runtime::getInstanceFromRuntimeData(ContextRuntimeData* contextRuntimeData,
											  Uptr instanceId)
{
	Compartment* compartment = getCompartmentRuntimeData(contextRuntimeData)->compartment;
	Platform::RWMutex::ShareableLock compartmentLock(compartment->mutex);
	WAVM_ASSERT(compartment->instances.contains(instanceId));
	return compartment->instances[instanceId];
}

Table* Runtime::getTableFromRuntimeData(ContextRuntimeData* contextRuntimeData, Uptr tableId)
{
	Compartment* compartment = getCompartmentRuntimeData(contextRuntimeData)->compartment;
	Platform::RWMutex::ShareableLock compartmentLock(compartment->mutex);
	WAVM_ASSERT(compartment->tables.contains(tableId));
	return compartment->tables[tableId];
}

Memory* Runtime::getMemoryFromRuntimeData(ContextRuntimeData* contextRuntimeData, Uptr memoryId)
{
	Compartment* compartment = getCompartmentRuntimeData(contextRuntimeData)->compartment;
	Platform::RWMutex::ShareableLock compartmentLock(compartment->mutex);
	return compartment->memories[memoryId];
}

Foreign* Runtime::createForeign(Compartment* compartment,
								void* userData,
								void (*finalizer)(void*),
								std::string&& debugName)
{
	Foreign* foreign = new Foreign(compartment, std::move(debugName));
	setUserData(foreign, userData, finalizer);

	{
		Platform::RWMutex::ExclusiveLock lock(compartment->mutex);
		foreign->id = compartment->foreigns.add(UINTPTR_MAX, foreign);
		if(foreign->id == UINTPTR_MAX)
		{
			delete foreign;
			return nullptr;
		}
	}

	return foreign;
}
