#pragma once

#include <memory>
#include <string>
#include <vector>
#include "WAVM/IR/FeatureSpec.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Diagnostics.h"

// Declare some types to avoid including the full definition.
namespace WAVM {
	namespace IR {
		struct Module;
	}
	namespace WASM {
		struct LoadError;
	}
};

// Declare the different kinds of objects. They are only declared as incomplete struct types here,
// and Runtime clients will only handle opaque pointers to them.
#define WAVM_DECLARE_OBJECT_TYPE(kindId, kindName, Type)                                           \
	struct Type;                                                                                   \
                                                                                                   \
	WAVM_API void addGCRoot(const Type* type);                                                     \
	WAVM_API void removeGCRoot(const Type* type) noexcept;                                         \
                                                                                                   \
	WAVM_API Runtime::Type* as##kindName(Object* object);                                          \
	WAVM_API Runtime::Type* as##kindName##Nullable(Object* object);                                \
	WAVM_API const Runtime::Type* as##kindName(const Object* object);                              \
	WAVM_API const Runtime::Type* as##kindName##Nullable(const Object* object);                    \
	WAVM_API Object* asObject(Type* object);                                                       \
	WAVM_API const Object* asObject(const Runtime::Type* object);                                  \
                                                                                                   \
	WAVM_API void setUserData(                                                                     \
		Runtime::Type* object, void* userData, void (*finalizer)(void*) = nullptr);                \
	WAVM_API void* getUserData(const Runtime::Type* object);                                       \
                                                                                                   \
	template<> inline Runtime::Type* as<Type>(Object * object) { return as##kindName(object); }    \
	template<> inline const Runtime::Type* as<const Type>(const Object* object)                    \
	{                                                                                              \
		return as##kindName(object);                                                               \
	}                                                                                              \
                                                                                                   \
	WAVM_API const std::string& getDebugName(const Type*);

namespace WAVM { namespace Runtime {

	struct Object;

	// Tests whether an object is of the given type.
	WAVM_API bool isA(const Object* object, const IR::ExternType& type);

	WAVM_API IR::ExternType getExternType(const Object* object);

	WAVM_API void setUserData(Object* object, void* userData, void (*finalizer)(void*) = nullptr);
	WAVM_API void* getUserData(const Object* object);

	WAVM_API const std::string& getDebugName(const Object*);

	inline Object* asObject(Object* object) { return object; }
	inline const Object* asObject(const Object* object) { return object; }

	template<typename Type> Type* as(Object* object);
	template<typename Type> const Type* as(const Object* object);

	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::function, Function, Function);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::table, Table, Table);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::memory, Memory, Memory);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::global, Global, Global);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::exceptionType, ExceptionType, ExceptionType);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::instance, Instance, Instance);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::context, Context, Context);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::compartment, Compartment, Compartment);
	WAVM_DECLARE_OBJECT_TYPE(ObjectKind::foreign, Foreign, Foreign);

	// The result of growing a table or memory.
	enum class GrowResult
	{
		success,
		outOfMemory,
		outOfQuota,
		outOfMaxSize
	};

	//
	// Garbage collection
	//

	// A GC root pointer.
	template<typename ObjectType> struct GCPointer
	{
		GCPointer() : value(nullptr) {}
		GCPointer(ObjectType* inValue)
		{
			value = inValue;
			if(value) { addGCRoot(value); }
		}
		GCPointer(const GCPointer<ObjectType>& inCopy)
		{
			value = inCopy.value;
			if(value) { addGCRoot(value); }
		}
		GCPointer(GCPointer<ObjectType>&& inMove) noexcept
		{
			value = inMove.value;
			inMove.value = nullptr;
		}
		~GCPointer()
		{
			if(value) { removeGCRoot(value); }
		}

		void operator=(ObjectType* inValue)
		{
			if(value) { removeGCRoot(value); }
			value = inValue;
			if(value) { addGCRoot(value); }
		}
		void operator=(const GCPointer<ObjectType>& inCopy)
		{
			if(value) { removeGCRoot(value); }
			value = inCopy.value;
			if(value) { addGCRoot(value); }
		}
		void operator=(GCPointer<ObjectType>&& inMove) noexcept
		{
			if(value) { removeGCRoot(value); }
			value = inMove.value;
			inMove.value = nullptr;
		}

		operator ObjectType*() const { return value; }
		ObjectType& operator*() const { return *value; }
		ObjectType* operator->() const { return value; }

	private:
		ObjectType* value;
	};

	// Increments the object's counter of root references.
	WAVM_API void addGCRoot(const Object* object);

	// Decrements the object's counter of root referencers.
	WAVM_API void removeGCRoot(const Object* object) noexcept;

	// Frees any unreferenced objects owned by a compartment.
	WAVM_API void collectCompartmentGarbage(Compartment* compartment);

	// Clears the given GC root reference to a compartment, and collects garbage for it. Returns
	// true if the entire compartment was freed by the operation, or false if there are remaining
	// root references that can reach it.
	WAVM_API bool tryCollectCompartment(GCPointer<Compartment>&& compartment);

	//
	// Exception types
	//

#define WAVM_ENUM_INTRINSIC_EXCEPTION_TYPES(visit)                                                 \
	visit(outOfBoundsMemoryAccess, WAVM::IR::ValueType::externref, WAVM::IR::ValueType::i64);      \
	visit(outOfBoundsTableAccess, WAVM::IR::ValueType::externref, WAVM::IR::ValueType::i64);       \
	visit(outOfBoundsDataSegmentAccess,                                                            \
		  WAVM::IR::ValueType::externref,                                                          \
		  WAVM::IR::ValueType::i64,                                                                \
		  WAVM::IR::ValueType::i64);                                                               \
	visit(outOfBoundsElemSegmentAccess,                                                            \
		  WAVM::IR::ValueType::externref,                                                          \
		  WAVM::IR::ValueType::i64,                                                                \
		  WAVM::IR::ValueType::i64);                                                               \
	visit(stackOverflow);                                                                          \
	visit(integerDivideByZeroOrOverflow);                                                          \
	visit(invalidFloatOperation);                                                                  \
	visit(invokeSignatureMismatch);                                                                \
	visit(reachedUnreachable);                                                                     \
	visit(indirectCallSignatureMismatch, WAVM::IR::ValueType::funcref, WAVM::IR::ValueType::i64);  \
	visit(uninitializedTableElement, WAVM::IR::ValueType::externref, WAVM::IR::ValueType::i64);    \
	visit(calledAbort);                                                                            \
	visit(calledUnimplementedIntrinsic);                                                           \
	visit(outOfMemory);                                                                            \
	visit(misalignedAtomicMemoryAccess, WAVM::IR::ValueType::i64);                                 \
	visit(waitOnUnsharedMemory, WAVM::IR::ValueType::externref);                                   \
	visit(invalidArgument);																		   \
	visit(timeoutExpired);

	// Information about a runtime exception.
	namespace ExceptionTypes {
#define DECLARE_INTRINSIC_EXCEPTION_TYPE(name, ...) WAVM_API extern ExceptionType* name;
		WAVM_ENUM_INTRINSIC_EXCEPTION_TYPES(DECLARE_INTRINSIC_EXCEPTION_TYPE)
#undef DECLARE_INTRINSIC_EXCEPTION_TYPE
	};

	// Creates an exception type instance.
	WAVM_API ExceptionType* createExceptionType(Compartment* compartment,
												IR::ExceptionType sig,
												std::string&& debugName);

	// Returns a string that describes the given exception type.
	WAVM_API std::string describeExceptionType(const ExceptionType* type);

	// Returns the parameter types for an exception type instance.
	WAVM_API IR::TypeTuple getExceptionTypeParameters(const ExceptionType* type);

	//
	// Resource quotas
	//

	struct ResourceQuota;
	typedef std::shared_ptr<ResourceQuota> ResourceQuotaRef;
	typedef std::shared_ptr<const ResourceQuota> ResourceQuotaConstRef;
	typedef const std::shared_ptr<ResourceQuota>& ResourceQuotaRefParam;
	typedef const std::shared_ptr<const ResourceQuota>& ResourceQuotaConstRefParam;

	WAVM_API ResourceQuotaRef createResourceQuota();

	WAVM_API Uptr getResourceQuotaMaxTableElems(ResourceQuotaConstRefParam);
	WAVM_API Uptr getResourceQuotaCurrentTableElems(ResourceQuotaConstRefParam);
	WAVM_API void setResourceQuotaMaxTableElems(ResourceQuotaRefParam, Uptr maxTableElems);

	WAVM_API Uptr getResourceQuotaMaxMemoryPages(ResourceQuotaConstRefParam);
	WAVM_API Uptr getResourceQuotaCurrentMemoryPages(ResourceQuotaConstRefParam);
	WAVM_API void setResourceQuotaMaxMemoryPages(ResourceQuotaRefParam, Uptr maxMemoryPages);

	//
	// Exceptions
	//

	struct Exception;

	// Exception UserData
	WAVM_API void setUserData(Exception* exception, void* userData, void (*finalizer)(void*));
	WAVM_API void* getUserData(const Exception* exception);

	// Creates a runtime exception.
	WAVM_API Exception* createException(ExceptionType* type,
										const IR::UntaggedValue* arguments,
										Uptr numArguments,
										Platform::CallStack&& callStack);

	// Destroys a runtime exception.
	WAVM_API void destroyException(Exception* exception);

	// Returns the type of an exception.
	WAVM_API ExceptionType* getExceptionType(const Exception* exception);

	// Returns a specific argument of an exception.
	WAVM_API IR::UntaggedValue getExceptionArgument(const Exception* exception, Uptr argIndex);

	// Returns the call stack at the origin of an exception.
	WAVM_API const Platform::CallStack& getExceptionCallStack(const Exception* exception);

	// Returns a string that describes the given exception cause.
	WAVM_API std::string describeException(const Exception* exception);

	// Throws a runtime exception.
	[[noreturn]] WAVM_API void throwException(Exception* exception);

	// Creates and throws a runtime exception.
	[[noreturn]] WAVM_API void throwException(ExceptionType* type,
											  const std::vector<IR::UntaggedValue>& arguments = {});

	// Calls a thunk and catches any runtime exceptions that occur within it. Note that the
	// catchThunk takes ownership of the exception, and is responsible for calling destroyException.
	WAVM_API void catchRuntimeExceptions(const std::function<void()>& thunk,
										 const std::function<void(Exception*)>& catchThunk);

	// Calls a thunk and ensures that any signals that occur within the thunk will be thrown as
	// runtime exceptions.
	WAVM_API void unwindSignalsAsExceptions(const std::function<void()>& thunk);

	// Describes the source of an instruction; may be either WASM or native code.
	struct InstructionSource
	{
		enum class Type
		{
			unknown,
			native,
			wasm,
		};
		Type type;

		// Only one of native or wasm will be initialized based on type.
		// This could be stored in a union, but Platform::InstructionSource's default constructor
		// and destructor are non-trivial, and so would require manual construction/destruction when
		// InstructionSource::type changes.
		Platform::InstructionSource native;
		struct
		{
			Runtime::Function* function;
			Uptr instructionIndex;
		} wasm;

		InstructionSource() : type(Type::unknown) {}
	};

	WAVM_API std::string asString(const InstructionSource& source);

	// Looks up the source of an instruction from either a native or WASM module.
	bool getInstructionSourceByAddress(Uptr ip, InstructionSource& outSource);

	// Describes a call stack.
	WAVM_API std::vector<std::string> describeCallStack(const Platform::CallStack& callStack);

	//
	// Functions
	//

	// Invokes a Function with the given array of arguments, and writes the results to the given
	// results array. The sizes of the arguments and results arrays must match the number of
	// arguments/results of the provided function type. If the provided function type does not match
	// the actual type of the function, then an invokeSignatureMismatch exception is thrown.
	WAVM_API void invokeFunction(Context* context,
								 const Function* function,
								 IR::FunctionType invokeSig = IR::FunctionType(),
								 const IR::UntaggedValue arguments[] = nullptr,
								 IR::UntaggedValue results[] = nullptr);

	// Returns the type of a Function.
	WAVM_API IR::FunctionType getFunctionType(const Function* function);

	//
	// Tables
	//

	// Creates a Table. May return null if the memory allocation fails.
	WAVM_API Table* createTable(Compartment* compartment,
								IR::TableType type,
								Object* element,
								std::string&& debugName,
								ResourceQuotaRefParam resourceQuota = ResourceQuotaRef());

	// Reads an element from the table. Throws an outOfBoundsTableAccess exception if index is
	// out-of-bounds.
	WAVM_API Object* getTableElement(const Table* table, Uptr index);

	// Writes an element to the table, a returns the previous value of the element.
	// Throws an outOfBoundsTableAccess exception if index is out-of-bounds.
	WAVM_API Object* setTableElement(Table* table, Uptr index, Object* newValue);

	// Gets the current size of the table.
	WAVM_API Uptr getTableNumElements(const Table* table);

	// Returns the type of a table.
	WAVM_API IR::TableType getTableType(const Table* table);

	// Grows or shrinks the size of a table by numElements. Returns the previous size of the table.
	WAVM_API GrowResult growTable(Table* table,
								  Uptr numElements,
								  Uptr* outOldNumElems = nullptr,
								  Object* initialElement = nullptr);

	//
	// Memories
	//

	// Creates a Memory. May return null if the memory allocation fails.
	WAVM_API Memory* createMemory(Compartment* compartment,
								  IR::MemoryType type,
								  std::string&& debugName,
								  ResourceQuotaRefParam resourceQuota = ResourceQuotaRef());

	// Gets the base address of the memory's data.
	WAVM_API U8* getMemoryBaseAddress(Memory* memory);

	// Gets the current size of the memory in pages.
	WAVM_API Uptr getMemoryNumPages(const Memory* memory);

	// Returns the type of a memory.
	WAVM_API IR::MemoryType getMemoryType(const Memory* memory);

	// Grows or shrinks the size of a memory by numPages. Returns the previous size of the memory.
	WAVM_API GrowResult growMemory(Memory* memory, Uptr numPages, Uptr* outOldNumPages = nullptr);

	// Unmaps a range of memory pages within the memory's address-space.
	WAVM_API void unmapMemoryPages(Memory* memory, Uptr pageIndex, Uptr numPages);

	// Validates that an offset range is wholly inside a Memory's virtual address range.
	// Note that this returns an address range that may fault on access, though it's guaranteed not
	// to be mapped by anything other than the given Memory.
	WAVM_API U8* getReservedMemoryOffsetRange(Memory* memory, Uptr offset, Uptr numBytes);

	// Validates that an offset range is wholly inside a Memory's committed pages.
	WAVM_API U8* getValidatedMemoryOffsetRange(Memory* memory, Uptr offset, Uptr numBytes);

	// Validates an access to a single element of memory at the given offset, and returns a
	// reference to it.
	template<typename Value> Value& memoryRef(Memory* memory, Uptr offset)
	{
		return *(Value*)getValidatedMemoryOffsetRange(memory, offset, sizeof(Value));
	}

	// Validates an access to multiple elements of memory at the given offset, and returns a pointer
	// to it.
	template<typename Value> Value* memoryArrayPtr(Memory* memory, Uptr offset, Uptr numElements)
	{
		return (Value*)getValidatedMemoryOffsetRange(memory, offset, numElements * sizeof(Value));
	}

	//
	// Globals
	//

	// Creates a Global with the specified type. The initial value is set to the appropriate zero.
	WAVM_API Global* createGlobal(Compartment* compartment,
								  IR::GlobalType type,
								  std::string&& debugName,
								  ResourceQuotaRefParam resourceQuota = ResourceQuotaRef());

	// Initializes a Global with the specified value. May not be called more than once/Global.
	WAVM_API void initializeGlobal(Global* global, IR::Value value);

	// Reads the current value of a global.
	WAVM_API IR::Value getGlobalValue(const Context* context, const Global* global);

	// Writes a new value to a global, and returns the previous value.
	WAVM_API IR::Value setGlobalValue(Context* context, const Global* global, IR::Value newValue);

	// Returns the type of a global.
	WAVM_API IR::GlobalType getGlobalType(const Global* global);

	//
	// Modules
	//

	struct Module;
	typedef std::shared_ptr<Module> ModuleRef;
	typedef std::shared_ptr<const Module> ModuleConstRef;
	typedef const std::shared_ptr<Module>& ModuleRefParam;
	typedef const std::shared_ptr<const Module>& ModuleConstRefParam;

	// Compiles an IR module to object code.
	WAVM_API ModuleRef compileModule(const IR::Module& irModule);

	// Load and compiles a binary module, returning either an error or a module.
	// If true is returned, the load succeeded, and outModule contains the loaded module.
	// If false is returned, the load failed. If outError != nullptr, *outError will contain the
	// error that caused the load to fail.
	WAVM_API bool loadBinaryModule(const U8* wasmBytes,
								   Uptr numWASMBytes,
								   ModuleRef& outModule,
								   const IR::FeatureSpec& featureSpec = IR::FeatureSpec(),
								   WASM::LoadError* outError = nullptr);

	// Loads a previously compiled module from a combination of an IR module and the object code
	// returned by getObjectCode for the previously compiled module.
	WAVM_API ModuleRef loadPrecompiledModule(const IR::Module& irModule,
											 const std::vector<U8>& objectCode);

	// Accesses the IR for a compiled module.
	WAVM_API const IR::Module& getModuleIR(ModuleConstRefParam module);

	// Extracts the compiled object code for a module. This may be used as an input to
	// loadPrecompiledModule to bypass redundant compilations of the module.
	WAVM_API std::vector<U8> getObjectCode(ModuleConstRefParam module);

	//
	// Instances
	//

	typedef std::vector<Object*> ImportBindings;

	// Instantiates a module, bindings its imports to the specified objects. May throw a runtime
	// exception for bad segment offsets.
	WAVM_API Instance* instantiateModule(Compartment* compartment,
										 ModuleConstRefParam module,
										 ImportBindings&& imports,
										 std::string&& debugName,
										 ResourceQuotaRefParam resourceQuota = ResourceQuotaRef());

	// Gets the start function of a Instance.
	WAVM_API Function* getStartFunction(const Instance* instance);

	// Gets the default table/memory for a Instance.
	WAVM_API Memory* getDefaultMemory(const Instance* instance);
	WAVM_API Table* getDefaultTable(const Instance* instance);

	// Gets an object exported by a Instance by name.
	WAVM_API Object* getInstanceExport(const Instance* instance, const std::string& name);

	// Gets an object exported by a Instance by name and type. If the module exports an object
	// with the given name, but the type doesn't match, returns nullptr.
	WAVM_API Object* getTypedInstanceExport(const Instance* instance,
											const std::string& name,
											const IR::ExternType& type);
	WAVM_API Function* getTypedInstanceExport(const Instance* instance,
											  const std::string& name,
											  const IR::FunctionType& type);
	WAVM_API Table* getTypedInstanceExport(const Instance* instance,
										   const std::string& name,
										   const IR::TableType& type);
	WAVM_API Memory* getTypedInstanceExport(const Instance* instance,
											const std::string& name,
											const IR::MemoryType& type);
	WAVM_API Global* getTypedInstanceExport(const Instance* instance,
											const std::string& name,
											const IR::GlobalType& type);
	WAVM_API ExceptionType* getTypedInstanceExport(const Instance* instance,
												   const std::string& name,
												   const IR::ExceptionType& type);

	// Gets an array of the objects exported by an instance. The array indices correspond to the
	// IR::Module::exports array.
	WAVM_API const std::vector<Object*>& getInstanceExports(const Instance* instance);

	//
	// Compartments
	//

	WAVM_API Compartment* createCompartment(std::string&& debugName = "");

	WAVM_API Compartment* cloneCompartment(const Compartment* compartment,
										   std::string&& debugName = "");

	WAVM_API Object* remapToClonedCompartment(const Object* object,
											  const Compartment* newCompartment);
	WAVM_API Function* remapToClonedCompartment(const Function* function,
												const Compartment* newCompartment);
	WAVM_API Table* remapToClonedCompartment(const Table* table, const Compartment* newCompartment);
	WAVM_API Memory* remapToClonedCompartment(const Memory* memory,
											  const Compartment* newCompartment);
	WAVM_API Global* remapToClonedCompartment(const Global* global,
											  const Compartment* newCompartment);
	WAVM_API ExceptionType* remapToClonedCompartment(const ExceptionType* exceptionType,
													 const Compartment* newCompartment);
	WAVM_API Instance* remapToClonedCompartment(const Instance* instance,
												const Compartment* newCompartment);
	WAVM_API Foreign* remapToClonedCompartment(const Foreign* foreign,
											   const Compartment* newCompartment);

	WAVM_API bool isInCompartment(const Object* object, const Compartment* compartment);

	//
	// Contexts
	//

	WAVM_API Context* createContext(Compartment* compartment, std::string&& debugName = "");

	WAVM_API struct ContextRuntimeData* getContextRuntimeData(const Context* context);
	WAVM_API Context* getContextFromRuntimeData(struct ContextRuntimeData* contextRuntimeData);
	WAVM_API Compartment* getCompartmentFromContextRuntimeData(
		struct ContextRuntimeData* contextRuntimeData);

	WAVM_API Compartment* getCompartment(const Context* context);

	// Creates a new context, initializing its mutable global state from the given context.
	WAVM_API Context* cloneContext(const Context* context, Compartment* newCompartment);

	// Sets a callback to check call stack depth. The callback is invoked on function entry.
	// Since stacks are user-provided, user must provide their own callback to check stack depth.
	WAVM_API void setCheckStackDepthCallback(Context* context, void (*callback)());

	//
	// Foreign objects
	//

	WAVM_API Foreign* createForeign(Compartment* compartment,
									void* userData,
									void (*finalizer)(void*),
									std::string&& debugName = "");

	//
	// Object caching
	//

	struct ObjectCacheInterface
	{
		virtual ~ObjectCacheInterface() {}

		virtual std::vector<U8> getCachedObject(const U8* wasmBytes,
												Uptr numWASMBytes,
												std::function<std::vector<U8>()>&& compileThunk)
			= 0;
	};

	WAVM_API void setGlobalObjectCache(std::shared_ptr<ObjectCacheInterface>&& objectCache);
}}
