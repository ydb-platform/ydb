#pragma once

//
// Data structures that are used to share data between WAVM C++ code and the compiled WASM code.
//

#include <atomic>
#include <cassert>
#include <map>
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/Diagnostics.h"

namespace WAVM { namespace LLVMJIT {
	struct Module;
}}

namespace WAVM { namespace Runtime {
	// Forward declarations
	struct Compartment;
	struct Context;
	struct ExceptionType;
	struct Object;
	struct Table;
	struct Memory;

	// Runtime object types. This must be a superset of IR::ExternKind, with IR::ExternKind
	// values having the same representation in Runtime::ObjectKind.
	enum class ObjectKind : U8
	{
		invalid = (U8)IR::ExternKind::invalid,

		// Standard object kinds that may be imported/exported from WebAssembly modules.
		function = (U8)IR::ExternKind::function,
		table = (U8)IR::ExternKind::table,
		memory = (U8)IR::ExternKind::memory,
		global = (U8)IR::ExternKind::global,
		exceptionType = (U8)IR::ExternKind::exceptionType,

		// Runtime-specific object kinds that are only used by transient runtime objects.
		instance,
		context,
		compartment,
		foreign,

		dynamicLinkingWeakFunctionImport,
	};
	static_assert(Uptr(IR::ExternKind::function) == Uptr(ObjectKind::function),
				  "IR::ExternKind::function != ObjectKind::function");
	static_assert(Uptr(IR::ExternKind::table) == Uptr(ObjectKind::table),
				  "IR::ExternKind::table != ObjectKind::table");
	static_assert(Uptr(IR::ExternKind::memory) == Uptr(ObjectKind::memory),
				  "IR::ExternKind::memory != ObjectKind::memory");
	static_assert(Uptr(IR::ExternKind::global) == Uptr(ObjectKind::global),
				  "IR::ExternKind::global != ObjectKind::global");
	static_assert(Uptr(IR::ExternKind::exceptionType) == Uptr(ObjectKind::exceptionType),
				  "IR::ExternKind::exceptionType != ObjectKind::exceptionType");

	static constexpr Uptr contextNumBytes = 16384;
	static constexpr Uptr maxThunkArgAndReturnBytes = 256;
	static constexpr Uptr maxMutableGlobals
		= (contextNumBytes - maxThunkArgAndReturnBytes - sizeof(Context*))
		  / sizeof(IR::UntaggedValue);
	static constexpr Uptr contextRuntimeDataAlignment = 16384;

	static_assert(sizeof(IR::UntaggedValue) * IR::maxReturnValues <= maxThunkArgAndReturnBytes,
				  "maxThunkArgAndReturnBytes must be large enough to hold IR::maxReturnValues * "
				  "sizeof(UntaggedValue)");

	struct ContextRuntimeData
	{
		U8 thunkArgAndReturnData[maxThunkArgAndReturnBytes];
		Context* context;
		IR::UntaggedValue mutableGlobals[maxMutableGlobals];
	};

	static_assert(sizeof(ContextRuntimeData) == contextNumBytes,
				  "ContextRuntimeData isn't the expected size");

	struct MemoryRuntimeData
	{
		void* base;
		std::atomic<Uptr> numPages;
		Uptr endAddress;
	};

	static_assert(sizeof(MemoryRuntimeData) == sizeof(Uptr) * 3,
				  "MemoryRuntimeData isn't the expected size");

	struct TableRuntimeData
	{
		void* base;
		Uptr endIndex;
	};

	static_assert(sizeof(TableRuntimeData) == sizeof(Uptr) * 2,
				  "TableRuntimeData isn't the expected size");

	static constexpr Uptr maxMemories = 255;
	static constexpr Uptr compartmentReservedBytes = Uptr(2) * 1024 * 1024 * 1024;
	static constexpr Uptr compartmentNonContextBytes = Uptr(2) * 1024 * 1024;
	static constexpr Uptr maxTables = (compartmentNonContextBytes - sizeof(Compartment*)
									   - maxMemories * sizeof(MemoryRuntimeData))
									  / sizeof(TableRuntimeData);
	static constexpr Uptr compartmentRuntimeDataAlignmentLog2 = 31;

	struct CompartmentRuntimeData
	{
		Compartment* compartment;
		MemoryRuntimeData memories[maxMemories];
		TableRuntimeData tables[maxTables];
		ContextRuntimeData contexts[1]; // Actually [maxContexts], but at least MSVC doesn't allow
										// declaring arrays that large.
	};

	static constexpr Uptr maxContexts
		= (compartmentReservedBytes - offsetof(CompartmentRuntimeData, contexts))
		  / sizeof(ContextRuntimeData);

	static_assert(offsetof(CompartmentRuntimeData, contexts) % 4096 == 0,
				  "CompartmentRuntimeData::contexts isn't page-aligned");
	static_assert(U64(offsetof(CompartmentRuntimeData, contexts))
						  + U64(maxContexts) * sizeof(ContextRuntimeData)
					  == compartmentReservedBytes,
				  "CompartmentRuntimeData isn't the expected size");

	struct Exception
	{
		Uptr typeId;
		ExceptionType* type;
		U8 isUserException;
		Platform::CallStack callStack;
		void* userData;
		void (*finalizeUserData)(void*);
		IR::UntaggedValue arguments[1];

		Exception(Uptr inTypeId,
				  ExceptionType* inType,
				  bool inIsUserException,
				  Platform::CallStack&& inCallStack)
		: typeId(inTypeId)
		, type(inType)
		, isUserException(inIsUserException ? 1 : 0)
		, callStack(std::move(inCallStack))
		, userData(nullptr)
		, finalizeUserData(nullptr)
		{
		}

		~Exception();

		static Uptr calcNumBytes(Uptr numArguments)
		{
			if(numArguments == 0) { numArguments = 1; }
			return offsetof(Exception, arguments) + numArguments * sizeof(IR::UntaggedValue);
		}
	};

	struct Object
	{
		const ObjectKind kind;
	};

	struct WeakFunction
		: public Object
	{
		const std::string name;
		const Uptr index; // The index in the array with such layout: [imports][functions].

		WeakFunction(std::string name, Uptr index)
			: Object(ObjectKind::dynamicLinkingWeakFunctionImport)
			, name(std::move(name))
			, index(index)
		{ }
	};

	typedef Runtime::ContextRuntimeData* (*InvokeThunkPointer)(const Runtime::Function*,
															   Runtime::ContextRuntimeData*,
															   const IR::UntaggedValue* arguments,
															   IR::UntaggedValue* results);

	// Metadata about a function, used to hold data that can't be emitted directly in an object
	// file, or must be mutable.
	struct FunctionMutableData
	{
		LLVMJIT::Module* jitModule = nullptr;
		Runtime::Function* function = nullptr;
		Uptr numCodeBytes = 0;
		std::atomic<Uptr> numRootReferences{0};
		std::map<U32, U32> offsetToOpIndexMap;
		std::string debugName;
		std::atomic<InvokeThunkPointer> invokeThunk{nullptr};
		void* userData{nullptr};
		void (*finalizeUserData)(void*);

		FunctionMutableData(std::string&& inDebugName)
		: debugName(inDebugName), userData(nullptr), finalizeUserData(nullptr)
		{
		}

		~FunctionMutableData()
		{
			assert(numRootReferences.load(std::memory_order_acquire) == 0);
			if(finalizeUserData) { (*finalizeUserData)(userData); }
		}
	};

	struct Function
	{
		Object object;
		FunctionMutableData* mutableData;
		const Uptr instanceId;
		const IR::FunctionType::Encoding encodedType;
		const U8 code[1];

		Function(FunctionMutableData* inMutableData,
				 Uptr inInstanceId,
				 IR::FunctionType::Encoding inEncodedType)
		: object{ObjectKind::function}
		, mutableData(inMutableData)
		, instanceId(inInstanceId)
		, encodedType(inEncodedType)
		, code{0xcc} // int3
		{
		}
	};

	static_assert(offsetof(Runtime::Function, object) == sizeof(Uptr) * 0,
				  "Function prefix must match Runtime::Function layout");
	static_assert(offsetof(Runtime::Function, mutableData) == sizeof(Uptr) * 1,
				  "Function prefix must match Runtime::Function layout");
	static_assert(offsetof(Runtime::Function, instanceId) == sizeof(Uptr) * 2,
				  "Function prefix must match Runtime::Function layout");
	static_assert(offsetof(Runtime::Function, encodedType) == sizeof(Uptr) * 3,
				  "Function prefix must match Runtime::Function layout");
	static_assert(offsetof(Runtime::Function, code) == sizeof(Uptr) * 4,
				  "Function prefix must match Runtime::Function layout");

	inline CompartmentRuntimeData* getCompartmentRuntimeData(ContextRuntimeData* contextRuntimeData)
	{
		return reinterpret_cast<CompartmentRuntimeData*>(
			reinterpret_cast<Uptr>(contextRuntimeData)
			& -(Iptr(1) << compartmentRuntimeDataAlignmentLog2));
	}
}}
