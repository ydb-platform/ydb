#pragma once

#include <initializer_list>
#include <string>
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Types.h"
#include "WAVM/IR/Value.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Runtime/Runtime.h"

namespace WAVM { namespace Runtime {
	struct ContextRuntimeData;
}}

namespace WAVM { namespace Intrinsics {
	struct ModuleImpl;
	struct Function;

	struct Module
	{
		ModuleImpl* impl = nullptr;

		WAVM_API ~Module();
	};

	WAVM_API Runtime::Instance* instantiateModule(
		Runtime::Compartment* compartment,
		const std::initializer_list<const Intrinsics::Module*>& moduleRefs,
		std::string&& debugName);

	// An intrinsic function.
	struct Function
	{
		WAVM_API Function(Intrinsics::Module* moduleRef,
						  const char* inName,
						  void* inNativeFunction,
						  IR::FunctionType type);

		const char* getName() const { return name; }
		IR::FunctionType getType() const { return type; }
		void* getNativeFunction() const { return nativeFunction; }

	private:
		const char* name;
		IR::FunctionType type;
		void* nativeFunction;
	};

	// The base class of Intrinsic globals.
	struct Global
	{
		WAVM_API Global(Intrinsics::Module* moduleRef,
						const char* inName,
						IR::ValueType inType,
						IR::Value inValue);

		const char* getName() const { return name; }
		IR::ValueType getType() const { return type; }
		IR::Value getValue() const { return value; }

	private:
		const char* name;
		IR::ValueType type;
		IR::Value value;
	};

	// An immutable global that provides typed initialization and reading of the global's value.
	template<typename Value> struct GenericGlobal : Global
	{
		GenericGlobal(Intrinsics::Module* moduleRef, const char* inName, Value inValue)
		: Global(moduleRef,
				 inName,
				 IR::inferValueType<Value>(),
				 IR::Value(IR::inferValueType<Value>(), inValue))
		{
		}
	};

	// Intrinsic memories and tables
	struct Memory
	{
		WAVM_API
		Memory(Intrinsics::Module* moduleRef, const char* inName, const IR::MemoryType& inType);

		const char* getName() const { return name; }
		IR::MemoryType getType() const { return type; }

	private:
		const char* name;
		const IR::MemoryType type;
	};

	struct Table
	{
		WAVM_API
		Table(Intrinsics::Module* moduleRef, const char* inName, const IR::TableType& inType);

		const char* getName() const { return name; }
		IR::TableType getType() const { return type; }

	private:
		const char* name;
		const IR::TableType type;
	};

	// Create a new return type for intrinsic functions that return their result in the
	// ContextRuntimeData buffer.
	template<typename Result> struct ResultInContextRuntimeData;

	template<typename Result>
	ResultInContextRuntimeData<Result>* resultInContextRuntimeData(
		Runtime::ContextRuntimeData* contextRuntimeData,
		Result result)
	{
		*reinterpret_cast<Result*>(contextRuntimeData) = result;
		return reinterpret_cast<ResultInContextRuntimeData<Result>*>(contextRuntimeData);
	}

	template<typename R, typename... Args>
	IR::FunctionType inferIntrinsicFunctionType(R (*)(Runtime::ContextRuntimeData*, Args...))
	{
		return IR::FunctionType(IR::inferResultType<R>(),
								IR::TypeTuple({IR::inferValueType<Args>()...}),
								WAVM::IR::CallingConvention::intrinsic);
	}
	template<typename R, typename... Args>
	IR::FunctionType inferIntrinsicWithContextSwitchFunctionType(
		ResultInContextRuntimeData<R>* (*)(Runtime::ContextRuntimeData*, Args...))
	{
		return IR::FunctionType(IR::inferResultType<R>(),
								IR::TypeTuple({IR::inferValueType<Args>()...}),
								WAVM::IR::CallingConvention::intrinsicWithContextSwitch);
	}
}}

#define WAVM_DEFINE_INTRINSIC_MODULE(name)                                                         \
	WAVM::Intrinsics::Module* getIntrinsicModule_##name()                                          \
	{                                                                                              \
		static WAVM::Intrinsics::Module module;                                                    \
		return &module;                                                                            \
	}

#define WAVM_DECLARE_INTRINSIC_MODULE(name)                                                        \
	extern WAVM::Intrinsics::Module* getIntrinsicModule_##name();

#define WAVM_INTRINSIC_MODULE_REF(name) getIntrinsicModule_##name()

#define WAVM_DEFINE_INTRINSIC_FUNCTION(module, nameString, Result, cName, ...)                     \
	static Result cName(WAVM::Runtime::ContextRuntimeData* contextRuntimeData, ##__VA_ARGS__);     \
	static WAVM::Intrinsics::Function cName##Intrinsic(                                            \
		getIntrinsicModule_##module(),                                                             \
		nameString,                                                                                \
		(void*)&cName,                                                                             \
		WAVM::Intrinsics::inferIntrinsicFunctionType(&cName));                                     \
	static Result cName(WAVM::Runtime::ContextRuntimeData* contextRuntimeData, ##__VA_ARGS__)

#define WAVM_DEFINE_INTRINSIC_FUNCTION_WITH_CONTEXT_SWITCH(module, nameString, Result, cName, ...) \
	static WAVM::Intrinsics::ResultInContextRuntimeData<Result>* cName(                            \
		WAVM::Runtime::ContextRuntimeData* contextRuntimeData, ##__VA_ARGS__);                     \
	static WAVM::Intrinsics::Function cName##Intrinsic(                                            \
		getIntrinsicModule_##module(),                                                             \
		nameString,                                                                                \
		(void*)&cName,                                                                             \
		WAVM::Intrinsics::inferIntrinsicWithContextSwitchFunctionType(&cName));                    \
	static WAVM::Intrinsics::ResultInContextRuntimeData<Result>* cName(                            \
		WAVM::Runtime::ContextRuntimeData* contextRuntimeData, ##__VA_ARGS__)

#define WAVM_DEFINE_UNIMPLEMENTED_INTRINSIC_FUNCTION(module, nameString, Result, cName, ...)       \
	WAVM_DEFINE_INTRINSIC_FUNCTION(module, nameString, Result, cName, __VA_ARGS__)                 \
	{                                                                                              \
		WAVM::Runtime::throwException(                                                             \
			WAVM::Runtime::ExceptionTypes::calledUnimplementedIntrinsic);                          \
	}

// Macros for defining intrinsic globals, memories, and tables.
#define WAVM_DEFINE_INTRINSIC_GLOBAL(module, name, Value, cName, initializer)                      \
	static WAVM::Intrinsics::GenericGlobal<Value> cName(                                           \
		getIntrinsicModule_##module(), name, initializer);

#define WAVM_DEFINE_INTRINSIC_MEMORY(module, cName, name, type)                                    \
	static WAVM::Intrinsics::Memory cName(getIntrinsicModule_##module(), #name, type);
#define WAVM_DEFINE_INTRINSIC_TABLE(module, cName, name, type)                                     \
	static WAVM::Intrinsics::Table cName(getIntrinsicModule_##module(), #name, type);
