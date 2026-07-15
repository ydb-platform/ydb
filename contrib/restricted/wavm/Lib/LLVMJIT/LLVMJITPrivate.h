#pragma once

#include <cctype>
#include <string>
#include <vector>
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/LLVMJIT/LLVMJIT.h"
#include "WAVM/Platform/Mutex.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

#if defined(_MSC_VER) && defined(__clang__)
#define PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS                                                     \
	_Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wswitch-enum\"")
#define POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS _Pragma("GCC diagnostic pop")
#elif defined(_MSC_VER)
// Disable all VC warnings in the LLVM headers
#define PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS                                                     \
	__pragma(warning(push, 0));                                                                    \
	__pragma(warning(disable : 4702));                                                             \
	__pragma(warning(disable : 4244));
#define POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS __pragma(warning(pop));
#else
#define PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#define POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#endif

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/SmallVector.h>
#include <llvm/Config/llvm-config.h>
#include <llvm/DebugInfo/DWARF/DWARFContext.h>
#include <llvm/ExecutionEngine/JITSymbol.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/Type.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/DataTypes.h>
#include <llvm/Target/TargetMachine.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

#define LAZY_PARSE_DWARF_LINE_INFO (LLVM_VERSION_MAJOR >= 9)

namespace llvm {
	class LoadedObjectInfo;

	namespace object {
		class SectionRef;
	}
}

#if LLVM_VERSION_MAJOR >= 11
using FixedVectorType = llvm::FixedVectorType;
#else
using FixedVectorType = llvm::VectorType;
#endif

namespace WAVM { namespace LLVMJIT {
	typedef llvm::SmallVector<llvm::Value*, 1> ValueVector;
	typedef llvm::SmallVector<llvm::PHINode*, 1> PHIVector;

	struct LLVMContext : llvm::LLVMContext
	{
		llvm::Type* i8Type;
		llvm::Type* i16Type;
		llvm::Type* i32Type;
		llvm::Type* i64Type;
		llvm::Type* f32Type;
		llvm::Type* f64Type;

		llvm::PointerType* i8PtrType;

		FixedVectorType* i8x8Type;
		FixedVectorType* i16x4Type;
		FixedVectorType* i32x2Type;
		FixedVectorType* i64x1Type;
		FixedVectorType* f32x2Type;
		FixedVectorType* f64x1Type;

		FixedVectorType* i8x16Type;
		FixedVectorType* i16x8Type;
		FixedVectorType* i32x4Type;
		FixedVectorType* i64x2Type;
		FixedVectorType* f32x4Type;
		FixedVectorType* f64x2Type;

		FixedVectorType* i8x32Type;
		FixedVectorType* i16x16Type;
		FixedVectorType* i32x8Type;
		FixedVectorType* i64x4Type;

		FixedVectorType* i8x48Type;
		FixedVectorType* i16x24Type;
		FixedVectorType* i32x12Type;
		FixedVectorType* i64x6Type;

		FixedVectorType* i8x64Type;
		FixedVectorType* i16x32Type;
		FixedVectorType* i32x16Type;
		FixedVectorType* i64x8Type;

		llvm::Type* externrefType;

		// Zero constants of each type.
		llvm::Constant* typedZeroConstants[IR::numValueTypes];

		// Maps a type ID to the corresponding LLVM type.
		llvm::Type* valueTypes[IR::numValueTypes];

		LLVMContext();
	};

	// Overloaded functions that compile a literal value to a LLVM constant of the right type.
	inline llvm::ConstantInt* emitLiteral(llvm::LLVMContext& llvmContext, U32 value)
	{
		return llvm::ConstantInt::get(llvmContext, llvm::APInt(32, (U64)value, false));
	}
	inline llvm::ConstantInt* emitLiteral(llvm::LLVMContext& llvmContext, I32 value)
	{
		return llvm::ConstantInt::get(llvmContext, llvm::APInt(32, (I64)value, false));
	}
	inline llvm::ConstantInt* emitLiteral(llvm::LLVMContext& llvmContext, U64 value)
	{
		return llvm::ConstantInt::get(llvmContext, llvm::APInt(64, value, false));
	}
	inline llvm::ConstantInt* emitLiteral(llvm::LLVMContext& llvmContext, I64 value)
	{
		return llvm::ConstantInt::get(llvmContext, llvm::APInt(64, value, false));
	}
	inline llvm::Constant* emitLiteral(llvm::LLVMContext& llvmContext, F32 value)
	{
		return llvm::ConstantFP::get(llvmContext, llvm::APFloat(value));
	}
	inline llvm::Constant* emitLiteral(llvm::LLVMContext& llvmContext, F64 value)
	{
		return llvm::ConstantFP::get(llvmContext, llvm::APFloat(value));
	}
	inline llvm::Constant* emitLiteral(llvm::LLVMContext& llvmContext, bool value)
	{
		return llvm::ConstantInt::get(llvmContext, llvm::APInt(1, value ? 1 : 0, false));
	}
	inline llvm::Constant* emitLiteral(llvm::LLVMContext& llvmContext, V128 value)
	{
		return llvm::ConstantVector::get(
			{llvm::ConstantInt::get(llvmContext, llvm::APInt(64, value.u64x2[0], false)),
			 llvm::ConstantInt::get(llvmContext, llvm::APInt(64, value.u64x2[1], false))});
	}
	inline llvm::Constant* emitLiteralIptr(U64 iptrValue, llvm::Type* iptrType)
	{
		return llvm::ConstantInt::get(iptrType, iptrValue);
	}

	// Converts a WebAssembly type to a LLVM type.
	inline llvm::Type* asLLVMType(LLVMContext& llvmContext, IR::ValueType type)
	{
		WAVM_ASSERT(type < (IR::ValueType)IR::numValueTypes);
		return llvmContext.valueTypes[Uptr(type)];
	}

	inline llvm::Type* asLLVMType(LLVMContext& llvmContext, IR::TypeTuple typeTuple)
	{
		llvm::Type** llvmTypes = (llvm::Type**)alloca(sizeof(llvm::Type*) * typeTuple.size());
		for(Uptr typeIndex = 0; typeIndex < typeTuple.size(); ++typeIndex)
		{ llvmTypes[typeIndex] = asLLVMType(llvmContext, typeTuple[typeIndex]); }
		return llvm::StructType::get(llvmContext,
									 llvm::ArrayRef<llvm::Type*>(llvmTypes, typeTuple.size()));
	}

	inline bool areResultsReturnedDirectly(IR::TypeTuple results)
	{
		// On X64, the calling conventions can return up to 3 i64s and 4 float/vectors.
		// For simplicity, just allow up to 3 values to be returned, including the implicitly
		// returned context pointer.
		static constexpr Uptr maxDirectlyReturnedValues = 3;
		return results.size() + 1 <= maxDirectlyReturnedValues;
	}

	inline llvm::StructType* getLLVMReturnStructType(LLVMContext& llvmContext,
													 IR::TypeTuple results)
	{
		if(areResultsReturnedDirectly(results))
		{
			// A limited number of results can be packed into a struct and returned directly.
			return llvm::StructType::get(llvmContext.i8PtrType, asLLVMType(llvmContext, results));
		}
		else
		{
			// If there are too many results to be returned directly, they will be returned in the
			// context arg/return memory block.
			return llvm::StructType::get(llvmContext.i8PtrType);
		}
	}

	inline llvm::Constant* getZeroedLLVMReturnStruct(LLVMContext& llvmContext,
													 IR::TypeTuple resultType)
	{
		return llvm::Constant::getNullValue(getLLVMReturnStructType(llvmContext, resultType));
	}

	// Converts a WebAssembly function type to a LLVM type.
	inline llvm::FunctionType* asLLVMType(LLVMContext& llvmContext, IR::FunctionType functionType)
	{
		const IR::CallingConvention callingConvention = functionType.callingConvention();

		Uptr numParameters;
		llvm::Type** llvmArgTypes;
		if(callingConvention == IR::CallingConvention::cAPICallback)
		{
			numParameters = 2;
			llvmArgTypes = (llvm::Type**)alloca(sizeof(llvm::Type*) * numParameters);
			llvmArgTypes[0] = llvmContext.i8PtrType;
			llvmArgTypes[1] = llvmContext.i8PtrType;
		}
		else
		{
			const Uptr numImplicitParameters
				= callingConvention == IR::CallingConvention::c ? 0 : 1;
			numParameters = numImplicitParameters + functionType.params().size();
			llvmArgTypes = (llvm::Type**)alloca(sizeof(llvm::Type*) * numParameters);
			if(callingConvention != IR::CallingConvention::c)
			{ llvmArgTypes[0] = llvmContext.i8PtrType; }

			for(Uptr argIndex = 0; argIndex < functionType.params().size(); ++argIndex)
			{
				llvmArgTypes[argIndex + numImplicitParameters]
					= asLLVMType(llvmContext, functionType.params()[argIndex]);
			}
		}

		llvm::Type* llvmReturnType;
		switch(callingConvention)
		{
		case IR::CallingConvention::wasm:
			llvmReturnType = getLLVMReturnStructType(llvmContext, functionType.results());
			break;

		case IR::CallingConvention::cAPICallback:
		case IR::CallingConvention::intrinsicWithContextSwitch:
			llvmReturnType = llvmContext.i8PtrType;
			break;

		case IR::CallingConvention::intrinsic:
		case IR::CallingConvention::c:
			switch(functionType.results().size())
			{
			case 0: llvmReturnType = llvm::Type::getVoidTy(llvmContext); break;
			case 1: llvmReturnType = asLLVMType(llvmContext, functionType.results()[0]); break;
			default: Errors::fatal("intrinsics/C functions returning >1 result isn't supported");
			}
			break;

		default: WAVM_UNREACHABLE();
		};
		return llvm::FunctionType::get(
			llvmReturnType, llvm::ArrayRef<llvm::Type*>(llvmArgTypes, numParameters), false);
	}

	inline llvm::CallingConv::ID asLLVMCallingConv(IR::CallingConvention callingConvention)
	{
		switch(callingConvention)
		{
		case IR::CallingConvention::wasm: return llvm::CallingConv::Fast;

		case IR::CallingConvention::intrinsic:
		case IR::CallingConvention::intrinsicWithContextSwitch:
		case IR::CallingConvention::cAPICallback:
		case IR::CallingConvention::c: return llvm::CallingConv::C;

		default: WAVM_UNREACHABLE();
		}
	}

	inline llvm::Value* getMemoryIdFromOffset(llvm::Constant* memoryOffset, llvm::IRBuilder<>& irBuilder)
	{
        return irBuilder.CreateExactUDiv(
			llvm::ConstantExpr::getSub(
				memoryOffset,
				emitLiteralIptr(offsetof(Runtime::CompartmentRuntimeData, memories),
								memoryOffset->getType())),
			emitLiteralIptr(sizeof(Runtime::MemoryRuntimeData), memoryOffset->getType()));
	}

	inline llvm::Value* getTableIdFromOffset(llvm::Constant* tableOffset, llvm::IRBuilder<>& irBuilder)
	{
        return irBuilder.CreateExactUDiv(
			llvm::ConstantExpr::getSub(
				tableOffset,
				emitLiteralIptr(offsetof(Runtime::CompartmentRuntimeData, tables),
								tableOffset->getType())),
			emitLiteralIptr(sizeof(Runtime::TableRuntimeData), tableOffset->getType()));
	}

	inline llvm::Type* getIptrType(LLVMContext& llvmContext, U32 numPointerBytes)
	{
		switch(numPointerBytes)
		{
		case 4: return llvmContext.i32Type;
		case 8: return llvmContext.i64Type;
		default: Errors::fatalf("Unexpected pointer size: %u bytes", numPointerBytes);
		};
	}

	inline void setRuntimeFunctionPrefix(LLVMContext& llvmContext,
										 llvm::Type* iptrType,
										 llvm::Function* function,
										 llvm::Constant* mutableData,
										 llvm::Constant* instanceId,
										 llvm::Constant* typeId)
	{
		function->setPrefixData(
			llvm::ConstantArray::get(llvm::ArrayType::get(iptrType, 4),
									 {emitLiteralIptr(U64(Runtime::ObjectKind::function), iptrType),
									  mutableData,
									  instanceId,
									  typeId}));
	}

	inline void setFunctionAttributes(llvm::TargetMachine* targetMachine, llvm::Function* function)
	{
		// For now, no attributes need to be set on Win32.
		if(targetMachine->getTargetTriple().getOS() != llvm::Triple::Win32)
		{
			auto attrs = function->getAttributes();

			// LLVM 9+ has a more general purpose frame-pointer=(all|non-leaf|none) attribute that
			// WAVM should use once we can depend on it.
			attrs = attrs.addAttributeAtIndex(function->getContext(),
									   llvm::AttributeList::FunctionIndex,
									   "no-frame-pointer-elim",
									   "true");

			// Set the probe-stack attribute: this will cause functions that allocate more than a
			// page of stack space to call the wavm_probe_stack function defined in POSIX.S
			attrs = attrs.addAttributeAtIndex(function->getContext(),
									   llvm::AttributeList::FunctionIndex,
									   "probe-stack",
									   "wavm_probe_stack");

			function->setAttributes(attrs);
		}
	}

	// Functions that map between the symbols used for externally visible functions and the function
	inline std::string getExternalName(const char* baseName, Uptr index)
	{
		return std::string(baseName) + std::to_string(index);
	}

	// Reproduces how LLVM symbols are mangled to make object symbols for the current platform.
	inline std::string mangleSymbol(std::string&& symbol)
	{
#if((defined(_WIN32) && !defined(_WIN64))) || defined(__APPLE__)
		return std::string("_") + std::move(symbol);
#else
		return std::move(symbol);
#endif
	}

	// The inverse of mangleSymbol
	inline std::string demangleSymbol(std::string&& symbol)
	{
#if((defined(_WIN32) && !defined(_WIN64))) || defined(__APPLE__)
		WAVM_ASSERT(symbol[0] == '_');
		return std::move(symbol).substr(1);
#else
		return std::move(symbol);
#endif
	}

	// Emits LLVM IR for a module.
	void emitModule(const IR::Module& irModule,
					LLVMContext& llvmContext,
					llvm::Module& outLLVMModule,
					llvm::TargetMachine* targetMachine);

	// Used to override LLVM's default behavior of looking up unresolved symbols in DLL exports.
	llvm::JITEvaluatedSymbol resolveJITImport(llvm::StringRef name);

	struct ModuleMemoryManager;
	struct GlobalModuleState;

	// Encapsulates a loaded module.
	struct Module
	{
		HashMap<std::string, Runtime::Function*> nameToFunctionMap;
		std::map<Uptr, Runtime::Function*> addressToFunctionMap;
		std::string debugName;

#if LAZY_PARSE_DWARF_LINE_INFO
		Platform::Mutex dwarfContextMutex;
		std::unique_ptr<llvm::DWARFContext> dwarfContext;
#endif

		Module(const std::vector<U8>& inObjectBytes,
			   HashMap<std::string, Uptr>* importedSymbolMap,
			   bool shouldLogMetrics,
			   std::string&& inDebugName,
			   const std::unordered_map<std::string, std::string>& weakFunctionsToPatch = {});

		~Module();

	private:
		ModuleMemoryManager* memoryManager;

		// Module holds a shared pointer to GlobalModuleState to ensure that on exit it is not
		// destructed until after all Modules have been destructed.
		std::shared_ptr<GlobalModuleState> globalModuleState;

		// Have to keep copies of these around because until LLVM 8, GDB registration listener uses
		// their pointers as keys for deregistration.
#if LLVM_VERSION_MAJOR < 8
		std::vector<U8> objectBytes;
		std::unique_ptr<llvm::object::ObjectFile> object;
#endif
	};

	extern std::unique_ptr<llvm::TargetMachine> getTargetMachine(const TargetSpec& targetSpec);
	extern TargetValidationResult validateTargetMachine(
		const std::unique_ptr<llvm::TargetMachine>& targetMachine,
		const IR::FeatureSpec& featureSpec);

	extern std::vector<U8> compileLLVMModule(LLVMContext& llvmContext,
											 llvm::Module&& llvmModule,
											 bool shouldLogMetrics,
											 llvm::TargetMachine* targetMachine);

	extern void processSEHTables(U8* imageBase,
								 const llvm::LoadedObjectInfo& loadedObject,
								 const llvm::object::SectionRef& pdataSection,
								 const U8* pdataCopy,
								 Uptr pdataNumBytes,
								 const llvm::object::SectionRef& xdataSection,
								 const U8* xdataCopy,
								 Uptr sehTrampolineAddress);
}}
