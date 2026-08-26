#include <vector>
#include "EmitFunctionContext.h"
#include "EmitModuleContext.h"
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Module.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/IR/Constant.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;

//
// Local variables
//

void EmitFunctionContext::local_get(GetOrSetVariableImm<false> imm)
{
	WAVM_ASSERT(imm.variableIndex < localPointers.size());
	push(irBuilder.CreateLoad(
		localPointerElementTypes[imm.variableIndex],
		localPointers[imm.variableIndex]));
}
void EmitFunctionContext::local_set(GetOrSetVariableImm<false> imm)
{
	WAVM_ASSERT(imm.variableIndex < localPointers.size());
	auto value = irBuilder.CreateBitCast(
		pop(), localPointerElementTypes[imm.variableIndex]);
	irBuilder.CreateStore(value, localPointers[imm.variableIndex]);
}
void EmitFunctionContext::local_tee(GetOrSetVariableImm<false> imm)
{
	WAVM_ASSERT(imm.variableIndex < localPointers.size());
	auto value = irBuilder.CreateBitCast(
		getValueFromTop(), localPointerElementTypes[imm.variableIndex]);
	irBuilder.CreateStore(value, localPointers[imm.variableIndex]);
}

//
// Global variables
//

static llvm::Value* getImportedImmutableGlobalValue(EmitFunctionContext& functionContext,
													Uptr importedGlobalIndex,
													ValueType valueType)
{
	// The symbol for an imported global will point to the global's immutable value.
	return functionContext.loadFromUntypedPointer(
		functionContext.moduleContext.globals[importedGlobalIndex],
		asLLVMType(functionContext.llvmContext, valueType),
		getTypeByteWidth(valueType));
}

void EmitFunctionContext::global_get(GetOrSetVariableImm<true> imm)
{
	WAVM_ASSERT(imm.variableIndex < irModule.globals.size());
	GlobalType globalType = irModule.globals.getType(imm.variableIndex);

	llvm::Value* value = nullptr;
	if(globalType.isMutable)
	{
		// If the global is mutable, the symbol will be bound to an offset into the
		// ContextRuntimeData::globalData that its value is stored at.
		llvm::Value* globalDataOffset = irBuilder.CreatePtrToInt(
			moduleContext.globals[imm.variableIndex], moduleContext.iptrType);
		llvm::Value* globalPointer = irBuilder.CreateInBoundsGEP(
			llvmContext.i8Type, irBuilder.CreateLoad(llvmContext.i8PtrType, contextPointerVariable), {globalDataOffset});
		value = loadFromUntypedPointer(globalPointer,
									   asLLVMType(llvmContext, globalType.valueType),
									   getTypeByteWidth(globalType.valueType));
	}
	else if(!irModule.globals.isDef(imm.variableIndex))
	{
		value = getImportedImmutableGlobalValue(*this, imm.variableIndex, globalType.valueType);
	}
	else
	{
		// If the value is an immutable global definition, emit its literal value.
		const IR::GlobalDef& globalDef = irModule.globals.getDef(imm.variableIndex);

		switch(globalDef.initializer.type)
		{
		case InitializerExpression::Type::i32_const:
			value = emitLiteral(llvmContext, globalDef.initializer.i32);
			break;
		case InitializerExpression::Type::i64_const:
			value = emitLiteral(llvmContext, globalDef.initializer.i64);
			break;
		case InitializerExpression::Type::f32_const:
			value = emitLiteral(llvmContext, globalDef.initializer.f32);
			break;
		case InitializerExpression::Type::f64_const:
			value = emitLiteral(llvmContext, globalDef.initializer.f64);
			break;
		case InitializerExpression::Type::v128_const:
			value = emitLiteral(llvmContext, globalDef.initializer.v128);
			break;
		case InitializerExpression::Type::global_get: {
			const Uptr importedGlobalIndex = globalDef.initializer.ref;
			WAVM_ASSERT(!irModule.globals.isDef(importedGlobalIndex));
			WAVM_ASSERT(!irModule.globals.getType(importedGlobalIndex).isMutable);
			value
				= getImportedImmutableGlobalValue(*this, importedGlobalIndex, globalType.valueType);
			break;
		}
		case InitializerExpression::Type::ref_null:
			value = llvm::Constant::getNullValue(llvmContext.externrefType);
			break;
		case InitializerExpression::Type::ref_func: {
			llvm::Value* referencedFunction = moduleContext.functions[globalDef.initializer.ref];
			llvm::Value* codeAddress
				= irBuilder.CreatePtrToInt(referencedFunction, moduleContext.iptrType);
			llvm::Value* functionAddress = irBuilder.CreateSub(
				codeAddress,
				emitLiteralIptr(offsetof(Runtime::Function, code), moduleContext.iptrType));
			llvm::Value* externref
				= irBuilder.CreateIntToPtr(functionAddress, llvmContext.externrefType);
			value = externref;
			break;
		}

		case InitializerExpression::Type::invalid:
		default: WAVM_UNREACHABLE();
		};
	}

	push(value);
}
void EmitFunctionContext::global_set(GetOrSetVariableImm<true> imm)
{
	WAVM_ASSERT(imm.variableIndex < irModule.globals.size());
	GlobalType globalType = irModule.globals.getType(imm.variableIndex);
	WAVM_ASSERT(globalType.isMutable);
	llvm::Type* llvmValueType = asLLVMType(llvmContext, globalType.valueType);

	llvm::Value* value = irBuilder.CreateBitCast(pop(), llvmValueType);

	// If the global is mutable, the symbol will be bound to an offset into the
	// ContextRuntimeData::globalData that its value is stored at.
	llvm::Value* globalDataOffset = irBuilder.CreatePtrToInt(
		moduleContext.globals[imm.variableIndex], moduleContext.iptrType);
	llvm::Value* globalPointer = irBuilder.CreateInBoundsGEP(
		llvmContext.i8Type, irBuilder.CreateLoad(llvmContext.i8PtrType, contextPointerVariable), {globalDataOffset});
	storeToUntypedPointer(value, globalPointer);
}
