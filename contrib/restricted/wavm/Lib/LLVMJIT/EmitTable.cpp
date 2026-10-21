#include "EmitContext.h"
#include "EmitFunctionContext.h"
#include "EmitModuleContext.h"
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/RuntimeABI/RuntimeABI.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;

void EmitFunctionContext::ref_null(ReferenceTypeImm imm)
{
	push(llvm::Constant::getNullValue(llvmContext.externrefType));
}

void EmitFunctionContext::ref_is_null(NoImm)
{
	llvm::Value* reference = pop();
	llvm::Value* null = llvm::Constant::getNullValue(llvmContext.externrefType);
	llvm::Value* isNull = irBuilder.CreateICmpEQ(reference, null);
	push(coerceBoolToI32(isNull));
}

void EmitFunctionContext::ref_func(FunctionRefImm imm)
{
	llvm::Value* referencedFunction = moduleContext.functions[imm.functionIndex];
	llvm::Value* codeAddress = irBuilder.CreatePtrToInt(referencedFunction, moduleContext.iptrType);
	llvm::Value* functionAddress = irBuilder.CreateSub(
		codeAddress, emitLiteralIptr(offsetof(Runtime::Function, code), moduleContext.iptrType));
	llvm::Value* externref = irBuilder.CreateIntToPtr(functionAddress, llvmContext.externrefType);
	push(externref);
}

void EmitFunctionContext::table_get(TableImm imm)
{
	llvm::Value* index = pop();
	llvm::Value* result = emitRuntimeIntrinsic(
		"table.get",
		FunctionType({ValueType::externref},
					 TypeTuple({moduleContext.iptrValueType, moduleContext.iptrValueType}),
					 IR::CallingConvention::intrinsic),
		{zext(index, moduleContext.iptrType),
		 getTableIdFromOffset(moduleContext.tableOffsets[imm.tableIndex], irBuilder)})[0];
	push(result);
}

void EmitFunctionContext::table_set(TableImm imm)
{
	llvm::Value* value = pop();
	llvm::Value* index = pop();
	emitRuntimeIntrinsic("table.set",
						 FunctionType({},
									  TypeTuple({moduleContext.iptrValueType,
												 ValueType::externref,
												 moduleContext.iptrValueType}),
									  IR::CallingConvention::intrinsic),
						 {zext(index, moduleContext.iptrType),
						  value,
						  getTableIdFromOffset(moduleContext.tableOffsets[imm.tableIndex], irBuilder)});
}

void EmitFunctionContext::table_init(ElemSegmentAndTableImm imm)
{
	auto numElements = pop();
	auto sourceOffset = pop();
	auto destOffset = pop();
	emitRuntimeIntrinsic("table.init",
						 FunctionType({},
									  TypeTuple({moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType}),
									  IR::CallingConvention::intrinsic),
						 {zext(destOffset, moduleContext.iptrType),
						  zext(sourceOffset, moduleContext.iptrType),
						  zext(numElements, moduleContext.iptrType),
						  moduleContext.instanceId,
						  getTableIdFromOffset(moduleContext.tableOffsets[imm.tableIndex], irBuilder),
						  emitLiteralIptr(imm.elemSegmentIndex, moduleContext.iptrType)});
}

void EmitFunctionContext::elem_drop(ElemSegmentImm imm)
{
	emitRuntimeIntrinsic(
		"elem.drop",
		FunctionType({},
					 TypeTuple({moduleContext.iptrValueType, moduleContext.iptrValueType}),
					 IR::CallingConvention::intrinsic),
		{moduleContext.instanceId, emitLiteral(llvmContext, imm.elemSegmentIndex)});
}

void EmitFunctionContext::table_copy(TableCopyImm imm)
{
	auto numElements = pop();
	auto sourceOffset = pop();
	auto destOffset = pop();

	emitRuntimeIntrinsic("table.copy",
						 FunctionType({},
									  TypeTuple({moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType}),
									  IR::CallingConvention::intrinsic),
						 {zext(destOffset, moduleContext.iptrType),
						  zext(sourceOffset, moduleContext.iptrType),
						  zext(numElements, moduleContext.iptrType),
						  getTableIdFromOffset(moduleContext.tableOffsets[imm.destTableIndex], irBuilder),
						  getTableIdFromOffset(moduleContext.tableOffsets[imm.sourceTableIndex], irBuilder)});
}

void EmitFunctionContext::table_fill(TableImm imm)
{
	auto numElements = pop();
	auto value = pop();
	auto destOffset = pop();

	emitRuntimeIntrinsic("table.fill",
						 FunctionType({},
									  TypeTuple({moduleContext.iptrValueType,
												 ValueType::externref,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType}),
									  IR::CallingConvention::intrinsic),
						 {zext(destOffset, moduleContext.iptrType),
						  value,
						  zext(numElements, moduleContext.iptrType),
						  getTableIdFromOffset(moduleContext.tableOffsets[imm.tableIndex], irBuilder)});
}

void EmitFunctionContext::table_grow(TableImm imm)
{
	llvm::Value* deltaNumElements = pop();
	llvm::Value* value = pop();
	ValueVector previousNumElements = emitRuntimeIntrinsic(
		"table.grow",
		FunctionType(
			TypeTuple(moduleContext.iptrValueType),
			TypeTuple(
				{ValueType::externref, moduleContext.iptrValueType, moduleContext.iptrValueType}),
			IR::CallingConvention::intrinsic),
		{value,
		 zext(deltaNumElements, moduleContext.iptrType),
		 getTableIdFromOffset(moduleContext.tableOffsets[imm.tableIndex], irBuilder)});
	WAVM_ASSERT(previousNumElements.size() == 1);
	const TableType& tableType = moduleContext.irModule.tables.getType(imm.tableIndex);
	push(coerceIptrToIndex(tableType.indexType, previousNumElements[0]));
}
void EmitFunctionContext::table_size(TableImm imm)
{
	ValueVector currentNumElements
		= emitRuntimeIntrinsic("table.size",
							   FunctionType(TypeTuple(moduleContext.iptrValueType),
											TypeTuple(moduleContext.iptrValueType),
											IR::CallingConvention::intrinsic),
							   {getTableIdFromOffset(moduleContext.tableOffsets[imm.tableIndex], irBuilder)});
	WAVM_ASSERT(currentNumElements.size() == 1);
	const TableType& tableType = moduleContext.irModule.tables.getType(imm.tableIndex);
	push(coerceIptrToIndex(tableType.indexType, currentNumElements[0]));
}
