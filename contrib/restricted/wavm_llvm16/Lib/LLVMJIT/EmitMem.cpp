#include "EmitContext.h"
#include "EmitFunctionContext.h"
#include "EmitModuleContext.h"
#include "LLVMJITPrivate.h"
#include "WAVM/IR/IR.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Platform/VectorOverMMap.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/InlineAsm.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>
#include <llvm/Support/AtomicOrdering.h>

#if LLVM_VERSION_MAJOR >= 10
#include <llvm/IR/IntrinsicsAArch64.h>
#endif
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;

enum class BoundsCheckOp
{
	clampToGuardRegion,
	trapOnOutOfBounds
};

static llvm::Value* getMemoryNumPages(EmitFunctionContext& functionContext, Uptr memoryIndex)
{
	llvm::Constant* memoryOffset = functionContext.moduleContext.memoryOffsets[memoryIndex];

	// Load the number of memory pages from the compartment runtime data.
	llvm::LoadInst* memoryNumPagesLoad = functionContext.loadFromUntypedPointer(
		functionContext.irBuilder.CreateInBoundsGEP(
			llvm::Type::getInt8Ty(functionContext.irBuilder.getContext()),
			functionContext.getCompartmentAddress(),
			{llvm::ConstantExpr::getAdd(
				memoryOffset,
				emitLiteralIptr(offsetof(Runtime::MemoryRuntimeData, numPages),
								functionContext.moduleContext.iptrType))}),
		functionContext.moduleContext.iptrType,
		functionContext.moduleContext.iptrAlignment);
	memoryNumPagesLoad->setAtomic(llvm::AtomicOrdering::Acquire);

	return memoryNumPagesLoad;
}

static llvm::Value* getMemoryNumBytes(EmitFunctionContext& functionContext, Uptr memoryIndex)
{
	return functionContext.irBuilder.CreateMul(
		getMemoryNumPages(functionContext, memoryIndex),
		emitLiteralIptr(IR::numBytesPerPage, functionContext.moduleContext.iptrType));
}

// Bounds checks a sandboxed memory address + offset, and returns an offset relative to the memory
// base address that is guaranteed to be within the virtual address space allocated for the linear
// memory object.
static llvm::Value* getOffsetAndBoundedAddress(EmitFunctionContext& functionContext,
											   Uptr memoryIndex,
											   llvm::Value* address,
											   llvm::Value* numBytes,
											   U64 offset,
											   BoundsCheckOp boundsCheckOp)
{
	const MemoryType& memoryType
		= functionContext.moduleContext.irModule.memories.getType(memoryIndex);
	const bool is32bitMemoryOn64bitHost
		= memoryType.indexType == IndexType::i32
		  && functionContext.moduleContext.iptrValueType == ValueType::i64;

	llvm::IRBuilder<>& irBuilder = functionContext.irBuilder;

	numBytes = irBuilder.CreateZExt(numBytes, address->getType());
	WAVM_ASSERT(numBytes->getType() == address->getType());

	if(memoryType.indexType == IndexType::i32)
	{
		// zext a 32-bit address and number of bytes to the target machine pointer width.
		// This is crucial for security, as LLVM will otherwise implicitly sign extend it to the
		// target machine pointer width in the GEP below, interpreting it as a signed offset and
		// allowing access to memory outside the sandboxed memory range.
		address = irBuilder.CreateZExt(address, functionContext.moduleContext.iptrType);
		numBytes = irBuilder.CreateZExt(numBytes, functionContext.moduleContext.iptrType);
	}

	// If the offset is greater than the size of the guard region, add it before bounds checking,
	// and check for overflow.
	if(offset && offset >= Runtime::VectorOverMMap::getNumGuardBytes())
	{
		llvm::Constant* offsetConstant
			= emitLiteralIptr(offset, functionContext.moduleContext.iptrType);

		if(is32bitMemoryOn64bitHost)
		{
			// This is a 64-bit add of two numbers zero-extended from 32-bit, so it can't overflow.
			address = irBuilder.CreateAdd(address, offsetConstant);
		}
		else
		{
			llvm::Value* addressPlusOffsetAndOverflow
				= functionContext.callLLVMIntrinsic({functionContext.moduleContext.iptrType},
													llvm::Intrinsic::uadd_with_overflow,
													{address, offsetConstant});

			llvm::Value* addressPlusOffset
				= irBuilder.CreateExtractValue(addressPlusOffsetAndOverflow, {0});
			llvm::Value* addressPlusOffsetOverflowed
				= irBuilder.CreateExtractValue(addressPlusOffsetAndOverflow, {1});

			address
				= irBuilder.CreateOr(addressPlusOffset,
									 irBuilder.CreateSExt(addressPlusOffsetOverflowed,
														  functionContext.moduleContext.iptrType));
		}
	}

	if(boundsCheckOp == BoundsCheckOp::trapOnOutOfBounds)
	{
		// If the caller requires a trap, test whether the addressed bytes are within the bounds of
		// the memory, and if not call a trap intrinsic.
		llvm::Value* memoryNumBytes = getMemoryNumBytes(functionContext, memoryIndex);
		llvm::Value* memoryNumBytesMinusNumBytes = irBuilder.CreateSub(memoryNumBytes, numBytes);
		llvm::Value* numBytesWasGreaterThanMemoryNumBytes
			= irBuilder.CreateICmpUGT(memoryNumBytesMinusNumBytes, memoryNumBytes);
		functionContext.emitConditionalTrapIntrinsic(
			irBuilder.CreateOr(numBytesWasGreaterThanMemoryNumBytes,
							   irBuilder.CreateICmpUGT(address, memoryNumBytesMinusNumBytes)),
			"memoryOutOfBoundsTrap",
			FunctionType(TypeTuple{},
						 TypeTuple{functionContext.moduleContext.iptrValueType,
								   functionContext.moduleContext.iptrValueType,
								   functionContext.moduleContext.iptrValueType,
								   functionContext.moduleContext.iptrValueType},
						 IR::CallingConvention::intrinsic),
			{address,
			 numBytes,
			 memoryNumBytes,
			 emitLiteralIptr(memoryIndex, functionContext.moduleContext.iptrType)});
	}
	else
	{
		// For all other cases (e.g. 64-bit addresses on 64-bit targets), it's not possible for the
		// runtime to reserve the full range of addresses, so this function must clamp addresses to
		// the guard region.

		llvm::Value* endAddress
			= irBuilder.CreateLoad(
                    functionContext.moduleContext.iptrType,
                    functionContext.memoryInfos[memoryIndex].endAddressVariable);
		address = irBuilder.CreateSelect(
			irBuilder.CreateICmpULT(address, endAddress), address, endAddress);
	}

	// If the offset is less than the size of the guard region, then add it after bounds checking.
	// This avoids the need to check the addition for overflow, and allows it to be used as the
	// displacement in x86 addresses. Additionally, it allows the LLVM optimizer to reuse the bounds
	// checking code for consecutive loads/stores to the same address.
	if(offset && offset < Runtime::VectorOverMMap::getNumGuardBytes())
	{
		llvm::Constant* offsetConstant
			= emitLiteralIptr(offset, functionContext.moduleContext.iptrType);

		address = irBuilder.CreateAdd(address, offsetConstant);
	}

	return address;
}

llvm::Value* EmitFunctionContext::coerceAddressToPointer(llvm::Value* boundedAddress,
														 llvm::Type* memoryType,
														 Uptr memoryIndex)
{
	llvm::Value* memoryBasePointer
		= irBuilder.CreateLoad(llvmContext.i8PtrType, memoryInfos[memoryIndex].basePointerVariable);
	llvm::Value* bytePointer = irBuilder.CreateInBoundsGEP(llvmContext.i8Type, memoryBasePointer, boundedAddress);

	// Cast the pointer to the appropriate type.
	return irBuilder.CreatePointerCast(bytePointer, memoryType->getPointerTo());
}

//
// Memory size operators
// These just call out to wavmIntrinsics.growMemory/currentMemory, passing a pointer to the default
// memory for the module.
//

void EmitFunctionContext::memory_grow(MemoryImm imm)
{
	llvm::Value* deltaNumPages = pop();
	ValueVector resultTuple = emitRuntimeIntrinsic(
		"memory.grow",
		FunctionType(TypeTuple(moduleContext.iptrValueType),
					 TypeTuple({moduleContext.iptrValueType, moduleContext.iptrValueType}),
					 IR::CallingConvention::intrinsic),
		{zext(deltaNumPages, moduleContext.iptrType),
		 getMemoryIdFromOffset(moduleContext.memoryOffsets[imm.memoryIndex], irBuilder)});
	WAVM_ASSERT(resultTuple.size() == 1);
	const MemoryType& memoryType = moduleContext.irModule.memories.getType(imm.memoryIndex);
	push(coerceIptrToIndex(memoryType.indexType, resultTuple[0]));
}
void EmitFunctionContext::memory_size(MemoryImm imm)
{
	const MemoryType& memoryType = moduleContext.irModule.memories.getType(imm.memoryIndex);
	push(coerceIptrToIndex(memoryType.indexType, getMemoryNumPages(*this, imm.memoryIndex)));
}

//
// Memory bulk operators.
//

void EmitFunctionContext::memory_init(DataSegmentAndMemImm imm)
{
	auto numBytes = pop();
	auto sourceOffset = pop();
	auto destAddress = pop();
	emitRuntimeIntrinsic("memory.init",
						 FunctionType({},
									  TypeTuple({moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType,
												 moduleContext.iptrValueType}),
									  IR::CallingConvention::intrinsic),
						 {zext(destAddress, moduleContext.iptrType),
						  zext(sourceOffset, moduleContext.iptrType),
						  zext(numBytes, moduleContext.iptrType),
						  moduleContext.instanceId,
						  getMemoryIdFromOffset(moduleContext.memoryOffsets[imm.memoryIndex], irBuilder),
						  emitLiteral(llvmContext, imm.dataSegmentIndex)});
}

void EmitFunctionContext::data_drop(DataSegmentImm imm)
{
	emitRuntimeIntrinsic(
		"data.drop",
		FunctionType({},
					 TypeTuple({moduleContext.iptrValueType, moduleContext.iptrValueType}),
					 IR::CallingConvention::intrinsic),
		{moduleContext.instanceId, emitLiteralIptr(imm.dataSegmentIndex, moduleContext.iptrType)});
}

void EmitFunctionContext::memory_copy(MemoryCopyImm imm)
{
	llvm::Value* numBytes = pop();
	llvm::Value* sourceAddress = pop();
	llvm::Value* destAddress = pop();

	llvm::Value* sourceBoundedAddress = getOffsetAndBoundedAddress(
		*this, imm.sourceMemoryIndex, sourceAddress, numBytes, 0, BoundsCheckOp::trapOnOutOfBounds);
	llvm::Value* destBoundedAddress = getOffsetAndBoundedAddress(
		*this, imm.destMemoryIndex, destAddress, numBytes, 0, BoundsCheckOp::trapOnOutOfBounds);

	llvm::Value* sourcePointer
		= coerceAddressToPointer(sourceBoundedAddress, llvmContext.i8Type, imm.sourceMemoryIndex);
	llvm::Value* destPointer
		= coerceAddressToPointer(destBoundedAddress, llvmContext.i8Type, imm.destMemoryIndex);

	llvm::Value* numBytesUptr = irBuilder.CreateZExt(numBytes, moduleContext.iptrType);

	// Use the LLVM memmove instruction to do the copy.
#if LLVM_VERSION_MAJOR < 7
	irBuilder.CreateMemMove(destPointer, sourcePointer, numBytesUptr, 1, true);
#else
	irBuilder.CreateMemMove(
		destPointer, LLVM_ALIGNMENT(1), sourcePointer, LLVM_ALIGNMENT(1), numBytesUptr, true);
#endif
}

void EmitFunctionContext::memory_fill(MemoryImm imm)
{
	llvm::Value* numBytes = pop();
	llvm::Value* value = pop();
	llvm::Value* destAddress = pop();

	llvm::Value* destBoundedAddress = getOffsetAndBoundedAddress(
		*this, imm.memoryIndex, destAddress, numBytes, 0, BoundsCheckOp::trapOnOutOfBounds);
	llvm::Value* destPointer
		= coerceAddressToPointer(destBoundedAddress, llvmContext.i8Type, imm.memoryIndex);

	llvm::Value* numBytesUptr = irBuilder.CreateZExt(numBytes, moduleContext.iptrType);

	// Use the LLVM memset instruction to do the fill.
	irBuilder.CreateMemSet(destPointer,
						   irBuilder.CreateTrunc(value, llvmContext.i8Type),
						   numBytesUptr,
						   LLVM_ALIGNMENT(1),
						   true);
}

//
// Load/store operators
//

#define EMIT_LOAD_OP(destType, name, llvmMemoryType, numBytesLog2, conversionOp)                   \
	void EmitFunctionContext::name(LoadOrStoreImm<numBytesLog2> imm)                               \
	{                                                                                              \
		auto address = pop();                                                                      \
		auto boundedAddress = getOffsetAndBoundedAddress(                                          \
			*this,                                                                                 \
			imm.memoryIndex,                                                                       \
			address,                                                                               \
			llvm::ConstantInt::get(address->getType(), U64(1 << numBytesLog2)),                    \
			imm.offset,                                                                            \
			BoundsCheckOp::clampToGuardRegion);                                                    \
		auto pointer = coerceAddressToPointer(boundedAddress, llvmMemoryType, imm.memoryIndex);    \
		auto load = irBuilder.CreateLoad(llvmMemoryType, pointer);                                 \
		/* Don't trust the alignment hint provided by the WebAssembly code, since the load can't   \
		 * trap if it's wrong. */                                                                  \
		load->setAlignment(LLVM_ALIGNMENT(1));                                                     \
		load->setVolatile(true);                                                                   \
		push(conversionOp(load, destType));                                                        \
	}
#define EMIT_STORE_OP(name, llvmMemoryType, numBytesLog2, conversionOp)                            \
	void EmitFunctionContext::name(LoadOrStoreImm<numBytesLog2> imm)                               \
	{                                                                                              \
		auto value = pop();                                                                        \
		auto address = pop();                                                                      \
		auto boundedAddress = getOffsetAndBoundedAddress(                                          \
			*this,                                                                                 \
			imm.memoryIndex,                                                                       \
			address,                                                                               \
			llvm::ConstantInt::get(address->getType(), U64(1 << numBytesLog2)),                    \
			imm.offset,                                                                            \
			BoundsCheckOp::clampToGuardRegion);                                                    \
		auto pointer = coerceAddressToPointer(boundedAddress, llvmMemoryType, imm.memoryIndex);    \
		auto memoryValue = conversionOp(value, llvmMemoryType);                                    \
		auto store = irBuilder.CreateStore(memoryValue, pointer);                                  \
		store->setVolatile(true);                                                                  \
		/* Don't trust the alignment hint provided by the WebAssembly code, since the store can't  \
		 * trap if it's wrong. */                                                                  \
		store->setAlignment(LLVM_ALIGNMENT(1));                                                    \
	}

EMIT_LOAD_OP(llvmContext.i32Type, i32_load8_s, llvmContext.i8Type, 0, sext)
EMIT_LOAD_OP(llvmContext.i32Type, i32_load8_u, llvmContext.i8Type, 0, zext)
EMIT_LOAD_OP(llvmContext.i32Type, i32_load16_s, llvmContext.i16Type, 1, sext)
EMIT_LOAD_OP(llvmContext.i32Type, i32_load16_u, llvmContext.i16Type, 1, zext)
EMIT_LOAD_OP(llvmContext.i64Type, i64_load8_s, llvmContext.i8Type, 0, sext)
EMIT_LOAD_OP(llvmContext.i64Type, i64_load8_u, llvmContext.i8Type, 0, zext)
EMIT_LOAD_OP(llvmContext.i64Type, i64_load16_s, llvmContext.i16Type, 1, sext)
EMIT_LOAD_OP(llvmContext.i64Type, i64_load16_u, llvmContext.i16Type, 1, zext)
EMIT_LOAD_OP(llvmContext.i64Type, i64_load32_s, llvmContext.i32Type, 2, sext)
EMIT_LOAD_OP(llvmContext.i64Type, i64_load32_u, llvmContext.i32Type, 2, zext)

EMIT_LOAD_OP(llvmContext.i32Type, i32_load, llvmContext.i32Type, 2, identity)
EMIT_LOAD_OP(llvmContext.i64Type, i64_load, llvmContext.i64Type, 3, identity)
EMIT_LOAD_OP(llvmContext.f32Type, f32_load, llvmContext.f32Type, 2, identity)
EMIT_LOAD_OP(llvmContext.f64Type, f64_load, llvmContext.f64Type, 3, identity)

EMIT_STORE_OP(i32_store8, llvmContext.i8Type, 0, trunc)
EMIT_STORE_OP(i64_store8, llvmContext.i8Type, 0, trunc)
EMIT_STORE_OP(i32_store16, llvmContext.i16Type, 1, trunc)
EMIT_STORE_OP(i64_store16, llvmContext.i16Type, 1, trunc)
EMIT_STORE_OP(i32_store, llvmContext.i32Type, 2, trunc)
EMIT_STORE_OP(i64_store32, llvmContext.i32Type, 2, trunc)
EMIT_STORE_OP(i64_store, llvmContext.i64Type, 3, identity)
EMIT_STORE_OP(f32_store, llvmContext.f32Type, 2, identity)
EMIT_STORE_OP(f64_store, llvmContext.f64Type, 3, identity)

EMIT_STORE_OP(v128_store, value->getType(), 4, identity)
EMIT_LOAD_OP(llvmContext.i64x2Type, v128_load, llvmContext.i64x2Type, 4, identity)

EMIT_LOAD_OP(llvmContext.i8x16Type, v128_load8_splat, llvmContext.i8Type, 0, splat<16>)
EMIT_LOAD_OP(llvmContext.i16x8Type, v128_load16_splat, llvmContext.i16Type, 1, splat<8>)
EMIT_LOAD_OP(llvmContext.i32x4Type, v128_load32_splat, llvmContext.i32Type, 2, splat<4>)
EMIT_LOAD_OP(llvmContext.i64x2Type, v128_load64_splat, llvmContext.i64Type, 3, splat<2>)

EMIT_LOAD_OP(llvmContext.i16x8Type, v128_load8x8_s, llvmContext.i8x8Type, 3, sext)
EMIT_LOAD_OP(llvmContext.i16x8Type, v128_load8x8_u, llvmContext.i8x8Type, 3, zext)
EMIT_LOAD_OP(llvmContext.i32x4Type, v128_load16x4_s, llvmContext.i16x4Type, 3, sext)
EMIT_LOAD_OP(llvmContext.i32x4Type, v128_load16x4_u, llvmContext.i16x4Type, 3, zext)
EMIT_LOAD_OP(llvmContext.i64x2Type, v128_load32x2_s, llvmContext.i32x2Type, 3, sext)
EMIT_LOAD_OP(llvmContext.i64x2Type, v128_load32x2_u, llvmContext.i32x2Type, 3, zext)

EMIT_LOAD_OP(llvmContext.i32x4Type,
			 v128_load32_zero,
			 llvmContext.i32Type,
			 2,
			 insertInZeroedVector<4>)
EMIT_LOAD_OP(llvmContext.i64x2Type,
			 v128_load64_zero,
			 llvmContext.i64Type,
			 3,
			 insertInZeroedVector<2>)

static void emitLoadLane(EmitFunctionContext& functionContext,
						 llvm::Type* llvmVectorType,
						 const BaseLoadOrStoreImm& loadOrStoreImm,
						 Uptr laneIndex,
						 Uptr numBytesLog2)
{
	llvm::Value* vector = functionContext.pop();
	vector = functionContext.irBuilder.CreateBitCast(vector, llvmVectorType);

	llvm::Value* address = functionContext.pop();
	llvm::Value* boundedAddress = getOffsetAndBoundedAddress(
		functionContext,
		loadOrStoreImm.memoryIndex,
		address,
		llvm::ConstantInt::get(address->getType(), U64(1) << numBytesLog2),
		loadOrStoreImm.offset,
		BoundsCheckOp::clampToGuardRegion);
	llvm::Value* pointer = functionContext.coerceAddressToPointer(
		boundedAddress, llvmVectorType->getScalarType(), loadOrStoreImm.memoryIndex);
	llvm::LoadInst* load = functionContext.irBuilder.CreateLoad(llvmVectorType->getScalarType(), pointer);
	// Don't trust the alignment hint provided by the WebAssembly code, since the load can't trap if
	// it's wrong.
	load->setAlignment(LLVM_ALIGNMENT(1));
	load->setVolatile(true);

	vector = functionContext.irBuilder.CreateInsertElement(vector, load, laneIndex);
	functionContext.push(vector);
}

static void emitStoreLane(EmitFunctionContext& functionContext,
						  llvm::Type* llvmVectorType,
						  const BaseLoadOrStoreImm& loadOrStoreImm,
						  Uptr laneIndex,
						  Uptr numBytesLog2)
{
	llvm::Value* vector = functionContext.pop();
	vector = functionContext.irBuilder.CreateBitCast(vector, llvmVectorType);

	auto lane = functionContext.irBuilder.CreateExtractElement(vector, laneIndex);

	llvm::Value* address = functionContext.pop();
	llvm::Value* boundedAddress = getOffsetAndBoundedAddress(
		functionContext,
		loadOrStoreImm.memoryIndex,
		address,
		llvm::ConstantInt::get(address->getType(), U64(1) << numBytesLog2),
		loadOrStoreImm.offset,
		BoundsCheckOp::clampToGuardRegion);
	llvm::Value* pointer = functionContext.coerceAddressToPointer(
		boundedAddress, lane->getType(), loadOrStoreImm.memoryIndex);
	llvm::StoreInst* store = functionContext.irBuilder.CreateStore(lane, pointer);
	// Don't trust the alignment hint provided by the WebAssembly code, since the load can't trap if
	// it's wrong.
	store->setAlignment(LLVM_ALIGNMENT(1));
	store->setVolatile(true);
}

#define EMIT_LOAD_LANE_OP(name, llvmVectorType, naturalAlignmentLog2, numLanes)                    \
	void EmitFunctionContext::name(LoadOrStoreLaneImm<naturalAlignmentLog2, numLanes> imm)         \
	{                                                                                              \
		emitLoadLane(*this, llvmVectorType, imm, imm.laneIndex, U64(1) << naturalAlignmentLog2);   \
	}
#define EMIT_STORE_LANE_OP(name, llvmVectorType, naturalAlignmentLog2, numLanes)                   \
	void EmitFunctionContext::name(LoadOrStoreLaneImm<naturalAlignmentLog2, numLanes> imm)         \
	{                                                                                              \
		emitStoreLane(*this, llvmVectorType, imm, imm.laneIndex, U64(1) << naturalAlignmentLog2);  \
	}
EMIT_LOAD_LANE_OP(v128_load8_lane, llvmContext.i8x16Type, 0, 16)
EMIT_LOAD_LANE_OP(v128_load16_lane, llvmContext.i16x8Type, 1, 8)
EMIT_LOAD_LANE_OP(v128_load32_lane, llvmContext.i32x4Type, 2, 4)
EMIT_LOAD_LANE_OP(v128_load64_lane, llvmContext.i64x2Type, 3, 2)
EMIT_STORE_LANE_OP(v128_store8_lane, llvmContext.i8x16Type, 0, 16)
EMIT_STORE_LANE_OP(v128_store16_lane, llvmContext.i16x8Type, 1, 8)
EMIT_STORE_LANE_OP(v128_store32_lane, llvmContext.i32x4Type, 2, 4)
EMIT_STORE_LANE_OP(v128_store64_lane, llvmContext.i64x2Type, 3, 2)

static void emitLoadInterleaved(EmitFunctionContext& functionContext,
								llvm::Type* llvmValueType,
								llvm::Intrinsic::ID aarch64IntrinsicID,
								const BaseLoadOrStoreImm& imm,
								U32 numVectors,
								U32 numLanes,
								U64 numBytes)
{
	static constexpr U32 maxVectors = 4;
	static constexpr U32 maxLanes = 16;
	WAVM_ASSERT(numVectors <= maxVectors);
	WAVM_ASSERT(numLanes <= maxLanes);

	auto address = functionContext.pop();
	auto boundedAddress
		= getOffsetAndBoundedAddress(functionContext,
									 imm.memoryIndex,
									 address,
									 llvm::ConstantInt::get(address->getType(), numBytes),
									 imm.offset,
									 BoundsCheckOp::clampToGuardRegion);
	auto pointer
		= functionContext.coerceAddressToPointer(boundedAddress, llvmValueType, imm.memoryIndex);
	if(functionContext.moduleContext.targetArch == llvm::Triple::aarch64)
	{
		auto results = functionContext.callLLVMIntrinsic(
			{llvmValueType, llvmValueType->getPointerTo()}, aarch64IntrinsicID, {pointer});
		for(U32 vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex)
		{
			functionContext.push(
				functionContext.irBuilder.CreateExtractValue(results, vectorIndex));
		}
	}
	else
	{
		llvm::Value* loads[maxVectors];
		for(U32 vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex)
		{
			auto load
				= functionContext.irBuilder.CreateLoad(llvmValueType, functionContext.irBuilder.CreateInBoundsGEP(
					llvmValueType, pointer, {emitLiteral(functionContext.llvmContext, U32(vectorIndex))}));
			/* Don't trust the alignment hint provided by the WebAssembly code, since the load
			 * can't trap if it's wrong. */
			load->setAlignment(LLVM_ALIGNMENT(1));
			load->setVolatile(true);
			loads[vectorIndex] = load;
		}
		for(U32 vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex)
		{
			llvm::Value* deinterleavedVector = llvm::UndefValue::get(llvmValueType);
			for(U32 laneIndex = 0; laneIndex < numLanes; ++laneIndex)
			{
				const Uptr interleavedElementIndex = laneIndex * numVectors + vectorIndex;
				deinterleavedVector = functionContext.irBuilder.CreateInsertElement(
					deinterleavedVector,
					functionContext.irBuilder.CreateExtractElement(
						loads[interleavedElementIndex / numLanes],
						interleavedElementIndex % numLanes),
					laneIndex);
			}
			functionContext.push(deinterleavedVector);
		}
	}
}

static void emitStoreInterleaved(EmitFunctionContext& functionContext,
								 llvm::Type* llvmValueType,
								 llvm::Intrinsic::ID aarch64IntrinsicID,
								 const IR::BaseLoadOrStoreImm& imm,
								 U32 numVectors,
								 U32 numLanes,
								 U64 numBytes)
{
	static constexpr U32 maxVectors = 4;
	WAVM_ASSERT(numVectors <= 4);

	llvm::Value* values[maxVectors];
	for(U32 vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex)
	{
		values[numVectors - vectorIndex - 1]
			= functionContext.irBuilder.CreateBitCast(functionContext.pop(), llvmValueType);
	}
	auto address = functionContext.pop();
	auto boundedAddress
		= getOffsetAndBoundedAddress(functionContext,
									 imm.memoryIndex,
									 address,
									 llvm::ConstantInt::get(address->getType(), numBytes),
									 imm.offset,
									 BoundsCheckOp::clampToGuardRegion);
	auto pointer
		= functionContext.coerceAddressToPointer(boundedAddress, llvmValueType, imm.memoryIndex);
	if(functionContext.moduleContext.targetArch == llvm::Triple::aarch64)
	{
		llvm::Value* args[maxVectors + 1];
		for(U32 vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex)
		{
			args[vectorIndex] = values[vectorIndex];
			args[numVectors] = pointer;
		}
		functionContext.callLLVMIntrinsic({llvmValueType, llvmValueType->getPointerTo()},
										  aarch64IntrinsicID,
										  llvm::ArrayRef<llvm::Value*>(args, numVectors + 1));
	}
	else
	{
		for(U32 vectorIndex = 0; vectorIndex < numVectors; ++vectorIndex)
		{
			llvm::Value* interleavedVector = llvm::UndefValue::get(llvmValueType);
			for(U32 laneIndex = 0; laneIndex < numLanes; ++laneIndex)
			{
				const Uptr interleavedElementIndex = vectorIndex * numLanes + laneIndex;
				const Uptr deinterleavedVectorIndex = interleavedElementIndex % numVectors;
				const Uptr deinterleavedLaneIndex = interleavedElementIndex / numVectors;
				interleavedVector = functionContext.irBuilder.CreateInsertElement(
					interleavedVector,
					functionContext.irBuilder.CreateExtractElement(values[deinterleavedVectorIndex],
																   deinterleavedLaneIndex),
					laneIndex);
			}
			auto store = functionContext.irBuilder.CreateStore(
				interleavedVector,
				functionContext.irBuilder.CreateInBoundsGEP(
					llvmValueType, pointer, {emitLiteral(functionContext.llvmContext, U32(vectorIndex))}));
			store->setVolatile(true);
			store->setAlignment(LLVM_ALIGNMENT(1));
		}
	}
}

#define EMIT_LOAD_INTERLEAVED_OP(name, llvmValueType, naturalAlignmentLog2, numVectors, numLanes)  \
	void EmitFunctionContext::name(LoadOrStoreImm<naturalAlignmentLog2> imm)                       \
	{                                                                                              \
		emitLoadInterleaved(*this,                                                                 \
							llvmValueType,                                                         \
							llvm::Intrinsic::aarch64_neon_ld##numVectors,                          \
							imm,                                                                   \
							numVectors,                                                            \
							numLanes,                                                              \
							U64(1 << naturalAlignmentLog2) * numVectors);                          \
	}

#define EMIT_STORE_INTERLEAVED_OP(name, llvmValueType, naturalAlignmentLog2, numVectors, numLanes) \
	void EmitFunctionContext::name(LoadOrStoreImm<naturalAlignmentLog2> imm)                       \
	{                                                                                              \
		emitStoreInterleaved(*this,                                                                \
							 llvmValueType,                                                        \
							 llvm::Intrinsic::aarch64_neon_st##numVectors,                         \
							 imm,                                                                  \
							 numVectors,                                                           \
							 numLanes,                                                             \
							 U64(1 << naturalAlignmentLog2) * numVectors);                         \
	}

EMIT_LOAD_INTERLEAVED_OP(v8x16_load_interleaved_2, llvmContext.i8x16Type, 4, 2, 16)
EMIT_LOAD_INTERLEAVED_OP(v8x16_load_interleaved_3, llvmContext.i8x16Type, 4, 3, 16)
EMIT_LOAD_INTERLEAVED_OP(v8x16_load_interleaved_4, llvmContext.i8x16Type, 4, 4, 16)
EMIT_LOAD_INTERLEAVED_OP(v16x8_load_interleaved_2, llvmContext.i16x8Type, 4, 2, 8)
EMIT_LOAD_INTERLEAVED_OP(v16x8_load_interleaved_3, llvmContext.i16x8Type, 4, 3, 8)
EMIT_LOAD_INTERLEAVED_OP(v16x8_load_interleaved_4, llvmContext.i16x8Type, 4, 4, 8)
EMIT_LOAD_INTERLEAVED_OP(v32x4_load_interleaved_2, llvmContext.i32x4Type, 4, 2, 4)
EMIT_LOAD_INTERLEAVED_OP(v32x4_load_interleaved_3, llvmContext.i32x4Type, 4, 3, 4)
EMIT_LOAD_INTERLEAVED_OP(v32x4_load_interleaved_4, llvmContext.i32x4Type, 4, 4, 4)
EMIT_LOAD_INTERLEAVED_OP(v64x2_load_interleaved_2, llvmContext.i64x2Type, 4, 2, 2)
EMIT_LOAD_INTERLEAVED_OP(v64x2_load_interleaved_3, llvmContext.i64x2Type, 4, 3, 2)
EMIT_LOAD_INTERLEAVED_OP(v64x2_load_interleaved_4, llvmContext.i64x2Type, 4, 4, 2)

EMIT_STORE_INTERLEAVED_OP(v8x16_store_interleaved_2, llvmContext.i8x16Type, 4, 2, 16)
EMIT_STORE_INTERLEAVED_OP(v8x16_store_interleaved_3, llvmContext.i8x16Type, 4, 3, 16)
EMIT_STORE_INTERLEAVED_OP(v8x16_store_interleaved_4, llvmContext.i8x16Type, 4, 4, 16)
EMIT_STORE_INTERLEAVED_OP(v16x8_store_interleaved_2, llvmContext.i16x8Type, 4, 2, 8)
EMIT_STORE_INTERLEAVED_OP(v16x8_store_interleaved_3, llvmContext.i16x8Type, 4, 3, 8)
EMIT_STORE_INTERLEAVED_OP(v16x8_store_interleaved_4, llvmContext.i16x8Type, 4, 4, 8)
EMIT_STORE_INTERLEAVED_OP(v32x4_store_interleaved_2, llvmContext.i32x4Type, 4, 2, 4)
EMIT_STORE_INTERLEAVED_OP(v32x4_store_interleaved_3, llvmContext.i32x4Type, 4, 3, 4)
EMIT_STORE_INTERLEAVED_OP(v32x4_store_interleaved_4, llvmContext.i32x4Type, 4, 4, 4)
EMIT_STORE_INTERLEAVED_OP(v64x2_store_interleaved_2, llvmContext.i64x2Type, 4, 2, 2)
EMIT_STORE_INTERLEAVED_OP(v64x2_store_interleaved_3, llvmContext.i64x2Type, 4, 3, 2)
EMIT_STORE_INTERLEAVED_OP(v64x2_store_interleaved_4, llvmContext.i64x2Type, 4, 4, 2)

void EmitFunctionContext::trapIfMisalignedAtomic(llvm::Value* address, U32 alignmentLog2)
{
	if(alignmentLog2 > 0)
	{
		emitConditionalTrapIntrinsic(
			irBuilder.CreateICmpNE(
				llvmContext.typedZeroConstants[(Uptr)ValueType::i64],
				irBuilder.CreateAnd(
					address, emitLiteralIptr((U64(1) << alignmentLog2) - 1, address->getType()))),
			"misalignedAtomicTrap",
			FunctionType(TypeTuple{}, TypeTuple{ValueType::i64}, IR::CallingConvention::intrinsic),
			{address});
	}
}

void EmitFunctionContext::memory_atomic_notify(AtomicLoadOrStoreImm<2> imm)
{
	llvm::Value* numWaiters = pop();
	llvm::Value* address = pop();
	llvm::Value* boundedAddress
		= getOffsetAndBoundedAddress(*this,
									 imm.memoryIndex,
									 address,
									 llvm::ConstantInt::get(address->getType(), U64(4)),
									 imm.offset,
									 BoundsCheckOp::clampToGuardRegion);
	trapIfMisalignedAtomic(boundedAddress, imm.alignmentLog2);
	push(emitRuntimeIntrinsic(
		"memory.atomic.notify",
		FunctionType(
			TypeTuple{ValueType::i32},
			TypeTuple{moduleContext.iptrValueType, ValueType::i32, moduleContext.iptrValueType},
			IR::CallingConvention::intrinsic),
		{boundedAddress,
		 numWaiters,
		 getMemoryIdFromOffset(moduleContext.memoryOffsets[imm.memoryIndex], irBuilder)})[0]);
}
void EmitFunctionContext::memory_atomic_wait32(AtomicLoadOrStoreImm<2> imm)
{
	llvm::Value* timeout = pop();
	llvm::Value* expectedValue = pop();
	llvm::Value* address = pop();
	llvm::Value* boundedAddress
		= getOffsetAndBoundedAddress(*this,
									 imm.memoryIndex,
									 address,
									 llvm::ConstantInt::get(address->getType(), U64(4)),
									 imm.offset,
									 BoundsCheckOp::clampToGuardRegion);
	trapIfMisalignedAtomic(boundedAddress, imm.alignmentLog2);
	push(emitRuntimeIntrinsic(
		"memory.atomic.wait32",
		FunctionType(TypeTuple{ValueType::i32},
					 TypeTuple{moduleContext.iptrValueType,
							   ValueType::i32,
							   ValueType::i64,
							   moduleContext.iptrValueType},
					 IR::CallingConvention::intrinsic),
		{boundedAddress,
		 expectedValue,
		 timeout,
		 getMemoryIdFromOffset(moduleContext.memoryOffsets[imm.memoryIndex], irBuilder)})[0]);
}
void EmitFunctionContext::memory_atomic_wait64(AtomicLoadOrStoreImm<3> imm)
{
	llvm::Value* timeout = pop();
	llvm::Value* expectedValue = pop();
	llvm::Value* address = pop();
	llvm::Value* boundedAddress
		= getOffsetAndBoundedAddress(*this,
									 imm.memoryIndex,
									 address,
									 llvm::ConstantInt::get(address->getType(), U64(8)),
									 imm.offset,
									 BoundsCheckOp::clampToGuardRegion);
	trapIfMisalignedAtomic(boundedAddress, imm.alignmentLog2);
	push(emitRuntimeIntrinsic(
		"memory.atomic.wait64",
		FunctionType(TypeTuple{ValueType::i32},
					 TypeTuple{moduleContext.iptrValueType,
							   ValueType::i64,
							   ValueType::i64,
							   moduleContext.iptrValueType},
					 IR::CallingConvention::intrinsic),
		{boundedAddress,
		 expectedValue,
		 timeout,
		 getMemoryIdFromOffset(moduleContext.memoryOffsets[imm.memoryIndex], irBuilder)})[0]);
}

void EmitFunctionContext::atomic_fence(AtomicFenceImm imm)
{
	switch(imm.order)
	{
	case MemoryOrder::sequentiallyConsistent:
		irBuilder.CreateFence(llvm::AtomicOrdering::SequentiallyConsistent);
		break;
	default: WAVM_UNREACHABLE();
	};
}

#define EMIT_ATOMIC_LOAD_OP(valueTypeId, name, llvmMemoryType, numBytesLog2, memToValue)           \
	void EmitFunctionContext::valueTypeId##_##name(AtomicLoadOrStoreImm<numBytesLog2> imm)         \
	{                                                                                              \
		auto address = pop();                                                                      \
		auto boundedAddress = getOffsetAndBoundedAddress(                                          \
			*this,                                                                                 \
			imm.memoryIndex,                                                                       \
			address,                                                                               \
			llvm::ConstantInt::get(address->getType(), U64(1 << numBytesLog2)),                    \
			imm.offset,                                                                            \
			BoundsCheckOp::clampToGuardRegion);                                                    \
		trapIfMisalignedAtomic(boundedAddress, numBytesLog2);                                      \
		auto pointer = coerceAddressToPointer(boundedAddress, llvmMemoryType, imm.memoryIndex);    \
		auto load = irBuilder.CreateLoad(llvmMemoryType, pointer);                                                 \
		load->setAlignment(LLVM_ALIGNMENT(U64(1) << imm.alignmentLog2));                           \
		load->setVolatile(true);                                                                   \
		load->setAtomic(llvm::AtomicOrdering::SequentiallyConsistent);                             \
		push(memToValue(load, asLLVMType(llvmContext, ValueType::valueTypeId)));                   \
	}
#define EMIT_ATOMIC_STORE_OP(valueTypeId, name, llvmMemoryType, numBytesLog2, valueToMem)          \
	void EmitFunctionContext::valueTypeId##_##name(AtomicLoadOrStoreImm<numBytesLog2> imm)         \
	{                                                                                              \
		auto value = pop();                                                                        \
		auto address = pop();                                                                      \
		auto boundedAddress = getOffsetAndBoundedAddress(                                          \
			*this,                                                                                 \
			imm.memoryIndex,                                                                       \
			address,                                                                               \
			llvm::ConstantInt::get(address->getType(), U64(1 << numBytesLog2)),                    \
			imm.offset,                                                                            \
			BoundsCheckOp::clampToGuardRegion);                                                    \
		trapIfMisalignedAtomic(boundedAddress, numBytesLog2);                                      \
		auto pointer = coerceAddressToPointer(boundedAddress, llvmMemoryType, imm.memoryIndex);    \
		auto memoryValue = valueToMem(value, llvmMemoryType);                                      \
		auto store = irBuilder.CreateStore(memoryValue, pointer);                                  \
		store->setVolatile(true);                                                                  \
		store->setAlignment(LLVM_ALIGNMENT(U64(1) << imm.alignmentLog2));                          \
		store->setAtomic(llvm::AtomicOrdering::SequentiallyConsistent);                            \
	}
EMIT_ATOMIC_LOAD_OP(i32, atomic_load, llvmContext.i32Type, 2, identity)
EMIT_ATOMIC_LOAD_OP(i64, atomic_load, llvmContext.i64Type, 3, identity)

EMIT_ATOMIC_LOAD_OP(i32, atomic_load8_u, llvmContext.i8Type, 0, zext)
EMIT_ATOMIC_LOAD_OP(i32, atomic_load16_u, llvmContext.i16Type, 1, zext)
EMIT_ATOMIC_LOAD_OP(i64, atomic_load8_u, llvmContext.i8Type, 0, zext)
EMIT_ATOMIC_LOAD_OP(i64, atomic_load16_u, llvmContext.i16Type, 1, zext)
EMIT_ATOMIC_LOAD_OP(i64, atomic_load32_u, llvmContext.i32Type, 2, zext)

EMIT_ATOMIC_STORE_OP(i32, atomic_store, llvmContext.i32Type, 2, identity)
EMIT_ATOMIC_STORE_OP(i64, atomic_store, llvmContext.i64Type, 3, identity)

EMIT_ATOMIC_STORE_OP(i32, atomic_store8, llvmContext.i8Type, 0, trunc)
EMIT_ATOMIC_STORE_OP(i32, atomic_store16, llvmContext.i16Type, 1, trunc)
EMIT_ATOMIC_STORE_OP(i64, atomic_store8, llvmContext.i8Type, 0, trunc)
EMIT_ATOMIC_STORE_OP(i64, atomic_store16, llvmContext.i16Type, 1, trunc)
EMIT_ATOMIC_STORE_OP(i64, atomic_store32, llvmContext.i32Type, 2, trunc)

#define EMIT_ATOMIC_CMPXCHG(                                                                       \
	valueTypeId, name, llvmMemoryType, numBytesLog2, memToValue, valueToMem)                       \
	void EmitFunctionContext::valueTypeId##_##name(AtomicLoadOrStoreImm<numBytesLog2> imm)         \
	{                                                                                              \
		auto replacementValue = valueToMem(pop(), llvmMemoryType);                                 \
		auto expectedValue = valueToMem(pop(), llvmMemoryType);                                    \
		auto address = pop();                                                                      \
		auto boundedAddress = getOffsetAndBoundedAddress(                                          \
			*this,                                                                                 \
			imm.memoryIndex,                                                                       \
			address,                                                                               \
			llvm::ConstantInt::get(address->getType(), U64(1 << numBytesLog2)),                    \
			imm.offset,                                                                            \
			BoundsCheckOp::clampToGuardRegion);                                                    \
		trapIfMisalignedAtomic(boundedAddress, numBytesLog2);                                      \
		auto pointer = coerceAddressToPointer(boundedAddress, llvmMemoryType, imm.memoryIndex);    \
		auto atomicCmpXchg                                                                         \
			= irBuilder.CreateAtomicCmpXchg(pointer,                                               \
											expectedValue,                                         \
											replacementValue,                                      \
                                            {},                                                    \
											llvm::AtomicOrdering::SequentiallyConsistent,          \
											llvm::AtomicOrdering::SequentiallyConsistent);         \
		atomicCmpXchg->setVolatile(true);                                                          \
		auto previousValue = irBuilder.CreateExtractValue(atomicCmpXchg, {0});                     \
		push(memToValue(previousValue, asLLVMType(llvmContext, ValueType::valueTypeId)));          \
	}

EMIT_ATOMIC_CMPXCHG(i32, atomic_rmw8_cmpxchg_u, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_CMPXCHG(i32, atomic_rmw16_cmpxchg_u, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_CMPXCHG(i32, atomic_rmw_cmpxchg, llvmContext.i32Type, 2, identity, identity)

EMIT_ATOMIC_CMPXCHG(i64, atomic_rmw8_cmpxchg_u, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_CMPXCHG(i64, atomic_rmw16_cmpxchg_u, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_CMPXCHG(i64, atomic_rmw32_cmpxchg_u, llvmContext.i32Type, 2, zext, trunc)
EMIT_ATOMIC_CMPXCHG(i64, atomic_rmw_cmpxchg, llvmContext.i64Type, 3, identity, identity)

#define EMIT_ATOMIC_RMW(                                                                           \
	valueTypeId, name, rmwOpId, llvmMemoryType, numBytesLog2, memToValue, valueToMem)              \
	void EmitFunctionContext::valueTypeId##_##name(AtomicLoadOrStoreImm<numBytesLog2> imm)         \
	{                                                                                              \
		auto value = valueToMem(pop(), llvmMemoryType);                                            \
		auto address = pop();                                                                      \
		auto boundedAddress = getOffsetAndBoundedAddress(                                          \
			*this,                                                                                 \
			imm.memoryIndex,                                                                       \
			address,                                                                               \
			llvm::ConstantInt::get(address->getType(), U64(1 << numBytesLog2)),                    \
			imm.offset,                                                                            \
			BoundsCheckOp::clampToGuardRegion);                                                    \
		trapIfMisalignedAtomic(boundedAddress, numBytesLog2);                                      \
		auto pointer = coerceAddressToPointer(boundedAddress, llvmMemoryType, imm.memoryIndex);    \
		auto atomicRMW = irBuilder.CreateAtomicRMW(llvm::AtomicRMWInst::BinOp::rmwOpId,            \
												   pointer,                                        \
												   value,                                          \
                                                   {},                                             \
												   llvm::AtomicOrdering::SequentiallyConsistent);  \
		atomicRMW->setVolatile(true);                                                              \
		push(memToValue(atomicRMW, asLLVMType(llvmContext, ValueType::valueTypeId)));              \
	}

EMIT_ATOMIC_RMW(i32, atomic_rmw8_xchg_u, Xchg, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw16_xchg_u, Xchg, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw_xchg, Xchg, llvmContext.i32Type, 2, identity, identity)

EMIT_ATOMIC_RMW(i64, atomic_rmw8_xchg_u, Xchg, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw16_xchg_u, Xchg, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw32_xchg_u, Xchg, llvmContext.i32Type, 2, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw_xchg, Xchg, llvmContext.i64Type, 3, identity, identity)

EMIT_ATOMIC_RMW(i32, atomic_rmw8_add_u, Add, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw16_add_u, Add, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw_add, Add, llvmContext.i32Type, 2, identity, identity)

EMIT_ATOMIC_RMW(i64, atomic_rmw8_add_u, Add, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw16_add_u, Add, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw32_add_u, Add, llvmContext.i32Type, 2, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw_add, Add, llvmContext.i64Type, 3, identity, identity)

EMIT_ATOMIC_RMW(i32, atomic_rmw8_sub_u, Sub, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw16_sub_u, Sub, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw_sub, Sub, llvmContext.i32Type, 2, identity, identity)

EMIT_ATOMIC_RMW(i64, atomic_rmw8_sub_u, Sub, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw16_sub_u, Sub, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw32_sub_u, Sub, llvmContext.i32Type, 2, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw_sub, Sub, llvmContext.i64Type, 3, identity, identity)

EMIT_ATOMIC_RMW(i32, atomic_rmw8_and_u, And, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw16_and_u, And, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw_and, And, llvmContext.i32Type, 2, identity, identity)

EMIT_ATOMIC_RMW(i64, atomic_rmw8_and_u, And, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw16_and_u, And, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw32_and_u, And, llvmContext.i32Type, 2, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw_and, And, llvmContext.i64Type, 3, identity, identity)

EMIT_ATOMIC_RMW(i32, atomic_rmw8_or_u, Or, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw16_or_u, Or, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw_or, Or, llvmContext.i32Type, 2, identity, identity)

EMIT_ATOMIC_RMW(i64, atomic_rmw8_or_u, Or, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw16_or_u, Or, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw32_or_u, Or, llvmContext.i32Type, 2, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw_or, Or, llvmContext.i64Type, 3, identity, identity)

EMIT_ATOMIC_RMW(i32, atomic_rmw8_xor_u, Xor, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw16_xor_u, Xor, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i32, atomic_rmw_xor, Xor, llvmContext.i32Type, 2, identity, identity)

EMIT_ATOMIC_RMW(i64, atomic_rmw8_xor_u, Xor, llvmContext.i8Type, 0, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw16_xor_u, Xor, llvmContext.i16Type, 1, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw32_xor_u, Xor, llvmContext.i32Type, 2, zext, trunc)
EMIT_ATOMIC_RMW(i64, atomic_rmw_xor, Xor, llvmContext.i64Type, 3, identity, identity)
