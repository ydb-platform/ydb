#include <stdint.h>
#include "EmitFunctionContext.h"
#include "EmitModuleContext.h"
#include "EmitWorkarounds.h"
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/BasicTypes.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/InstrTypes.h>
#include <llvm/IR/Intrinsics.h>

#if LLVM_VERSION_MAJOR >= 10
#include <llvm/IR/IntrinsicsAArch64.h>
#include <llvm/IR/IntrinsicsX86.h>
#endif
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

namespace llvm {
	class Type;
	class Value;
}

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;

static llvm::Type* getWithNewIntBitWidth(llvm::Type* type, U32 newBitWidth)
{
#if LLVM_VERSION_MAJOR >= 10
	return type->getWithNewBitWidth(newBitWidth);
#else
	WAVM_ASSERT(type->isIntOrIntVectorTy());
	llvm::Type* newType = llvm::Type::getIntNTy(type->getContext(), newBitWidth);
	if(type->isVectorTy())
	{ newType = FixedVectorType::get(newType, type->getVectorNumElements()); }
	return newType;
#endif
}

llvm::Value* EmitFunctionContext::extendHalfOfIntVector(
	llvm::Value* vector,
	Uptr baseSourceElementIndex,
	llvm::Value* (EmitFunctionContext::*extend)(llvm::Value*, llvm::Type*))
{
	FixedVectorType* vectorType = static_cast<FixedVectorType*>(vector->getType());
	FixedVectorType* halfVectorType = FixedVectorType::getHalfElementsVectorType(vectorType);

	llvm::Value* halfVector = llvm::UndefValue::get(halfVectorType);
	for(U64 index = 0; index < halfVectorType->getNumElements(); ++index)
	{
		halfVector = irBuilder.CreateInsertElement(
			halfVector,
			irBuilder.CreateExtractElement(vector, baseSourceElementIndex + index),
			index);
	}

	llvm::Type* halfExtendedVectorType
		= getWithNewIntBitWidth(halfVectorType, halfVectorType->getScalarSizeInBits() * 2);

	return (this->*extend)(halfVector, halfExtendedVectorType);
}

llvm::Value* EmitFunctionContext::insertIntoHalfZeroVector(llvm::Value* halfVector)
{
	FixedVectorType* halfVectorType = static_cast<FixedVectorType*>(halfVector->getType());
	llvm::Type* fullVectorType = FixedVectorType::get(halfVectorType->getScalarType(),
													  U32(halfVectorType->getNumElements() * 2));
	llvm::Value* result = llvm::Constant::getNullValue(fullVectorType);
	for(U32 elementIndex = 0; elementIndex < halfVectorType->getNumElements(); ++elementIndex)
	{
		result = irBuilder.CreateInsertElement(
			result, irBuilder.CreateExtractElement(halfVector, elementIndex), elementIndex);
	}
	return result;
}

#define EMIT_UNARY_OP(name, emitCode)                                                              \
	void EmitFunctionContext::name(NoImm)                                                          \
	{                                                                                              \
		auto operand = pop();                                                                      \
		push(emitCode);                                                                            \
	}

EMIT_UNARY_OP(i32_wrap_i64, trunc(operand, llvmContext.i32Type))
EMIT_UNARY_OP(i64_extend_i32_s, sext(operand, llvmContext.i64Type))
EMIT_UNARY_OP(i64_extend_i32_u, zext(operand, llvmContext.i64Type))

EMIT_UNARY_OP(f32_convert_i32_s, irBuilder.CreateSIToFP(operand, llvmContext.f32Type))
EMIT_UNARY_OP(f64_convert_i32_s, irBuilder.CreateSIToFP(operand, llvmContext.f64Type))
EMIT_UNARY_OP(f32_convert_i64_s, irBuilder.CreateSIToFP(operand, llvmContext.f32Type))
EMIT_UNARY_OP(f64_convert_i64_s, irBuilder.CreateSIToFP(operand, llvmContext.f64Type))
EMIT_UNARY_OP(f32_convert_i32_u, irBuilder.CreateUIToFP(operand, llvmContext.f32Type))
EMIT_UNARY_OP(f64_convert_i32_u, irBuilder.CreateUIToFP(operand, llvmContext.f64Type))
EMIT_UNARY_OP(f32_convert_i64_u, irBuilder.CreateUIToFP(operand, llvmContext.f32Type))
EMIT_UNARY_OP(f64_convert_i64_u, irBuilder.CreateUIToFP(operand, llvmContext.f64Type))

EMIT_UNARY_OP(f32x4_convert_i32x4_s,
			  irBuilder.CreateSIToFP(irBuilder.CreateBitCast(operand, llvmContext.i32x4Type),
									 llvmContext.f32x4Type));
EMIT_UNARY_OP(f32x4_convert_i32x4_u,
			  irBuilder.CreateUIToFP(irBuilder.CreateBitCast(operand, llvmContext.i32x4Type),
									 llvmContext.f32x4Type));

#define EMIT_F64x2_CONVERT_LOW_I32x4(_su, ConvertFunction)                                         \
	void EmitFunctionContext::f64x2_convert_low_i32x4##_su(IR::NoImm)                              \
	{                                                                                              \
		llvm::Value* vector = pop();                                                               \
		vector = irBuilder.CreateBitCast(vector, llvmContext.i32x4Type);                           \
		llvm::Value* halfVector = llvm::UndefValue::get(llvmContext.i32x2Type);                    \
		for(U64 laneIndex = 0; laneIndex < 2; ++laneIndex)                                         \
		{                                                                                          \
			halfVector = irBuilder.CreateInsertElement(                                            \
				halfVector, irBuilder.CreateExtractElement(vector, laneIndex), laneIndex);         \
		}                                                                                          \
		llvm::Value* result = irBuilder.ConvertFunction(halfVector, llvmContext.f64x2Type);        \
		push(result);                                                                              \
	}
EMIT_F64x2_CONVERT_LOW_I32x4(_s, CreateSIToFP) EMIT_F64x2_CONVERT_LOW_I32x4(_u, CreateUIToFP)

	EMIT_UNARY_OP(f32_demote_f64, irBuilder.CreateFPTrunc(operand, llvmContext.f32Type))
		EMIT_UNARY_OP(f64_promote_f32, emitF64Promote(operand, llvmContext.f64Type))

			void EmitFunctionContext::f32x4_demote_f64x2_zero(IR::NoImm)
{
	llvm::Value* vector = pop();
	vector = irBuilder.CreateBitCast(vector, llvmContext.f64x2Type);

	llvm::Value* halfResult = irBuilder.CreateFPTrunc(vector, llvmContext.f32x2Type);
	llvm::Value* result = insertIntoHalfZeroVector(halfResult);
	push(result);
}
void EmitFunctionContext::f64x2_promote_low_f32x4(IR::NoImm)
{
	llvm::Value* vector = pop();
	vector = irBuilder.CreateBitCast(vector, llvmContext.f32x4Type);

	llvm::Value* halfVector
		= irBuilder.CreateShuffleVector(vector,
										llvm::UndefValue::get(vector->getType()),
										llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>{0, 1});
	llvm::Value* result = emitF64Promote(halfVector, llvmContext.f64x2Type);
	push(result);
}

EMIT_UNARY_OP(f32_reinterpret_i32, irBuilder.CreateBitCast(operand, llvmContext.f32Type))
EMIT_UNARY_OP(f64_reinterpret_i64, irBuilder.CreateBitCast(operand, llvmContext.f64Type))
EMIT_UNARY_OP(i32_reinterpret_f32, irBuilder.CreateBitCast(operand, llvmContext.i32Type))
EMIT_UNARY_OP(i64_reinterpret_f64, irBuilder.CreateBitCast(operand, llvmContext.i64Type))

llvm::Value* EmitFunctionContext::emitF64Promote(llvm::Value* operand, llvm::Type* destType)
{
	llvm::Value* f64Operand = irBuilder.CreateFPExt(operand, destType);

#if LLVM_VERSION_MAJOR >= 10
	// Emit an nop experimental.constrained.fmul intrinsic on the result of the promote to make sure
	// the promote can't be optimized away.
	llvm::Value* one = emitLiteral(llvmContext, F64(1.0));
	if(destType->isVectorTy())
	{
		one = irBuilder.CreateVectorSplat(
			U32(static_cast<FixedVectorType*>(destType)->getNumElements()), one);
	}

	return callLLVMIntrinsic(
		{destType},
		llvm::Intrinsic::experimental_constrained_fmul,
		{f64Operand, one, moduleContext.fpRoundingModeMetadata, moduleContext.fpExceptionMetadata});
#else
	// Work around a pre-LLVM 10 bug with the constrained FP arithmetic intrinsics:
	// https://bugs.llvm.org/show_bug.cgi?id=43510

	llvm::Value* intOne = moduleContext.unoptimizableOne;
	llvm::Value* one = irBuilder.CreateSIToFP(intOne, llvmContext.f64Type);
	if(destType->isVectorTy())
	{
		one = irBuilder.CreateVectorSplat(
			U32(static_cast<FixedVectorType*>(destType)->getNumElements()), one);
	}

	return irBuilder.CreateFMul(f64Operand, one);
#endif
}

template<typename Float>
llvm::Value* EmitFunctionContext::emitTruncFloatToInt(ValueType destType,
													  bool isSigned,
													  Float minBounds,
													  Float maxBounds,
													  llvm::Value* operand)
{
	auto nanBlock = llvm::BasicBlock::Create(llvmContext, "FPToInt_nan", function);
	auto notNaNBlock = llvm::BasicBlock::Create(llvmContext, "FPToInt_notNaN", function);
	auto overflowBlock = llvm::BasicBlock::Create(llvmContext, "FPToInt_overflow", function);
	auto noOverflowBlock = llvm::BasicBlock::Create(llvmContext, "FPToInt_noOverflow", function);

	auto isNaN = createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_UNO, operand, operand);
	irBuilder.CreateCondBr(isNaN, nanBlock, notNaNBlock, moduleContext.likelyFalseBranchWeights);

	irBuilder.SetInsertPoint(nanBlock);
	emitRuntimeIntrinsic(
		"invalidFloatOperationTrap", FunctionType({}, {}, IR::CallingConvention::intrinsic), {});
	irBuilder.CreateUnreachable();

	irBuilder.SetInsertPoint(notNaNBlock);
	auto isOverflow
		= irBuilder.CreateOr(irBuilder.CreateFCmpOGE(operand, emitLiteral(llvmContext, maxBounds)),
							 irBuilder.CreateFCmpOLE(operand, emitLiteral(llvmContext, minBounds)));
	irBuilder.CreateCondBr(
		isOverflow, overflowBlock, noOverflowBlock, moduleContext.likelyFalseBranchWeights);

	irBuilder.SetInsertPoint(overflowBlock);
	emitRuntimeIntrinsic("divideByZeroOrIntegerOverflowTrap",
						 FunctionType({}, {}, IR::CallingConvention::intrinsic),
						 {});
	irBuilder.CreateUnreachable();

	irBuilder.SetInsertPoint(noOverflowBlock);
	return isSigned ? irBuilder.CreateFPToSI(operand, asLLVMType(llvmContext, destType))
					: irBuilder.CreateFPToUI(operand, asLLVMType(llvmContext, destType));
}

// We want the widest floating point bounds that can't be truncated to an integer.
// This isn't simply the min/max integer values converted to float, but the next greater(or lesser)
// float that would be truncated to an integer out of range of the target type.

EMIT_UNARY_OP(
	i32_trunc_f32_s,
	emitTruncFloatToInt<F32>(ValueType::i32, true, -2147483904.0f, 2147483648.0f, operand))
EMIT_UNARY_OP(i32_trunc_f64_s,
			  emitTruncFloatToInt<F64>(ValueType::i32, true, -2147483649.0, 2147483648.0, operand))
EMIT_UNARY_OP(i32_trunc_f32_u,
			  emitTruncFloatToInt<F32>(ValueType::i32, false, -1.0f, 4294967296.0f, operand))
EMIT_UNARY_OP(i32_trunc_f64_u,
			  emitTruncFloatToInt<F64>(ValueType::i32, false, -1.0, 4294967296.0, operand))

EMIT_UNARY_OP(i64_trunc_f32_s,
			  emitTruncFloatToInt<F32>(ValueType::i64,
									   true,
									   -9223373136366403584.0f,
									   9223372036854775808.0f,
									   operand))
EMIT_UNARY_OP(i64_trunc_f64_s,
			  emitTruncFloatToInt<F64>(ValueType::i64,
									   true,
									   -9223372036854777856.0,
									   9223372036854775808.0,
									   operand))
EMIT_UNARY_OP(
	i64_trunc_f32_u,
	emitTruncFloatToInt<F32>(ValueType::i64, false, -1.0f, 18446744073709551616.0f, operand))
EMIT_UNARY_OP(
	i64_trunc_f64_u,
	emitTruncFloatToInt<F64>(ValueType::i64, false, -1.0, 18446744073709551616.0, operand))

template<typename Int, typename Float>
llvm::Value* EmitFunctionContext::emitTruncFloatToIntSat(llvm::Type* destType,
														 bool isSigned,
														 Float minFloatBounds,
														 Float maxFloatBounds,
														 Int minIntBounds,
														 Int maxIntBounds,
														 llvm::Value* operand)
{
	llvm::Value* result = isSigned ? irBuilder.CreateFPToSI(operand, destType)
								   : irBuilder.CreateFPToUI(operand, destType);

	result = irBuilder.CreateSelect(
		irBuilder.CreateFCmpOGE(operand, emitLiteral(llvmContext, maxFloatBounds)),
		emitLiteral(llvmContext, maxIntBounds),
		result);
	result = irBuilder.CreateSelect(
		irBuilder.CreateFCmpOLE(operand, emitLiteral(llvmContext, minFloatBounds)),
		emitLiteral(llvmContext, minIntBounds),
		result);
	result = irBuilder.CreateSelect(
		createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_UNO, operand, operand),
		emitLiteral(llvmContext, Int(0)),
		result);

	return result;
}

EMIT_UNARY_OP(i32_trunc_sat_f32_s,
			  emitTruncFloatToIntSat(llvmContext.i32Type,
									 true,
									 F32(INT32_MIN),
									 F32(INT32_MAX),
									 INT32_MIN,
									 INT32_MAX,
									 operand))
EMIT_UNARY_OP(i32_trunc_sat_f64_s,
			  emitTruncFloatToIntSat(llvmContext.i32Type,
									 true,
									 F64(INT32_MIN),
									 F64(INT32_MAX),
									 INT32_MIN,
									 INT32_MAX,
									 operand))
EMIT_UNARY_OP(i32_trunc_sat_f32_u,
			  emitTruncFloatToIntSat(llvmContext.i32Type,
									 false,
									 0.0f,
									 F32(UINT32_MAX),
									 U32(0),
									 UINT32_MAX,
									 operand))
EMIT_UNARY_OP(i32_trunc_sat_f64_u,
			  emitTruncFloatToIntSat(llvmContext.i32Type,
									 false,
									 0.0,
									 F64(UINT32_MAX),
									 U32(0),
									 UINT32_MAX,
									 operand))
EMIT_UNARY_OP(i64_trunc_sat_f32_s,
			  emitTruncFloatToIntSat(llvmContext.i64Type,
									 true,
									 F32(INT64_MIN),
									 F32(INT64_MAX),
									 INT64_MIN,
									 INT64_MAX,
									 operand))
EMIT_UNARY_OP(i64_trunc_sat_f64_s,
			  emitTruncFloatToIntSat(llvmContext.i64Type,
									 true,
									 F64(INT64_MIN),
									 F64(INT64_MAX),
									 INT64_MIN,
									 INT64_MAX,
									 operand))
EMIT_UNARY_OP(i64_trunc_sat_f32_u,
			  emitTruncFloatToIntSat(llvmContext.i64Type,
									 false,
									 0.0f,
									 F32(UINT64_MAX),
									 U64(0),
									 UINT64_MAX,
									 operand))
EMIT_UNARY_OP(i64_trunc_sat_f64_u,
			  emitTruncFloatToIntSat(llvmContext.i64Type,
									 false,
									 0.0,
									 F64(UINT64_MAX),
									 U64(0),
									 UINT64_MAX,
									 operand))

template<typename Int, typename Float, Uptr numElements>
llvm::Value* EmitFunctionContext::emitTruncVectorFloatToIntSat(llvm::Type* destType,
															   bool isSigned,
															   Float minFloatBounds,
															   Float maxFloatBounds,
															   Int minIntBounds,
															   Int maxIntBounds,
															   Int nanResult,
															   llvm::Value* operand)
{
	auto result = isSigned ? irBuilder.CreateFPToSI(operand, destType)
						   : irBuilder.CreateFPToUI(operand, destType);

	auto minFloatBoundsVec
		= irBuilder.CreateVectorSplat(numElements, emitLiteral(llvmContext, minFloatBounds));
	auto maxFloatBoundsVec
		= irBuilder.CreateVectorSplat(numElements, emitLiteral(llvmContext, maxFloatBounds));

	result = emitVectorSelect(
		irBuilder.CreateFCmpOGE(operand, maxFloatBoundsVec),
		irBuilder.CreateVectorSplat(numElements, emitLiteral(llvmContext, maxIntBounds)),
		result);
	result = emitVectorSelect(
		irBuilder.CreateFCmpOLE(operand, minFloatBoundsVec),
		irBuilder.CreateVectorSplat(numElements, emitLiteral(llvmContext, minIntBounds)),
		result);
	result = emitVectorSelect(
		createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_UNO, operand, operand),
		irBuilder.CreateVectorSplat(numElements, emitLiteral(llvmContext, nanResult)),
		result);
	return result;
}

EMIT_UNARY_OP(
	i32x4_trunc_sat_f32x4_s,
	(emitTruncVectorFloatToIntSat<I32, F32, 4>)(llvmContext.i32x4Type,
												true,
												F32(INT32_MIN),
												F32(INT32_MAX),
												INT32_MIN,
												INT32_MAX,
												I32(0),
												irBuilder.CreateBitCast(operand,
																		llvmContext.f32x4Type)))
EMIT_UNARY_OP(
	i32x4_trunc_sat_f32x4_u,
	(emitTruncVectorFloatToIntSat<U32, F32, 4>)(llvmContext.i32x4Type,
												false,
												0.0f,
												F32(UINT32_MAX),
												U32(0),
												UINT32_MAX,
												U32(0),
												irBuilder.CreateBitCast(operand,
																		llvmContext.f32x4Type)))

EMIT_UNARY_OP(i32x4_trunc_sat_f64x2_s_zero,
			  insertIntoHalfZeroVector(emitTruncVectorFloatToIntSat<I32, F64, 2>(
				  llvmContext.i32x2Type,
				  true,
				  F64(INT32_MIN),
				  F64(INT32_MAX),
				  INT32_MIN,
				  INT32_MAX,
				  I32(0),
				  irBuilder.CreateBitCast(operand, llvmContext.f64x2Type))))

EMIT_UNARY_OP(i32x4_trunc_sat_f64x2_u_zero,
			  insertIntoHalfZeroVector(emitTruncVectorFloatToIntSat<U32, F64, 2>(
				  llvmContext.i32x2Type,
				  false,
				  F64(0),
				  F64(UINT32_MAX),
				  0,
				  UINT32_MAX,
				  I32(0),
				  irBuilder.CreateBitCast(operand, llvmContext.f64x2Type))))

EMIT_UNARY_OP(i32_extend8_s, sext(trunc(operand, llvmContext.i8Type), llvmContext.i32Type))
EMIT_UNARY_OP(i32_extend16_s, sext(trunc(operand, llvmContext.i16Type), llvmContext.i32Type))
EMIT_UNARY_OP(i64_extend8_s, sext(trunc(operand, llvmContext.i8Type), llvmContext.i64Type))
EMIT_UNARY_OP(i64_extend16_s, sext(trunc(operand, llvmContext.i16Type), llvmContext.i64Type))
EMIT_UNARY_OP(i64_extend32_s, sext(trunc(operand, llvmContext.i32Type), llvmContext.i64Type))

#define EMIT_SIMD_SPLAT(vectorType, coerceScalar, numLanes)                                        \
	void EmitFunctionContext::vectorType##_splat(IR::NoImm)                                        \
	{                                                                                              \
		auto scalar = pop();                                                                       \
		push(irBuilder.CreateVectorSplat(numLanes, coerceScalar));                                 \
	}
EMIT_SIMD_SPLAT(i8x16, trunc(scalar, llvmContext.i8Type), 16)
EMIT_SIMD_SPLAT(i16x8, trunc(scalar, llvmContext.i16Type), 8)
EMIT_SIMD_SPLAT(i32x4, scalar, 4)
EMIT_SIMD_SPLAT(i64x2, scalar, 2)
EMIT_SIMD_SPLAT(f32x4, scalar, 4)
EMIT_SIMD_SPLAT(f64x2, scalar, 2)

#define EMIT_SIMD_NARROW(name, sourceType, halfDestType, x86IntrinsicId, aarch64IntrinsicId)       \
	void EmitFunctionContext::name(IR::NoImm)                                                      \
	{                                                                                              \
		auto right = irBuilder.CreateBitCast(pop(), sourceType);                                   \
		auto left = irBuilder.CreateBitCast(pop(), sourceType);                                    \
		const llvm::Triple::ArchType targetArch                                                    \
			= moduleContext.targetMachine->getTargetTriple().getArch();                            \
		if(targetArch == llvm::Triple::x86_64 || targetArch == llvm::Triple::x86)                  \
		{ push(callLLVMIntrinsic({}, x86IntrinsicId, {left, right})); }                            \
		else if(targetArch == llvm::Triple::aarch64)                                               \
		{                                                                                          \
			llvm::Value* halfInput[2]{left, right};                                                \
			llvm::Value* result = llvm::UndefValue::get(llvmContext.i64x2Type);                    \
			for(U64 halfIndex = 0; halfIndex < 2; ++halfIndex)                                     \
			{                                                                                      \
				result = irBuilder.CreateInsertElement(                                            \
					result,                                                                        \
					irBuilder.CreateExtractElement(                                                \
						irBuilder.CreateBitCast(                                                   \
							callLLVMIntrinsic(                                                     \
								{halfDestType}, aarch64IntrinsicId, {halfInput[halfIndex]}),       \
							llvmContext.i64x1Type),                                                \
						U64(0)),                                                                   \
					halfIndex);                                                                    \
			}                                                                                      \
			push(result);                                                                          \
		}                                                                                          \
	}

EMIT_SIMD_NARROW(i8x16_narrow_i16x8_s,
				 llvmContext.i16x8Type,
				 llvmContext.i8x8Type,
				 llvm::Intrinsic::x86_sse2_packsswb_128,
				 llvm::Intrinsic::aarch64_neon_sqxtn)
EMIT_SIMD_NARROW(i8x16_narrow_i16x8_u,
				 llvmContext.i16x8Type,
				 llvmContext.i8x8Type,
				 llvm::Intrinsic::x86_sse2_packuswb_128,
				 llvm::Intrinsic::aarch64_neon_sqxtun)
EMIT_SIMD_NARROW(i16x8_narrow_i32x4_s,
				 llvmContext.i32x4Type,
				 llvmContext.i16x4Type,
				 llvm::Intrinsic::x86_sse2_packssdw_128,
				 llvm::Intrinsic::aarch64_neon_sqxtn)
EMIT_SIMD_NARROW(i16x8_narrow_i32x4_u,
				 llvmContext.i32x4Type,
				 llvmContext.i16x4Type,
				 llvm::Intrinsic::x86_sse41_packusdw,
				 llvm::Intrinsic::aarch64_neon_sqxtun)

#define EMIT_SIMD_EXTEND(name, sourceType, baseSourceElementIndex, extend)                         \
	void EmitFunctionContext::name(IR::NoImm)                                                      \
	{                                                                                              \
		auto operand = irBuilder.CreateBitCast(pop(), sourceType);                                 \
		llvm::Value* result = extendHalfOfIntVector(                                               \
			operand, baseSourceElementIndex, &EmitFunctionContext::extend);                        \
		push(result);                                                                              \
	}

EMIT_SIMD_EXTEND(i16x8_extend_low_i8x16_s, llvmContext.i8x16Type, 0, sext)
EMIT_SIMD_EXTEND(i16x8_extend_high_i8x16_s, llvmContext.i8x16Type, 8, sext)
EMIT_SIMD_EXTEND(i16x8_extend_low_i8x16_u, llvmContext.i8x16Type, 0, zext)
EMIT_SIMD_EXTEND(i16x8_extend_high_i8x16_u, llvmContext.i8x16Type, 8, zext)

EMIT_SIMD_EXTEND(i32x4_extend_low_i16x8_s, llvmContext.i16x8Type, 0, sext)
EMIT_SIMD_EXTEND(i32x4_extend_high_i16x8_s, llvmContext.i16x8Type, 4, sext)
EMIT_SIMD_EXTEND(i32x4_extend_low_i16x8_u, llvmContext.i16x8Type, 0, zext)
EMIT_SIMD_EXTEND(i32x4_extend_high_i16x8_u, llvmContext.i16x8Type, 4, zext)

EMIT_SIMD_EXTEND(i64x2_extend_low_i32x4_s, llvmContext.i32x4Type, 0, sext)
EMIT_SIMD_EXTEND(i64x2_extend_high_i32x4_s, llvmContext.i32x4Type, 2, sext)
EMIT_SIMD_EXTEND(i64x2_extend_low_i32x4_u, llvmContext.i32x4Type, 0, zext)
EMIT_SIMD_EXTEND(i64x2_extend_high_i32x4_u, llvmContext.i32x4Type, 2, zext)

void EmitFunctionContext::i8x16_bitmask(NoImm)
{
	auto i8x16Operand = irBuilder.CreateBitCast(pop(), llvmContext.i8x16Type);
	auto i1x16Mask = irBuilder.CreateICmpSLT(
		i8x16Operand, llvm::ConstantVector::getNullValue(llvmContext.i8x16Type));
	if(moduleContext.targetArch == llvm::Triple::x86_64
	   || moduleContext.targetArch == llvm::Triple::x86)
	{
		push(irBuilder.CreateZExt(irBuilder.CreateBitCast(i1x16Mask, llvmContext.i16Type),
								  llvmContext.i32Type));
	}
	else
	{
		auto i8x16Mask = irBuilder.CreateSExt(i1x16Mask, llvmContext.i8x16Type);
		auto constant1 = llvm::ConstantInt::get(llvmContext.i8Type, 1);
		auto constant2 = llvm::ConstantInt::get(llvmContext.i8Type, 2);
		auto constant4 = llvm::ConstantInt::get(llvmContext.i8Type, 4);
		auto constant8 = llvm::ConstantInt::get(llvmContext.i8Type, 8);
		auto constant16 = llvm::ConstantInt::get(llvmContext.i8Type, 16);
		auto constant32 = llvm::ConstantInt::get(llvmContext.i8Type, 32);
		auto constant64 = llvm::ConstantInt::get(llvmContext.i8Type, 64);
		auto constant128 = llvm::ConstantInt::get(llvmContext.i8Type, 128);
		auto i8x16OrthogonalBitMask = irBuilder.CreateAnd(i8x16Mask,
														  llvm::ConstantVector::get({constant1,
																					 constant2,
																					 constant4,
																					 constant8,
																					 constant16,
																					 constant32,
																					 constant64,
																					 constant128,
																					 constant1,
																					 constant2,
																					 constant4,
																					 constant8,
																					 constant16,
																					 constant32,
																					 constant64,
																					 constant128}));
		auto i8x8OriginalBitMaskA = irBuilder.CreateShuffleVector(
			i8x16OrthogonalBitMask,
			llvm::UndefValue::get(llvmContext.i8x16Type),
			llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>{0, 1, 2, 3, 4, 5, 6, 7});
		auto i8x8OriginalBitMaskB = irBuilder.CreateShuffleVector(
			i8x16OrthogonalBitMask,
			llvm::UndefValue::get(llvmContext.i8x16Type),
			llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>{8, 9, 10, 11, 12, 13, 14, 15});
		auto i8CombinedBitMaskA = callLLVMIntrinsic(
			{llvmContext.i8x8Type}, LLVM_INTRINSIC_VECTOR_REDUCE_ADD, {i8x8OriginalBitMaskA});
		auto i8CombinedBitMaskB = callLLVMIntrinsic(
			{llvmContext.i8x8Type}, LLVM_INTRINSIC_VECTOR_REDUCE_ADD, {i8x8OriginalBitMaskB});
		auto i32CombinedBitMask = irBuilder.CreateOr(
			irBuilder.CreateZExt(i8CombinedBitMaskA, llvmContext.i32Type),
			irBuilder.CreateShl(irBuilder.CreateZExt(i8CombinedBitMaskB, llvmContext.i32Type),
								emitLiteral(llvmContext, U32(8))));
		push(i32CombinedBitMask);
	}
}

void EmitFunctionContext::i16x8_bitmask(NoImm)
{
	auto i8x16Operand = irBuilder.CreateBitCast(pop(), llvmContext.i16x8Type);
	auto i1x8Mask = irBuilder.CreateICmpSLT(
		i8x16Operand, llvm::ConstantVector::getNullValue(llvmContext.i16x8Type));
	if(moduleContext.targetArch == llvm::Triple::x86_64
	   || moduleContext.targetArch == llvm::Triple::x86)
	{
		push(irBuilder.CreateZExt(irBuilder.CreateBitCast(i1x8Mask, llvmContext.i8Type),
								  llvmContext.i32Type));
	}
	else
	{
		auto i16x8Mask = irBuilder.CreateSExt(i1x8Mask, llvmContext.i16x8Type);
		auto constant1 = llvm::ConstantInt::get(llvmContext.i16Type, 1);
		auto constant2 = llvm::ConstantInt::get(llvmContext.i16Type, 2);
		auto constant4 = llvm::ConstantInt::get(llvmContext.i16Type, 4);
		auto constant8 = llvm::ConstantInt::get(llvmContext.i16Type, 8);
		auto constant16 = llvm::ConstantInt::get(llvmContext.i16Type, 16);
		auto constant32 = llvm::ConstantInt::get(llvmContext.i16Type, 32);
		auto constant64 = llvm::ConstantInt::get(llvmContext.i16Type, 64);
		auto constant128 = llvm::ConstantInt::get(llvmContext.i16Type, 128);
		auto i16x8OrthogonalBitMask = irBuilder.CreateAnd(i16x8Mask,
														  llvm::ConstantVector::get({constant1,
																					 constant2,
																					 constant4,
																					 constant8,
																					 constant16,
																					 constant32,
																					 constant64,
																					 constant128}));
		auto i16CombinedBitMask = callLLVMIntrinsic(
			{llvmContext.i16x8Type}, LLVM_INTRINSIC_VECTOR_REDUCE_ADD, {i16x8OrthogonalBitMask});
		push(irBuilder.CreateZExt(i16CombinedBitMask, llvmContext.i32Type));
	}
}

void EmitFunctionContext::i32x4_bitmask(NoImm)
{
	auto i32x4Operand = irBuilder.CreateBitCast(pop(), llvmContext.i32x4Type);
	auto i1x4Mask = irBuilder.CreateICmpSLT(
		i32x4Operand, llvm::ConstantVector::getNullValue(llvmContext.i32x4Type));
	if(moduleContext.targetArch == llvm::Triple::x86_64
	   || moduleContext.targetArch == llvm::Triple::x86)
	{
		push(irBuilder.CreateZExt(
			irBuilder.CreateBitCast(i1x4Mask, llvm::IntegerType::get(llvmContext, 4)),
			llvmContext.i32Type));
	}
	else
	{
		auto i32x4Mask = irBuilder.CreateSExt(i1x4Mask, llvmContext.i32x4Type);
		auto constant1 = llvm::ConstantInt::get(llvmContext.i32Type, 1);
		auto constant2 = llvm::ConstantInt::get(llvmContext.i32Type, 2);
		auto constant4 = llvm::ConstantInt::get(llvmContext.i32Type, 4);
		auto constant8 = llvm::ConstantInt::get(llvmContext.i32Type, 8);
		auto i32x4OrthogonalBitMask = irBuilder.CreateAnd(i32x4Mask,
														  llvm::ConstantVector::get({
															  constant1,
															  constant2,
															  constant4,
															  constant8,
														  }));
		auto i32CombinedBitMask = callLLVMIntrinsic(
			{llvmContext.i32x4Type}, LLVM_INTRINSIC_VECTOR_REDUCE_ADD, {i32x4OrthogonalBitMask});
		push(i32CombinedBitMask);
	}
}

void EmitFunctionContext::i64x2_bitmask(NoImm)
{
	auto i64x2Operand = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	auto i1x2Mask = irBuilder.CreateICmpSLT(
		i64x2Operand, llvm::ConstantVector::getNullValue(llvmContext.i64x2Type));
	if(moduleContext.targetArch == llvm::Triple::x86_64
	   || moduleContext.targetArch == llvm::Triple::x86)
	{
		push(irBuilder.CreateZExt(
			irBuilder.CreateBitCast(i1x2Mask, llvm::IntegerType::get(llvmContext, 2)),
			llvmContext.i32Type));
	}
	else
	{
		auto i64x2Mask = irBuilder.CreateSExt(i1x2Mask, llvmContext.i64x2Type);
		auto constant1 = llvm::ConstantInt::get(llvmContext.i64Type, 1);
		auto constant2 = llvm::ConstantInt::get(llvmContext.i64Type, 2);
		auto i64x2OrthogonalBitMask = irBuilder.CreateAnd(i64x2Mask,
														  llvm::ConstantVector::get({
															  constant1,
															  constant2,
														  }));
		auto i64CombinedBitMask = callLLVMIntrinsic(
			{llvmContext.i64x2Type}, LLVM_INTRINSIC_VECTOR_REDUCE_ADD, {i64x2OrthogonalBitMask});
		push(irBuilder.CreateTrunc(i64CombinedBitMask, llvmContext.i32Type));
	}
}

#define EMIT_SIMD_FP_ROUNDING(type)                                                                \
	EMIT_UNARY_OP(type##_ceil,                                                                     \
				  callLLVMIntrinsic({llvmContext.type##Type},                                      \
									llvm::Intrinsic::ceil,                                         \
									{irBuilder.CreateBitCast(operand, llvmContext.type##Type)}))   \
	EMIT_UNARY_OP(type##_floor,                                                                    \
				  callLLVMIntrinsic({llvmContext.type##Type},                                      \
									llvm::Intrinsic::floor,                                        \
									{irBuilder.CreateBitCast(operand, llvmContext.type##Type)}))   \
	EMIT_UNARY_OP(type##_trunc,                                                                    \
				  callLLVMIntrinsic({llvmContext.type##Type},                                      \
									llvm::Intrinsic::trunc,                                        \
									{irBuilder.CreateBitCast(operand, llvmContext.type##Type)}))   \
	EMIT_UNARY_OP(type##_nearest,                                                                  \
				  callLLVMIntrinsic({llvmContext.type##Type},                                      \
									llvm::Intrinsic::nearbyint,                                    \
									{irBuilder.CreateBitCast(operand, llvmContext.type##Type)}))

EMIT_SIMD_FP_ROUNDING(f32x4)
EMIT_SIMD_FP_ROUNDING(f64x2)
