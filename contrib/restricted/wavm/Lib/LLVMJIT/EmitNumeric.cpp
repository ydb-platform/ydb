#include <stdint.h>
#include "EmitContext.h"
#include "EmitFunctionContext.h"
#include "EmitModuleContext.h"
#include "EmitWorkarounds.h"
#include "LLVMJITPrivate.h"
#include "WAVM/IR/Operators.h"
#include "WAVM/IR/Types.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/FloatComponents.h"

PUSH_DISABLE_WARNINGS_FOR_LLVM_HEADERS
#include <llvm/ADT/APInt.h>
#include <llvm/ADT/ArrayRef.h>
#include <llvm/ADT/SmallVector.h>
#include <llvm/IR/BasicBlock.h>
#include <llvm/IR/Constant.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/InstrTypes.h>
#include <llvm/IR/Instructions.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/Type.h>
#include <llvm/IR/Value.h>

#if LLVM_VERSION_MAJOR >= 10
#include <llvm/IR/IntrinsicsAArch64.h>
#include <llvm/IR/IntrinsicsX86.h>
#endif
POP_DISABLE_WARNINGS_FOR_LLVM_HEADERS

using namespace WAVM;
using namespace WAVM::IR;
using namespace WAVM::LLVMJIT;

//
// Constant operators
//

#define EMIT_CONST(typeId, NativeType)                                                             \
	void EmitFunctionContext::typeId##_const(LiteralImm<NativeType> imm)                           \
	{                                                                                              \
		push(emitLiteral(llvmContext, imm.value));                                                 \
	}
EMIT_CONST(i32, I32)
EMIT_CONST(i64, I64)
EMIT_CONST(f32, F32)
EMIT_CONST(f64, F64)
EMIT_CONST(v128, V128)

//
// Numeric operator macros
//

#define EMIT_BINARY_OP(typeId, name, emitCode)                                                     \
	void EmitFunctionContext::typeId##_##name(NoImm)                                               \
	{                                                                                              \
		const ValueType type = ValueType::typeId;                                                  \
		WAVM_SUPPRESS_UNUSED(type);                                                                \
		auto right = pop();                                                                        \
		auto left = pop();                                                                         \
		push(emitCode);                                                                            \
	}

#define EMIT_INT_BINARY_OP(name, emitCode)                                                         \
	EMIT_BINARY_OP(i32, name, emitCode)                                                            \
	EMIT_BINARY_OP(i64, name, emitCode)

#define EMIT_FP_BINARY_OP(name, emitCode)                                                          \
	EMIT_BINARY_OP(f32, name, emitCode)                                                            \
	EMIT_BINARY_OP(f64, name, emitCode)

#define EMIT_UNARY_OP(typeId, name, emitCode)                                                      \
	void EmitFunctionContext::typeId##_##name(NoImm)                                               \
	{                                                                                              \
		const ValueType type = ValueType::typeId;                                                  \
		WAVM_SUPPRESS_UNUSED(type);                                                                \
		auto operand = pop();                                                                      \
		push(emitCode);                                                                            \
	}

#define EMIT_INT_UNARY_OP(name, emitCode)                                                          \
	EMIT_UNARY_OP(i32, name, emitCode)                                                             \
	EMIT_UNARY_OP(i64, name, emitCode)

#define EMIT_FP_UNARY_OP(name, emitCode)                                                           \
	EMIT_UNARY_OP(f32, name, emitCode)                                                             \
	EMIT_UNARY_OP(f64, name, emitCode)

#define EMIT_SIMD_BINARY_OP(name, llvmType, emitCode)                                              \
	void EmitFunctionContext::name(IR::NoImm)                                                      \
	{                                                                                              \
		llvm::Type* vectorType = llvmType;                                                         \
		WAVM_SUPPRESS_UNUSED(vectorType);                                                          \
		auto right = irBuilder.CreateBitCast(pop(), llvmType);                                     \
		WAVM_SUPPRESS_UNUSED(right);                                                               \
		auto left = irBuilder.CreateBitCast(pop(), llvmType);                                      \
		WAVM_SUPPRESS_UNUSED(left);                                                                \
		push(emitCode);                                                                            \
	}
#define EMIT_SIMD_UNARY_OP(name, llvmType, emitCode)                                               \
	void EmitFunctionContext::name(IR::NoImm)                                                      \
	{                                                                                              \
		auto operand = irBuilder.CreateBitCast(pop(), llvmType);                                   \
		WAVM_SUPPRESS_UNUSED(operand);                                                             \
		push(emitCode);                                                                            \
	}

#define EMIT_SIMD_INT_BINARY_OP(name, emitCode)                                                    \
	EMIT_SIMD_BINARY_OP(i8x16##_##name, llvmContext.i8x16Type, emitCode)                           \
	EMIT_SIMD_BINARY_OP(i16x8##_##name, llvmContext.i16x8Type, emitCode)                           \
	EMIT_SIMD_BINARY_OP(i32x4##_##name, llvmContext.i32x4Type, emitCode)                           \
	EMIT_SIMD_BINARY_OP(i64x2##_##name, llvmContext.i64x2Type, emitCode)

#define EMIT_SIMD_FP_BINARY_OP(name, emitCode)                                                     \
	EMIT_SIMD_BINARY_OP(f32x4##_##name, llvmContext.f32x4Type, emitCode)                           \
	EMIT_SIMD_BINARY_OP(f64x2##_##name, llvmContext.f64x2Type, emitCode)

#define EMIT_SIMD_INT_UNARY_OP(name, emitCode)                                                     \
	EMIT_SIMD_UNARY_OP(i8x16##_##name, llvmContext.i8x16Type, emitCode)                            \
	EMIT_SIMD_UNARY_OP(i16x8##_##name, llvmContext.i16x8Type, emitCode)                            \
	EMIT_SIMD_UNARY_OP(i32x4##_##name, llvmContext.i32x4Type, emitCode)                            \
	EMIT_SIMD_UNARY_OP(i64x2##_##name, llvmContext.i64x2Type, emitCode)

#define EMIT_SIMD_FP_UNARY_OP(name, emitCode)                                                      \
	EMIT_SIMD_UNARY_OP(f32x4##_##name, llvmContext.f32x4Type, emitCode)                            \
	EMIT_SIMD_UNARY_OP(f64x2##_##name, llvmContext.f64x2Type, emitCode)

//
// Int operators
//

llvm::Value* EmitFunctionContext::emitSRem(ValueType type, llvm::Value* left, llvm::Value* right)
{
	// Trap if the dividend is zero.
	trapDivideByZero(right);

	// LLVM's srem has undefined behavior where WebAssembly's rem_s defines that it should not trap
	// if the corresponding division would overflow a signed integer. To avoid this case, we just
	// branch around the srem if the INT_MAX%-1 case that overflows is detected.
	auto preOverflowBlock = irBuilder.GetInsertBlock();
	auto noOverflowBlock = llvm::BasicBlock::Create(llvmContext, "sremNoOverflow", function);
	auto endBlock = llvm::BasicBlock::Create(llvmContext, "sremEnd", function);
	auto noOverflow = irBuilder.CreateOr(
		irBuilder.CreateICmpNE(left,
							   type == ValueType::i32 ? emitLiteral(llvmContext, (U32)INT32_MIN)
													  : emitLiteral(llvmContext, (U64)INT64_MIN)),
		irBuilder.CreateICmpNE(right,
							   type == ValueType::i32 ? emitLiteral(llvmContext, (U32)-1)
													  : emitLiteral(llvmContext, (U64)-1)));
	irBuilder.CreateCondBr(
		noOverflow, noOverflowBlock, endBlock, moduleContext.likelyTrueBranchWeights);

	irBuilder.SetInsertPoint(noOverflowBlock);
	auto noOverflowValue = irBuilder.CreateSRem(left, right);
	irBuilder.CreateBr(endBlock);

	irBuilder.SetInsertPoint(endBlock);
	auto phi = irBuilder.CreatePHI(asLLVMType(llvmContext, type), 2);
	phi->addIncoming(llvmContext.typedZeroConstants[(Uptr)type], preOverflowBlock);
	phi->addIncoming(noOverflowValue, noOverflowBlock);
	return phi;
}

static llvm::Value* emitShiftCountMask(EmitContext& emitContext,
									   llvm::Value* shiftCount,
									   Uptr numBits)
{
	// LLVM's shifts have undefined behavior where WebAssembly specifies that the shift count will
	// wrap numbers greater than the bit count of the operands. This matches x86's native shift
	// instructions, but explicitly mask the shift count anyway to support other platforms, and
	// ensure the optimizer doesn't take advantage of the UB.
	llvm::Value* bitsMinusOne = llvm::ConstantInt::get(shiftCount->getType(), numBits - 1);
	return emitContext.irBuilder.CreateAnd(shiftCount, bitsMinusOne);
}

static llvm::Value* emitRotl(EmitContext& emitContext, llvm::Value* left, llvm::Value* right)
{
	llvm::Type* type = left->getType();
	llvm::Value* bitWidth = emitLiteral(emitContext.llvmContext, type->getIntegerBitWidth());
	llvm::Value* bitWidthMinusRight
		= emitContext.irBuilder.CreateSub(emitContext.irBuilder.CreateZExt(bitWidth, type), right);
	return emitContext.irBuilder.CreateOr(
		emitContext.irBuilder.CreateShl(
			left, emitShiftCountMask(emitContext, right, type->getIntegerBitWidth())),
		emitContext.irBuilder.CreateLShr(
			left, emitShiftCountMask(emitContext, bitWidthMinusRight, type->getIntegerBitWidth())));
}

static llvm::Value* emitRotr(EmitContext& emitContext, llvm::Value* left, llvm::Value* right)
{
	llvm::Type* type = left->getType();
	llvm::Value* bitWidth = emitLiteral(emitContext.llvmContext, type->getIntegerBitWidth());
	llvm::Value* bitWidthMinusRight
		= emitContext.irBuilder.CreateSub(emitContext.irBuilder.CreateZExt(bitWidth, type), right);
	return emitContext.irBuilder.CreateOr(
		emitContext.irBuilder.CreateShl(
			left, emitShiftCountMask(emitContext, bitWidthMinusRight, type->getIntegerBitWidth())),
		emitContext.irBuilder.CreateLShr(
			left, emitShiftCountMask(emitContext, right, type->getIntegerBitWidth())));
}

EMIT_INT_BINARY_OP(add, irBuilder.CreateAdd(left, right))
EMIT_INT_BINARY_OP(sub, irBuilder.CreateSub(left, right))
EMIT_INT_BINARY_OP(mul, irBuilder.CreateMul(left, right))
EMIT_INT_BINARY_OP(and_, irBuilder.CreateAnd(left, right))
EMIT_INT_BINARY_OP(or_, irBuilder.CreateOr(left, right))
EMIT_INT_BINARY_OP(xor_, irBuilder.CreateXor(left, right))
EMIT_INT_BINARY_OP(rotr, emitRotr(*this, left, right))
EMIT_INT_BINARY_OP(rotl, emitRotl(*this, left, right))

// Divides use trapDivideByZero to avoid the undefined behavior in LLVM's division instructions.
EMIT_INT_BINARY_OP(div_s,
				   (trapDivideByZeroOrIntegerOverflow(type, left, right),
					irBuilder.CreateSDiv(left, right)))
EMIT_INT_BINARY_OP(rem_s, emitSRem(type, left, right))
EMIT_INT_BINARY_OP(div_u, (trapDivideByZero(right), irBuilder.CreateUDiv(left, right)))
EMIT_INT_BINARY_OP(rem_u, (trapDivideByZero(right), irBuilder.CreateURem(left, right)))

// Explicitly mask the shift amount operand to the word size to avoid LLVM's undefined behavior.
EMIT_INT_BINARY_OP(shl,
				   irBuilder.CreateShl(left,
									   emitShiftCountMask(*this, right, getTypeBitWidth(type))))
EMIT_INT_BINARY_OP(shr_s,
				   irBuilder.CreateAShr(left,
										emitShiftCountMask(*this, right, getTypeBitWidth(type))))
EMIT_INT_BINARY_OP(shr_u,
				   irBuilder.CreateLShr(left,
										emitShiftCountMask(*this, right, getTypeBitWidth(type))))

EMIT_INT_UNARY_OP(clz,
				  callLLVMIntrinsic({operand->getType()},
									llvm::Intrinsic::ctlz,
									{operand, emitLiteral(llvmContext, false)}))
EMIT_INT_UNARY_OP(ctz,
				  callLLVMIntrinsic({operand->getType()},
									llvm::Intrinsic::cttz,
									{operand, emitLiteral(llvmContext, false)}))
EMIT_INT_UNARY_OP(popcnt,
				  callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::ctpop, {operand}))
void EmitFunctionContext::i8x16_popcnt(NoImm)
{
	llvm::Value* operand = pop();
	operand = irBuilder.CreateBitCast(operand, llvmContext.i8x16Type);
	llvm::Value* result
		= callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::ctpop, {operand});
	push(result);
}

EMIT_INT_UNARY_OP(
	eqz,
	coerceBoolToI32(irBuilder.CreateICmpEQ(operand, llvmContext.typedZeroConstants[(Uptr)type])))

//
// FP operators
//

EMIT_FP_BINARY_OP(add,
				  callLLVMIntrinsic({left->getType()},
									llvm::Intrinsic::experimental_constrained_fadd,
									{left,
									 right,
									 moduleContext.fpRoundingModeMetadata,
									 moduleContext.fpExceptionMetadata}))
EMIT_FP_BINARY_OP(sub,
				  callLLVMIntrinsic({left->getType()},
									llvm::Intrinsic::experimental_constrained_fsub,
									{left,
									 right,
									 moduleContext.fpRoundingModeMetadata,
									 moduleContext.fpExceptionMetadata}))
EMIT_FP_BINARY_OP(mul,
				  callLLVMIntrinsic({left->getType()},
									llvm::Intrinsic::experimental_constrained_fmul,
									{left,
									 right,
									 moduleContext.fpRoundingModeMetadata,
									 moduleContext.fpExceptionMetadata}))
EMIT_FP_BINARY_OP(div,
				  callLLVMIntrinsic({left->getType()},
									llvm::Intrinsic::experimental_constrained_fdiv,
									{left,
									 right,
									 moduleContext.fpRoundingModeMetadata,
									 moduleContext.fpExceptionMetadata}))
EMIT_FP_BINARY_OP(copysign,
				  callLLVMIntrinsic({left->getType()}, llvm::Intrinsic::copysign, {left, right}))

EMIT_FP_UNARY_OP(neg, irBuilder.CreateFNeg(operand))
EMIT_FP_UNARY_OP(abs, callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::fabs, {operand}))
EMIT_FP_UNARY_OP(sqrt,
				 callLLVMIntrinsic({operand->getType()},
								   llvm::Intrinsic::experimental_constrained_sqrt,
								   {operand,
									moduleContext.fpRoundingModeMetadata,
									moduleContext.fpExceptionMetadata}))

#define EMIT_FP_COMPARE_OP(name, pred, zextOrSext, llvmOperandType, llvmResultType)                \
	void EmitFunctionContext::name(NoImm)                                                          \
	{                                                                                              \
		auto right = irBuilder.CreateBitCast(pop(), llvmOperandType);                              \
		auto left = irBuilder.CreateBitCast(pop(), llvmOperandType);                               \
		push(zextOrSext(createFCmpWithWorkaround(irBuilder, pred, left, right), llvmResultType));  \
	}

#define EMIT_FP_COMPARE(name, pred)                                                                \
	EMIT_FP_COMPARE_OP(f32_##name, pred, zext, llvmContext.f32Type, llvmContext.i32Type)           \
	EMIT_FP_COMPARE_OP(f64_##name, pred, zext, llvmContext.f64Type, llvmContext.i32Type)           \
	EMIT_FP_COMPARE_OP(f32x4_##name, pred, sext, llvmContext.f32x4Type, llvmContext.i32x4Type)     \
	EMIT_FP_COMPARE_OP(f64x2_##name, pred, sext, llvmContext.f64x2Type, llvmContext.i64x2Type)

EMIT_FP_COMPARE(eq, llvm::CmpInst::FCMP_OEQ)
EMIT_FP_COMPARE(ne, llvm::CmpInst::FCMP_UNE)
EMIT_FP_COMPARE(lt, llvm::CmpInst::FCMP_OLT)
EMIT_FP_COMPARE(le, llvm::CmpInst::FCMP_OLE)
EMIT_FP_COMPARE(gt, llvm::CmpInst::FCMP_OGT)
EMIT_FP_COMPARE(ge, llvm::CmpInst::FCMP_OGE)

static llvm::Value* quietNaN(EmitFunctionContext& context,
							 llvm::Value* nan,
							 llvm::Value* quietNaNMask)
{
#if LLVM_VERSION_MAJOR >= 10
	// Converts a signaling NaN to a quiet NaN by adding zero to it.
	return context.callLLVMIntrinsic({nan->getType()},
									 llvm::Intrinsic::experimental_constrained_fadd,
									 {nan,
									  llvm::Constant::getNullValue(nan->getType()),
									  context.moduleContext.fpRoundingModeMetadata,
									  context.moduleContext.fpExceptionMetadata});
#else
	// Converts a signaling NaN to a quiet NaN by setting its top bit. This works around a LLVM bug
	// that is triggered by the above constrained fadd technique:
	// https://bugs.llvm.org/show_bug.cgi?id=43510
	return context.irBuilder.CreateBitCast(
		context.irBuilder.CreateOr(context.irBuilder.CreateBitCast(nan, quietNaNMask->getType()),
								   quietNaNMask),
		nan->getType());
#endif
}

static llvm::Value* emitFloatMin(EmitFunctionContext& context,
								 llvm::Value* left,
								 llvm::Value* right,
								 llvm::Type* intType,
								 llvm::Value* quietNaNMask)
{
	llvm::IRBuilder<>& irBuilder = context.irBuilder;
	llvm::Type* floatType = left->getType();
	llvm::Value* isLeftNaN
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_UNO, left, left);
	llvm::Value* isRightNaN
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_UNO, right, right);
	llvm::Value* isLeftLessThanRight
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OLT, left, right);
	llvm::Value* isLeftGreaterThanRight
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OGT, left, right);

	return irBuilder.CreateSelect(
		isLeftNaN,
		quietNaN(context, left, quietNaNMask),
		irBuilder.CreateSelect(
			isRightNaN,
			quietNaN(context, right, quietNaNMask),
			irBuilder.CreateSelect(
				isLeftLessThanRight,
				left,
				irBuilder.CreateSelect(
					isLeftGreaterThanRight,
					right,
					irBuilder.CreateBitCast(
						// If the numbers compare as equal, they may be zero with different signs.
						// Do a bitwise or of the pair to ensure that if either is negative, the
						// result will be negative.
						irBuilder.CreateOr(irBuilder.CreateBitCast(left, intType),
										   irBuilder.CreateBitCast(right, intType)),
						floatType)))));
}

static llvm::Value* emitFloatMax(EmitFunctionContext& context,
								 llvm::Value* left,
								 llvm::Value* right,
								 llvm::Type* intType,
								 llvm::Value* quietNaNMask)
{
	llvm::IRBuilder<>& irBuilder = context.irBuilder;
	llvm::Type* floatType = left->getType();
	llvm::Value* isLeftNaN
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_UNO, left, left);
	llvm::Value* isRightNaN
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_UNO, right, right);
	llvm::Value* isLeftLessThanRight
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OLT, left, right);
	llvm::Value* isLeftGreaterThanRight
		= createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OGT, left, right);

	return irBuilder.CreateSelect(
		isLeftNaN,
		quietNaN(context, left, quietNaNMask),
		irBuilder.CreateSelect(
			isRightNaN,
			quietNaN(context, right, quietNaNMask),
			irBuilder.CreateSelect(
				isLeftLessThanRight,
				right,
				irBuilder.CreateSelect(
					isLeftGreaterThanRight,
					left,
					irBuilder.CreateBitCast(
						// If the numbers compare as equal, they may be zero with different signs.
						// Do a bitwise and of the pair to ensure that if either is positive, the
						// result will be positive.
						irBuilder.CreateAnd(irBuilder.CreateBitCast(left, intType),
											irBuilder.CreateBitCast(right, intType)),
						floatType)))));
}

EMIT_FP_BINARY_OP(
	min,
	emitFloatMin(*this,
				 left,
				 right,
				 type == ValueType::f32 ? llvmContext.i32Type : llvmContext.i64Type,
				 type == ValueType::f32
					 ? emitLiteral(llvmContext, FloatComponents<F32>::canonicalSignificand)
					 : emitLiteral(llvmContext, FloatComponents<F64>::canonicalSignificand)))
EMIT_FP_BINARY_OP(
	max,
	emitFloatMax(*this,
				 left,
				 right,
				 type == ValueType::f32 ? llvmContext.i32Type : llvmContext.i64Type,
				 type == ValueType::f32
					 ? emitLiteral(llvmContext, FloatComponents<F32>::canonicalSignificand)
					 : emitLiteral(llvmContext, FloatComponents<F64>::canonicalSignificand)))
EMIT_FP_UNARY_OP(ceil, callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::ceil, {operand}))
EMIT_FP_UNARY_OP(floor, callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::floor, {operand}))
EMIT_FP_UNARY_OP(trunc, callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::trunc, {operand}))
EMIT_FP_UNARY_OP(nearest, callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::rint, {operand}))

EMIT_SIMD_INT_BINARY_OP(add, irBuilder.CreateAdd(left, right))
EMIT_SIMD_INT_BINARY_OP(sub, irBuilder.CreateSub(left, right))

#define EMIT_SIMD_SHIFT_OP(name, llvmType, createShift)                                            \
	void EmitFunctionContext::name(IR::NoImm)                                                      \
	{                                                                                              \
		FixedVectorType* vectorType = llvmType;                                                    \
		llvm::Type* scalarType = llvmType->getScalarType();                                        \
		WAVM_SUPPRESS_UNUSED(vectorType);                                                          \
		auto right = irBuilder.CreateVectorSplat(                                                  \
			(unsigned int)vectorType->getNumElements(),                                            \
			irBuilder.CreateZExtOrTrunc(                                                           \
				emitShiftCountMask(*this, pop(), scalarType->getIntegerBitWidth()), scalarType));  \
		WAVM_SUPPRESS_UNUSED(right);                                                               \
		auto left = irBuilder.CreateBitCast(pop(), llvmType);                                      \
		WAVM_SUPPRESS_UNUSED(left);                                                                \
		auto result = createShift(left, right);                                                    \
		push(result);                                                                              \
	}

#define EMIT_SIMD_SHIFT(name, emitCode)                                                            \
	EMIT_SIMD_SHIFT_OP(i8x16_##name, llvmContext.i8x16Type, emitCode)                              \
	EMIT_SIMD_SHIFT_OP(i16x8_##name, llvmContext.i16x8Type, emitCode)                              \
	EMIT_SIMD_SHIFT_OP(i32x4_##name, llvmContext.i32x4Type, emitCode)                              \
	EMIT_SIMD_SHIFT_OP(i64x2_##name, llvmContext.i64x2Type, emitCode)

EMIT_SIMD_SHIFT(shl, irBuilder.CreateShl)
EMIT_SIMD_SHIFT(shr_s, irBuilder.CreateAShr)
EMIT_SIMD_SHIFT(shr_u, irBuilder.CreateLShr)

EMIT_SIMD_BINARY_OP(i16x8_mul, llvmContext.i16x8Type, irBuilder.CreateMul(left, right))
EMIT_SIMD_BINARY_OP(i32x4_mul, llvmContext.i32x4Type, irBuilder.CreateMul(left, right))
EMIT_SIMD_BINARY_OP(i64x2_mul, llvmContext.i64x2Type, irBuilder.CreateMul(left, right))

#define EMIT_INT_COMPARE_OP(name, llvmOperandType, llvmDestType, pred, zextOrSext)                 \
	void EmitFunctionContext::name(IR::NoImm)                                                      \
	{                                                                                              \
		auto right = irBuilder.CreateBitCast(pop(), llvmOperandType);                              \
		auto left = irBuilder.CreateBitCast(pop(), llvmOperandType);                               \
		push(zextOrSext(createICmpWithWorkaround(irBuilder, pred, left, right), llvmDestType));    \
	}

#define EMIT_INT_COMPARE_U(name, pred)                                                             \
	EMIT_INT_COMPARE_OP(i32_##name, llvmContext.i32Type, llvmContext.i32Type, pred, zext)          \
	EMIT_INT_COMPARE_OP(i64_##name, llvmContext.i64Type, llvmContext.i32Type, pred, zext)          \
	EMIT_INT_COMPARE_OP(i8x16_##name, llvmContext.i8x16Type, llvmContext.i8x16Type, pred, sext)    \
	EMIT_INT_COMPARE_OP(i16x8_##name, llvmContext.i16x8Type, llvmContext.i16x8Type, pred, sext)    \
	EMIT_INT_COMPARE_OP(i32x4_##name, llvmContext.i32x4Type, llvmContext.i32x4Type, pred, sext)    \
	/* WebAssembly doesn't define unsigned compare ops for i64x2 */

#define EMIT_INT_COMPARE_S(name, pred)                                                             \
	EMIT_INT_COMPARE_U(name, pred)                                                                 \
	EMIT_INT_COMPARE_OP(i64x2_##name, llvmContext.i64x2Type, llvmContext.i64x2Type, pred, sext)

EMIT_INT_COMPARE_S(eq, llvm::CmpInst::ICMP_EQ)
EMIT_INT_COMPARE_S(ne, llvm::CmpInst::ICMP_NE)
EMIT_INT_COMPARE_S(lt_s, llvm::CmpInst::ICMP_SLT)
EMIT_INT_COMPARE_U(lt_u, llvm::CmpInst::ICMP_ULT)
EMIT_INT_COMPARE_S(le_s, llvm::CmpInst::ICMP_SLE)
EMIT_INT_COMPARE_U(le_u, llvm::CmpInst::ICMP_ULE)
EMIT_INT_COMPARE_S(gt_s, llvm::CmpInst::ICMP_SGT)
EMIT_INT_COMPARE_U(gt_u, llvm::CmpInst::ICMP_UGT)
EMIT_INT_COMPARE_S(ge_s, llvm::CmpInst::ICMP_SGE)
EMIT_INT_COMPARE_U(ge_u, llvm::CmpInst::ICMP_UGE)

EMIT_SIMD_INT_UNARY_OP(neg, irBuilder.CreateNeg(operand))

static llvm::Value* emitAddUnsignedSaturated(llvm::IRBuilder<>& irBuilder,
											 llvm::Value* left,
											 llvm::Value* right,
											 llvm::Type* type)
{
	left = irBuilder.CreateBitCast(left, type);
	right = irBuilder.CreateBitCast(right, type);
	llvm::Value* add = irBuilder.CreateAdd(left, right);
	return irBuilder.CreateSelect(
		irBuilder.CreateICmpUGT(left, add), llvm::Constant::getAllOnesValue(left->getType()), add);
}

static llvm::Value* emitSubUnsignedSaturated(llvm::IRBuilder<>& irBuilder,
											 llvm::Value* left,
											 llvm::Value* right,
											 llvm::Type* type)
{
	left = irBuilder.CreateBitCast(left, type);
	right = irBuilder.CreateBitCast(right, type);
	return irBuilder.CreateSub(
		irBuilder.CreateSelect(
			createICmpWithWorkaround(irBuilder, llvm::CmpInst::ICMP_UGT, left, right), left, right),
		right);
}

EMIT_SIMD_BINARY_OP(i8x16_add_sat_u,
					llvmContext.i8x16Type,
					emitAddUnsignedSaturated(irBuilder, left, right, llvmContext.i8x16Type))
EMIT_SIMD_BINARY_OP(i8x16_sub_sat_u,
					llvmContext.i8x16Type,
					emitSubUnsignedSaturated(irBuilder, left, right, llvmContext.i8x16Type))
EMIT_SIMD_BINARY_OP(i16x8_add_sat_u,
					llvmContext.i16x8Type,
					emitAddUnsignedSaturated(irBuilder, left, right, llvmContext.i16x8Type))
EMIT_SIMD_BINARY_OP(i16x8_sub_sat_u,
					llvmContext.i16x8Type,
					emitSubUnsignedSaturated(irBuilder, left, right, llvmContext.i16x8Type))

#if LLVM_VERSION_MAJOR >= 8
EMIT_SIMD_BINARY_OP(i8x16_add_sat_s,
					llvmContext.i8x16Type,
					callLLVMIntrinsic({llvmContext.i8x16Type},
									  llvm::Intrinsic::sadd_sat,
									  {left, right}))
EMIT_SIMD_BINARY_OP(i8x16_sub_sat_s,
					llvmContext.i8x16Type,
					callLLVMIntrinsic({llvmContext.i8x16Type},
									  llvm::Intrinsic::ssub_sat,
									  {left, right}))
EMIT_SIMD_BINARY_OP(i16x8_add_sat_s,
					llvmContext.i16x8Type,
					callLLVMIntrinsic({llvmContext.i16x8Type},
									  llvm::Intrinsic::sadd_sat,
									  {left, right}))
EMIT_SIMD_BINARY_OP(i16x8_sub_sat_s,
					llvmContext.i16x8Type,
					callLLVMIntrinsic({llvmContext.i16x8Type},
									  llvm::Intrinsic::ssub_sat,
									  {left, right}))
#else
EMIT_SIMD_BINARY_OP(i8x16_add_sat_s,
					llvmContext.i8x16Type,
					callLLVMIntrinsic({}, llvm::Intrinsic::x86_sse2_padds_b, {left, right}))
EMIT_SIMD_BINARY_OP(i8x16_sub_sat_s,
					llvmContext.i8x16Type,
					callLLVMIntrinsic({}, llvm::Intrinsic::x86_sse2_psubs_b, {left, right}))
EMIT_SIMD_BINARY_OP(i16x8_add_sat_s,
					llvmContext.i16x8Type,
					callLLVMIntrinsic({}, llvm::Intrinsic::x86_sse2_padds_w, {left, right}))
EMIT_SIMD_BINARY_OP(i16x8_sub_sat_s,
					llvmContext.i16x8Type,
					callLLVMIntrinsic({}, llvm::Intrinsic::x86_sse2_psubs_w, {left, right}))
#endif

#define EMIT_SIMD_INT_BINARY_OP_NO64(name, emitCode)                                               \
	EMIT_SIMD_BINARY_OP(i8x16##_##name, llvmContext.i8x16Type, emitCode)                           \
	EMIT_SIMD_BINARY_OP(i16x8##_##name, llvmContext.i16x8Type, emitCode)                           \
	EMIT_SIMD_BINARY_OP(i32x4##_##name, llvmContext.i32x4Type, emitCode)

EMIT_SIMD_INT_BINARY_OP_NO64(min_s,
							 irBuilder.CreateSelect(irBuilder.CreateICmpSLT(left, right),
													left,
													right))
EMIT_SIMD_INT_BINARY_OP_NO64(min_u,
							 irBuilder.CreateSelect(irBuilder.CreateICmpULT(left, right),
													left,
													right))
EMIT_SIMD_INT_BINARY_OP_NO64(max_s,
							 irBuilder.CreateSelect(irBuilder.CreateICmpSLT(left, right),
													right,
													left))
EMIT_SIMD_INT_BINARY_OP_NO64(max_u,
							 irBuilder.CreateSelect(irBuilder.CreateICmpULT(left, right),
													right,
													left))

llvm::Value* EmitFunctionContext::emitBitSelect(llvm::Value* mask,
												llvm::Value* trueValue,
												llvm::Value* falseValue)
{
	return irBuilder.CreateOr(irBuilder.CreateAnd(trueValue, mask),
							  irBuilder.CreateAnd(falseValue, irBuilder.CreateNot(mask)));
}

llvm::Value* EmitFunctionContext::emitVectorSelect(llvm::Value* condition,
												   llvm::Value* trueValue,
												   llvm::Value* falseValue)
{
	WAVM_ASSERT(condition->getType()->isVectorTy());

	llvm::Type* maskScalarType;
	switch(trueValue->getType()->getScalarSizeInBits())
	{
	case 64: maskScalarType = llvmContext.i64Type; break;
	case 32: maskScalarType = llvmContext.i32Type; break;
	case 16: maskScalarType = llvmContext.i16Type; break;
	case 8: maskScalarType = llvmContext.i8Type; break;
	default: WAVM_UNREACHABLE();
	};
	const U32 numElements
		= U32(static_cast<FixedVectorType*>(condition->getType())->getNumElements());
	llvm::Type* maskType = FixedVectorType::get(maskScalarType, numElements);
	llvm::Value* mask = sext(condition, maskType);

	return irBuilder.CreateBitCast(emitBitSelect(mask,
												 irBuilder.CreateBitCast(trueValue, maskType),
												 irBuilder.CreateBitCast(falseValue, maskType)),
								   trueValue->getType());
}

EMIT_SIMD_FP_BINARY_OP(add, irBuilder.CreateFAdd(left, right))
EMIT_SIMD_FP_BINARY_OP(sub, irBuilder.CreateFSub(left, right))
EMIT_SIMD_FP_BINARY_OP(mul, irBuilder.CreateFMul(left, right))
EMIT_SIMD_FP_BINARY_OP(div, irBuilder.CreateFDiv(left, right))

//
// SIMD minimum
//

EMIT_SIMD_BINARY_OP(f32x4_min,
					llvmContext.f32x4Type,
					emitFloatMin(*this,
								 left,
								 right,
								 llvmContext.i32x4Type,
								 irBuilder.CreateVectorSplat(
									 4,
									 emitLiteral(llvmContext,
												 FloatComponents<F32>::canonicalSignificand))))
EMIT_SIMD_BINARY_OP(f64x2_min,
					llvmContext.f64x2Type,
					emitFloatMin(*this,
								 left,
								 right,
								 llvmContext.i64x2Type,
								 irBuilder.CreateVectorSplat(
									 2,
									 emitLiteral(llvmContext,
												 FloatComponents<F64>::canonicalSignificand))))

//
// SIMD maximum
//

EMIT_SIMD_BINARY_OP(f32x4_max,
					llvmContext.f32x4Type,
					emitFloatMax(*this,
								 left,
								 right,
								 llvmContext.i32x4Type,
								 irBuilder.CreateVectorSplat(
									 4,
									 emitLiteral(llvmContext,
												 FloatComponents<F32>::canonicalSignificand))))
EMIT_SIMD_BINARY_OP(f64x2_max,
					llvmContext.f64x2Type,
					emitFloatMax(*this,
								 left,
								 right,
								 llvmContext.i64x2Type,
								 irBuilder.CreateVectorSplat(
									 2,
									 emitLiteral(llvmContext,
												 FloatComponents<F64>::canonicalSignificand))))

//
// SIMD pseudo-minimum: right < left ? right : left
//

EMIT_SIMD_BINARY_OP(f32x4_pmin,
					llvmContext.f32x4Type,
					irBuilder.CreateSelect(
						createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OLT, right, left),
						right,
						left))
EMIT_SIMD_BINARY_OP(f64x2_pmin,
					llvmContext.f64x2Type,
					irBuilder.CreateSelect(
						createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OLT, right, left),
						right,
						left))

//
// SIMD pseudo-maximum: left < right ? right : left
//

EMIT_SIMD_BINARY_OP(f32x4_pmax,
					llvmContext.f32x4Type,
					irBuilder.CreateSelect(
						createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OLT, left, right),
						right,
						left))
EMIT_SIMD_BINARY_OP(f64x2_pmax,
					llvmContext.f64x2Type,
					irBuilder.CreateSelect(
						createFCmpWithWorkaround(irBuilder, llvm::CmpInst::FCMP_OLT, left, right),
						right,
						left))

EMIT_SIMD_FP_UNARY_OP(neg, irBuilder.CreateFNeg(operand))
EMIT_SIMD_FP_UNARY_OP(abs,
					  callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::fabs, {operand}))
EMIT_SIMD_FP_UNARY_OP(sqrt,
					  callLLVMIntrinsic({operand->getType()}, llvm::Intrinsic::sqrt, {operand}))

void EmitFunctionContext::v128_any_true(IR::NoImm)
{
	llvm::Value* vector = pop();

	vector = irBuilder.CreateBitCast(vector, llvmContext.i64x2Type);

	llvm::Constant* zero = emitLiteral(llvmContext, U64(0));

	llvm::Value* boolResult = irBuilder.CreateOr(
		irBuilder.CreateICmpNE(irBuilder.CreateExtractElement(vector, U64(0)), zero),
		irBuilder.CreateICmpNE(irBuilder.CreateExtractElement(vector, U64(1)), zero));
	llvm::Value* i32Result
		= irBuilder.CreateZExt(boolResult, llvm::Type::getInt32Ty(irBuilder.getContext()));
	push(i32Result);
}

static llvm::Value* emitAllTrue(llvm::IRBuilder<>& irBuilder,
								llvm::Value* vector,
								FixedVectorType* vectorType)
{
	vector = irBuilder.CreateBitCast(vector, vectorType);

	const U32 numScalarBits = vectorType->getScalarSizeInBits();
	const Uptr numLanes = vectorType->getNumElements();
	llvm::Constant* zero
		= llvm::ConstantInt::get(vectorType->getScalarType(), llvm::APInt(numScalarBits, 0));

	llvm::Value* result = nullptr;
	for(Uptr laneIndex = 0; laneIndex < numLanes; ++laneIndex)
	{
		llvm::Value* scalar = irBuilder.CreateExtractElement(vector, laneIndex);
		llvm::Value* scalarBool = irBuilder.CreateICmpNE(scalar, zero);

		result = result ? irBuilder.CreateAnd(result, scalarBool) : scalarBool;
	}
	return irBuilder.CreateZExt(result, llvm::Type::getInt32Ty(irBuilder.getContext()));
}

EMIT_SIMD_UNARY_OP(i8x16_all_true,
				   llvmContext.i8x16Type,
				   emitAllTrue(irBuilder, operand, llvmContext.i8x16Type))
EMIT_SIMD_UNARY_OP(i16x8_all_true,
				   llvmContext.i16x8Type,
				   emitAllTrue(irBuilder, operand, llvmContext.i16x8Type))
EMIT_SIMD_UNARY_OP(i32x4_all_true,
				   llvmContext.i32x4Type,
				   emitAllTrue(irBuilder, operand, llvmContext.i32x4Type))
EMIT_SIMD_UNARY_OP(i64x2_all_true,
				   llvmContext.i64x2Type,
				   emitAllTrue(irBuilder, operand, llvmContext.i64x2Type))

void EmitFunctionContext::v128_and(IR::NoImm)
{
	auto right = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	auto left = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	push(irBuilder.CreateAnd(left, right));
}
void EmitFunctionContext::v128_or(IR::NoImm)
{
	auto right = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	auto left = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	push(irBuilder.CreateOr(left, right));
}
void EmitFunctionContext::v128_xor(IR::NoImm)
{
	auto right = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	auto left = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	push(irBuilder.CreateXor(left, right));
}
void EmitFunctionContext::v128_not(IR::NoImm)
{
	auto operand = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	push(irBuilder.CreateNot(operand));
}
void EmitFunctionContext::v128_andnot(IR::NoImm)
{
	auto right = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	auto left = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	push(irBuilder.CreateAnd(left, irBuilder.CreateNot(right)));
}

//
// SIMD extract_lane
//

#define EMIT_SIMD_EXTRACT_LANE_OP(name, llvmType, numLanes, coerceScalar)                          \
	void EmitFunctionContext::name(IR::LaneIndexImm<numLanes> imm)                                 \
	{                                                                                              \
		auto operand = irBuilder.CreateBitCast(pop(), llvmType);                                   \
		auto scalar = irBuilder.CreateExtractElement(operand, imm.laneIndex);                      \
		push(coerceScalar);                                                                        \
	}
EMIT_SIMD_EXTRACT_LANE_OP(i8x16_extract_lane_s,
						  llvmContext.i8x16Type,
						  16,
						  sext(scalar, llvmContext.i32Type))
EMIT_SIMD_EXTRACT_LANE_OP(i8x16_extract_lane_u,
						  llvmContext.i8x16Type,
						  16,
						  zext(scalar, llvmContext.i32Type))
EMIT_SIMD_EXTRACT_LANE_OP(i16x8_extract_lane_s,
						  llvmContext.i16x8Type,
						  8,
						  sext(scalar, llvmContext.i32Type))
EMIT_SIMD_EXTRACT_LANE_OP(i16x8_extract_lane_u,
						  llvmContext.i16x8Type,
						  8,
						  zext(scalar, llvmContext.i32Type))
EMIT_SIMD_EXTRACT_LANE_OP(i32x4_extract_lane, llvmContext.i32x4Type, 4, scalar)
EMIT_SIMD_EXTRACT_LANE_OP(i64x2_extract_lane, llvmContext.i64x2Type, 2, scalar)

EMIT_SIMD_EXTRACT_LANE_OP(f32x4_extract_lane, llvmContext.f32x4Type, 4, scalar)
EMIT_SIMD_EXTRACT_LANE_OP(f64x2_extract_lane, llvmContext.f64x2Type, 2, scalar)

//
// SIMD replace_lane
//

#define EMIT_SIMD_REPLACE_LANE_OP(typePrefix, llvmType, numLanes, coerceScalar)                    \
	void EmitFunctionContext::typePrefix##_replace_lane(IR::LaneIndexImm<numLanes> imm)            \
	{                                                                                              \
		auto scalar = pop();                                                                       \
		auto vector = irBuilder.CreateBitCast(pop(), llvmType);                                    \
		push(irBuilder.CreateInsertElement(vector, coerceScalar, imm.laneIndex));                  \
	}

EMIT_SIMD_REPLACE_LANE_OP(i8x16, llvmContext.i8x16Type, 16, trunc(scalar, llvmContext.i8Type))
EMIT_SIMD_REPLACE_LANE_OP(i16x8, llvmContext.i16x8Type, 8, trunc(scalar, llvmContext.i16Type))
EMIT_SIMD_REPLACE_LANE_OP(i32x4, llvmContext.i32x4Type, 4, scalar)
EMIT_SIMD_REPLACE_LANE_OP(i64x2, llvmContext.i64x2Type, 2, scalar)

EMIT_SIMD_REPLACE_LANE_OP(f32x4, llvmContext.f32x4Type, 4, scalar)
EMIT_SIMD_REPLACE_LANE_OP(f64x2, llvmContext.f64x2Type, 2, scalar)

void EmitFunctionContext::i8x16_swizzle(NoImm)
{
	auto indexVector = irBuilder.CreateBitCast(pop(), llvmContext.i8x16Type);
	auto elementVector = irBuilder.CreateBitCast(pop(), llvmContext.i8x16Type);

	if(moduleContext.targetArch == llvm::Triple::x86_64
	   || moduleContext.targetArch == llvm::Triple::x86)
	{
		// WASM defines any out-of-range index to write zero to the output vector, but x86 pshufb
		// just uses the index modulo 16, and only writes zero if the MSB of the index is 1. Do a
		// saturated add of 112 to set the MSB in any index >= 16 while leaving the index modulo 16
		// unchanged.
		auto constant112 = llvm::ConstantInt::get(llvmContext.i8Type, 112);
		auto saturatedIndexVector = emitAddUnsignedSaturated(
			irBuilder,
			indexVector,
			llvm::ConstantVector::getSplat(LLVM_ELEMENT_COUNT(16), constant112),
			llvmContext.i8x16Type);

		push(callLLVMIntrinsic(
			{}, llvm::Intrinsic::x86_ssse3_pshuf_b_128, {elementVector, saturatedIndexVector}));
	}
	else if(moduleContext.targetArch == llvm::Triple::aarch64)
	{
		push(callLLVMIntrinsic({llvmContext.i8x16Type},
							   llvm::Intrinsic::aarch64_neon_tbl1,
							   {elementVector, indexVector}));
	}
}

void EmitFunctionContext::i8x16_shuffle(IR::ShuffleImm<16> imm)
{
	auto right = irBuilder.CreateBitCast(pop(), llvmContext.i8x16Type);
	auto left = irBuilder.CreateBitCast(pop(), llvmContext.i8x16Type);
	LLVM_LANE_INDEX_TYPE laneIndices[16];
	for(Uptr laneIndex = 0; laneIndex < 16; ++laneIndex)
	{ laneIndices[laneIndex] = LLVM_LANE_INDEX_TYPE(imm.laneIndices[laneIndex]); }
	push(irBuilder.CreateShuffleVector(
		left, right, llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>(laneIndices, 16)));
}

void EmitFunctionContext::v128_bitselect(IR::NoImm)
{
	auto mask = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	auto falseValue = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	auto trueValue = irBuilder.CreateBitCast(pop(), llvmContext.i64x2Type);
	push(emitBitSelect(mask, trueValue, falseValue));
}

#define EMIT_SIMD_AVGR_OP(type, doubleWidthType)                                                   \
	void EmitFunctionContext::type##_avgr_u(IR::NoImm)                                             \
	{                                                                                              \
		auto right = irBuilder.CreateBitCast(pop(), llvmContext.type##Type);                       \
		auto left = irBuilder.CreateBitCast(pop(), llvmContext.type##Type);                        \
		auto rightZExt = irBuilder.CreateZExt(right, llvmContext.doubleWidthType##Type);           \
		auto leftZExt = irBuilder.CreateZExt(left, llvmContext.doubleWidthType##Type);             \
		auto oneZExt = llvm::ConstantVector::getSplat(                                             \
			LLVM_ELEMENT_COUNT(llvmContext.type##Type->getNumElements()),                          \
			llvm::ConstantInt::get(llvmContext.doubleWidthType##Type->getElementType(), 1));       \
		push(irBuilder.CreateTrunc(                                                                \
			irBuilder.CreateLShr(                                                                  \
				irBuilder.CreateAdd(irBuilder.CreateAdd(leftZExt, rightZExt), oneZExt), oneZExt),  \
			llvmContext.type##Type));                                                              \
	}

EMIT_SIMD_AVGR_OP(i8x16, i16x16)
EMIT_SIMD_AVGR_OP(i16x8, i32x8)

// SIMD integer absolute

#define EMIT_SIMD_INT_ABS_OP(type)                                                                 \
	void EmitFunctionContext::type##_abs(IR::NoImm)                                                \
	{                                                                                              \
		auto operand = irBuilder.CreateBitCast(pop(), llvmContext.type##Type);                     \
		push(irBuilder.CreateSelect(                                                               \
			irBuilder.CreateICmpSLT(operand,                                                       \
									llvm::Constant::getNullValue(llvmContext.type##Type)),         \
			irBuilder.CreateNeg(operand),                                                          \
			operand));                                                                             \
	}

EMIT_SIMD_INT_ABS_OP(i8x16)
EMIT_SIMD_INT_ABS_OP(i16x8)
EMIT_SIMD_INT_ABS_OP(i32x4)
EMIT_SIMD_INT_ABS_OP(i64x2)

//
// SIMD extending integer multiplication
//

#define EMIT_SIMD_EXTMUL(destType, _highlow_, sourceType, _su, baseSourceElementIndex, extend)     \
	void EmitFunctionContext::destType##_extmul##_highlow_##sourceType##_su(NoImm)                 \
	{                                                                                              \
		llvm::Value* right = pop();                                                                \
		llvm::Value* left = pop();                                                                 \
		left = irBuilder.CreateBitCast(left, llvmContext.sourceType##Type);                        \
		right = irBuilder.CreateBitCast(right, llvmContext.sourceType##Type);                      \
		left = extendHalfOfIntVector(left, baseSourceElementIndex, &EmitFunctionContext::extend);  \
		right                                                                                      \
			= extendHalfOfIntVector(right, baseSourceElementIndex, &EmitFunctionContext::extend);  \
		llvm::Value* result = irBuilder.CreateMul(left, right);                                    \
		push(result);                                                                              \
	}

#define EMIT_SIMD_EXTMUL_HIGHLOW_SU(destType, sourceType, numDestElements)                         \
	EMIT_SIMD_EXTMUL(destType, _high_, sourceType, _s, numDestElements, sext)                      \
	EMIT_SIMD_EXTMUL(destType, _high_, sourceType, _u, numDestElements, zext)                      \
	EMIT_SIMD_EXTMUL(destType, _low_, sourceType, _s, 0, sext)                                     \
	EMIT_SIMD_EXTMUL(destType, _low_, sourceType, _u, 0, zext)

EMIT_SIMD_EXTMUL_HIGHLOW_SU(i16x8, i8x16, 8)
EMIT_SIMD_EXTMUL_HIGHLOW_SU(i32x4, i16x8, 4)
EMIT_SIMD_EXTMUL_HIGHLOW_SU(i64x2, i32x4, 2)

//
// SIMD extending integer addition
//

#define EMIT_SIMD_EXTADD(type, halfType, _su, numOutputElements, extend)                           \
	void EmitFunctionContext::type##_extadd_pairwise_##halfType##_su(NoImm)                        \
	{                                                                                              \
		llvm::Value* vector = pop();                                                               \
		vector = irBuilder.CreateBitCast(vector, llvmContext.halfType##Type);                      \
		LLVM_LANE_INDEX_TYPE evenMask[numOutputElements];                                          \
		LLVM_LANE_INDEX_TYPE oddMask[numOutputElements];                                           \
		for(int elementIndex = 0; elementIndex < numOutputElements; ++elementIndex)                \
		{                                                                                          \
			evenMask[elementIndex] = elementIndex * 2 + 0;                                         \
			oddMask[elementIndex] = elementIndex * 2 + 1;                                          \
		}                                                                                          \
		llvm::Constant* undefVector = llvm::UndefValue::get(vector->getType());                    \
		llvm::Value* evenVector = irBuilder.CreateShuffleVector(                                   \
			vector,                                                                                \
			undefVector,                                                                           \
			llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>(evenMask, numOutputElements));                    \
		llvm::Value* oddVector = irBuilder.CreateShuffleVector(                                    \
			vector,                                                                                \
			undefVector,                                                                           \
			llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>(oddMask, numOutputElements));                     \
		llvm::Value* result = irBuilder.CreateAdd(extend(evenVector, llvmContext.type##Type),      \
												  extend(oddVector, llvmContext.type##Type));      \
		push(result);                                                                              \
	}

EMIT_SIMD_EXTADD(i16x8, i8x16, _s, 8, sext)
EMIT_SIMD_EXTADD(i16x8, i8x16, _u, 8, zext)
EMIT_SIMD_EXTADD(i32x4, i16x8, _s, 4, sext)
EMIT_SIMD_EXTADD(i32x4, i16x8, _u, 4, zext)

//
// SIMD dot product
//

void EmitFunctionContext::i32x4_dot_i16x8_s(NoImm)
{
	llvm::Value* right = irBuilder.CreateBitCast(pop(), llvmContext.i16x8Type);
	llvm::Value* left = irBuilder.CreateBitCast(pop(), llvmContext.i16x8Type);

	left = irBuilder.CreateSExt(left, llvmContext.i32x8Type);
	right = irBuilder.CreateSExt(right, llvmContext.i32x8Type);

	llvm::Value* product = irBuilder.CreateMul(left, right);

	constexpr LLVM_LANE_INDEX_TYPE numOutputElements = 4;
	LLVM_LANE_INDEX_TYPE evenMask[numOutputElements];
	LLVM_LANE_INDEX_TYPE oddMask[numOutputElements];
	for(LLVM_LANE_INDEX_TYPE elementIndex = 0; elementIndex < numOutputElements; ++elementIndex)
	{
		evenMask[elementIndex] = elementIndex * 2 + 0;
		oddMask[elementIndex] = elementIndex * 2 + 1;
	}
	llvm::Constant* undefVector = llvm::UndefValue::get(llvmContext.i32x8Type);
	llvm::Value* evenVector = irBuilder.CreateShuffleVector(
		product, undefVector, llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>(evenMask, numOutputElements));
	llvm::Value* oddVector = irBuilder.CreateShuffleVector(
		product, undefVector, llvm::ArrayRef<LLVM_LANE_INDEX_TYPE>(oddMask, numOutputElements));

	llvm::Value* result = irBuilder.CreateAdd(evenVector, oddVector);

	push(result);
}

//
// SIMD saturating integer Q-format rounding multiplication.
//

void EmitFunctionContext::i16x8_q15mulr_sat_s(NoImm)
{
	llvm::Value* right = pop();
	llvm::Value* left = pop();
	left = irBuilder.CreateBitCast(left, llvmContext.i16x8Type);
	right = irBuilder.CreateBitCast(right, llvmContext.i16x8Type);

	// Extend the inputs to 64-bit to avoid overflow.
	left = irBuilder.CreateSExt(left, llvmContext.i64x8Type);
	right = irBuilder.CreateSExt(right, llvmContext.i64x8Type);

	// result = saturateS16((left * right + 0x4000) >> 15)
	llvm::Value* product = irBuilder.CreateMul(left, right);
	llvm::Value* sum = irBuilder.CreateAdd(
		product, irBuilder.CreateVectorSplat(8, emitLiteral(llvmContext, U64(0x4000))));
	llvm::Value* shift = irBuilder.CreateAShr(sum, 15);
	llvm::Value* minSplat
		= irBuilder.CreateVectorSplat(8, emitLiteral(llvmContext, I64(INT16_MIN)));
	llvm::Value* maxSplat
		= irBuilder.CreateVectorSplat(8, emitLiteral(llvmContext, I64(INT16_MAX)));
	llvm::Value* saturate = irBuilder.CreateSelect(
		irBuilder.CreateICmpSGT(shift, maxSplat),
		maxSplat,
		irBuilder.CreateSelect(irBuilder.CreateICmpSLT(shift, minSplat), minSplat, shift));
	llvm::Value* result = irBuilder.CreateTrunc(saturate, llvmContext.i16x8Type);
	push(result);
}
