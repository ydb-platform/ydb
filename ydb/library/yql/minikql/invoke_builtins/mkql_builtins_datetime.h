#pragma once

#include "mkql_builtins_impl.h"

namespace NKikimr {
namespace NMiniKQL {

using TScaledDate = i64;

constexpr TScaledDate TDateScale = 86400000000ll;
constexpr TScaledDate TDatetimeScale = 1000000ll;

inline bool IsBadDateTime(TScaledDate val) {
    return val < 0 || val >= TScaledDate(NUdf::MAX_TIMESTAMP);
}

inline bool IsBadInterval(TScaledDate val) {
    return val <= -TScaledDate(NUdf::MAX_TIMESTAMP) || val >= TScaledDate(NUdf::MAX_TIMESTAMP);
}

template<typename TSrc> inline
TScaledDate ToScaledDate(typename TSrc::TLayout src);

template<typename TDst> inline
typename TDst::TLayout FromScaledDate(TScaledDate src);


template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDate>>(typename NUdf::TDataType<NUdf::TDate>::TLayout src) {
    return src * TDateScale;
}

template<> inline
NUdf::TDataType<NUdf::TDate>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDate>>(TScaledDate src) {
    return src / TDateScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDatetime>>(typename NUdf::TDataType<NUdf::TDatetime>::TLayout src) {
    return src * TDatetimeScale;
}

template<> inline
NUdf::TDataType<NUdf::TDatetime>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDatetime>>(TScaledDate src) {
    return src / TDatetimeScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTimestamp>>(typename NUdf::TDataType<NUdf::TTimestamp>::TLayout src) {
    return src;
}

template<> inline
NUdf::TDataType<NUdf::TTimestamp>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTimestamp>>(TScaledDate src) {
    return src;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TInterval>>(typename NUdf::TDataType<NUdf::TInterval>::TLayout src) {
    return src;
}

template<> inline
NUdf::TDataType<NUdf::TInterval>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TInterval>>(TScaledDate src) {
    return src;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDate>>(typename NUdf::TDataType<NUdf::TTzDate>::TLayout src) {
    return src * TDateScale;
}

template<> inline
NUdf::TDataType<NUdf::TTzDate>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDate>>(TScaledDate src) {
    return src / TDateScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(typename NUdf::TDataType<NUdf::TTzDatetime>::TLayout src) {
    return src * TDatetimeScale;
}

template<> inline
NUdf::TDataType<NUdf::TTzDatetime>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(TScaledDate src) {
    return src / TDatetimeScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>(typename NUdf::TDataType<NUdf::TTzTimestamp>::TLayout src) {
    return src;
}

template<> inline
NUdf::TDataType<NUdf::TTzTimestamp>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>(TScaledDate src) {
    return src;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDate32>>(typename NUdf::TDataType<NUdf::TDate32>::TLayout src) {
    return src * TDateScale;
}

template<> inline
NUdf::TDataType<NUdf::TDate32>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDate32>>(TScaledDate src) {
    return src / TDateScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(typename NUdf::TDataType<NUdf::TDatetime64>::TLayout src) {
    return src * TDatetimeScale;
}

template<> inline
NUdf::TDataType<NUdf::TDatetime64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(TScaledDate src) {
    return src / TDatetimeScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>(typename NUdf::TDataType<NUdf::TTimestamp64>::TLayout src) {
    return src;
}

template<> inline
NUdf::TDataType<NUdf::TTimestamp64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>(TScaledDate src) {
    return src;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TInterval64>>(typename NUdf::TDataType<NUdf::TInterval64>::TLayout src) {
    return src;
}

template<> inline
NUdf::TDataType<NUdf::TInterval64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TInterval64>>(TScaledDate src) {
    return src;
}

#ifndef MKQL_DISABLE_CODEGEN
inline Value* GenIsBadDateTime(Value* val, LLVMContext &context, BasicBlock* block) {
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, ConstantInt::get(Type::getInt64Ty(context), 0), "lt", block);
    const auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, ConstantInt::get(Type::getInt64Ty(context), NUdf::MAX_TIMESTAMP), "ge", block);
    const auto bad = BinaryOperator::CreateOr(lt, ge, "or", block);
    return bad;
}

inline Value* GenIsBadInterval(Value* val, LLVMContext &context, BasicBlock* block) {
    const auto le = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, val, ConstantInt::get(Type::getInt64Ty(context), -(i64)NUdf::MAX_TIMESTAMP), "le", block);
    const auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, ConstantInt::get(Type::getInt64Ty(context), +(i64)NUdf::MAX_TIMESTAMP), "ge", block);
    const auto bad = BinaryOperator::CreateOr(le, ge, "or", block);
    return bad;
}

template<typename TSrc> inline
Value* GenToScaledDate(Value* value, LLVMContext &context, BasicBlock* block);

template<typename TDst> inline
Value* GenFromScaledDate(Value* value, LLVMContext &context, BasicBlock* block);

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDate>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDate>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDateScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDate>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDateScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDate>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDatetime>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDatetimeScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDatetimeScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDatetime>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTimestamp>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<NUdf::TDataType<NUdf::TTimestamp>::TLayout, TScaledDate>(value, context, block);
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTimestamp>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTimestamp>::TLayout>(value, context, block);
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TInterval>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<NUdf::TDataType<NUdf::TInterval>::TLayout, TScaledDate>(value, context, block);
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TInterval>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TInterval>::TLayout>(value, context, block);
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTzDate>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TTzDate>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDateScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzDate>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDateScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTzDate>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TTzDatetime>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDatetimeScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDatetimeScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTzDatetime>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<NUdf::TDataType<NUdf::TTzTimestamp>::TLayout, TScaledDate>(value, context, block);
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzTimestamp>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTzTimestamp>::TLayout>(value, context, block);
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDate32>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDate32>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDateScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDate32>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDateScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDate32>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDatetime64>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDatetimeScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDatetimeScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDatetime64>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<NUdf::TDataType<NUdf::TTimestamp64>::TLayout, TScaledDate>(value, context, block);
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTimestamp64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTimestamp64>::TLayout>(value, context, block);
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TInterval64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<NUdf::TDataType<NUdf::TInterval64>::TLayout, TScaledDate>(value, context, block);
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TInterval64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TInterval64>::TLayout>(value, context, block);
}

#endif

}
}
