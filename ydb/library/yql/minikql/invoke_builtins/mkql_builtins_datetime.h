#pragma once

#include "mkql_builtins_impl.h"  // Y_IGNORE // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

using TScaledDate = i64;

constexpr TScaledDate DateScale = 86400000000ll;
constexpr TScaledDate DatetimeScale = 1000000ll;

template<typename TSrc> inline
TScaledDate ToScaledDate(typename TSrc::TLayout src);

template<typename TDst> inline
typename TDst::TLayout FromScaledDate(TScaledDate src);


template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDate>>(typename NUdf::TDataType<NUdf::TDate>::TLayout src) {
    return src * DateScale;
}

template<> inline
NUdf::TDataType<NUdf::TDate>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDate>>(TScaledDate src) {
    return src / DateScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDatetime>>(typename NUdf::TDataType<NUdf::TDatetime>::TLayout src) {
    return src * DatetimeScale;
}

template<> inline
NUdf::TDataType<NUdf::TDatetime>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDatetime>>(TScaledDate src) {
    return src / DatetimeScale;
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
    return src * DateScale;
}

template<> inline
NUdf::TDataType<NUdf::TTzDate>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDate>>(TScaledDate src) {
    return src / DateScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(typename NUdf::TDataType<NUdf::TTzDatetime>::TLayout src) {
    return src * DatetimeScale;
}

template<> inline
NUdf::TDataType<NUdf::TTzDatetime>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(TScaledDate src) {
    return src / DatetimeScale;
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
    return src * DateScale;
}

template<> inline
NUdf::TDataType<NUdf::TDate32>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDate32>>(TScaledDate src) {
    return src / DateScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(typename NUdf::TDataType<NUdf::TDatetime64>::TLayout src) {
    return src * DatetimeScale;
}

template<> inline
NUdf::TDataType<NUdf::TDatetime64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(TScaledDate src) {
    return src / DatetimeScale;
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
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDate32>>(typename NUdf::TDataType<NUdf::TTzDate32>::TLayout src) {
    return src * DateScale;
}

template<> inline
NUdf::TDataType<NUdf::TTzDate32>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDate32>>(TScaledDate src) {
    return src / DateScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>(typename NUdf::TDataType<NUdf::TTzDatetime64>::TLayout src) {
    return src * DatetimeScale;
}

template<> inline
NUdf::TDataType<NUdf::TTzDatetime64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>(TScaledDate src) {
    return src / DatetimeScale;
}

template<> inline
TScaledDate ToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>(typename NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout src) {
    return src;
}

template<> inline
NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>(TScaledDate src) {
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

template<typename TDateType>
inline bool IsBadDateTime(TScaledDate val) {
    static_assert(TDateType::Features & (NYql::NUdf::DateType | NYql::NUdf::TzDateType), "Date type expected");
    if constexpr (TDateType::Features & NYql::NUdf::BigDateType) {
        return val < NUdf::MIN_TIMESTAMP64 || val > NUdf::MAX_TIMESTAMP64;
    } else {
        return val < 0 || val >= TScaledDate(NUdf::MAX_TIMESTAMP);
    }
}

template<typename TDateType>
inline bool IsBadInterval(TScaledDate val) {
    static_assert(TDateType::Features & NYql::NUdf::TimeIntervalType, "Interval type expected");
    if constexpr (TDateType::Features & NYql::NUdf::BigDateType) {
        return val < -NUdf::MAX_INTERVAL64 || val > NUdf::MAX_INTERVAL64;
    } else {
        return val <= -TScaledDate(NUdf::MAX_TIMESTAMP) || val >= TScaledDate(NUdf::MAX_TIMESTAMP);
    }
}

template<typename TDateType>
inline bool IsBadScaledDate(TScaledDate val) {
    if constexpr (TDateType::Features & NYql::NUdf::TimeIntervalType) {
        return IsBadInterval<TDateType>(val);
    } else {
        return IsBadDateTime<TDateType>(val);
    }
}

#ifndef MKQL_DISABLE_CODEGEN

template<typename TLayout>
inline Value* GenIsInt64Overflow(Value* value, LLVMContext &context, BasicBlock* block) {
    if constexpr (std::is_same_v<ui64, TLayout>) {
        const auto i64Max = ConstantInt::get(value->getType(), static_cast<ui64>(std::numeric_limits<i64>::max()));
        return CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, value, i64Max, "ugt", block);
    } else {
        return ConstantInt::getFalse(context);
    }
}

template<typename TDateType>
inline Value* GenIsBadDateTime(Value* val, LLVMContext &context, BasicBlock* block) {
    static_assert(TDateType::Features & (NYql::NUdf::DateType | NYql::NUdf::TzDateType), "Date type expected");
    if constexpr (TDateType::Features & NYql::NUdf::BigDateType) {
        auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, ConstantInt::get(Type::getInt64Ty(context), NUdf::MIN_TIMESTAMP64), "lt", block);
        auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, ConstantInt::get(Type::getInt64Ty(context), NUdf::MAX_TIMESTAMP64), "ge", block);
        return BinaryOperator::CreateOr(lt, ge, "or", block);
    } else {
        auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, ConstantInt::get(Type::getInt64Ty(context), 0), "lt", block);
        auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, ConstantInt::get(Type::getInt64Ty(context), NUdf::MAX_TIMESTAMP), "ge", block);
        return BinaryOperator::CreateOr(lt, ge, "or", block);
    }
}

template<typename TDateType>
inline Value* GenIsBadInterval(Value* val, LLVMContext &context, BasicBlock* block) {
    static_assert(TDateType::Features & NYql::NUdf::TimeIntervalType, "Interval type expected");
    constexpr i64 lowerBound = (TDateType::Features & NYql::NUdf::BigDateType)
        ? (-NUdf::MAX_INTERVAL64 - 1)
        : -(i64)NUdf::MAX_TIMESTAMP;
    constexpr i64 upperBound = (TDateType::Features & NYql::NUdf::BigDateType)
        ? (NUdf::MAX_INTERVAL64 + 1)
        : (i64)NUdf::MAX_TIMESTAMP;
    auto le = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, val, ConstantInt::get(Type::getInt64Ty(context), lowerBound), "le", block);
    auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, ConstantInt::get(Type::getInt64Ty(context), upperBound), "ge", block);
    return BinaryOperator::CreateOr(le, ge, "or", block);
}

template<typename TDateType>
inline Value* GenIsBadScaledDate(Value* val, LLVMContext &context, BasicBlock* block) {
    if constexpr (TDateType::Features & NYql::NUdf::TimeIntervalType) {
        return GenIsBadInterval<TDateType>(val, context, block);
    } else {
        return GenIsBadDateTime<TDateType>(val, context, block);
    }
}

template<typename TSrc> inline
Value* GenToScaledDate(Value* value, LLVMContext &context, BasicBlock* block);

template<typename TDst> inline
Value* GenFromScaledDate(Value* value, LLVMContext &context, BasicBlock* block);

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDate>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDate>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DateScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDate>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DateScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDate>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDatetime>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DatetimeScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DatetimeScale), "div", block);
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
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DateScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzDate>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DateScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTzDate>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TTzDatetime>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DatetimeScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzDatetime>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DatetimeScale), "div", block);
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
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DateScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDate32>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DateScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDate32>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDatetime64>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DatetimeScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDatetime64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DatetimeScale), "div", block);
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
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTzDate32>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TTzDate32>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DateScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzDate32>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DateScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTzDate32>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TTzDatetime64>::TLayout, TScaledDate>(value, context, block);
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), DatetimeScale), "mul", block);
    return mul;
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzDatetime64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), DatetimeScale), "div", block);
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTzDatetime64>::TLayout>(div, context, block);
    return cast;
}

template<> inline
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout, TScaledDate>(value, context, block);
}

template<> inline
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTzTimestamp64>>(Value* value, LLVMContext &context, BasicBlock* block) {
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTzTimestamp64>::TLayout>(value, context, block);
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
