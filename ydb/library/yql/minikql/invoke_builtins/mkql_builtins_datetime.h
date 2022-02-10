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
TScaledDate ToScaledDate(TSrc src); 
 
template<typename TDst> inline 
TDst FromScaledDate(TScaledDate src); 
 
 
template<> inline 
TScaledDate ToScaledDate(NUdf::TDataType<NUdf::TDate>::TLayout src) { 
    return src * TDateScale; 
} 
 
template<> inline 
TScaledDate ToScaledDate(std::make_signed_t<NUdf::TDataType<NUdf::TDate>::TLayout> src) { 
    return src * TDateScale; 
} 
 
template<> inline 
NUdf::TDataType<NUdf::TDate>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDate>::TLayout>(TScaledDate src) { 
    return src / TDateScale; 
} 
 
template<> inline 
TScaledDate ToScaledDate(NUdf::TDataType<NUdf::TDatetime>::TLayout src) { 
    return src * TDatetimeScale; 
} 
 
template<> inline 
TScaledDate ToScaledDate(std::make_signed_t<NUdf::TDataType<NUdf::TDatetime>::TLayout> src) { 
    return src * TDatetimeScale; 
} 
 
template<> inline 
NUdf::TDataType<NUdf::TDatetime>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TDatetime>::TLayout>(TScaledDate src) { 
    return src / TDatetimeScale; 
} 
 
template<> inline 
TScaledDate ToScaledDate(NUdf::TDataType<NUdf::TTimestamp>::TLayout src) { 
    return src; 
} 
 
template<> inline 
NUdf::TDataType<NUdf::TTimestamp>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(TScaledDate src) { 
    return src; 
} 
 
template<> inline 
TScaledDate ToScaledDate(NUdf::TDataType<NUdf::TInterval>::TLayout src) { 
    return src; 
} 
 
template<> inline 
NUdf::TDataType<NUdf::TInterval>::TLayout FromScaledDate<NUdf::TDataType<NUdf::TInterval>::TLayout>(TScaledDate src) { 
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
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDate>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDate>::TLayout, TScaledDate>(value, context, block); 
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDateScale), "mul", block); 
    return mul; 
} 
 
template<> inline 
Value* GenToScaledDate<std::make_signed_t<NUdf::TDataType<NUdf::TDate>::TLayout>>(Value* value, LLVMContext &context, BasicBlock* block) { 
    const auto cast = StaticCast<std::make_signed_t<NUdf::TDataType<NUdf::TDate>::TLayout>, TScaledDate>(value, context, block); 
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDateScale), "mul", block); 
    return mul; 
} 
 
template<> inline 
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDate>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDateScale), "div", block); 
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDate>::TLayout>(div, context, block); 
    return cast; 
} 
 
template<> inline 
Value* GenToScaledDate<NUdf::TDataType<NUdf::TDatetime>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    const auto cast = StaticCast<NUdf::TDataType<NUdf::TDatetime>::TLayout, TScaledDate>(value, context, block); 
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDatetimeScale), "mul", block); 
    return mul; 
} 
 
template<> inline 
Value* GenToScaledDate<std::make_signed_t<NUdf::TDataType<NUdf::TDatetime>::TLayout>>(Value* value, LLVMContext &context, BasicBlock* block) { 
    const auto cast = StaticCast<std::make_signed_t<NUdf::TDataType<NUdf::TDatetime>::TLayout>, TScaledDate>(value, context, block); 
    const auto mul = BinaryOperator::CreateMul(cast, ConstantInt::get(cast->getType(), TDatetimeScale), "mul", block); 
    return mul; 
} 
 
template<> inline 
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TDatetime>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    const auto div = BinaryOperator::CreateSDiv(value, ConstantInt::get(value->getType(), TDatetimeScale), "div", block); 
    const auto cast = StaticCast<TScaledDate, NUdf::TDataType<NUdf::TDatetime>::TLayout>(div, context, block); 
    return cast; 
} 
 
template<> inline 
Value* GenToScaledDate<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    return StaticCast<NUdf::TDataType<NUdf::TTimestamp>::TLayout, TScaledDate>(value, context, block); 
} 
 
template<> inline 
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TTimestamp>::TLayout>(value, context, block); 
} 
 
template<> inline 
Value* GenToScaledDate<NUdf::TDataType<NUdf::TInterval>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    return StaticCast<NUdf::TDataType<NUdf::TInterval>::TLayout, TScaledDate>(value, context, block); 
} 
 
template<> inline 
Value* GenFromScaledDate<NUdf::TDataType<NUdf::TInterval>::TLayout>(Value* value, LLVMContext &context, BasicBlock* block) { 
    return StaticCast<TScaledDate, NUdf::TDataType<NUdf::TInterval>::TLayout>(value, context, block); 
} 
 
#endif 
 
} 
} 
