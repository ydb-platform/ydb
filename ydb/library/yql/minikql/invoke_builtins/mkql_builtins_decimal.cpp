#include "mkql_builtins_decimal.h" // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {
namespace NDecimal {
#ifndef MKQL_DISABLE_CODEGEN

ConstantInt* GenConstant(NYql::NDecimal::TInt128 value, LLVMContext &context) {
    const auto& pair = NYql::NDecimal::MakePair(value);
    const uint64_t init[] = {pair.first, pair.second};
    return ConstantInt::get(context, APInt(128, 2, init));
}

template<bool IncludeBounds>
Value* GenInBounds(Value* val, ConstantInt* low, ConstantInt* high, BasicBlock* block) {
    const auto lt = CmpInst::Create(Instruction::ICmp, IncludeBounds ? ICmpInst::ICMP_SLE : ICmpInst::ICMP_SLT, val, high, "lt", block);
    const auto gt = CmpInst::Create(Instruction::ICmp, IncludeBounds ? ICmpInst::ICMP_SGE : ICmpInst::ICMP_SGT, val, low, "gt", block);
    const auto good = BinaryOperator::CreateAnd(lt, gt, "and", block);
    return good;
}

template<bool IncludeBounds>
Value* GenOutOfBounds(Value* val, ConstantInt* low, ConstantInt* high, BasicBlock* block) {
    const auto lt = CmpInst::Create(Instruction::ICmp, IncludeBounds ? ICmpInst::ICMP_SLE : ICmpInst::ICMP_SLT, val, low, "lt", block);
    const auto gt = CmpInst::Create(Instruction::ICmp, IncludeBounds ? ICmpInst::ICMP_SGE : ICmpInst::ICMP_SGT, val, high, "gt", block);
    const auto bad = BinaryOperator::CreateOr(lt, gt, "or", block);
    return bad;
}

template Value* GenInBounds<true>(Value* val, ConstantInt* low, ConstantInt* high, BasicBlock* block);
template Value* GenInBounds<false>(Value* val, ConstantInt* low, ConstantInt* high, BasicBlock* block);
template Value* GenOutOfBounds<true>(Value* val, ConstantInt* low, ConstantInt* high, BasicBlock* block);
template Value* GenOutOfBounds<false>(Value* val, ConstantInt* low, ConstantInt* high, BasicBlock* block);

Value* GenIsError(Value* val, LLVMContext &context, BasicBlock* block) {
    const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, GetDecimalNan(context), "gt", block);
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, GetDecimalMinusNan(context), "lt", block);
    const auto bad = BinaryOperator::CreateOr(lt, gt, "or", block);
    return bad;
}

Value* GenIsNormal(Value* val, LLVMContext &context, BasicBlock* block) {
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, GetDecimalPlusInf(context), "lt", block);
    const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, GetDecimalMinusInf(context), "gt", block);
    const auto good = BinaryOperator::CreateAnd(lt, gt, "and", block);
    return good;
}

Value* GenIsAbnormal(Value* val, LLVMContext &context, BasicBlock* block) {
    const auto le = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, val, GetDecimalMinusInf(context), "le", block);
    const auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, GetDecimalPlusInf(context), "ge", block);
    const auto bad = BinaryOperator::CreateOr(le, ge, "or", block);
    return bad;
}

Value* GenIsComparable(Value* val, LLVMContext &context, BasicBlock* block) {
    const auto le = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, val, GetDecimalPlusInf(context), "le", block);
    const auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, GetDecimalMinusInf(context), "ge", block);
    const auto good = BinaryOperator::CreateAnd(le, ge, "and", block);
    return good;
}

Value* GenIsNonComparable(Value* val, LLVMContext &context, BasicBlock* block) {
    const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, GetDecimalPlusInf(context), "gt", block);
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, GetDecimalMinusInf(context), "lt", block);
    const auto bad = BinaryOperator::CreateOr(gt, lt, "or", block);
    return bad;
}
#endif
}
}
}
