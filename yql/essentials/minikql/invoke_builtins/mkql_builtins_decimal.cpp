#include "mkql_builtins_decimal.h" // Y_IGNORE

#include <array>

namespace NKikimr::NMiniKQL::NDecimal {
#ifndef MKQL_DISABLE_CODEGEN

namespace {

Value* GenCompareMantissas(Value* left, Value* right, BasicBlock* block) {
    const auto less = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, left, right, "less", block);
    const auto greater = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, left, right, "greater", block);
    const auto resultType = Type::getInt32Ty(block->getContext());
    const auto nonLess = SelectInst::Create(greater, ConstantInt::get(resultType, 1),
                                            ConstantInt::get(resultType, 0), "non_less", block);
    return SelectInst::Create(
        less, ConstantInt::getSigned(resultType, -1), nonLess, "comparison", block);
}

Value* GenMagnitude(Value* value, BasicBlock* block) {
    const auto negative = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, value,
                                          ConstantInt::get(value->getType(), 0), "negative", block);
    return SelectInst::Create(
        negative, BinaryOperator::CreateNeg(value, "negated", block), value, "magnitude", block);
}

Value* GenCompareScaledMagnitudes(Value* lowerScaleMagnitude, Value* higherScaleMagnitude,
                                  Value* multiplier, Value* multiplicationLimit, BasicBlock*& block) {
    auto& context = block->getContext();
    const auto multiplyBlock = BasicBlock::Create(context, "multiply", block->getParent());
    const auto done = BasicBlock::Create(context, "scaled_done", block->getParent());
    const auto resultType = Type::getInt32Ty(context);
    const auto result = PHINode::Create(resultType, 2U, "scaled_comparison", done);
    result->addIncoming(ConstantInt::get(resultType, 1), block);
    const auto exceedsLimit = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT,
                                              lowerScaleMagnitude, multiplicationLimit, "exceeds_limit", block);
    BranchInst::Create(done, multiplyBlock, exceedsLimit, block);

    block = multiplyBlock;
    const auto scaledMagnitude = BinaryOperator::CreateMul(
        lowerScaleMagnitude, multiplier, "scaled_magnitude", block);
    result->addIncoming(GenCompareMantissas(scaledMagnitude, higherScaleMagnitude, block), block);
    BranchInst::Create(done, block);
    block = done;
    return result;
}

Value* GenShouldScale(Value* left, Value* right, BasicBlock* block) {
    auto& context = block->getContext();
    const auto bothNormal = BinaryOperator::CreateAnd(
        GenIsNormal(left, context, block), GenIsNormal(right, context, block), "both_normal", block);
    const auto leftNegative = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, left,
                                              ConstantInt::get(left->getType(), 0), "left_negative", block);
    const auto rightNegative = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, right,
                                               ConstantInt::get(right->getType(), 0), "right_negative", block);
    const auto sameSign = CmpInst::Create(
        Instruction::ICmp, ICmpInst::ICMP_EQ, leftNegative, rightNegative, "same_sign", block);
    return BinaryOperator::CreateAnd(bothNormal, sameSign, "should_scale", block);
}

Value* GenNormalComparison(Value* left, Value* right, i8 scaleDifference, BasicBlock*& block) {
    auto& context = block->getContext();
    const ui8 difference =
        static_cast<ui8>(scaleDifference > 0 ? scaleDifference : -scaleDifference);
    const auto leftMagnitude = GenMagnitude(left, block);
    const auto rightMagnitude = GenMagnitude(right, block);
    const auto lowerScaleMagnitude = scaleDifference > 0 ? leftMagnitude : rightMagnitude;
    const auto higherScaleMagnitude = scaleDifference > 0 ? rightMagnitude : leftMagnitude;
    const auto multiplier = GenConstant(static_cast<NYql::NDecimal::TInt128>(
                                            NYql::NDecimal::GetDivider(difference)), context);
    const auto multiplicationLimit = GenConstant(static_cast<NYql::NDecimal::TInt128>(
                                                     NYql::NDecimal::GetMultiplicationLimit(difference)), context);
    auto comparison = GenCompareScaledMagnitudes(
        lowerScaleMagnitude, higherScaleMagnitude, multiplier, multiplicationLimit, block);
    comparison = scaleDifference < 0
                     ? BinaryOperator::CreateNeg(comparison, "reversed", block)
                     : comparison;
    const auto negative = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, left,
                                          ConstantInt::get(left->getType(), 0), "negative_result", block);
    return SelectInst::Create(negative,
                              BinaryOperator::CreateNeg(comparison, "negative_reversed", block), comparison,
                              "signed_comparison", block);
}

Value* GenCompare(Value* left, Value* right, i8 scaleDifference, BasicBlock*& block) {
    auto& context = block->getContext();
    const auto direct = GenCompareMantissas(left, right, block);
    if (scaleDifference == 0) {
        return direct;
    }

    const auto scaledBlock = BasicBlock::Create(context, "scale", block->getParent());
    const auto done = BasicBlock::Create(context, "done", block->getParent());
    const auto result = PHINode::Create(direct->getType(), 2U, "decimal_comparison", done);
    result->addIncoming(direct, block);
    BranchInst::Create(scaledBlock, done, GenShouldScale(left, right, block), block);

    block = scaledBlock;
    const auto comparison = GenNormalComparison(left, right, scaleDifference, block);
    result->addIncoming(comparison, block);
    BranchInst::Create(done, block);
    block = done;
    return result;
}

Value* GenComparison(Value* left, Value* right, i8 scaleDifference,
                     CmpInst::Predicate predicate, BasicBlock*& block) {
    const auto comparison = GenCompare(left, right, scaleDifference, block);
    return CmpInst::Create(Instruction::ICmp, predicate, comparison,
                           ConstantInt::get(comparison->getType(), 0), "decimal_predicate", block);
}

Value* GenBothComparable(Value* left, Value* right, BasicBlock* block) {
    auto& context = block->getContext();
    return BinaryOperator::CreateAnd(
        GenIsComparable(left, context, block), GenIsComparable(right, context, block),
        "both_comparable", block);
}

Value* GenOrderedComparison(Value* left, Value* right, i8 scaleDifference,
                            bool aggregate, CmpInst::Predicate predicate, BasicBlock*& block) {
    const auto result = GenComparison(left, right, scaleDifference, predicate, block);
    return aggregate
               ? result
               : BinaryOperator::CreateAnd(
                     GenBothComparable(left, right, block), result, "comparable_result", block);
}

} // namespace

ConstantInt* GenConstant(NYql::NDecimal::TInt128 value, LLVMContext& context) {
    const auto& pair = NYql::NDecimal::MakePair(value);
    const std::array<uint64_t, 2> init = {pair.first, pair.second};
    return ConstantInt::get(context, APInt(128, 2, init.data()));
}

template <bool IncludeBounds>
Value* GenInBounds(Value* val, ConstantInt* low, ConstantInt* high, BasicBlock* block) {
    const auto lt = CmpInst::Create(Instruction::ICmp, IncludeBounds ? ICmpInst::ICMP_SLE : ICmpInst::ICMP_SLT, val, high, "lt", block);
    const auto gt = CmpInst::Create(Instruction::ICmp, IncludeBounds ? ICmpInst::ICMP_SGE : ICmpInst::ICMP_SGT, val, low, "gt", block);
    const auto good = BinaryOperator::CreateAnd(lt, gt, "and", block);
    return good;
}

template <bool IncludeBounds>
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

Value* GenIsError(Value* val, LLVMContext& context, BasicBlock* block) {
    const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, GetDecimalNan(context), "gt", block);
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, GetDecimalMinusNan(context), "lt", block);
    const auto bad = BinaryOperator::CreateOr(lt, gt, "or", block);
    return bad;
}

Value* GenIsNormal(Value* val, LLVMContext& context, BasicBlock* block) {
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, GetDecimalPlusInf(context), "lt", block);
    const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, GetDecimalMinusInf(context), "gt", block);
    const auto good = BinaryOperator::CreateAnd(lt, gt, "and", block);
    return good;
}

Value* GenIsAbnormal(Value* val, LLVMContext& context, BasicBlock* block) {
    const auto le = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, val, GetDecimalMinusInf(context), "le", block);
    const auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, GetDecimalPlusInf(context), "ge", block);
    const auto bad = BinaryOperator::CreateOr(le, ge, "or", block);
    return bad;
}

Value* GenIsComparable(Value* val, LLVMContext& context, BasicBlock* block) {
    const auto le = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, val, GetDecimalPlusInf(context), "le", block);
    const auto ge = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGE, val, GetDecimalMinusInf(context), "ge", block);
    const auto good = BinaryOperator::CreateAnd(le, ge, "and", block);
    return good;
}

Value* GenIsNonComparable(Value* val, LLVMContext& context, BasicBlock* block) {
    const auto gt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, val, GetDecimalPlusInf(context), "gt", block);
    const auto lt = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, GetDecimalMinusInf(context), "lt", block);
    const auto bad = BinaryOperator::CreateOr(gt, lt, "or", block);
    return bad;
}

Value* GenIsLess(
    Value* left, Value* right, i8 scaleDifference, bool aggregate, BasicBlock*& block) {
    return GenOrderedComparison(
        left, right, scaleDifference, aggregate, ICmpInst::ICMP_SLT, block);
}

Value* GenIsGreater(
    Value* left, Value* right, i8 scaleDifference, bool aggregate, BasicBlock*& block) {
    return GenOrderedComparison(
        left, right, scaleDifference, aggregate, ICmpInst::ICMP_SGT, block);
}

Value* GenIsEqual(
    Value* left, Value* right, i8 scaleDifference, bool aggregate, BasicBlock*& block) {
    const auto result = GenComparison(
        left, right, scaleDifference, ICmpInst::ICMP_EQ, block);
    return aggregate
               ? result
               : BinaryOperator::CreateAnd(
                     GenIsComparable(left, block->getContext(), block), result, "comparable_result", block);
}

Value* GenIsLessOrEqual(
    Value* left, Value* right, i8 scaleDifference, bool aggregate, BasicBlock*& block) {
    return GenOrderedComparison(
        left, right, scaleDifference, aggregate, ICmpInst::ICMP_SLE, block);
}

Value* GenIsGreaterOrEqual(
    Value* left, Value* right, i8 scaleDifference, bool aggregate, BasicBlock*& block) {
    return GenOrderedComparison(
        left, right, scaleDifference, aggregate, ICmpInst::ICMP_SGE, block);
}

Value* GenIsNotEqual(
    Value* left, Value* right, i8 scaleDifference, bool aggregate, BasicBlock*& block) {
    const auto result = GenComparison(
        left, right, scaleDifference, ICmpInst::ICMP_NE, block);
    return aggregate
               ? result
               : BinaryOperator::CreateOr(
                     GenIsNonComparable(right, block->getContext(), block), result, "non_comparable_result", block);
}
#endif
} // namespace NKikimr::NMiniKQL::NDecimal
