#include "mkql_decimal_add_sub.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins_decimal.h>     // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/utils/runtime_dispatch.h>

#include <type_traits>

namespace NKikimr::NMiniKQL {

namespace {

enum class EDecimalIntegralOperation {
    Add,
    Subtract,
};

template <bool IsLeftOptional, bool IsRightOptional>
bool AllPresent(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right) {
    return (!IsLeftOptional || left) && (!IsRightOptional || right);
}

#ifndef MKQL_DISABLE_CODEGEN
template <bool IsLeftOptional, bool IsRightOptional>
Value* GenAllPresent(Value* left, Value* right, LLVMContext& context, BasicBlock* block) {
    Value* leftPresent = ConstantInt::getTrue(context);
    if constexpr (IsLeftOptional) {
        leftPresent = IsExists(left, block, context);
    }
    Value* rightPresent = ConstantInt::getTrue(context);
    if constexpr (IsRightOptional) {
        rightPresent = IsExists(right, block, context);
    }
    return BinaryOperator::CreateAnd(leftPresent, rightPresent, "present", block);
}

Value* GenInfinity(
    Value* isPositive,
    LLVMContext& context,
    BasicBlock* block)
{
    return SelectInst::Create(
        isPositive,
        GetDecimalPlusInf(context),
        GetDecimalMinusInf(context),
        "infinity",
        block);
}

Value* GenCalculatedDecimalIntegralAddSub(
    EDecimalIntegralOperation operation,
    Value* left,
    Value* right,
    ui8 precision,
    ui8 scale,
    LLVMContext& context,
    BasicBlock* block)
{
    const auto scaleMultiplier = static_cast<NYql::NDecimal::TInt128>(NYql::NDecimal::GetDivider(scale));
    const auto scaledRight = BinaryOperator::CreateMul(
        right, NDecimal::GenConstant(scaleMultiplier, context), "scaled_right", block);
    const auto value = operation == EDecimalIntegralOperation::Add
                           ? BinaryOperator::CreateAdd(left, scaledRight, "add", block)
                           : BinaryOperator::CreateSub(left, scaledRight, "sub", block);
    const auto [lowerBound, upperBound] = NYql::NDecimal::GetBounds(precision);
    const auto inBounds = NDecimal::GenInBounds(
        value, NDecimal::GenConstant(lowerBound, context), NDecimal::GenConstant(upperBound, context), block);
    const auto resultIsPositive = CmpInst::Create(
        Instruction::ICmp, ICmpInst::ICMP_SGT, value, ConstantInt::get(value->getType(), 0), "result_is_positive", block);
    return SelectInst::Create(
        inBounds, value, GenInfinity(resultIsPositive, context, block), "calculated_result", block);
}

Value* GenNormalDecimalIntegralAddSub(
    EDecimalIntegralOperation operation,
    Value* left,
    Value* right,
    ui8 precision,
    ui8 scale,
    const TCodegenContext& ctx,
    BasicBlock* block)
{
    auto& context = ctx.Codegen.GetContext();
    const auto integralOverflowThreshold = NYql::NDecimal::GetIntegralAddSubOverflowThreshold(precision, scale);
    const auto mayProduceFiniteResult = NDecimal::GenInBounds(
        right, NDecimal::GenConstant(-integralOverflowThreshold, context), NDecimal::GenConstant(integralOverflowThreshold, context), block);
    const auto calculatedResult = GenCalculatedDecimalIntegralAddSub(
        operation, left, right, precision, scale, context, block);
    const auto overflowIsPositive = CmpInst::Create(
        Instruction::ICmp,
        operation == EDecimalIntegralOperation::Add ? ICmpInst::ICMP_SGT : ICmpInst::ICMP_SLT,
        right,
        ConstantInt::get(right->getType(), 0),
        "overflow_is_positive",
        block);
    const auto unavoidableOverflow = GenInfinity(overflowIsPositive, context, block);
    return SelectInst::Create(mayProduceFiniteResult, calculatedResult, unavoidableOverflow, "normal_result", block);
}

template <EDecimalIntegralOperation Operation, typename TRight>
Value* GenDecimalIntegralAddSub(
    Value* left,
    Value* right,
    ui8 precision,
    ui8 scale,
    const TCodegenContext& ctx,
    BasicBlock*& block)
{
    auto& context = ctx.Codegen.GetContext();
    const auto leftValue = GetterForInt128(left, block);
    const auto extendedRight = CastInst::Create(
        std::is_signed_v<TRight> ? Instruction::SExt : Instruction::ZExt,
        GetterFor<TRight>(right, context, block), Type::getInt128Ty(context), "extended_right", block);
    const auto normalResult = GenNormalDecimalIntegralAddSub(
        Operation, leftValue, extendedRight, precision, scale, ctx, block);
    const auto leftIsNan = CmpInst::Create(
        Instruction::ICmp, ICmpInst::ICMP_EQ, leftValue, GetDecimalNan(context), "left_is_nan", block);
    const auto specialResult = SelectInst::Create(
        leftIsNan, GetDecimalNan(context), leftValue, "special_result", block);
    return SetterForInt128(
        SelectInst::Create(NDecimal::GenIsNormal(leftValue, context, block), normalResult, specialResult, "result", block), block);
}
#endif

template <EDecimalIntegralOperation Operation, bool IsLeftOptional, bool IsRightOptional, typename TRight>
class TDecimalIntegralAddSubWrapper final
    : public TMutableCodegeneratorNode<TDecimalIntegralAddSubWrapper<Operation, IsLeftOptional, IsRightOptional, TRight>> {
    using TBaseComputation = TMutableCodegeneratorNode<TDecimalIntegralAddSubWrapper<Operation, IsLeftOptional, IsRightOptional, TRight>>;

public:
    TDecimalIntegralAddSubWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right, ui8 precision, ui8 scale)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Operation_(precision, scale)
#ifndef MKQL_DISABLE_CODEGEN
        , Precision_(precision)
        , Scale_(scale)
#endif
        , Left_(left)
        , Right_(right)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const auto& left = Left_->GetValue(compCtx);
        const auto& right = Right_->GetValue(compCtx);
        if (!AllPresent<IsLeftOptional, IsRightOptional>(left, right)) {
            return {};
        }
        return NUdf::TUnboxedValuePod(Operation_.Do(left.GetInt128(), right.template Get<TRight>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto left = GetNodeValue(Left_, ctx, block);
        const auto right = GetNodeValue(Right_, ctx, block);
        if constexpr (IsLeftOptional || IsRightOptional) {
            const auto present = GenAllPresent<IsLeftOptional, IsRightOptional>(left, right, context, block);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto calculate = BasicBlock::Create(context, "calculate", ctx.Func);
            const auto result = PHINode::Create(Type::getInt128Ty(context), 2U, "result", done);
            result->addIncoming(ConstantInt::get(Type::getInt128Ty(context), 0), block);
            BranchInst::Create(calculate, done, present, block);
            block = calculate;
            result->addIncoming(GenDecimalIntegralAddSub<Operation, TRight>(left, right, Precision_, Scale_, ctx, block), block);
            BranchInst::Create(done, block);
            block = done;
            return result;
        }
        return GenDecimalIntegralAddSub<Operation, TRight>(left, right, Precision_, Scale_, ctx, block);
    }
#endif

private:
    void RegisterDependencies() const final {
        this->DependsOn(Left_);
        this->DependsOn(Right_);
    }

    const std::conditional_t<
        Operation == EDecimalIntegralOperation::Add,
        NYql::NDecimal::TDecimalAdd<TRight>,
        NYql::NDecimal::TDecimalSub<TRight>>
        Operation_;
#ifndef MKQL_DISABLE_CODEGEN
    const ui8 Precision_;
    const ui8 Scale_;
#endif
    IComputationNode* const Left_;
    IComputationNode* const Right_;
};

template <
    bool IsLeftOptional,
    bool IsRightOptional,
    EDecimalIntegralOperation Operation,
    typename TRight>
IComputationNode* CreateDecimalIntegralAddSubWrapper(
    std::integral_constant<EDecimalIntegralOperation, Operation>,
    std::type_identity<TRight>,
    TComputationMutables& mutables,
    IComputationNode* left,
    IComputationNode* right,
    ui8 precision,
    ui8 scale)
{
    return new TDecimalIntegralAddSubWrapper<Operation, IsLeftOptional, IsRightOptional, TRight>(
        mutables, left, right, precision, scale);
}

template <EDecimalIntegralOperation Operation, typename TRight>
IComputationNode* MakeDecimalIntegralAddSubWrapper(
    bool isLeftOptional,
    bool isRightOptional,
    const TComputationNodeFactoryContext& ctx,
    IComputationNode* left,
    IComputationNode* right,
    ui8 precision,
    ui8 scale)
{
    using TOperation = std::integral_constant<EDecimalIntegralOperation, Operation>;
    return YQL_RUNTIME_DISPATCH(
        CreateDecimalIntegralAddSubWrapper, 2, isLeftOptional, isRightOptional, TOperation{}, std::type_identity<TRight>{},
        ctx.Mutables, left, right, precision, scale);
}

template <EDecimalIntegralOperation Operation>
IComputationNode* DispatchDecimalIntegralAddSubWrapper(
    const TDataType* rightType,
    bool isLeftOptional,
    bool isRightOptional,
    const TComputationNodeFactoryContext& ctx,
    IComputationNode* left,
    IComputationNode* right,
    ui8 precision,
    ui8 scale)
{
    switch (rightType->GetSchemeType()) {
#define MAKE_PRIMITIVE_TYPE(type)                                 \
    case NUdf::TDataType<type>::Id:                               \
        return MakeDecimalIntegralAddSubWrapper<Operation, type>( \
            isLeftOptional, isRightOptional, ctx, left, right, precision, scale);
        INTEGRAL_VALUE_TYPES(MAKE_PRIMITIVE_TYPE)
#undef MAKE_PRIMITIVE_TYPE
        default:
            MKQL_ENSURE(false, "Unsupported right operand type: " << rightType->GetSchemeType());
    }
}

template <EDecimalIntegralOperation Operation>
IComputationNode* WrapDecimalIntegralAddSub(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    bool isLeftOptional;
    bool isRightOptional;
    const TDataType* leftDataType = UnpackOptionalData(callable.GetInput(0), isLeftOptional);
    const TDataType* rightType = UnpackOptionalData(callable.GetInput(1), isRightOptional);
    MKQL_ENSURE(leftDataType->GetSchemeType() == NUdf::TDataType<NUdf::TDecimal>::Id, "Expected decimal left operand");
    const auto* leftType = static_cast<const TDataDecimalType*>(leftDataType);
    MKQL_ENSURE(
        NUdf::GetDataTypeInfo(*rightType->GetDataSlot()).Features & NUdf::IntegralType,
        "Expected integral right operand");
    const auto [precision, scale] = leftType->GetParams();
    return DispatchDecimalIntegralAddSubWrapper<Operation>(rightType, isLeftOptional, isRightOptional, ctx,
                                                           LocateNode(ctx.NodeLocator, callable, 0), LocateNode(ctx.NodeLocator, callable, 1), precision, scale);
}

} // namespace

IComputationNode* WrapDecimalIntegralAdd(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapDecimalIntegralAddSub<EDecimalIntegralOperation::Add>(callable, ctx);
}

IComputationNode* WrapDecimalIntegralSub(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapDecimalIntegralAddSub<EDecimalIntegralOperation::Subtract>(callable, ctx);
}

} // namespace NKikimr::NMiniKQL
