#include "mkql_decimal_mul.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins_decimal.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/public/decimal/yql_decimal.h>

extern "C" NYql::NDecimal::TInt128 DecimalMulAndDivNormalDivider(NYql::NDecimal::TInt128 a, NYql::NDecimal::TInt128 b, NYql::NDecimal::TInt128 c) {
    return NYql::NDecimal::MulAndDivNormalDivider(a, b, c);
}

extern "C" NYql::NDecimal::TInt128 DecimalMul(NYql::NDecimal::TInt128 a, NYql::NDecimal::TInt128 b) {
    return NYql::NDecimal::Mul(a, b);
}

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsLeftOptional, bool IsRightOptional>
class TDecimalMulWrapper : public TMutableCodegeneratorNode<TDecimalMulWrapper<IsLeftOptional, IsRightOptional>>, NYql::NDecimal::TDecimalMultiplicator<NYql::NDecimal::TInt128> {
    typedef TMutableCodegeneratorNode<TDecimalMulWrapper<IsLeftOptional, IsRightOptional>> TBaseComputation;
public:
    TDecimalMulWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right, ui8 precision, ui8 scale)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , NYql::NDecimal::TDecimalMultiplicator<NYql::NDecimal::TInt128>(precision, scale)
        , Left(left)
        , Right(right)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const auto& left = Left->GetValue(compCtx);
        const auto& right = Right->GetValue(compCtx);

        if (IsLeftOptional && !left)
            return NUdf::TUnboxedValuePod();

        if (IsRightOptional && !right)
            return NUdf::TUnboxedValuePod();

        return NUdf::TUnboxedValuePod(Do(left.GetInt128(), right.GetInt128()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);

        const bool useMulAddDiv = Divider_ > 1;
        const auto name = useMulAddDiv ? "DecimalMulAndDivNormalDivider" : "DecimalMul";
        const auto fnType = useMulAddDiv ?
            FunctionType::get(valType, { valType, valType, valType }, false):
            FunctionType::get(valType, { valType, valType}, false);

        ctx.Codegen.AddGlobalMapping(name,  useMulAddDiv ? reinterpret_cast<const void*>(&DecimalMulAndDivNormalDivider) : reinterpret_cast<const void*>(&DecimalMul));
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);

        const auto left = GetNodeValue(Left, ctx, block);
        const auto right = GetNodeValue(Right, ctx, block);

        if constexpr (IsLeftOptional || IsRightOptional) {
            const auto test = IsLeftOptional && IsRightOptional ?
                BinaryOperator::CreateAnd(left, right, "test", block):
                IsLeftOptional ? left : right;

            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto result = PHINode::Create(valType, 2, "result", done);
            result->addIncoming(test, block);

            BranchInst::Create(done, good, IsEmpty(test, block, context), block);

            block = good;

            Value* muldiv;
            if (useMulAddDiv) {
                muldiv = CallInst::Create(func, { GetterForInt128(left, block), GetterForInt128(right, block), NDecimal::GenConstant(Divider_, context) }, "mul_and_div", block);
            } else {
                muldiv = CallInst::Create(func, { GetterForInt128(left, block), GetterForInt128(right, block) }, "mul", block);
            }

            const auto ok = NDecimal::GenInBounds(muldiv, NDecimal::GenConstant(-Bound_, context), NDecimal::GenConstant(+Bound_, context), block);
            const auto nan = NDecimal::GenIsNonComparable(muldiv, context, block);
            const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, muldiv, ConstantInt::get(muldiv->getType(), 0), "plus", block);

            const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
            const auto bad = SelectInst::Create(nan, GetDecimalNan(context), inf, "bad", block);
            const auto res = SelectInst::Create(ok, muldiv, bad, "res", block);

            result->addIncoming(SetterForInt128(res, block), block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            Value* muldiv;
            if (useMulAddDiv) {
                muldiv = CallInst::Create(func, { GetterForInt128(left, block), GetterForInt128(right, block), NDecimal::GenConstant(Divider_, context) }, "mul_and_div", block);
            } else {
                muldiv = CallInst::Create(func, { GetterForInt128(left, block), GetterForInt128(right, block) }, "mul", block);
            }

            const auto ok = NDecimal::GenInBounds(muldiv, NDecimal::GenConstant(-Bound_, context), NDecimal::GenConstant(+Bound_, context), block);
            const auto nan = NDecimal::GenIsNonComparable(muldiv, context, block);
            const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, muldiv, ConstantInt::get(muldiv->getType(), 0), "plus", block);

            const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
            const auto bad = SelectInst::Create(nan, GetDecimalNan(context), inf, "bad", block);
            const auto res = SelectInst::Create(ok, muldiv, bad, "res", block);

            return SetterForInt128(res, block);
        }
    }
#endif

private:
    void RegisterDependencies() const final {
        this->DependsOn(Left);
        this->DependsOn(Right);
    }

    IComputationNode* const Left;
    IComputationNode* const Right;
};

template<bool IsLeftOptional, bool IsRightOptional, typename TRight>
class TDecimalMulIntegralWrapper : public TMutableCodegeneratorNode<TDecimalMulIntegralWrapper<IsLeftOptional, IsRightOptional, TRight>>, NYql::NDecimal::TDecimalMultiplicator<TRight> {
    typedef TMutableCodegeneratorNode<TDecimalMulIntegralWrapper<IsLeftOptional, IsRightOptional, TRight>> TBaseComputation;
    using NYql::NDecimal::TDecimalMultiplicator<TRight>::Bound_;

public:
    TDecimalMulIntegralWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right, ui8 precision)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , NYql::NDecimal::TDecimalMultiplicator<TRight>(precision)
        , Left(left)
        , Right(right)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const auto& left = Left->GetValue(compCtx);
        const auto& right = Right->GetValue(compCtx);

        if (IsLeftOptional && !left)
            return NUdf::TUnboxedValuePod();

        if (IsRightOptional && !right)
            return NUdf::TUnboxedValuePod();

        return NUdf::TUnboxedValuePod(this->Do(left.GetInt128(), right.template Get<TRight>()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);

        const auto name = "DecimalMul";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&DecimalMul));
        const auto fnType =
            FunctionType::get(valType, { valType, valType }, false);
        const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);

        const auto left = GetNodeValue(Left, ctx, block);
        const auto right = GetNodeValue(Right, ctx, block);

        if constexpr (IsLeftOptional || IsRightOptional) {
            const auto test = IsLeftOptional && IsRightOptional ?
                BinaryOperator::CreateAnd(left, right, "test", block):
                IsLeftOptional ? left : right;

            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto result = PHINode::Create(valType, 2, "result", done);
            result->addIncoming(test, block);

            BranchInst::Create(done, good, IsEmpty(test, block, context), block);

            block = good;

            const auto cast = std::is_signed<TRight>() ?
                static_cast<CastInst*>(new SExtInst(GetterFor<TRight>(right, context, block), valType, "sext", block)):
                static_cast<CastInst*>(new ZExtInst(GetterFor<TRight>(right, context, block), valType, "zext", block));
            const auto mul = CallInst::Create(func, {GetterForInt128(left, block), cast}, "div", block);

            const auto ok = NDecimal::GenInBounds(mul, NDecimal::GenConstant(-Bound_, context), NDecimal::GenConstant(+Bound_, context), block);
            const auto nan = NDecimal::GenIsNonComparable(mul, context, block);
            const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, mul, ConstantInt::get(mul->getType(), 0), "plus", block);

            const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
            const auto bad = SelectInst::Create(nan, GetDecimalNan(context), inf, "bad", block);
            const auto res = SelectInst::Create(ok, mul, bad, "res", block);

            result->addIncoming(SetterForInt128(res, block), block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            const auto cast = std::is_signed<TRight>() ?
                static_cast<CastInst*>(new SExtInst(GetterFor<TRight>(right, context, block), valType, "sext", block)):
                static_cast<CastInst*>(new ZExtInst(GetterFor<TRight>(right, context, block), valType, "zext", block));
            const auto mul = CallInst::Create(func, {GetterForInt128(left, block), cast}, "div", block);

            const auto ok = NDecimal::GenInBounds(mul, NDecimal::GenConstant(-Bound_, context), NDecimal::GenConstant(+Bound_, context), block);
            const auto nan = NDecimal::GenIsNonComparable(mul, context, block);
            const auto plus = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, mul, ConstantInt::get(mul->getType(), 0), "plus", block);

            const auto inf = SelectInst::Create(plus, GetDecimalPlusInf(context), GetDecimalMinusInf(context), "inf", block);
            const auto bad = SelectInst::Create(nan, GetDecimalNan(context), inf, "bad", block);
            const auto res = SelectInst::Create(ok, mul, bad, "res", block);

            return SetterForInt128(res, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Left);
        this->DependsOn(Right);
    }

    IComputationNode* const Left;
    IComputationNode* const Right;
};


}

IComputationNode* WrapDecimalMul(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    bool isOptionalLeft, isOptionalRight;

    const auto leftType = static_cast<TDataDecimalType*>(UnpackOptionalData(callable.GetInput(0), isOptionalLeft));
    const auto rightType = UnpackOptionalData(callable.GetInput(1), isOptionalRight);

    auto left = LocateNode(ctx.NodeLocator, callable, 0);
    auto right = LocateNode(ctx.NodeLocator, callable, 1);

    switch (rightType->GetSchemeType()) {
        case NUdf::TDataType<NUdf::TDecimal>::Id:
            MKQL_ENSURE(static_cast<TDataDecimalType*>(rightType)->IsSameType(*leftType), "Operands type mismatch");

            if (isOptionalLeft && isOptionalRight)
                return new TDecimalMulWrapper<true, true>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second);
            else if (isOptionalLeft)
                return new TDecimalMulWrapper<true, false>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second);
            else if (isOptionalRight)
                return new TDecimalMulWrapper<false, true>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second);
            else
                return new TDecimalMulWrapper<false, false>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second);
#define MAKE_PRIMITIVE_TYPE_MUL(type) \
        case NUdf::TDataType<type>::Id: \
            if (isOptionalLeft && isOptionalRight) \
                return new TDecimalMulIntegralWrapper<true, true, type>(ctx.Mutables, left, right, leftType->GetParams().first); \
            else if (isOptionalLeft) \
                return new TDecimalMulIntegralWrapper<true, false, type>(ctx.Mutables, left, right, leftType->GetParams().first); \
            else if (isOptionalRight) \
                return new TDecimalMulIntegralWrapper<false, true, type>(ctx.Mutables, left, right, leftType->GetParams().first); \
            else \
                return new TDecimalMulIntegralWrapper<false, false, type>(ctx.Mutables, left, right, leftType->GetParams().first);
        INTEGRAL_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_MUL)
#undef MAKE_PRIMITIVE_TYPE_MUL
        default:
            Y_ABORT("Unupported type.");
    }
}

}
}
