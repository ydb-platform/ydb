#include "mkql_decimal_div.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins_decimal.h> // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsLeftOptional, bool IsRightOptional>
class TDecimalModWrapper : public TMutableCodegeneratorNode<TDecimalModWrapper<IsLeftOptional, IsRightOptional>> {
    typedef TMutableCodegeneratorNode<TDecimalModWrapper<IsLeftOptional, IsRightOptional>> TBaseComputation;
public:
    TDecimalModWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
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

        return NUdf::TUnboxedValuePod(NYql::NDecimal::Mod(left.GetInt128(), right.GetInt128()));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);

        const auto left = GetNodeValue(Left, ctx, block);
        const auto right = GetNodeValue(Right, ctx, block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        const auto zero = ConstantInt::get(valType, 0ULL);
        const auto result = PHINode::Create(valType, IsLeftOptional || IsRightOptional ? 3 : 2, "result", done);

        if constexpr (IsLeftOptional || IsRightOptional) {
            const auto test = IsLeftOptional && IsRightOptional ?
                BinaryOperator::CreateAnd(left, right, "test", block):
                IsLeftOptional ? left : right;

            result->addIncoming(zero, block);
            BranchInst::Create(done, good, IsEmpty(test, block), block);

            block = good;

            const auto lv = GetterForInt128(left, block);
            const auto rv = GetterForInt128(right, block);

            const auto lbad = NDecimal::GenIsAbnormal(lv, context, block);
            const auto rbad = NDecimal::GenIsAbnormal(rv, context, block);
            const auto bad = BinaryOperator::CreateOr(lbad, rbad, "bad", block);
            const auto nul = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, rv, zero, "check", block);
            const auto nan = BinaryOperator::CreateOr(bad, nul, "nan", block);

            const auto norm = BasicBlock::Create(context, "norm", ctx.Func);

            result->addIncoming(SetterForInt128(GetDecimalNan(context), block), block);
            BranchInst::Create(done, norm, nan, block);

            block = norm;
            const auto srem = BinaryOperator::CreateSRem(lv, rv, "srem", block);
            result->addIncoming(SetterForInt128(srem, block), block);
        } else {
            const auto lv = GetterForInt128(left, block);
            const auto rv = GetterForInt128(right, block);

            const auto lbad = NDecimal::GenIsAbnormal(lv, context, block);
            const auto rbad = NDecimal::GenIsAbnormal(rv, context, block);
            const auto bad = BinaryOperator::CreateOr(lbad, rbad, "bad", block);
            const auto nul = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, rv, zero, "check", block);
            const auto nan = BinaryOperator::CreateOr(bad, nul, "nan", block);

            result->addIncoming(SetterForInt128(GetDecimalNan(context), block), block);
            BranchInst::Create(done, good, nan, block);

            block = good;
            const auto srem = BinaryOperator::CreateSRem(lv, rv, "srem", block);
            result->addIncoming(SetterForInt128(srem, block), block);
        }

        BranchInst::Create(done, block);
        block = done;
        return result;
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
class TDecimalModIntegralWrapper : public TMutableCodegeneratorNode<TDecimalModIntegralWrapper<IsLeftOptional, IsRightOptional, TRight>> {
    typedef TMutableCodegeneratorNode<TDecimalModIntegralWrapper<IsLeftOptional, IsRightOptional, TRight>> TBaseComputation;
public:
    TDecimalModIntegralWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right, ui8 precision, ui8 scale)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Left(left)
        , Right(right)
        , Bound(NYql::NDecimal::GetDivider(precision - scale))
        , Divider(NYql::NDecimal::GetDivider(scale))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto left = Left->GetValue(compCtx);
        const auto& right = Right->GetValue(compCtx);

        if (IsLeftOptional && !left)
            return NUdf::TUnboxedValuePod();

        if (IsRightOptional && !right)
            return NUdf::TUnboxedValuePod();

        const auto r = NYql::NDecimal::TInt128(right.Get<TRight>());

        if constexpr (std::is_signed<TRight>::value) {
            if (r >= +Bound || r <= -Bound)
                return left.Release();
        } else {
            if (r >= Bound)
                return left.Release();
        }

        return NUdf::TUnboxedValuePod(NYql::NDecimal::Mod(left.GetInt128(), NYql::NDecimal::Mul(Divider, r)));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valType = Type::getInt128Ty(context);
        const auto divider = NDecimal::GenConstant(Divider, context);

        const auto left = GetNodeValue(Left, ctx, block);
        const auto right = GetNodeValue(Right, ctx, block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        const auto zero = ConstantInt::get(valType, 0ULL);
        const auto result = PHINode::Create(valType, IsLeftOptional || IsRightOptional ? 3 : 2, "result", done);

        if constexpr (IsLeftOptional || IsRightOptional) {
            const auto test = IsLeftOptional && IsRightOptional ?
                BinaryOperator::CreateAnd(left, right, "test", block):
                IsLeftOptional ? left : right;

            result->addIncoming(zero, block);
            BranchInst::Create(done, good, IsEmpty(test, block), block);

            block = good;

            const auto lv = GetterForInt128(left, block);
            const auto cast = std::is_signed<TRight>() ?
                static_cast<CastInst*>(new SExtInst(GetterFor<TRight>(right, context, block), valType, "sext", block)):
                static_cast<CastInst*>(new ZExtInst(GetterFor<TRight>(right, context, block), valType, "zext", block));

            const auto out = std::is_signed<TRight>() ?
                NDecimal::GenOutOfBounds(cast, NDecimal::GenConstant(-Bound, context), NDecimal::GenConstant(+Bound, context), block):
                CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, cast, NDecimal::GenConstant(Bound, context), "out", block);

            const auto nul = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, cast, zero, "check", block);

            const auto lbad = NDecimal::GenIsAbnormal(lv, context, block);
            const auto rbad = BinaryOperator::CreateOr(out, nul, "or", block);
            const auto bad = BinaryOperator::CreateOr(lbad, rbad, "bad", block);

            const auto norm = BasicBlock::Create(context, "norm", ctx.Func);

            const auto spec = SelectInst::Create(out, left, SetterForInt128(GetDecimalNan(context), block), "spec", block);
            result->addIncoming(spec, block);
            BranchInst::Create(done, norm, bad, block);

            block = norm;
            const auto mul = BinaryOperator::CreateMul(divider, cast, "mul", block);
            const auto srem = BinaryOperator::CreateSRem(lv, mul, "srem", block);
            result->addIncoming(SetterForInt128(srem, block), block);
        } else {
            const auto lv = GetterForInt128(left, block);
            const auto cast = std::is_signed<TRight>() ?
                static_cast<CastInst*>(new SExtInst(GetterFor<TRight>(right, context, block), valType, "sext", block)):
                static_cast<CastInst*>(new ZExtInst(GetterFor<TRight>(right, context, block), valType, "zext", block));

            const auto out = std::is_signed<TRight>() ?
                NDecimal::GenOutOfBounds(cast, NDecimal::GenConstant(-Bound, context), NDecimal::GenConstant(+Bound, context), block):
                CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, cast, NDecimal::GenConstant(Bound, context), "out", block);

            const auto nul = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, cast, zero, "check", block);

            const auto lbad = NDecimal::GenIsAbnormal(lv, context, block);
            const auto rbad = BinaryOperator::CreateOr(out, nul, "or", block);
            const auto bad = BinaryOperator::CreateOr(lbad, rbad, "bad", block);

            const auto spec = SelectInst::Create(out, left, SetterForInt128(GetDecimalNan(context), block), "spec", block);

            result->addIncoming(spec, block);
            BranchInst::Create(done, good, bad, block);

            block = good;
            const auto mul = BinaryOperator::CreateMul(divider, cast, "mul", block);
            const auto srem = BinaryOperator::CreateSRem(lv, mul, "srem", block);
            result->addIncoming(SetterForInt128(srem, block), block);
        }

        BranchInst::Create(done, block);
        block = done;
        return result;
    }
#endif

private:
    void RegisterDependencies() const final {
        this->DependsOn(Left);
        this->DependsOn(Right);
    }

    IComputationNode* const Left;
    IComputationNode* const Right;
    const NYql::NDecimal::TInt128 Bound;
    const NYql::NDecimal::TInt128 Divider;
};

}

IComputationNode* WrapDecimalMod(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
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
                return new TDecimalModWrapper<true, true>(ctx.Mutables, left, right);
            else if (isOptionalLeft)
                return new TDecimalModWrapper<true, false>(ctx.Mutables, left, right);
            else if (isOptionalRight)
                return new TDecimalModWrapper<false, true>(ctx.Mutables, left, right);
            else
                return new TDecimalModWrapper<false, false>(ctx.Mutables, left, right);
#define MAKE_PRIMITIVE_TYPE_MOD(type) \
        case NUdf::TDataType<type>::Id: \
            if (isOptionalLeft && isOptionalRight) \
                return new TDecimalModIntegralWrapper<true, true, type>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second); \
            else if (isOptionalLeft) \
                return new TDecimalModIntegralWrapper<true, false, type>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second); \
            else if (isOptionalRight) \
                return new TDecimalModIntegralWrapper<false, true, type>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second); \
            else \
                return new TDecimalModIntegralWrapper<false, false, type>(ctx.Mutables, left, right, leftType->GetParams().first, leftType->GetParams().second);
        INTEGRAL_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_MOD)
#undef MAKE_PRIMITIVE_TYPE_MOD
        default:
            Y_ABORT("Unupported type.");
    }
}

}
}
