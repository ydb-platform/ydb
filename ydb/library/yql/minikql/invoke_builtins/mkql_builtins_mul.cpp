#include "mkql_builtins_impl.h"  // Y_IGNORE
#include "mkql_builtins_datetime.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TMul : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TMul<TLeft, TRight, TOutput>> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;

    static TOutput Do(TOutput left, TOutput right)
    {
        return left * right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* left, Value* right, const TCodegenContext&, BasicBlock*& block)
    {
        return std::is_floating_point<TOutput>() ?
            BinaryOperator::CreateFMul(left, right, "mul", block):
            BinaryOperator::CreateMul(left, right, "mul", block);
    }
#endif
};

template<typename TLeft, typename TRight, typename TOutput>
struct TNumMulInterval {
    static_assert(TOutput::Features & NYql::NUdf::TimeIntervalType, "Output must be interval type");
    static_assert(std::is_integral_v<typename TLeft::TLayout>, "Left must be integral");
    static_assert(std::is_integral_v<typename TRight::TLayout>, "Right must be integral");

    static constexpr auto NullMode = TKernel::ENullMode::AlwaysNull;

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = static_cast<typename TOutput::TLayout>(left.template Get<typename TLeft::TLayout>());
        const auto rv = static_cast<typename TOutput::TLayout>(right.template Get<typename TRight::TLayout>());
        const auto ret = lv * rv;
        if (ret == 0) {
            return NUdf::TUnboxedValuePod(ret);
        }
        if constexpr (std::is_same_v<ui64, typename TLeft::TLayout>) {
            if (left.Get<ui64>() > static_cast<ui64>(std::numeric_limits<i64>::max())) {
                return NUdf::TUnboxedValuePod();
            }
        }
        if constexpr (std::is_same_v<ui64, typename TRight::TLayout>) {
            if (right.Get<ui64>() > static_cast<ui64>(std::numeric_limits<i64>::max())) {
                return NUdf::TUnboxedValuePod();
            }
        }
        i64 lvAbs = (lv > 0) ? lv : -lv;
        i64 rvAbs = (rv > 0) ? rv : -rv;
        if (rvAbs != 0 && (std::numeric_limits<i64>::max() / rvAbs < lvAbs)) {
            return NUdf::TUnboxedValuePod();
        }
        return IsBadInterval<TOutput>(ret) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(ret);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto bbMain = BasicBlock::Create(context, "bbMain", ctx.Func);
        const auto bbDone = BasicBlock::Create(context, "bbDone", ctx.Func);
        const auto resultType = Type::getInt128Ty(context);
        const auto result = PHINode::Create(resultType, 3, "result", bbDone);

        const auto lv = GetterFor<typename TLeft::TLayout>(left, context, block);
        const auto lhs = StaticCast<typename TLeft::TLayout, i64>(lv, context, block);
        const auto rv = GetterFor<typename TRight::TLayout>(right, context, block);
        const auto rhs = StaticCast<typename TRight::TLayout, i64>(rv, context, block);
        const auto mul = BinaryOperator::CreateMul(lhs, rhs, "mul", block);
        const auto zero = ConstantInt::get(Type::getInt64Ty(context), 0);
        const auto mulZero = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, mul, zero, "mulZero", block);
        const auto res = SetterFor<typename TOutput::TLayout>(mul, context, block);

        BranchInst::Create(bbDone, bbMain, mulZero, block);
        result->addIncoming(res, block);

        block = bbMain;

        const auto lhsAbs = SelectInst::Create(
                CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, lhs, zero, "lhsPos", block),
                lhs,
                BinaryOperator::CreateNeg(lhs, "lhsNeg", block),
                "lhsAbs", block);
        const auto rhsAbs = SelectInst::Create(
                CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, rhs, zero, "rhsPos", block),
                rhs,
                BinaryOperator::CreateNeg(rhs, "rhsNeg", block),
                "rhsAbs", block);
        const auto i64Max = ConstantInt::get(Type::getInt64Ty(context), std::numeric_limits<i64>::max());
        const auto div = BinaryOperator::CreateSDiv(i64Max, rhsAbs, "div", block);
        const auto mulOverflow = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, lhsAbs, div, "mulOverflow", block);
        const auto i64Overflow = BinaryOperator::CreateOr(
                GenIsInt64Overflow<typename TLeft::TLayout>(lv, context, block),
                GenIsInt64Overflow<typename TRight::TLayout>(rv, context, block),
                "i64Overflow", block);
        const auto bad = BinaryOperator::CreateOr(
                BinaryOperator::CreateOr(i64Overflow, mulOverflow, "overflow", block),
                GenIsBadInterval<TOutput>(mul, context, block),
                "bad", block);
        const auto null = ConstantInt::get(resultType, 0);
        const auto sel = SelectInst::Create(bad, null, res, "sel", block);

        result->addIncoming(sel, block);
        BranchInst::Create(bbDone, block);
        block = bbDone;
        return result;
    }
#endif
};

}

template <typename TInterval>
void RegisterIntervalMul(IBuiltinFunctionRegistry& registry) {
    RegisterFunctionBinPolyOpt<NUdf::TDataType<ui8>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<i8>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<ui16>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<i16>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<ui32>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<i32>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<ui64>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<NUdf::TDataType<i64>, TInterval,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");

    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui8>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i8>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui16>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i16>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui32>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i32>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<ui64>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinPolyOpt<TInterval, NUdf::TDataType<i64>,
        TInterval, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
}

void RegisterMul(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TMul, TBinaryArgsOpt>(registry, "Mul");
    RegisterIntervalMul<NUdf::TDataType<NUdf::TInterval>>(registry);
    RegisterIntervalMul<NUdf::TDataType<NUdf::TInterval64>>(registry);
}

template <typename TInterval>
void RegisterIntervalMul(TKernelFamilyBase& owner) {
    AddBinaryKernelPoly<NUdf::TDataType<i8>, TInterval, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<NUdf::TDataType<ui8>, TInterval, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<NUdf::TDataType<i16>, TInterval, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<NUdf::TDataType<ui16>, TInterval, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<NUdf::TDataType<i32>, TInterval, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<NUdf::TDataType<ui32>, TInterval, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<NUdf::TDataType<i64>, TInterval, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<NUdf::TDataType<ui64>, TInterval, TInterval, TNumMulInterval>(owner);

    AddBinaryKernelPoly<TInterval, NUdf::TDataType<i8>, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<TInterval, NUdf::TDataType<ui8>, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<TInterval, NUdf::TDataType<i16>, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<TInterval, NUdf::TDataType<ui16>, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<TInterval, NUdf::TDataType<i32>, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<TInterval, NUdf::TDataType<ui32>, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<TInterval, NUdf::TDataType<i64>, TInterval, TNumMulInterval>(owner);
    AddBinaryKernelPoly<TInterval, NUdf::TDataType<ui64>, TInterval, TNumMulInterval>(owner);
}

void RegisterMul(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TKernelFamilyBase>();

    AddBinaryIntegralKernels<TMul>(*family);
    AddBinaryRealKernels<TMul>(*family);

    RegisterIntervalMul<NUdf::TDataType<NUdf::TInterval>>(*family);
    RegisterIntervalMul<NUdf::TDataType<NUdf::TInterval64>>(*family);

    kernelFamilyMap["Mul"] = std::move(family);
}

} // namespace NMiniKQL
} // namespace NKikimr
