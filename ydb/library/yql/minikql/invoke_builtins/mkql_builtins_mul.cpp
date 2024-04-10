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
    static_assert(TOutput::Features & NYql::NUdf::TimeIntervalType, "Interval type expected");
    static_assert(std::is_integral_v<typename TLeft::TLayout>, "left must be integral");
    static_assert(std::is_integral_v<typename TRight::TLayout>, "right must be integral");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = static_cast<typename TOutput::TLayout>(left.template Get<typename TLeft::TLayout>());
        const auto rv = static_cast<typename TOutput::TLayout>(right.template Get<typename TRight::TLayout>());
        const auto ret = lv * rv;
        return IsBadInterval<TOutput>(ret) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(ret);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = StaticCast<typename TLeft::TLayout, i64>(
                GetterFor<typename TLeft::TLayout>(left, context, block), context, block);
        const auto rhs = StaticCast<typename TRight::TLayout, i64>(
                GetterFor<typename TRight::TLayout>(right, context, block), context, block);
        const auto mul = BinaryOperator::CreateMul(lhs, rhs, "mul", block);
        const auto full = SetterFor<typename TOutput::TLayout>(mul, context, block);
        const auto bad = GenIsBadInterval<TOutput>(mul, context, block);
        const auto zero = ConstantInt::get(Type::getInt128Ty(context), 0);
        const auto sel = SelectInst::Create(bad, zero, full, "sel", block);
        return sel;
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

void RegisterMul(TKernelFamilyMap& kernelFamilyMap) {
    kernelFamilyMap["Mul"] = std::make_unique<TBinaryNumericKernelFamily<TMul, TMul>>();
}

} // namespace NMiniKQL
} // namespace NKikimr
