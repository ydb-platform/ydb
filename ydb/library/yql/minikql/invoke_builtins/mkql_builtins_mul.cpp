#include "mkql_builtins_impl.h"
#include "mkql_builtins_datetime.h"

#include <ydb/library/yql/minikql/mkql_type_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TLeft, typename TRight, typename TOutput>
struct TMul : public TSimpleArithmeticBinary<TLeft, TRight, TOutput, TMul<TLeft, TRight, TOutput>> {
    static constexpr bool DefaultNulls = true;

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
    static_assert(std::is_same<TOutput, i64>::value, "expected i64");
    static_assert(std::is_integral<TLeft>::value, "left must be integral");
    static_assert(std::is_integral<TRight>::value, "right must be integral");

    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = static_cast<TOutput>(left.template Get<TLeft>());
        const auto rv = static_cast<TOutput>(right.template Get<TRight>());
        const auto ret = lv * rv;
        return IsBadInterval(ret) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(FromScaledDate<TOutput>(ret));;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto lhs = StaticCast<TLeft, i64>(GetterFor<TLeft>(left, context, block), context, block);
        const auto rhs = StaticCast<TRight, i64>(GetterFor<TRight>(right, context, block), context, block);
        const auto mul = BinaryOperator::CreateMul(lhs, rhs, "mul", block);
        const auto full = SetterFor<TOutput>(mul, context, block);
        const auto bad = GenIsBadInterval(mul, context, block);
        const auto zero = ConstantInt::get(Type::getInt128Ty(context), 0);
        const auto sel = SelectInst::Create(bad, zero, full, "sel", block);
        return sel;
    }
#endif
};

}

void RegisterMul(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNumericFunctionOpt<TMul, TBinaryArgsOpt>(registry, "Mul");

    RegisterFunctionBinOpt<NUdf::TDataType<ui8>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<i8>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<ui16>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<i16>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<ui32>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<i32>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<ui64>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<i64>, NUdf::TDataType<NUdf::TInterval>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");

    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<ui8>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<i8>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<ui16>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<i16>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<ui32>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<i32>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<ui64>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
    RegisterFunctionBinOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<i64>,
        NUdf::TDataType<NUdf::TInterval>, TNumMulInterval, TBinaryArgsOptWithNullableResult>(registry, "Mul");
}

void RegisterMul(TKernelFamilyMap& kernelFamilyMap) {
    kernelFamilyMap["Mul"] = std::make_unique<TBinaryNumericKernelFamily<TMul>>();
}

} // namespace NMiniKQL
} // namespace NKikimr
