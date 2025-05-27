#include "mkql_builtins_decimal.h" // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TMinus : public TSimpleArithmeticUnary<TInput, TOutput, TMinus<TInput, TOutput>> {
    static constexpr auto NullMode = TKernel::ENullMode::Default;

    static TOutput Do(TInput val)
    {
        return -val;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext&, BasicBlock*& block)
    {
        if constexpr (std::is_integral<TInput>())
            return BinaryOperator::CreateNeg(arg, "neg", block);
        else
            return UnaryOperator::CreateFNeg(arg, "neg", block);
    }
#endif
};

struct TDecimalMinus: TDecimalUnary<TDecimalMinus> {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        const auto v = arg.GetInt128();
        return NYql::NDecimal::IsComparable(v) ? NUdf::TUnboxedValuePod(-v) : arg;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        const auto val = GetterForInt128(arg, block);
        const auto ok = NDecimal::GenIsComparable(val, ctx.Codegen.GetContext(), block);
        const auto neg = BinaryOperator::CreateNeg(val, "neg", block);
        const auto res = SelectInst::Create(ok, SetterForInt128(neg, block), arg, "result", block);
        return res;
    }
#endif
};
}

void RegisterMinus(IBuiltinFunctionRegistry& registry) {
    RegisterUnaryNumericFunctionOpt<TMinus, TUnaryArgsOpt>(registry, "Minus");
    NDecimal::RegisterUnaryFunction<TDecimalMinus, TUnaryArgsOpt>(registry, "Minus");
    RegisterFunctionUnOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>, TMinus, TUnaryArgsOpt>(registry, "Minus");
    RegisterFunctionUnOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval64>, TMinus, TUnaryArgsOpt>(registry, "Minus");
}

void RegisterMinus(TKernelFamilyMap& kernelFamilyMap) {
    auto family = std::make_unique<TUnaryNumericKernelFamily<TMinus>>();
    AddUnaryDecimalKernels<TDecimalMinus>(*family);
    kernelFamilyMap["Minus"] = std::move(family);
}

} // namespace NMiniKQL
} // namespace NKikimr
