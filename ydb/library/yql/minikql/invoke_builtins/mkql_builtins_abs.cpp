#include "mkql_builtins_decimal.h"

#include <cmath>

namespace NKikimr {
namespace NMiniKQL {

namespace {
template <typename T, std::enable_if_t<std::is_unsigned<T>::value>* = nullptr>
inline T Abs(T v) {
    return v;
}

template <typename T, std::enable_if_t<std::is_floating_point<T>::value>* = nullptr>
inline T Abs(T v) {
    return std::fabs(v);
}

template <typename T, std::enable_if_t<std::is_signed<T>::value && std::is_integral<T>::value>* = nullptr>
inline T Abs(T v) {
    return std::abs(v);
}

template<typename TInput, typename TOutput>
struct TAbs : public TSimpleArithmeticUnary<TInput, TOutput, TAbs<TInput, TOutput>> {
    static TOutput Do(TInput val)
    {
        return Abs<TInput>(val);
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext& ctx, BasicBlock*& block)
    {
        if (std::is_unsigned<TInput>())
            return arg;

        if (std::is_floating_point<TInput>()) {
            auto& module = ctx.Codegen->GetModule();
            const auto fnType = FunctionType::get(arg->getType(), {arg->getType()}, false);
            const auto& name = GetFuncNameForType<TInput>("llvm.fabs");
            const auto func = module.getOrInsertFunction(name, fnType).getCallee();
            const auto res = CallInst::Create(fnType, func, {arg}, "fabs", block);
            return res;
        } else {
            const auto zero = ConstantInt::get(arg->getType(), 0);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, arg, zero, "check", block);
            const auto neg = BinaryOperator::CreateNeg(arg, "neg", block);
            const auto res = SelectInst::Create(check, neg, arg, "result", block);
            return res;
        }
    }
#endif
};

struct TDecimalAbs {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        const auto a = arg.GetInt128();
        return a < 0 ? NUdf::TUnboxedValuePod(-a) : arg;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext&, BasicBlock*& block)
    {
        const auto val = GetterForInt128(arg, block);
        const auto zero = ConstantInt::get(val->getType(), 0);
        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, val, zero, "check", block);
        const auto neg = BinaryOperator::CreateNeg(val, "neg", block);
        const auto res = SelectInst::Create(check, SetterForInt128(neg, block), arg, "result", block);
        return res;
    }
#endif
};

}

void RegisterAbs(IBuiltinFunctionRegistry& registry) {
    RegisterUnaryNumericFunctionOpt<TAbs, TUnaryArgsOpt>(registry, "Abs");
    RegisterFunctionUnOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>, TAbs, TUnaryArgsOpt>(registry, "Abs");
    NDecimal::RegisterUnaryFunction<TDecimalAbs, TUnaryArgsOpt>(registry, "Abs");
}

} // namespace NMiniKQL
} // namespace NKikimr
