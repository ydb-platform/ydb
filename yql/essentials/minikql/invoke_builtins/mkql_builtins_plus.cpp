#include "mkql_builtins_decimal.h" // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<typename TInput, typename TOutput>
struct TPlus : public TSimpleArithmeticUnary<TInput, TOutput, TPlus<TInput, TOutput>> {
    static TOutput Do(TInput val)
    {
        return +val;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Gen(Value* arg, const TCodegenContext&, BasicBlock*&)
    {
        return arg;
    }
#endif
};

struct TDecimalPlus {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& arg) {
        return arg;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* arg, const TCodegenContext&, BasicBlock*&)
    {
        return arg;
    }
#endif
};
}

void RegisterPlus(IBuiltinFunctionRegistry& registry) {
    RegisterUnaryNumericFunctionOpt<TPlus, TUnaryArgsOpt>(registry, "Plus");
    NDecimal::RegisterUnaryFunction<TDecimalPlus, TUnaryArgsOpt>(registry, "Plus");
    RegisterFunctionUnOpt<NUdf::TDataType<NUdf::TInterval>, NUdf::TDataType<NUdf::TInterval>, TPlus, TUnaryArgsOpt>(registry, "Plus");
    RegisterFunctionUnOpt<NUdf::TDataType<NUdf::TInterval64>, NUdf::TDataType<NUdf::TInterval64>, TPlus, TUnaryArgsOpt>(registry, "Plus");
}

} // namespace NMiniKQL
} // namespace NKikimr
