#include "mkql_builtins_decimal.h" // Y_IGNORE

#include <cmath>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <typename TLeft, typename TRight, typename TOutput, bool IsRightOptional>
struct TNanvl {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        const auto lv = left.Get<TLeft>();
        if (!std::isnan(lv)) {
            return std::is_same<TLeft, TOutput>() ? left : NUdf::TUnboxedValuePod(static_cast<TOutput>(lv));
        }

        if (std::is_same<TRight, TOutput>()) {
            return right;
        } else {
            if (IsRightOptional && !right) {
                return NUdf::TUnboxedValuePod();
            }

            const auto rv = right.Get<TRight>();
            return NUdf::TUnboxedValuePod(static_cast<TOutput>(rv));
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        auto& module = ctx.Codegen.GetModule();
        const auto val = GetterFor<TLeft>(left, context, block);
        const auto fnType = FunctionType::get(Type::getInt1Ty(context), {val->getType()}, false);
        const auto name = std::is_same<TLeft, float>() ? "MyFloatIsNan" : "MyDoubleIsNan";
        ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(static_cast<bool(*)(TLeft)>(&std::isnan)));
        const auto func = module.getOrInsertFunction(name, fnType).getCallee();
        const auto isnan = CallInst::Create(fnType, func, {val}, "isnan", block);

        const auto lout = std::is_same<TLeft, TOutput>() ? left : SetterFor<TOutput>(StaticCast<TLeft, TOutput>(val, context, block), context, block);
        const auto rout = std::is_same<TRight, TOutput>() ? right : SetterFor<TOutput>(StaticCast<TRight, TOutput>(GetterFor<TRight>(right, context, block), context, block), context, block);

        if (IsRightOptional && !std::is_same<TRight, TOutput>()) {
            const auto nanvl = SelectInst::Create(isnan, rout, lout, "nanvl", block);
            return nanvl;
        } else {
            const auto nanvl = SelectInst::Create(isnan, rout, lout, "nanvl", block);
            return nanvl;
        }
    }
#endif
};

struct TDecimalNanvl {
    static NUdf::TUnboxedValuePod Execute(const NUdf::TUnboxedValuePod& left, const NUdf::TUnboxedValuePod& right)
    {
        return NYql::NDecimal::IsComparable(left.GetInt128()) ? left : right;
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* left, Value* right, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto good = NDecimal::GenIsComparable(GetterForInt128(left, block), context, block);
        const auto sel = SelectInst::Create(good, left, right, "sel", block);
        return sel;
    }
#endif
};

template <
    typename TInput1, typename TInput2, typename TOutput,
    template<typename, typename, typename, bool> class TFunc,
    template<typename, typename, typename, bool, bool> class TArgs
>
void RegisterBinaryNavlLeftOpt(IBuiltinFunctionRegistry& registry, const char* name) {
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout, false>, TArgs<TInput1, TInput2, TOutput, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout, true>, TArgs<TInput1, TInput2, TOutput, false, true>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout, false>, TArgs<TInput1, TInput2, TOutput, true, false>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TFunc<typename TInput1::TLayout, typename TInput2::TLayout, typename TOutput::TLayout, true>, TArgs<TInput1, TInput2, TOutput, true, true>, TBinaryWrap<true, false>>(registry, name);
}

void RegisterBinaryNavlFunction(IBuiltinFunctionRegistry& registry, const char* name) {
    RegisterBinaryNavlLeftOpt<NUdf::TDataType<float>, NUdf::TDataType<float>, NUdf::TDataType<float>, TNanvl, TBinaryArgsOpt>(registry, name);
    RegisterBinaryNavlLeftOpt<NUdf::TDataType<float>, NUdf::TDataType<double>, NUdf::TDataType<double>, TNanvl, TBinaryArgsOpt>(registry, name);
    RegisterBinaryNavlLeftOpt<NUdf::TDataType<double>, NUdf::TDataType<float>, NUdf::TDataType<double>, TNanvl, TBinaryArgsOpt>(registry, name);
    RegisterBinaryNavlLeftOpt<NUdf::TDataType<double>, NUdf::TDataType<double>, NUdf::TDataType<double>, TNanvl, TBinaryArgsOpt>(registry, name);
}

void RegisterBinaryNavlDecimal(IBuiltinFunctionRegistry& registry, const char* name) {
    RegisterFunctionImpl<TDecimalNanvl, TBinaryArgsOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, false, false>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TDecimalNanvl, TBinaryArgsOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, false, true>, TBinaryWrap<false, false>>(registry, name);
    RegisterFunctionImpl<TDecimalNanvl, TBinaryArgsOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, true, false>, TBinaryWrap<true, false>>(registry, name);
    RegisterFunctionImpl<TDecimalNanvl, TBinaryArgsOpt<NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, NUdf::TDataType<NUdf::TDecimal>, true, true>, TBinaryWrap<true, false>>(registry, name);
}

}

void RegisterNanvl(IBuiltinFunctionRegistry& registry) {
    RegisterBinaryNavlFunction(registry, "Nanvl");
    RegisterBinaryNavlDecimal(registry, "Nanvl");
}

} // namespace NMiniKQL
} // namespace NKikimr
