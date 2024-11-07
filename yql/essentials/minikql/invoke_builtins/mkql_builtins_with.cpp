#include "mkql_builtins_compare.h"

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool Reverse>
NUdf::TUnboxedValuePod StringWith(const NUdf::TUnboxedValuePod full, const NUdf::TUnboxedValuePod part) {
    const std::string_view one = full.AsStringRef();
    const std::string_view two = part.AsStringRef();
    return NUdf::TUnboxedValuePod(Reverse ? one.ends_with(two) : one.starts_with(two));
}

NUdf::TUnboxedValuePod StringContains(const NUdf::TUnboxedValuePod full, const NUdf::TUnboxedValuePod part) {
    const std::string_view one = full.AsStringRef();
    const std::string_view two = part.AsStringRef();
    return NUdf::TUnboxedValuePod(one.contains(two));
}

template <NUdf::TUnboxedValuePod (*StringFunc)(const NUdf::TUnboxedValuePod full, const NUdf::TUnboxedValuePod part)>
struct TStringWith {
    static NUdf::TUnboxedValuePod Execute(NUdf::TUnboxedValuePod string, NUdf::TUnboxedValuePod sub)
    {
        return StringFunc(string, sub);
    }
#ifndef MKQL_DISABLE_CODEGEN
    static Value* Generate(Value* string, Value* sub, const TCodegenContext& ctx, BasicBlock*& block)
    {
        auto& context = ctx.Codegen.GetContext();
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(StringFunc));
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(string->getType(), {string->getType(), sub->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "func", block);
            const auto result = CallInst::Create(funType, funcPtr, {string, sub}, "has", block);
            return result;
        } else {
            const auto ptrArg = new AllocaInst(string->getType(), 0U, "arg", block);
            const auto ptrSub = new AllocaInst(sub->getType(), 0U, "sub", block);
            const auto ptrResult = new AllocaInst(string->getType(), 0U, "result", block);
            new StoreInst(string, ptrArg, block);
            new StoreInst(sub, ptrSub, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {ptrResult->getType(), ptrArg->getType(), ptrSub->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "func", block);
            CallInst::Create(funType, funcPtr, {ptrResult, ptrArg, ptrSub}, "", block);
            const auto result = new LoadInst(string->getType(), ptrResult, "has", block);
            return result;
        }
    }
#endif
};

template<NUdf::EDataSlot> using TStartsWith = TStringWith<&StringWith<false>>;
template<NUdf::EDataSlot> using TEndsWith = TStringWith<&StringWith<true>>;
template<NUdf::EDataSlot> using TContains = TStringWith<&StringContains>;

}

void RegisterWith(IBuiltinFunctionRegistry& registry) {
    RegisterCompareStrings<TStartsWith, TCompareArgsOpt, false>(registry, "StartsWith");
    RegisterCompareStrings<TEndsWith, TCompareArgsOpt, false>(registry, "EndsWith");
    RegisterCompareStrings<TContains, TCompareArgsOpt, false>(registry, "StringContains");
}

} // namespace NMiniKQL
} // namespace NKikimr
