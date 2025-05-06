#include "mkql_builtins_compare.h"
#include "mkql_builtins_with_impl.h"

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IgnoreCase>
NUdf::TUnboxedValuePod StartsWith(const NUdf::TUnboxedValuePod full, const NUdf::TUnboxedValuePod part) {
    const std::string_view one = full.AsStringRef();
    const std::string_view two = part.AsStringRef();
    return NUdf::TUnboxedValuePod(StringStartsWith<IgnoreCase>(one, two));
}

template<bool IgnoreCase>
NUdf::TUnboxedValuePod EndsWith(const NUdf::TUnboxedValuePod full, const NUdf::TUnboxedValuePod part) {
    const std::string_view one = full.AsStringRef();
    const std::string_view two = part.AsStringRef();
    return NUdf::TUnboxedValuePod(StringEndsWith<IgnoreCase>(one, two));
}

template<bool IgnoreCase>
NUdf::TUnboxedValuePod StringContains(const NUdf::TUnboxedValuePod full, const NUdf::TUnboxedValuePod part) {
    const std::string_view one = full.AsStringRef();
    const std::string_view two = part.AsStringRef();
    return NUdf::TUnboxedValuePod(StringContainsSubstring<IgnoreCase>(one, two));
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
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<StringFunc>());
        const auto funType = FunctionType::get(string->getType(), {string->getType(), sub->getType()}, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "func", block);
        const auto result = CallInst::Create(funType, funcPtr, {string, sub}, "has", block);
        return result;
    }
#endif
};

template<NUdf::EDataSlot> using TStartsWith = TStringWith<&StartsWith<false>>;
template<NUdf::EDataSlot> using TStartsWithIgnoreCase = TStringWith<&StartsWith<true>>;
template<NUdf::EDataSlot> using TEndsWith = TStringWith<&EndsWith<false>>;
template<NUdf::EDataSlot> using TEndsWithIgnoreCase = TStringWith<&EndsWith<true>>;
template<NUdf::EDataSlot> using TContains = TStringWith<&StringContains<false>>;
template<NUdf::EDataSlot> using TContainsIgnoreCase = TStringWith<&StringContains<true>>;

}

void RegisterWith(IBuiltinFunctionRegistry& registry) {
    RegisterCompareStrings<TStartsWith, TCompareArgsOpt, false>(registry, "StartsWith");
    RegisterCompareStrings<TStartsWithIgnoreCase, TCompareArgsOpt, false>(registry, "StartsWithIgnoreCase");
    RegisterCompareStrings<TEndsWith, TCompareArgsOpt, false>(registry, "EndsWith");
    RegisterCompareStrings<TEndsWithIgnoreCase, TCompareArgsOpt, false>(registry, "EndsWithIgnoreCase");
    RegisterCompareStrings<TContains, TCompareArgsOpt, false>(registry, "StringContains");
    RegisterCompareStrings<TContainsIgnoreCase, TCompareArgsOpt, false>(registry, "StringContainsIgnoreCase");
}

} // namespace NMiniKQL
} // namespace NKikimr
