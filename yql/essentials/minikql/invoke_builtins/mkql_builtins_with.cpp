#include "mkql_builtins_compare.h"

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool Reverse>
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
        return EmitFunctionCall<StringFunc>(string->getType(), {string, sub}, ctx, block);
    }
#endif
};

template <NUdf::EDataSlot>
using TStartsWith = TStringWith<&StringWith<false>>;
template <NUdf::EDataSlot>
using TEndsWith = TStringWith<&StringWith<true>>;
template <NUdf::EDataSlot>
using TContains = TStringWith<&StringContains>;

} // namespace

void RegisterWith(IBuiltinFunctionRegistry& registry) {
    RegisterCompareStrings<TStartsWith, TCompareArgsOpt, false>(registry, "StartsWith");
    RegisterCompareStrings<TEndsWith, TCompareArgsOpt, false>(registry, "EndsWith");
    RegisterCompareStrings<TContains, TCompareArgsOpt, false>(registry, "StringContains");
}

} // namespace NMiniKQL
} // namespace NKikimr
