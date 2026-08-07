#ifndef WASM_TYPE_BUILDER_INL_H_
#error "Direct inclusion of this file is not allowed, include type_builder.h"
// For the sake of sane code completion.
#include "type_builder.h"
#endif

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
EWebAssemblyValueType InferType();

////////////////////////////////////////////////////////////////////////////////

TWebAssemblyRuntimeType GetTypeId(
    bool intrinsic,
    EWebAssemblyValueType returnType,
    TRange<EWebAssemblyValueType> argumentTypes);

template <typename THead, typename... TTail>
Y_FORCE_INLINE void InferTypesImpl(TMutableRange<EWebAssemblyValueType> range)
{
    range.Front() = InferType<THead>();
    if constexpr (sizeof...(TTail) > 0) {
        InferTypesImpl<TTail...>(range.Slice(1, range.Size()));
    }
}

template <typename... TArguments>
Y_FORCE_INLINE void InferTypes(TMutableRange<EWebAssemblyValueType> range)
{
    if constexpr (sizeof...(TArguments) > 0) {
        InferTypesImpl<TArguments...>(range);
    }
}

template <bool IsIntrinsic, typename TResult, typename... TArguments>
Y_FORCE_INLINE TWebAssemblyRuntimeType TFunctionTypeBuilder<IsIntrinsic, TResult(TArguments...)>::Get()
{
    static const auto typeId = [] {
        std::array<EWebAssemblyValueType, sizeof...(TArguments)> argumentTypes;
        InferTypes<TArguments...>(TMutableRange(argumentTypes));
        return GetTypeId(IsIntrinsic, InferType<TResult>(), TRange(argumentTypes));
    }();
    return typeId;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
