#ifndef WASM_FUNCTION_INL_H_
#error "Direct inclusion of this file is not allowed, include function.h"
// For the sake of sane code completion.
#include "function.h"
#endif

#include <array>
#include <bit>
#include <type_traits>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

union alignas(16) TWavmPodValue
{
    ui64 Data;
    char Padding[16];
};

template <typename T>
Y_FORCE_INLINE ui64 EncodeWavmArgument(const T& value)
{
    if constexpr (std::is_same_v<T, float>) {
        return std::bit_cast<ui32>(value);
    } else if constexpr (sizeof(T) == sizeof(ui64)) {
        return std::bit_cast<ui64>(value);
    } else if constexpr (sizeof(T) == sizeof(ui32)) {
        return std::bit_cast<ui32>(value);
    } else {
        static_assert(sizeof(T) == sizeof(ui64) || sizeof(T) == sizeof(ui32),
            "Unsupported argument size for WebAssembly invoke");
        return 0;
    }
}

template <typename T>
Y_FORCE_INLINE T DecodeWavmResult(ui64 data)
{
    if constexpr (std::is_same_v<T, float>) {
        return std::bit_cast<float>(static_cast<ui32>(data));
    } else if constexpr (std::is_same_v<T, double>) {
        return std::bit_cast<double>(data);
    } else if constexpr (std::is_pointer_v<T>) {
        return std::bit_cast<T>(data);
    } else {
        return static_cast<T>(data);
    }
}

Y_FORCE_INLINE void ConvertToWavmArguments(TMutableRange<TWavmPodValue> range)
{
    YT_ASSERT(range.Empty());
}

template <typename THead, typename... TTail>
Y_FORCE_INLINE void ConvertToWavmArguments(
    TMutableRange<TWavmPodValue> range,
    const THead& head,
    TTail&... tail)
{
    range[0].Data = EncodeWavmArgument(head);
    ConvertToWavmArguments(range.Slice(1, range.Size()), tail...);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void WavmInvoke(
    IWebAssemblyCompartment* compartment,
    TWebAssemblyRuntimeType runtimeType,
    TCompartmentFunctionId runtimeFunction,
    TWavmPodValue* result,
    TRange<TWavmPodValue> arguments);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename TResult, typename... TArgs>
Y_FORCE_INLINE TCompartmentFunction<TResult(TArgs...)>::TCompartmentFunction(
    IWebAssemblyCompartment* compartment,
    TTypedInvokeFunction function)
    : Compartment_(compartment)
    , Function_(function)
    , RuntimeType_(
        Compartment_
        ? TFunctionTypeBuilder</*intrinsic*/ false, TResult(TArgs...)>::Get()
        : TWebAssemblyRuntimeType{nullptr})
    , RuntimeFunction_(
        Compartment_
        ? Compartment_->GetFunction(std::bit_cast<size_t>(Function_))
        : nullptr)
{ }

template <typename TResult, typename... TArgs>
Y_FORCE_INLINE TCompartmentFunction<TResult(TArgs...)>::TCompartmentFunction(
    IWebAssemblyCompartment* compartment,
    const TString& name)
    : Compartment_(compartment)
    , Function_(nullptr)
    , RuntimeType_(
        Compartment_
        ? TFunctionTypeBuilder</*intrinsic*/ false, TResult(TArgs...)>::Get()
        : TWebAssemblyRuntimeType{nullptr})
    , RuntimeFunction_(
        Compartment_
        ? Compartment_->GetFunction(name)
        : nullptr)
{ }

template <typename TResult, typename... TArgs>
Y_FORCE_INLINE TResult TCompartmentFunction<TResult(TArgs...)>::operator()(TArgs... args) const
{
    static_assert(
        std::is_integral_v<TResult> ||
            std::is_floating_point_v<TResult> ||
            std::is_same_v<TResult, void> ||
            std::is_pointer_v<TResult>,
        "Unsupported result type for WebAssembly invoke");

    if (Compartment_) {
        std::array<TWavmPodValue, sizeof...(TArgs)> arguments;

        ConvertToWavmArguments(
            TMutableRange(arguments.data(), arguments.size()),
            args...);

        if constexpr (std::is_same_v<TResult, void>) {
            NDetail::WavmInvoke(
                Compartment_,
                RuntimeType_,
                RuntimeFunction_,
                nullptr,
                TRange(arguments.data(),
                arguments.size()));

            return;
        }

        TWavmPodValue result;
        result.Data = 0;

        NDetail::WavmInvoke(
            Compartment_,
            RuntimeType_,
            RuntimeFunction_,
            &result,
            TRange(arguments.data(),
            arguments.size()));

        return DecodeWavmResult<TResult>(result.Data);
    }

    return Function_(args...);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TResult, typename... TArgs>
Y_FORCE_INLINE TCompartmentFunction<TResult(TArgs...)> PrepareFunction(
    TResult(*function)(TArgs...))
{
    return TCompartmentFunction<TResult(TArgs...)>(GetCurrentCompartment(), function);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
