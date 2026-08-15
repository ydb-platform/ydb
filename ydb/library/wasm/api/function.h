#pragma once

#include "compartment.h"
#include "type_builder.h"

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

using TCompartmentFunctionId = void*;

template <typename TResult, typename... TArgs>
class TCompartmentFunction;

template <typename TResult, typename... TArgs>
class TCompartmentFunction<TResult(TArgs...)>
{
public:
    using TTypedInvokeFunction = TResult(*)(TArgs...);

    TCompartmentFunction(IWebAssemblyCompartment* compartment, TTypedInvokeFunction function);
    TCompartmentFunction(IWebAssemblyCompartment* compartment, const TString& name);

    TResult operator()(TArgs... args) const;

private:
    IWebAssemblyCompartment* Compartment_;
    TTypedInvokeFunction Function_;
    TWebAssemblyRuntimeType RuntimeType_;
    TCompartmentFunctionId RuntimeFunction_;
};

template <typename TResult, typename... TArgs>
TCompartmentFunction<TResult(TArgs...)> PrepareFunction(
    TResult(*function)(TArgs...));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm

#define WASM_FUNCTION_INL_H_
#include "function-inl.h"
#undef WASM_FUNCTION_INL_H_
