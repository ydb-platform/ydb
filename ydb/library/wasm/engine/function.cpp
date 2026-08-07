#include "wavm_private_imports.h"

#include <ydb/library/wasm/api/function.h>

namespace NYdb::NWasm {

using namespace WAVM;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

void WavmInvoke(
    IWebAssemblyCompartment* compartment,
    TWebAssemblyRuntimeType type,
    TCompartmentFunctionId runtimeFunction,
    TWavmPodValue* result,
    TRange<TWavmPodValue> arguments)
{
    // TODO: GetFunction(name) may return null (typo / missing export). Passing
    // null into invokeFunction crashes inside WAVM. Before throwing here, find
    // a catch site so the process does not terminate on an uncaught exception
    // (TCompartmentFunction callers / actor boundaries currently do not catch
    // yexception from this path).
    const auto wavmType = IR::FunctionType(
        IR::FunctionType::Encoding{
            std::bit_cast<Uptr>(type)});

    Runtime::invokeFunction(
        static_cast<Runtime::Context*>(compartment->GetContext()),
        static_cast<Runtime::Function*>(runtimeFunction),
        wavmType,
        std::bit_cast<IR::UntaggedValue*>(arguments.Begin()),
        std::bit_cast<IR::UntaggedValue*>(result));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
