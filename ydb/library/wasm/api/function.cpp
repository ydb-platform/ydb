#include "function.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

Y_WEAK void WavmInvoke(
    IWebAssemblyCompartment* /*compartment*/,
    TWebAssemblyRuntimeType /*runtimeType*/,
    TCompartmentFunctionId /*runtimeFunction*/,
    TWavmPodValue* /*result*/,
    TRange<TWavmPodValue> /*arguments*/)
{
    YT_UNIMPLEMENTED();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
