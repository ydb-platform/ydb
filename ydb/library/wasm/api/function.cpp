#include "function.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Y_WEAK is a no-op on Windows (COFF); omitting the stub avoids duplicate
// symbols with the strong override in library/wasm/engine.
#if defined(__GNUC__)
Y_WEAK void WavmInvoke(
    IWebAssemblyCompartment* /*compartment*/,
    TWebAssemblyRuntimeType /*runtimeType*/,
    TCompartmentFunctionId /*runtimeFunction*/,
    TWavmPodValue* /*result*/,
    TRange<TWavmPodValue> /*arguments*/)
{
    YT_UNIMPLEMENTED();
}
#endif

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
