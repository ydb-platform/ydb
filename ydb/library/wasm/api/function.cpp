#include "function.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Y_WEAK is a no-op on Windows (COFF); omitting the stub avoids duplicate
// symbols with the strong override in library/wasm/engine.
// In the GN build (shared libraries) the engine is a link slot provider and
// the stubs would win over it at run time: they are left out there.
#if defined(__GNUC__) && !defined(GN_SLOT_SELF_wasm)
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
