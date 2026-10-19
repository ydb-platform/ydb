#include "compartment.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

// Y_WEAK is a no-op on Windows (COFF); omitting stubs avoids duplicate
// symbols with the strong overrides in library/wasm/engine.
// In the GN build (shared libraries) the engine is a link slot provider and
// the stubs would win over it at run time: they are left out there.
#if defined(__GNUC__) && !defined(GN_SLOT_SELF_wasm)

Y_WEAK void IWebAssemblyCompartment::AddPrecompiledModule(const TModuleBytecode& /*bytecode*/, TStringBuf /*name*/)
{
    YT_UNIMPLEMENTED();
}

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateImageFromSdk(const TModuleBytecode& /*bytecode*/)
{
    YT_UNIMPLEMENTED();
}

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateEmptyImage()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateMinimalRuntimeImage()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateStandardRuntimeImage()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK std::unique_ptr<IWebAssemblyCompartment> CreateQueryLanguageImage()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK IWebAssemblyCompartment* GetCurrentCompartment()
{
    YT_UNIMPLEMENTED();
}

Y_WEAK void SetCurrentCompartment(IWebAssemblyCompartment*)
{
    YT_UNIMPLEMENTED();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
