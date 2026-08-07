#include "compartment.h"

#include <library/cpp/yt/assert/assert.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
