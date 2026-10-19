#include "compartment.h"
#include "data_transfer.h"
#include "function.h"
#include "memory_pool.h"
#include "type_builder.h"

#include <library/cpp/yt/assert/assert.h>

// The default implementations of the API: weak, overridden by the strong
// definitions of ydb/library/wasm/engine when a program links it.
// Y_WEAK is a no-op on Windows (COFF); omitting the stubs avoids duplicate
// symbols with the strong overrides there.
// In the GN build (shared libraries) this file is a link slot provider of its
// own, wasm_default (see ya.make): a program links either it or the engine.

#if defined(__GNUC__)

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

Y_WEAK TCopyGuard::~TCopyGuard()
{
    if (Compartment_ != nullptr && CopiedOffset_ != 0) {
        try {
            Compartment_->FreeBytes(CopiedOffset_);
        } catch (...) {
            // FreeBytes may throw; never throw from dtor.
        }
    }
}

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

Y_WEAK TWebAssemblyMemoryPool::~TWebAssemblyMemoryPool()
{
    try {
        Clear();
    } catch (...) {
        // FreeBytes may throw; never throw from dtor.
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TWebAssemblyRuntimeType GetTypeId(
    bool /*intrinsic*/,
    EWebAssemblyValueType /*returnType*/,
    TRange<EWebAssemblyValueType> /*argumentTypes*/)
{
    YT_UNIMPLEMENTED();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm

#endif
