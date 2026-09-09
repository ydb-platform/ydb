#pragma once

#include "bridge_types.h"

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/type_builder.h>
#include <ydb/library/wasm/engine/intrinsics.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>

#include <bit>

namespace NKikimr::NUdfStore::NWasm {

//! Adapts a plain host function to the shape WAVM wants for an intrinsic: it
//! prepends the ContextRuntimeData parameter WAVM passes and checks that the
//! call left the current compartment where it found it, since a swapped
//! compartment turns every later PtrFromVM into a wild pointer.
template <class TSignature>
struct TMakeUdfHostIntrinsic;

template <class TResult, class... TArgs>
struct TMakeUdfHostIntrinsic<TResult(TArgs...)>
{
    template <TResult(*FunctionPtr)(TArgs...)>
    static TResult Wrapper(WAVM::Runtime::ContextRuntimeData*, TArgs... args)
    {
        if (IsInsideGuestCallback()) {
            // Guest code the host itself invoked, calling back in. The host is
            // mid-update and may be holding a reference into a live value, so
            // a module whose "sbrk" unrefs that value would have the host read
            // freed memory right after. Nothing legitimate needs this.
            ythrow yexception()
                << "Bridge: host intrinsic called re-entrantly from guest code the host invoked";
        }
        auto* compartmentBeforeCall = NYdb::NWasm::GetCurrentCompartment();
        Y_DEFER {
            auto* compartmentAfterCall = NYdb::NWasm::GetCurrentCompartment();
            YT_VERIFY(compartmentBeforeCall == compartmentAfterCall);
        };
        return FunctionPtr(args...);
    }
};

} // namespace NKikimr::NUdfStore::NWasm

//! Publish `Fn` to guest modules under `ExportName` with the given function
//! `Signature`. Defines two names the caller can refer to: `Intrinsic##Export
//! Name` (the wrapper) and `IntrinsicFunction##ExportName` (the registration
//! static). Something must reference the latter, or the linker is free to drop
//! the whole object file and with it the registration; see the
//! KeepIntrinsicsLinked anchors.
#define WASM_INTRINSIC(ExportName, Fn, Signature) \
    constexpr auto Intrinsic##ExportName = \
        &::NKikimr::NUdfStore::NWasm::TMakeUdfHostIntrinsic<Signature>::Wrapper<&Fn>; \
    [[gnu::used]] static WAVM::Intrinsics::Function IntrinsicFunction##ExportName( \
        ::NYdb::NWasm::getIntrinsicModule_standard(), \
        #ExportName, \
        reinterpret_cast<void*>(Intrinsic##ExportName), \
        WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{ \
            std::bit_cast<WAVM::Uptr>(::NYdb::NWasm::TFunctionTypeBuilder<true, Signature>::Get()) \
        }));
