#pragma once

#include "registry.h"

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/memory_pool.h>

#include <yql/essentials/public/udf/udf_types.h>

namespace NKikimr::NUdfStore::NWasm {

struct TWasmUdfInvocationContext {
    NYdb::NWasm::TWebAssemblyMemoryPool WebAssemblyPool;
    //! Declared result type of the running bridge UDF, so the guest can ask
    //! the host to build a container of exactly that type (BridgeMakeDict).
    const NYql::NUdf::TType* ResultType = nullptr;

    explicit TWasmUdfInvocationContext(NYdb::NWasm::IWebAssemblyCompartment* compartment)
        : WebAssemblyPool(compartment)
    {}
};

inline TWasmUdfInvocationContext*& CurrentInvocationContextSlot() {
    static thread_local TWasmUdfInvocationContext* current = nullptr;
    return current;
}

inline TWasmUdfInvocationContext* GetCurrentInvocationContext() {
    return CurrentInvocationContextSlot();
}

//! Arms the wall-clock budget of a UDF call, unless we are already inside one.
//! A UDF reached through BridgeRun spends the budget of the call that entered
//! WASM: rearming there would give a guest that keeps recursing an endless
//! deadline.
inline void StartUdfDeadlineUnlessNested(NYdb::NWasm::IWebAssemblyCompartment* compartment) {
    if (GetCurrentInvocationContext()) {
        return;
    }
    compartment->SetTimeout(TDuration::Minutes(1));
    compartment->StartDeadlineTimer();
}

class TCurrentInvocationContextGuard {
public:
    explicit TCurrentInvocationContextGuard(TWasmUdfInvocationContext* context)
        : Previous_(CurrentInvocationContextSlot())
    {
        CurrentInvocationContextSlot() = context;
    }

    ~TCurrentInvocationContextGuard() {
        CurrentInvocationContextSlot() = Previous_;
    }

    TCurrentInvocationContextGuard(const TCurrentInvocationContextGuard&) = delete;
    TCurrentInvocationContextGuard& operator=(const TCurrentInvocationContextGuard&) = delete;

private:
    TWasmUdfInvocationContext* Previous_;
};

} // namespace NKikimr::NUdfStore::NWasm
