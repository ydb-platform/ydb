#pragma once

#include "registry.h"

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/memory_pool.h>

namespace NKikimr::NUdfStore::NWasm {

struct TWasmUdfInvocationContext {
    NYdb::NWasm::TWebAssemblyMemoryPool WebAssemblyPool;

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
