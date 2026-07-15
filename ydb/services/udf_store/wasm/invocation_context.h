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

} // namespace NKikimr::NUdfStore::NWasm
