#pragma once

#include <ydb/library/wasm/api/compartment.h>
#include <ydb/library/wasm/api/memory_pool.h>

namespace NWasm::NYQL {

struct TWasmUdfInvocationContext
{
    NYdb::NWasm::TWebAssemblyMemoryPool WebAssemblyPool;

    explicit TWasmUdfInvocationContext(NYdb::NWasm::IWebAssemblyCompartment* compartment)
        : WebAssemblyPool(compartment)
    {}
};

} // namespace NWasm::NYQL
