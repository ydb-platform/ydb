#pragma once

#include <util/system/types.h>

namespace NWasm::NYQL {

// Layout shared between host UDF and wasm bridge exports.
struct TWasmResult
{
    ui64 DataPtr = 0;
    ui64 DataLen = 0;
    i32 Status = 0;
    i32 Padding = 0;
    ui64 ErrorPtr = 0;
    ui64 ErrorLen = 0;
};

static_assert(sizeof(TWasmResult) == 40);

using TBridgeFunction = NYdb::NWasm::TCompartmentFunction<void(uintptr_t, ui64, uintptr_t)>;

} // namespace NWasm::NYQL
