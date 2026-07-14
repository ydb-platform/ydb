#pragma once

// Helpers and conventions for wasm memory-based arguments/results.
//
// WebAssembly API patterns (see ydb/library/wasm/unittests/wasm_ut.cpp):
// - Host -> VM: CopyIntoCompartment(TStringBuf|vector<i64>, compartment) returns TCopyGuard;
//   pass guard.GetCopiedOffset() as i64 wasm argument.
// - VM -> Host: wasm returns pointer as i64/uintptr offset; read via PtrFromVM(compartment, ptr, len).
// - Per-invoke allocations: TWebAssemblyMemoryPool on top of AllocateBytes/FreeBytes.
//
// Requires wasm module with imported env.memory. Scalar-only modules do not need this path.
// Bridge ABI (bridge_abi.hpp) is the intended transport for arbitrary YQL values later.

#include <ydb/library/wasm/api/compartment.h>

namespace NWasm::NYQL {

// Memory marshaling currently lives in wasm_invoke.cpp; keep this header as the
// short ABI note for YT-style CopyIntoCompartment / PtrFromVM usage.

} // namespace NWasm::NYQL
