#pragma once

#include <util/generic/string.h>

#include <WAVM/Platform/Diagnostics.h>
#include <WAVM/Inline/BasicTypes.h>

namespace NKikimr::NUdfStore::NWasm {

//! Formats only user-module wasm frames (drops host!, thnk!, wasm!env!).
//! Frame text from describeCallStack looks like:
//!   "0x7f… wasm!Module!func+op at path:line"
TString FormatUserWasmCallStack(const WAVM::Platform::CallStack& callStack);

//! Captures the current call stack and formats user wasm frames only.
TString FormatUserWasmCallStackFromCurrent(WAVM::Uptr omitTopFrames = 1);

} // namespace NKikimr::NUdfStore::NWasm
