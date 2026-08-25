#include "call_stack.h"

#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <util/string/builder.h>

namespace NKikimr::NUdfStore::NWasm {
namespace {

bool IsUserWasmFrame(TStringBuf frame) {
    const auto wasmPos = frame.find("wasm!");
    if (wasmPos == TStringBuf::npos) {
        return false;
    }
    TStringBuf rest = frame.Tail(wasmPos + /*"wasm!"*/ 5);
    if (rest.StartsWith("env!")) {
        return false;
    }
    return true;
}

} // namespace

TString FormatUserWasmCallStack(const WAVM::Platform::CallStack& callStack) {
    const auto description = WAVM::Runtime::describeCallStack(callStack);
    TStringBuilder backtrace;
    int i = 0;
    for (const auto& item : description) {
        if (!IsUserWasmFrame(item)) {
            continue;
        }
        backtrace << i++ << ". " << item << '\n';
    }
    if (i == 0) {
        backtrace << "<no user wasm frames>\n";
    }
    return backtrace;
}

TString FormatUserWasmCallStackFromCurrent(WAVM::Uptr omitTopFrames) {
    return FormatUserWasmCallStack(WAVM::Platform::captureCallStack(omitTopFrames));
}

} // namespace NKikimr::NUdfStore::NWasm
