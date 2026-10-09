#include "runtime.h"

#include "../host_intrinsic.h"

#include <util/generic/yexception.h>

namespace NKikimr::NUdfStore::NWasm::NAsync {
namespace {

TInvocation& Current() {
    auto* invocation = GetCurrentAsyncInvocation();
    Y_ENSURE(invocation, "Async import called outside an async guest entry");
    return *invocation;
}

ui64 Start(ui64 request, ui64 size) {
    auto& ctx = Current();
    return ctx.Runtime->StartOperation(ctx.Call, ctx.Runtime->ReadGuest(request, size));
}

ui32 Poll(ui64 operation) {
    auto& ctx = Current();
    return static_cast<ui32>(ctx.Runtime->PollOperation(ctx.Call, operation));
}

ui64 Timer(ui64 delayMicroseconds) {
    auto& ctx = Current();
    return ctx.Runtime->StartTimer(ctx.Call, delayMicroseconds);
}

ui64 Size(ui64 operation) {
    auto& ctx = Current();
    return ctx.Runtime->ReadOperation(ctx.Call, operation).size();
}

void Read(ui64 operation, ui64 output, ui64 size) {
    auto& ctx = Current();
    ctx.Runtime->WriteGuest(output, size, ctx.Runtime->ReadOperation(ctx.Call, operation));
}

void Cancel(ui64 operation) {
    auto& ctx = Current();
    ctx.Runtime->CancelOperation(ctx.Call, operation);
}

void Drop(ui64 operation) {
    auto& ctx = Current();
    ctx.Runtime->DropOperation(ctx.Call, operation);
}

void Wait(ui64 operation) {
    auto& ctx = Current();
    ctx.Runtime->Wait(ctx.Call, operation);
}

void Complete(ui64 result, ui64 size, ui32 status) {
    Y_ENSURE(status <= static_cast<ui32>(EOperationStatus::Cancelled), "Invalid async result status");
    auto& ctx = Current();
    ctx.Runtime->Complete(ctx.Call, ctx.Runtime->ReadGuest(result, size), static_cast<EOperationStatus>(status));
}

WASM_INTRINSIC(WasmAsyncOperationStart, Start, decltype(Start))
WASM_INTRINSIC(WasmAsyncTimerStart, Timer, decltype(Timer))
WASM_INTRINSIC(WasmAsyncOperationPoll, Poll, decltype(Poll))
WASM_INTRINSIC(WasmAsyncOperationSize, Size, decltype(Size))
WASM_INTRINSIC(WasmAsyncOperationRead, Read, decltype(Read))
WASM_INTRINSIC(WasmAsyncOperationCancel, Cancel, decltype(Cancel))
WASM_INTRINSIC(WasmAsyncOperationDrop, Drop, decltype(Drop))
WASM_INTRINSIC(WasmAsyncCallWait, Wait, decltype(Wait))
WASM_INTRINSIC(WasmAsyncCallComplete, Complete, decltype(Complete))

} // namespace

void KeepAsyncHostIntrinsicsLinked() {
    [[maybe_unused]] WAVM::Intrinsics::Function* volatile anchor = &IntrinsicFunctionWasmAsyncOperationStart;
}

} // namespace NKikimr::NUdfStore::NWasm::NAsync
