#pragma once

#include <cstdint>

namespace NYdb::NWasm::NAsync {

// Experimental P1 ABI. Byte payloads deliberately do not expose Bridge handles.
inline constexpr uint32_t AbiVersion = 1;
using THandle = uint64_t;

enum class EOperationKind : uint32_t {
    Request,
    Timer,
};

enum class EOperationStatus : uint32_t {
    Pending,
    Ready,
    Failed,
    Cancelled,
};

} // namespace NYdb::NWasm::NAsync

extern "C" {
uint64_t WasmAsyncOperationStart(const void* request, uint64_t size);
uint64_t WasmAsyncTimerStart(uint64_t delayMicroseconds);
uint32_t WasmAsyncOperationPoll(uint64_t operation);
uint64_t WasmAsyncOperationSize(uint64_t operation);
void WasmAsyncOperationRead(uint64_t operation, void* output, uint64_t size);
void WasmAsyncOperationCancel(uint64_t operation);
void WasmAsyncOperationDrop(uint64_t operation);
void WasmAsyncCallWait(uint64_t operation);
void WasmAsyncCallComplete(const void* result, uint64_t size, uint32_t status);
}
