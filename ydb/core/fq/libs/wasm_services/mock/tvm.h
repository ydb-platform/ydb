#pragma once

#include <ydb/core/fq/libs/wasm_services/tvm.h>

#include <chrono>
#include <functional>

namespace NFq::NWasmServices {

using TMockTvmClock = std::function<std::chrono::steady_clock::time_point()>;

// Returns a fake ticket after delay, without sleeping or starting a thread.
// An injected clock must be nonblocking and thread-safe (used by deterministic tests).
std::shared_ptr<ITvmTicketProvider> CreateMockTvmTicketProvider(
    TDuration delay, TMockTvmClock clock = std::chrono::steady_clock::now);

} // namespace NFq::NWasmServices
