#pragma once

#include <ydb/core/fq/libs/wasm_services/tvm.h>

namespace NFq::NWasmServices {

// Schedule one completion and return a nonblocking cancellation function.
// Tests inject a controlled scheduler; the default uses an actor-system timer.
using TMockTvmScheduler = std::function<std::function<void()>(TDuration, std::function<void()>)>;

// Request must run in an actor activation when using the default scheduler.
std::shared_ptr<ITvmTicketProvider> CreateMockTvmTicketProvider(TDuration delay, TMockTvmScheduler scheduler = {});

}   // namespace NFq::NWasmServices
