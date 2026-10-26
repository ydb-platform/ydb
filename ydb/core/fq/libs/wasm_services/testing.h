#pragma once

#include "transport.h"

#include <functional>
#include <memory>

namespace NFq::NWasmServices {

// Install before starting requests. Tests can hold native completion after
// publication to exercise teardown while it owns the final transport reference.
struct TTransportTestAccess {
    using TCompletionPublished = std::function<void(std::weak_ptr<void>)>;
    static void SetCompletionPublishedHook(TTransportExecutor& executor, TCompletionPublished hook);
    static bool IsClosed(const TTransportExecutor& executor);
};

}   // namespace NFq::NWasmServices
