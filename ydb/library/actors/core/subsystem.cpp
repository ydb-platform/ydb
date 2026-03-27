#include "subsystem.h"

#include <atomic>

namespace NActors {

    namespace {
        std::atomic_size_t Counter_ = 0;
    }

    size_t TSubSystemRegistry::NextIndex() noexcept {
        return Counter_.fetch_add(1, std::memory_order_relaxed);
    }

} // namespace NActors
