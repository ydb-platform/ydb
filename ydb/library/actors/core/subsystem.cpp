#include "subsystem.h"

#include <atomic>

namespace NActors {

    size_t TSubSystemRegistry::NextIndex() noexcept {
        static std::atomic_size_t counter = 0;
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

} // namespace NActors
