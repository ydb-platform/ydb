#include "colors.h"

#include <atomic>

namespace NYdbWorkload {

namespace {
    std::atomic<TColorsProvider> ColorsProvider{nullptr};
}

void SetColorsProvider(TColorsProvider provider) {
    ColorsProvider.store(provider, std::memory_order_relaxed);
}

NColorizer::TColors& GetColors(IOutputStream& out) {
    if (const auto provider = ColorsProvider.load(std::memory_order_relaxed)) {
        return provider(out);
    }
    return NColorizer::AutoColors(out);
}

} // namespace NYdbWorkload


