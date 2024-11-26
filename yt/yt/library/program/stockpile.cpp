#include "stockpile.h"

#include <library/cpp/yt/memory/leaky_singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TStockpileManager* TStockpileManager::Get()
{
    return LeakySingleton<TStockpileManager>();
}

void TStockpileManager::Reconfigure(TStockpileOptions options)
{
    auto guard = Guard(SpinLock_);

    ShouldProceed_.store(false);

    for (const auto& thread : Threads_) {
        thread->join();
    }

    Threads_.clear();
    ShouldProceed_.store(true);

    for (int threadIndex = 0; threadIndex < options.ThreadCount; ++threadIndex) {
        Threads_.push_back(std::make_unique<std::thread>([options, this] {
            RunStockpileThread(options, &ShouldProceed_);
        }));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
