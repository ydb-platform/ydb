#include "stockpile.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TStockpileManager* TStockpileManager::Get()
{
    return Singleton<TStockpileManager>();
}

void TStockpileManager::Reconfigure(TStockpileOptions options)
{
    auto guard = Guard(SpinLock_);

    ThreadState_->ShouldProceed.store(false);

    for (auto& thread : Threads_) {
        thread->join();
    }

    Threads_.clear();
    ThreadState_ = New<TStockpileThreadState>();
    ThreadState_->ShouldProceed.store(true, std::memory_order_release);

    for (int threadIndex = 0; threadIndex < options.ThreadCount; ++threadIndex) {
        Threads_.push_back(std::make_unique<std::thread>([options, state = ThreadState_] {
            RunStockpileThread(options, &state->ShouldProceed);
        }));
    }
}

TStockpileManager::~TStockpileManager()
{
    ThreadState_->ShouldProceed.store(false);

    for (auto& thread : Threads_) {
        thread->detach();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
