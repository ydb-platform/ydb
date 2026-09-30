#include "allocation_cache.h"

#include <limits>

namespace NActors {

namespace {
    thread_local TAllocationCacheWorker* CurrentWorker = nullptr;
}

TAllocationCacheWorker* TAllocationCacheWorker::GetCurrent() noexcept {
    return CurrentWorker;
}

void TAllocationCacheWorker::SetCurrent(TAllocationCacheWorker* worker) noexcept {
    CurrentWorker = worker;
}

TAllocationCacheSubSystem::~TAllocationCacheSubSystem() {
    Y_ABORT_UNLESS(Workers.empty(), "allocation-cache workers must stop before their subsystem");
}

TAllocationCacheWorker::~TAllocationCacheWorker() {
    Caches.clear();
    if (Owner) {
        Owner->UnregisterWorker(Counters);
    }
}

void TAllocationCacheSubSystem::RegisterFamily(size_t family, size_t budget,
        size_t binCount, size_t minimumSize, TFactory factory) {
    Y_ABORT_UNLESS(!Frozen, "allocation cache families are frozen");
    if (Families.size() <= family) {
        Families.resize(family + 1);
    }
    Y_ABORT_UNLESS(!Families[family].Factory, "duplicate allocation cache family");
    Y_ABORT_UNLESS(budget <= std::numeric_limits<size_t>::max() - WorkerBudget);
    WorkerBudget += budget;
    Families[family] = {binCount, minimumSize, std::move(factory)};
}

void TAllocationCacheSubSystem::OnBeforeStart(TActorSystem&) {
    Frozen = true;
}

void TAllocationCacheSubSystem::OnExecutorThreadStart(TThreadContext*) {
    Y_ABORT_UNLESS(!TAllocationCacheWorker::GetCurrent());
    auto worker = CreateWorker();
    auto* current = worker.get();
    {
        std::lock_guard guard(WorkersMutex);
        ExecutorWorkers.push_back(std::move(worker));
    }
    TAllocationCacheWorker::SetCurrent(current);
}

void TAllocationCacheSubSystem::OnExecutorThreadStop(TThreadContext*) {
    auto* current = TAllocationCacheWorker::GetCurrent();
    std::unique_ptr<TAllocationCacheWorker> worker;
    {
        std::lock_guard guard(WorkersMutex);
        auto it = std::find_if(ExecutorWorkers.begin(), ExecutorWorkers.end(),
            [current](const auto& entry) { return entry.get() == current; });
        Y_ABORT_UNLESS(it != ExecutorWorkers.end());
        worker = std::move(*it);
        ExecutorWorkers.erase(it);
    }
    TAllocationCacheWorker::SetCurrent(nullptr);
    // Destruction unregisters counters under WorkersMutex.
}

std::unique_ptr<TAllocationCacheWorker> TAllocationCacheSubSystem::CreateWorker() {
    Y_ABORT_UNLESS(Frozen);
    auto counters = std::make_unique<TAllocationCacheWorkerCounters>();
    counters->Families.reserve(Families.size());
    auto worker = std::make_unique<TAllocationCacheWorker>();
    worker->Caches.reserve(Families.size());
    worker->CachePointers.resize(Families.size());
    for (size_t family = 0; family < Families.size(); ++family) {
        const auto& config = Families[family];
        counters->Families.emplace_back(config.BinCount, config.MinimumSize);
        if (config.Factory) {
            worker->Caches.push_back(config.Factory(&counters->Families.back()));
            worker->CachePointers[family] = worker->Caches.back().get();
        }
    }
    std::lock_guard guard(WorkersMutex);
    worker->Counters = counters.get();
    Workers.push_back(std::move(counters));
    worker->Owner = this;
    return worker;
}

void TAllocationCacheSubSystem::UnregisterWorker(TAllocationCacheWorkerCounters* counters) {
    std::lock_guard guard(WorkersMutex);
    std::erase_if(Workers, [counters](const auto& entry) { return entry.get() == counters; });
}

TAllocationCacheProcessStats TAllocationCacheSubSystem::GetCachedStats(size_t family) const {
    TAllocationCacheProcessStats stats;
    std::lock_guard guard(WorkersMutex);
    for (const auto& entry : Workers) {
        if (family < entry->Families.size()) {
            stats.Add(entry->Families[family].GetCachedStats());
        }
    }
    return stats;
}

} // namespace NActors
