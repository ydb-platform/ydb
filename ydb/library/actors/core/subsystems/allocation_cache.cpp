#include "allocation_cache.h"

#include <limits>

namespace NActors {

namespace {
    thread_local TAllocationCacheWorker* CurrentWorker = nullptr;
    std::atomic<size_t> FamilyCounter = 0;
}

size_t TAllocationCacheFamilyRegistry::NextId() noexcept {
    return FamilyCounter.fetch_add(1, std::memory_order_relaxed);
}

TAllocationCacheWorker* TAllocationCacheWorker::GetCurrent() noexcept {
    return CurrentWorker;
}

void TAllocationCacheWorker::SetCurrent(TAllocationCacheWorker* worker) noexcept {
    CurrentWorker = worker;
    if (TlsThreadContext) {
        TlsThreadContext->AllocationCachePointers = worker ? worker->CachePointers : std::vector<void*>{};
    }
}

TAllocationCacheSubSystem::~TAllocationCacheSubSystem() {
    Y_ABORT_UNLESS(Workers.empty(), "allocation-cache workers must stop before their subsystem");
}

TAllocationCacheWorker::~TAllocationCacheWorker() {
    if (Owner) {
        Owner->UnregisterWorker(Counters);
    }
    Caches.clear();
}

void TAllocationCacheSubSystem::RegisterFamily(size_t family, size_t budget, const TString& name, TFactory factory) {
    Y_ABORT_UNLESS(!Frozen, "allocation cache families are frozen");
    if (Families.size() <= family) {
        Families.resize(family + 1);
    }
    Y_ABORT_UNLESS(!Families[family].Factory, "duplicate allocation cache family");
    Y_ABORT_UNLESS(budget <= std::numeric_limits<size_t>::max() - WorkerBudget);
    Y_ABORT_UNLESS(!name.empty(), "allocation cache family name is empty");
    for (const auto& entry : Families) {
        Y_ABORT_UNLESS(!entry.Factory || entry.Name != name, "duplicate allocation cache family name");
    }
    WorkerBudget += budget;
    Families[family] = {name, std::move(factory)};
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
    counters->Families.resize(Families.size());
    auto worker = std::make_unique<TAllocationCacheWorker>();
    worker->Caches.reserve(Families.size());
    worker->CachePointers.resize(Families.size());
    for (size_t family = 0; family < Families.size(); ++family) {
        const auto& config = Families[family];
        if (config.Factory) {
            worker->Caches.push_back(config.Factory(&counters->Families[family]));
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

void TAllocationCacheSubSystem::GetFamilyStats(std::vector<TAllocationCacheFamilyStats>* stats) const {
    stats->clear();
    stats->reserve(Families.size());
    std::lock_guard guard(WorkersMutex);
    for (size_t family = 0; family < Families.size(); ++family) {
        if (!Families[family].Factory) {
            continue;
        }
        auto& snapshot = stats->emplace_back();
        snapshot.Name = Families[family].Name;
        for (const auto& worker : Workers) {
            snapshot.Stats.Add(worker->Families[family].GetCachedStats());
        }
    }
}

} // namespace NActors
