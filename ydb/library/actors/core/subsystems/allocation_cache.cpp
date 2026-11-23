#include "allocation_cache.h"

#include <ydb/library/actors/core/thread_context.h>

#include <limits>

namespace NActors {

namespace {
    thread_local TAllocationCacheWorker* CurrentWorker = nullptr;
    std::atomic<size_t> FamilyCounter = SystemAllocationCacheFamilyCount;
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
        TlsThreadContext->AllocationCachePointers = worker ? worker->CachePointers : TAllocationCachePointers{};
    }
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

void TAllocationCacheSubSystem::OnExecutorThreadPrepare(TThreadContext* context) {
    Y_ABORT_UNLESS(Workers.emplace(context, CreateWorker()).second);
}

void TAllocationCacheSubSystem::OnExecutorThreadStart(TThreadContext* context) {
    Y_ABORT_UNLESS(!TAllocationCacheWorker::GetCurrent());
    const auto it = Workers.find(context);
    Y_ABORT_UNLESS(it != Workers.end());
    TAllocationCacheWorker::SetCurrent(it->second.get());
}

void TAllocationCacheSubSystem::OnExecutorThreadStop(TThreadContext*) {
    TAllocationCacheWorker::SetCurrent(nullptr);
}

std::unique_ptr<TAllocationCacheWorker> TAllocationCacheSubSystem::CreateWorker() {
    Y_ABORT_UNLESS(Frozen);
    auto worker = std::make_unique<TAllocationCacheWorker>();
    worker->Counters.Families.resize(Families.size());
    worker->Caches.reserve(Families.size());
    worker->CachePointers.resize(Families.size());
    for (size_t family = 0; family < Families.size(); ++family) {
        const auto& config = Families[family];
        if (config.Factory) {
            worker->Caches.push_back(config.Factory(&worker->Counters.Families[family]));
            worker->CachePointers[family] = worker->Caches.back().get();
        }
    }
    return worker;
}

TAllocationCacheProcessStats TAllocationCacheSubSystem::GetCachedStats(size_t family) const {
    TAllocationCacheProcessStats stats;
    for (const auto& entry : Workers) {
        if (family < entry.second->Counters.Families.size()) {
            stats.Add(entry.second->Counters.Families[family].GetCachedStats());
        }
    }
    return stats;
}

void TAllocationCacheSubSystem::GetFamilyStats(std::vector<TAllocationCacheFamilyStats>* stats) const {
    stats->clear();
    stats->reserve(Families.size());
    for (size_t family = 0; family < Families.size(); ++family) {
        if (!Families[family].Factory) {
            continue;
        }
        auto& snapshot = stats->emplace_back();
        snapshot.Name = Families[family].Name;
        for (const auto& worker : Workers) {
            snapshot.Stats.Add(worker.second->Counters.Families[family].GetCachedStats());
        }
    }
}

} // namespace NActors
