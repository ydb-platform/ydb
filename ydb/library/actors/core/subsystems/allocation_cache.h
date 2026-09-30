#pragma once

#include <ydb/library/actors/core/allocation_cache.h>
#include <ydb/library/actors/core/subsystem.h>

#include <functional>
#include <mutex>

namespace NActors {

// Frontends register each tag's size classes, budget and cache factory during
// dependency resolution. OnBeforeStart freezes this configuration. Executor
// thread hooks create one cache per family and publish the worker in private TLS;
// Allocate/Free select a concrete cache directly, without virtual calls or locks.
// The subsystem owns executor workers and their atomic counter records. Workers
// destroy caches before unregistering counters; the subsystem outlives workers.
// Statistics readers hold WorkersMutex while sampling records, so concurrent
// worker removal cannot invalidate them. Values are approximate during activity.
// Family budgets limit retained block capacity, excluding metadata. Missing
// caches use heap allocation with the same size rounding, allowing same-tag
// blocks to be freed on another worker or actor system.

// Different template specializations share an ownership container through void*.
// The factory supplies the matching typed deleter; hot-path calls use CachePointers.
using TLocalAllocationCache = std::unique_ptr<void, void (*)(void*)>;

class TAllocationCacheSubSystem;
struct TAllocationCacheWorkerCounters {
    // Built once before publication; never resized while caches borrow addresses.
    std::vector<TAllocationCacheCounters> Families;
};

template<class TTag> class TAllocationCacheFrontend;

// One instance per physical executor, independent of pool and worker ids.
// Owned by the cache subsystem for the executor thread lifetime.
class TAllocationCacheWorker {
public:
    TAllocationCacheWorker() = default;
    ~TAllocationCacheWorker();
    static TAllocationCacheWorker* GetCurrent() noexcept;
    static void SetCurrent(TAllocationCacheWorker* worker) noexcept;

    // Scoped bindings borrow the prior table; the prior worker must outlive them.
    explicit TAllocationCacheWorker(const TAllocationCacheWorker* previous)
        : CachePointers(previous ? previous->CachePointers : std::vector<void*>{})
    {}

    template<class TTag>
    TAllocationCache<TTag>* Get() const noexcept;

    template<class TTag>
    void Bind(TAllocationCache<TTag>* cache) {
        const size_t family = TAllocationCacheFrontend<TTag>::FamilyId();
        if (CachePointers.size() <= family) {
            CachePointers.resize(family + 1);
        }
        CachePointers[family] = cache;
    }

    void GetCachedStats(size_t family, TAllocationCacheProcessStats* stats) const noexcept {
        *stats = {};
        if (Counters && family < Counters->Families.size()) {
            *stats = Counters->Families[family].GetCachedStats();
        }
    }

private:
    friend class TAllocationCacheSubSystem;
    TAllocationCacheSubSystem* Owner = nullptr;
    TAllocationCacheWorkerCounters* Counters = nullptr;
    std::vector<TLocalAllocationCache> Caches;
    std::vector<void*> CachePointers;
};

class TAllocationCacheSubSystem final : public ISubSystem {
public:
    using TFactory = std::function<TLocalAllocationCache(TAllocationCacheCounters*)>;
    ~TAllocationCacheSubSystem() override;
    void RegisterFamily(size_t family, size_t budget, size_t binCount, size_t minimumSize, TFactory factory);
    void OnBeforeStart(TActorSystem&) override;
    void OnExecutorThreadStart(TThreadContext* context) override;
    void OnExecutorThreadStop(TThreadContext* context) override;
    std::unique_ptr<TAllocationCacheWorker> CreateWorker();
    TAllocationCacheProcessStats GetCachedStats(size_t family) const;
    size_t GetWorkerBudget() const noexcept { return WorkerBudget; }

private:
    friend class TAllocationCacheWorker;
    void UnregisterWorker(TAllocationCacheWorkerCounters* counters);
    struct TFamily {
        size_t BinCount = 0;
        size_t MinimumSize = 0;
        TFactory Factory;
    };
    std::vector<TFamily> Families;
    size_t WorkerBudget = 0;
    bool Frozen = false;
    // Protects worker lists and counter-record lifetime during statistics reads.
    // Never acquired by Allocate/Free.
    mutable std::mutex WorkersMutex;
    std::vector<std::unique_ptr<TAllocationCacheWorkerCounters>> Workers;
    std::vector<std::unique_ptr<TAllocationCacheWorker>> ExecutorWorkers;
};

// Register this frontend as a subsystem. Its dependency callback registers the
// family automatically; callers never need a second family-registration step.
// The tag is the allocation ABI: size classes and default-new alignment are
// stable for its entire lifetime, including frees in another actor system.
// The actor system installs the single allocation-cache subsystem.
// Allocate/Free are synchronous and do not access the subsystem registry.
// Extended alignment is unsupported. Free requires a nonnull block allocated
// by this tag and the original requested size.
// Family budgets bound retained block capacity, excluding cache bookkeeping.
// They are isolated; the worker's retained-capacity bound is their checked sum.
template<class TTag>
class TAllocationCacheFrontend : public ISubSystem {
public:
    explicit TAllocationCacheFrontend(size_t budget)
        : Budget(budget)
    {}

    static size_t FamilyId() noexcept {
        return TSubSystemRegistry::TItem<TAllocationCacheFrontend<TTag>>::Index();
    }

    TSubSystemDependencies GetDependencies() const override {
        return DependsOn<TAllocationCacheSubSystem>();
    }

    void OnDependenciesResolved(const TResolvedSubSystemDependencies& dependencies) override {
        auto* system = static_cast<TAllocationCacheSubSystem*>(dependencies.front().Instance);
        system->RegisterFamily(FamilyId(), Budget,
            TAllocationCache<TTag>::BinCount, TAllocationCache<TTag>::MinAllocationSize,
            [budget = Budget](TAllocationCacheCounters* counters) {
                return TLocalAllocationCache(new TAllocationCache<TTag>(budget, counters), [](void* cache) {
                    delete static_cast<TAllocationCache<TTag>*>(cache);
                });
            });
    }

    [[nodiscard]] static void* Allocate(size_t size) {
        return TAllocationCache<TTag>::AllocateCurrent(size);
    }

    static void Free(void* block, size_t size) noexcept {
        TAllocationCache<TTag>::Free(block, size);
    }

private:
    const size_t Budget;
};

template<class TTag>
TAllocationCache<TTag>* TAllocationCacheWorker::Get() const noexcept {
    const size_t family = TAllocationCacheFrontend<TTag>::FamilyId();
    return family < CachePointers.size()
        ? static_cast<TAllocationCache<TTag>*>(CachePointers[family])
        : nullptr;
}

template<class TTag>
TAllocationCache<TTag>* TAllocationCache<TTag>::GetCurrent() noexcept {
    auto* worker = TAllocationCacheWorker::GetCurrent();
    return worker ? worker->Get<TTag>() : nullptr;
}

} // namespace NActors
