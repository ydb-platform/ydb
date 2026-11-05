#pragma once

#include <ydb/library/actors/core/allocation_cache.h>
#include <ydb/library/actors/core/allocation_cache_families.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/string.h>

#include <functional>
#include <unordered_map>

namespace NActors {

// Families register cache factories during dependency resolution. Configuration
// is frozen before pool preparation. Preparation creates all executor caches;
// thread start/stop only bind/unbind them. The worker registry stays immutable
// while threads run and after they stop, until subsystem destruction.
// Statistics sample embedded atomic counters without locks. Readers must finish
// before subsystem destruction; snapshots are approximate during activity.
// Allocate/Free use concrete caches directly, without virtual calls or locks.

// Different template specializations share an ownership container through void*.
// The factory supplies the matching typed deleter; hot-path calls use CachePointers.
using TLocalAllocationCache = std::unique_ptr<void, void (*)(void*)>;

// Process-wide ids shared by every actor system, independent of subsystem ids.
class TAllocationCacheFamilyRegistry {
public:
    static size_t NextId() noexcept;
};

struct TAllocationCacheFamilyStats {
    TString Name;
    TAllocationCacheProcessStats Stats;
};

class TAllocationCacheSubSystem;
struct TAllocationCacheWorkerCounters {
    // Read-only views of embedded counters, owned together with the caches.
    std::vector<TAllocationCacheCounters> Families;
};

template<class TTag> class TAllocationCacheFamily;

// One instance per physical executor, independent of pool and worker ids.
// Executor caches remain owned by the subsystem until its destruction.
class TAllocationCacheWorker {
public:
    TAllocationCacheWorker() = default;
    ~TAllocationCacheWorker() = default;
    static TAllocationCacheWorker* GetCurrent() noexcept;
    static void SetCurrent(TAllocationCacheWorker* worker) noexcept;

    // Scoped bindings borrow the prior table; the prior worker must outlive them.
    explicit TAllocationCacheWorker(const TAllocationCacheWorker* previous)
        : CachePointers(previous ? previous->CachePointers : TAllocationCachePointers{})
    {}

    template<class TTag>
    TAllocationCache<TTag>* Get() const noexcept;

    template<class TTag>
    void Bind(TAllocationCache<TTag>* cache) {
        const size_t family = TAllocationCacheFamily<TTag>::FamilyId();
        if (CachePointers.size() <= family) {
            CachePointers.resize(family + 1);
        }
        CachePointers[family] = cache;
    }

    void GetCachedStats(size_t family, TAllocationCacheProcessStats* stats) const noexcept {
        *stats = {};
        if (family < Counters.Families.size()) {
            *stats = Counters.Families[family].GetCachedStats();
        }
    }

private:
    friend class TAllocationCacheSubSystem;
    TAllocationCacheWorkerCounters Counters;
    std::vector<TLocalAllocationCache> Caches;
    TAllocationCachePointers CachePointers;
};

class TAllocationCacheSubSystem final : public ISubSystem {
public:
    using TFactory = std::function<TLocalAllocationCache(TAllocationCacheCounters*)>;
    void OnExecutorThreadPrepare(TThreadContext* context) override;
    void RegisterFamily(size_t family, size_t budget, const TString& name, TFactory factory);
    void OnBeforeStart(TActorSystem&) override;
    void OnExecutorThreadStart(TThreadContext* context) override;
    void OnExecutorThreadStop(TThreadContext* context) override;
    // Standalone workers are not added to actor-system statistics.
    std::unique_ptr<TAllocationCacheWorker> CreateWorker();
    TAllocationCacheProcessStats GetCachedStats(size_t family) const;
    void GetFamilyStats(std::vector<TAllocationCacheFamilyStats>* stats) const;
    size_t GetWorkerBudget() const noexcept { return WorkerBudget; }

private:
    struct TFamily {
        TString Name;
        TFactory Factory;
    };
    std::vector<TFamily> Families;
    size_t WorkerBudget = 0;
    bool Frozen = false;
    // Built before threads start; immutable until subsystem destruction.
    std::unordered_map<TThreadContext*, std::unique_ptr<TAllocationCacheWorker>> Workers;
};

// Register this family as a subsystem. Its dependency callback registers the
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
class TAllocationCacheFamily : public ISubSystem {
public:
    explicit TAllocationCacheFamily(size_t budget)
        : Budget(budget)
    {
        (void)FamilyId();
    }

    static size_t FamilyId() noexcept {
        if constexpr (SystemAllocationCacheFamilyId<TTag>() < SystemAllocationCacheFamilyCount) {
            return SystemAllocationCacheFamilyId<TTag>();
        } else {
            static const size_t id = TAllocationCacheFamilyRegistry::NextId();
            return id;
        }
    }

    TSubSystemDependencies GetDependencies() const override {
        return DependsOn<TAllocationCacheSubSystem>();
    }

    void OnDependenciesResolved(const TResolvedSubSystemDependencies& dependencies) override {
        auto* system = static_cast<TAllocationCacheSubSystem*>(dependencies.front().Instance);
        system->RegisterFamily(FamilyId(), Budget, TTag::Name,
            [budget = Budget](TAllocationCacheCounters* counters) {
                auto cache = TLocalAllocationCache(new TAllocationCache<TTag>(budget), [](void* cache) {
                    delete static_cast<TAllocationCache<TTag>*>(cache);
                });
                *counters = static_cast<TAllocationCache<TTag>*>(cache.get())->GetCountersView();
                return cache;
            });
    }

    [[nodiscard]] Y_FORCE_INLINE static void* Allocate(size_t size) {
        return TAllocationCache<TTag>::AllocateCurrent(size);
    }

    Y_FORCE_INLINE static void Free(void* block, size_t size) noexcept {
        TAllocationCache<TTag>::Free(block, size);
    }

private:
    const size_t Budget;
};

template<class TTag>
TAllocationCache<TTag>* TAllocationCacheWorker::Get() const noexcept {
    const size_t family = TAllocationCacheFamily<TTag>::FamilyId();
    return family < CachePointers.size()
        ? static_cast<TAllocationCache<TTag>*>(CachePointers[family])
        : nullptr;
}


} // namespace NActors
