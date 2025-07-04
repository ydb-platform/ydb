#pragma once
#include "stage_features.h"

#include <ydb/core/tx/limiter/grouped_memory/service/counters.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TGroupGuard;
class TScopeGuard;
class TProcessGuard;

class TGroupGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY(ui64, ExternalScopeId, 0);
    YDB_READONLY(ui64, GroupId, 0);

public:
    TGroupGuard(const NActors::TActorId& actorId, const ui64 processId, const ui64 externalScopeId, const ui64 groupId);

    ~TGroupGuard();
};

class TProcessGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, ProcessId, 0);

public:
    TProcessGuard(const NActors::TActorId& actorId, const ui64 processId, const std::vector<std::shared_ptr<TStageFeatures>>& stages);

    std::shared_ptr<TScopeGuard> BuildScopeGuard(const ui32 scopeId) const {
        return std::make_shared<TScopeGuard>(ActorId, ProcessId, scopeId);
    }

    ~TProcessGuard();
};

class TScopeGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY(ui64, ScopeId, 0);

public:
    TScopeGuard(const NActors::TActorId& actorId, const ui64 processId, const ui64 scopeId);

    std::shared_ptr<TGroupGuard> BuildGroupGuard(const std::optional<ui64> extGroupId = std::nullopt) const {
        if (extGroupId) {
            return std::make_shared<TGroupGuard>(ActorId, ProcessId, ScopeId, *extGroupId);
        } else {
            static TAtomicCounter counter = 0;
            return std::make_shared<TGroupGuard>(ActorId, ProcessId, ScopeId, counter.Inc());
        }
    }

    ~TScopeGuard();
};

class TAllocationGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, ProcessId, 0)
    YDB_READONLY(ui64, ScopeId, 0)
    YDB_READONLY(ui64, AllocationId, 0)
    YDB_READONLY(ui64, Memory, 0)
    bool Released = false;

public:
    TAllocationGuard(const ui64 processId, const ui64 scopeId, const ui64 allocationId, const NActors::TActorId actorId, const ui64 memory)
        : ActorId(actorId)
        , ProcessId(processId)
        , ScopeId(scopeId)
        , AllocationId(allocationId)
        , Memory(memory) {
    }

    void Release() {
        AFL_VERIFY(!Released);
        Released = true;
    }

    void Update(const ui64 newVolume);

    ~TAllocationGuard();
};

class TCompositeAllocationGuard {
private:
    std::set<std::shared_ptr<TAllocationGuard>> Guards;

public:
    void Add(const std::shared_ptr<TAllocationGuard>& guard) {
        Guards.insert(std::move(guard));
    }

    ui64 GetMemory() const {
        ui64 memory = 0;
        for (const auto& guard : Guards) {
            memory += guard->GetMemory();
        }
        return memory;
    }
};

class IAllocation {
private:
    static inline TAtomicCounter Counter = 0;
    YDB_READONLY(ui64, Identifier, Counter.Inc());
    YDB_READONLY(ui64, Memory, 0);
    bool Allocated = false;
    virtual void DoOnAllocationImpossible(const TString& errorMessage) = 0;
    virtual bool DoOnAllocated(
        std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) = 0;

public:
    virtual ~IAllocation() = default;
    IAllocation(const ui64 mem)
        : Memory(mem) {
    }

    void ResetAllocation() {
        Allocated = false;
    }

    bool IsAllocated() const {
        return Allocated;
    }

    void OnAllocationImpossible(const TString& errorMessage) {
        DoOnAllocationImpossible(errorMessage);
    }

    [[nodiscard]] bool OnAllocated(
        std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
