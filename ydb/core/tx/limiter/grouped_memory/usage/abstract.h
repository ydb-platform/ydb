#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TGroupGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, GroupId, 0);

public:
    TGroupGuard(const NActors::TActorId& actorId, const ui64 groupId)
        : ActorId(actorId)
        , GroupId(groupId)
    {

    }

    ~TGroupGuard();
};

class TAllocationGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, AllocationId, 0)
    YDB_READONLY(ui64, Memory, 0)
public:
    TAllocationGuard(const NActors::TActorId actorId, const ui64 allocationId, const ui64 memory)
        : ActorId(actorId)
        , AllocationId(allocationId)
        , Memory(memory)
    {
    }

    void Update(const ui64 newVolume);

    ~TAllocationGuard();
};

class IAllocation {
private:
    static inline TAtomicCounter Counter = 0;
    YDB_READONLY(ui64, Identifier, Counter.Inc());
    YDB_READONLY(ui64, Memory, 0);
    bool Allocated = false;
    virtual void DoOnAllocated(
        std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) = 0;

public:
    virtual ~IAllocation() = default;
    IAllocation() = default;
    IAllocation(const ui64 mem)
        : Memory(mem) {
    }

    void SetMemoryForAllocation(const ui64 value) {
        Memory = value;
    }

    bool IsAllocated() const {
        return Allocated;
    }

    void OnAllocated(std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
