#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TGroupGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, GroupId, 0);

public:
    TGroupGuard(const NActors::TActorId& actorId, const ui64 groupId);

    ~TGroupGuard();
};

class TAllocationGuard {
private:
    const NActors::TActorId ActorId;
    YDB_READONLY(ui64, AllocationId, 0)
    YDB_READONLY(ui64, Memory, 0)
    bool Released = false;

public:
    TAllocationGuard(const NActors::TActorId actorId, const ui64 allocationId, const ui64 memory)
        : ActorId(actorId)
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

class TPositiveControlInteger {
private:
    ui64 Value = 0;

public:
    void Add(const ui64 value) {
        Value += value;
    }
    void Sub(const ui64 value) {
        AFL_VERIFY(value <= Value);
        Value -= value;
    }
    ui64 Val() const {
        return Value;
    }
};

class TStageFeatures {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY(ui64, Limit, 0);
    YDB_ACCESSOR_DEF(TPositiveControlInteger, Usage);
    YDB_ACCESSOR_DEF(TPositiveControlInteger, Waiting);
    std::shared_ptr<TStageFeatures> Owner;

public:
    TString DebugString() const {
        TStringBuilder result;
        result << "name=" << Name << ";limit=" << Limit << ";";
        if (Owner) {
            result << "owner=" << Owner->DebugString() << ";";
        }
        return result;
    }

    ui64 GetFullMemory() const {
        return Usage.Val() + Waiting.Val();
    }

    TStageFeatures(const TString& name, const ui64 limit, const std::shared_ptr<TStageFeatures>& owner)
        : Name(name)
        , Limit(limit)
        , Owner(owner) {
    }

    void Allocate(const ui64 volume) {
        Waiting.Sub(volume);
        Usage.Add(volume);
        if (Owner) {
            Owner->Allocate(volume);
        }
    }

    void Free(const ui64 volume, const bool allocated) {
        if (allocated) {
            Usage.Sub(volume);
        } else {
            Waiting.Sub(volume);
        }
        
        if (Owner) {
            Owner->Free(volume, allocated);
        }
    }

    void UpdateVolume(const ui64 from, const ui64 to, const bool allocated) {
        if (allocated) {
            Usage.Sub(from);
            Usage.Add(to);
        } else {
            Waiting.Sub(from);
            Waiting.Add(to);
        }

        if (Owner) {
            Owner->UpdateVolume(from, to, allocated);
        }
    }

    bool IsAllocatable(const ui64 volume, const ui64 additional) const {
        if (Limit < additional + Usage.Val() + volume) {
            return false;
        }
        if (Owner) {
            return Owner->IsAllocatable(volume, additional);
        }
        return true;
    }

    void Add(const ui64 volume, const bool allocated) {
        if (allocated) {
            Usage.Add(volume);
        } else {
            Waiting.Add(volume);
        }
        
        if (Owner) {
            Owner->Add(volume, allocated);
        }
    }
};

class IAllocation {
private:
    static inline TAtomicCounter Counter = 0;
    YDB_READONLY(ui64, Identifier, Counter.Inc());
    YDB_READONLY(ui64, Memory, 0);
    bool Allocated = false;
    virtual bool DoOnAllocated(
        std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation) = 0;

public:
    virtual ~IAllocation() = default;
    IAllocation() = default;
    IAllocation(const ui64 mem)
        : Memory(mem) {
    }

    void ResetAllocation() {
        Allocated = false;
    }

    void SetMemoryForAllocation(const ui64 value) {
        Memory = value;
    }

    bool IsAllocated() const {
        return Allocated;
    }

    [[nodiscard]] bool OnAllocated(std::shared_ptr<TAllocationGuard>&& guard, const std::shared_ptr<NGroupedMemoryManager::IAllocation>& allocation);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
