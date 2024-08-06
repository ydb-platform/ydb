#pragma once
#include "counters.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TManager {
private:
    const TConfig Config;
    const TString Name;
    const TCounters& Signals;
    const NActors::TActorId OwnerActorId;

    ui64 ResourceUsage = 0;

    class TAllocationInfo {
    private:
        std::shared_ptr<IAllocation> Allocation;
        YDB_READONLY_DEF(THashSet<ui64>, GroupIds);
        YDB_ACCESSOR(ui64, AllocatedVolume, 0);
        YDB_READONLY(ui64, Identifier, 0);

    public:
        void Allocate(const NActors::TActorId& ownerId) {
            AFL_VERIFY(Allocation);
            Allocation->OnAllocated(
                std::make_shared<TAllocationGuard>(ownerId, Allocation->GetIdentifier(), Allocation->GetMemory()), Allocation);
            Allocation = nullptr;
        }

        bool IsAllocated() const {
            return !Allocation;
        }

        bool IsEmpty() const {
            return GroupIds.empty();
        }

        void AddGroupId(const ui64 groupId) {
            AFL_VERIFY(GroupIds.emplace(groupId).second);
        }

        void RemoveGroup(const ui64 groupId) {
            AFL_VERIFY(GroupIds.erase(groupId));
        }

        TAllocationInfo(const std::shared_ptr<IAllocation>& allocation)
            : Allocation(allocation)
            , Identifier(Allocation->GetIdentifier())
        {
            AFL_VERIFY(Allocation);
            AFL_VERIFY(!Allocation->IsAllocated());
            AllocatedVolume = Allocation->GetMemory();
        }
    };

    class TGrouppedAllocations {
    private:
        THashMap<ui64, std::shared_ptr<TAllocationInfo>> Allocations;

    public:
        const THashMap<ui64, std::shared_ptr<TAllocationInfo>>& GetAllocations() const {
            return Allocations;
        }

        bool IsEmpty() const {
            return Allocations.empty();
        }

        void AddAllocation(const std::shared_ptr<TAllocationInfo>& allocation) {
            AFL_VERIFY(Allocations.emplace(allocation->GetIdentifier(), allocation).second);
        }

        void Remove(const std::shared_ptr<TAllocationInfo>& allocation) {
            AFL_VERIFY(Allocations.erase(allocation->GetIdentifier()));
        }

        std::vector<std::shared_ptr<TAllocationInfo>> AllocatePossible(TManager& manager, const bool force);
    };

    std::map<ui64, TGrouppedAllocations> WaitAllocations;
    std::map<ui64, TGrouppedAllocations> ReadyAllocations;
    THashMap<ui64, std::shared_ptr<TAllocationInfo>> AllocationInfo;
    THashMap<ui64, ui64> ExternalGroupIntoInternalGroup;

    ui64 CurrentInternalGroupId = 0;

    ui64 BuildInternalGroupId(const ui64 externalGroupId);
    ui64 GetInternalGroupIdVerified(const ui64 externalGroupId) const;

    const std::shared_ptr<TAllocationInfo>& RegisterAllocationImpl(const std::shared_ptr<IAllocation>& task);
    TAllocationInfo& GetAllocationInfoVerified(const ui64 allocationId) {
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        return *it->second;
    }

    std::optional<ui64> GetMinInternalGroupIdOptional() const;
    ui64 GetMinInternalGroupIdVerified() const;
    void TryAllocateWaiting();

public:
    TManager(const NActors::TActorId& ownerActorId, const TConfig& config, const TString& name, const TCounters& signals)
        : Config(config)
        , Name(name)
        , Signals(signals)
        , OwnerActorId(ownerActorId) {
        Y_UNUSED(Signals);
    }

    void RegisterAllocation(const std::shared_ptr<IAllocation>& task, const ui64 externalGroupId);

    void UnregisterAllocation(const ui64 allocationId);
    void UpdateAllocation(const ui64 allocationId, const ui64 volume);
    void UnregisterGroup(const ui64 usageGroupId);
    bool IsEmpty() const {
        return ResourceUsage == 0 && AllocationInfo.empty() && WaitAllocations.empty() && ReadyAllocations.empty();
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
