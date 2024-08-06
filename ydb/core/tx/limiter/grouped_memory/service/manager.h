#pragma once
#include "counters.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

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

class TManager {
private:
    const TConfig Config;
    const TString Name;
    const TCounters& Signals;
    const NActors::TActorId OwnerActorId;

    TPositiveControlInteger Allocated;
    TPositiveControlInteger Waiting;

    ui64 GetFreeMemory() const {
        if (Config.GetMemoryLimit() < Allocated.Val()) {
            return 0;
        } else {
            return Config.GetMemoryLimit() - Allocated.Val();
        }
    }

    class TAllocationInfo {
    private:
        std::shared_ptr<IAllocation> Allocation;
        YDB_READONLY_DEF(THashSet<ui64>, GroupIds);
        ui64 AllocatedVolume = 0;
        YDB_READONLY(ui64, Identifier, 0);
        TPositiveControlInteger* WaitMemory = nullptr;
        TPositiveControlInteger* AllocatedMemory = nullptr;

    public:
        ~TAllocationInfo() {
            if (IsAllocated()) {
                AllocatedMemory->Sub(AllocatedVolume);
            } else {
                WaitMemory->Sub(AllocatedVolume);
            }
        }

        void SetAllocatedVolume(const ui64 value) {
            if (IsAllocated()) {
                AllocatedMemory->Sub(AllocatedVolume);
            } else {
                WaitMemory->Sub(AllocatedVolume);
            }
            AllocatedVolume = value;
            if (IsAllocated()) {
                AllocatedMemory->Add(AllocatedVolume);
            } else {
                WaitMemory->Add(AllocatedVolume);
            }
        }

        ui64 GetAllocatedVolume() const {
            return AllocatedVolume;
        }

        void Allocate(const NActors::TActorId& ownerId) {
            AFL_VERIFY(Allocation);
            Allocation->OnAllocated(
                std::make_shared<TAllocationGuard>(ownerId, Allocation->GetIdentifier(), Allocation->GetMemory()), Allocation);
            Allocation = nullptr;
            AllocatedMemory->Add(AllocatedVolume);
            WaitMemory->Sub(AllocatedVolume);
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

        TAllocationInfo(const std::shared_ptr<IAllocation>& allocation, TPositiveControlInteger* allocatedMemory,
            TPositiveControlInteger* waitMemory)
            : Allocation(allocation)
            , Identifier(Allocation->GetIdentifier())
            , WaitMemory(waitMemory)
            , AllocatedMemory(allocatedMemory)
        {
            AFL_VERIFY(Allocation);
            AllocatedVolume = Allocation->GetMemory();
            if (allocation->IsAllocated()) {
                Allocation = nullptr;
                AllocatedMemory->Add(AllocatedVolume);
            } else {
                WaitMemory->Add(AllocatedVolume);
            }
        }
    };

    class TGrouppedAllocations {
    private:
        THashMap<ui64, std::shared_ptr<TAllocationInfo>> Allocations;

    public:
        THashMap<ui64, std::shared_ptr<TAllocationInfo>> ExtractAllocations() {
            return std::move(Allocations);
        }

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

        std::vector<std::shared_ptr<TAllocationInfo>> AllocatePossible(const bool force, const ui64 freeMemory);
    };

    class TAllocationGroups {
    private:
        std::map<ui64, TGrouppedAllocations> Groups;

    public:
        bool IsEmpty() const {
            return Groups.empty();
        }

        void AllocateTo(TManager& manager, TAllocationGroups& destination) {
            if (Groups.empty()) {
                return;
            }
            const ui64 minGroupId = manager.GetMinInternalGroupIdVerified();
            for (auto it = Groups.begin(); it != Groups.end();) {
                auto internalGroupId = it->first;
                auto allocated = it->second.AllocatePossible(internalGroupId == minGroupId, manager.GetFreeMemory());
                for (auto&& i : allocated) {
                    i->Allocate(manager.OwnerActorId);
                    for (auto&& g : i->GetGroupIds()) {
                        it->second.Remove(i);
                        destination.AddAllocation(g, i);
                    }
                }
                if (!it->second.IsEmpty()) {
                    break;
                }
                it = Groups.erase(it);
            }
        }

        std::optional<THashMap<ui64, std::shared_ptr<TAllocationInfo>>> ExtractGroup(const ui64 id) {
            auto it = Groups.find(id);
            if (it == Groups.end()) {
                return std::nullopt;
            }
            auto result = it->second.ExtractAllocations();
            Groups.erase(it);
            return result;
        }

        std::optional<ui64> GetMinGroupId() const {
            if (Groups.size()) {
                return Groups.begin()->first;
            } else {
                return std::nullopt;
            }
        }

        [[nodiscard]] bool RemoveAllocation(const ui64 groupId, const std::shared_ptr<TAllocationInfo>& allocation) {
            auto groupIt = Groups.find(groupId);
            if (groupIt == Groups.end()) {
                return false;
            }
            groupIt->second.Remove(allocation);
            if (groupIt->second.IsEmpty()) {
                Groups.erase(groupIt);
            }
            return true;
        }

        void AddAllocation(const ui64 groupId, const std::shared_ptr<TAllocationInfo>& allocation) {
            Groups[groupId].AddAllocation(allocation);
        }

        void AddAllocations(const ui64 groupId, const std::vector<std::shared_ptr<TAllocationInfo>>& allocated) {
            auto& readyAllocations = Groups[groupId];
            for (auto&& i : allocated) {
                readyAllocations.AddAllocation(i);
            }
        }
    };

    TAllocationGroups WaitAllocations;
    TAllocationGroups ReadyAllocations;
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
    void RefreshSignals() const {
        Signals.MemoryUsageBytes->Set(Allocated.Val());
        Signals.MemoryWaitingBytes->Set(Waiting.Val());
    }

public:
    TManager(const NActors::TActorId& ownerActorId, const TConfig& config, const TString& name, const TCounters& signals)
        : Config(config)
        , Name(name)
        , Signals(signals)
        , OwnerActorId(ownerActorId) {
    }

    void RegisterAllocation(const std::shared_ptr<IAllocation>& task, const ui64 externalGroupId);

    void UnregisterAllocation(const ui64 allocationId);
    void UpdateAllocation(const ui64 allocationId, const ui64 volume);
    void UnregisterGroup(const ui64 usageGroupId);
    bool IsEmpty() const {
        return AllocationInfo.empty() && WaitAllocations.IsEmpty() && ReadyAllocations.IsEmpty();
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
