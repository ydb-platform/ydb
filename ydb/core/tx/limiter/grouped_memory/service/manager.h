#pragma once
#include "counters.h"

#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>

#include <ydb/library/accessor/validator.h>
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

    class TAllocationInfo {
    private:
        std::shared_ptr<IAllocation> Allocation;
        YDB_READONLY_DEF(THashSet<ui64>, GroupIds);
        ui64 AllocatedVolume = 0;
        YDB_READONLY(ui64, Identifier, 0);
        const std::shared_ptr<TStageFeatures> Stage;

    public:
        ~TAllocationInfo() {
            Stage->Free(AllocatedVolume, IsAllocated());
            AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "destroy")("allocation_id", Identifier)("stage", Stage->GetName());
        }

        bool IsAllocatable(const ui64 additional) const {
            return Stage->IsAllocatable(AllocatedVolume, additional);
        }

        void SetAllocatedVolume(const ui64 value) {
            Stage->UpdateVolume(AllocatedVolume, value, IsAllocated());
            AllocatedVolume = value;
        }

        ui64 GetAllocatedVolume() const {
            return AllocatedVolume;
        }

        [[nodiscard]] bool Allocate(const NActors::TActorId& ownerId) {
            AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocated")("allocation_id", Identifier)("stage", Stage->GetName());
            AFL_VERIFY(Allocation);
            const bool result = Allocation->OnAllocated(
                std::make_shared<TAllocationGuard>(ownerId, Allocation->GetIdentifier(), Allocation->GetMemory()), Allocation);
            if (result) {
                Stage->Allocate(AllocatedVolume);
                Allocation = nullptr;
            }
            return result;
        }

        bool IsAllocated() const {
            return !Allocation;
        }

        bool IsEmpty() const {
            return GroupIds.empty();
        }

        void AddGroupId(const ui64 groupId) {
            AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "add_group")("allocation_id", Identifier)("allocation_group_id", groupId)(
                "stage", Stage->GetName());
            AFL_VERIFY(GroupIds.emplace(groupId).second);
        }

        void RemoveGroup(const ui64 groupId) {
            AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "remove_group")("allocation_id", Identifier)(
                "allocation_group_id", groupId)("stage", Stage->GetName());
            AFL_VERIFY(GroupIds.erase(groupId));
        }

        TAllocationInfo(const std::shared_ptr<IAllocation>& allocation, const std::shared_ptr<TStageFeatures>& stage)
            : Allocation(allocation)
            , Identifier(TValidator::CheckNotNull(Allocation)->GetIdentifier())
            , Stage(stage) {
            AFL_VERIFY(Stage);
            AFL_VERIFY(Allocation);
            AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "add")("id", Allocation->GetIdentifier())("stage", Stage->GetName());
            AllocatedVolume = Allocation->GetMemory();
            Stage->Add(AllocatedVolume, Allocation->IsAllocated());
            if (allocation->IsAllocated()) {
                AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocated_on_add")("allocation_id", Identifier)(
                    "stage", Stage->GetName());
                Allocation = nullptr;
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

        [[nodiscard]] bool Remove(const std::shared_ptr<TAllocationInfo>& allocation) {
            return Allocations.erase(allocation->GetIdentifier());
        }

        std::vector<std::shared_ptr<TAllocationInfo>> AllocatePossible(const bool force);
    };

    class TAllocationGroups {
    private:
        std::map<ui64, TGrouppedAllocations> Groups;

    public:
        bool IsEmpty() const {
            return Groups.empty();
        }

        void AllocateTo(TManager& manager, TAllocationGroups& destination) {
            while (true) {
                std::vector<ui64> toRemove;
                for (auto it = Groups.begin(); it != Groups.end();) {
                    ui64 minGroupId = manager.GetMinInternalGroupIdVerified();
                    auto internalGroupId = it->first;
                    auto allocated = it->second.AllocatePossible(internalGroupId == minGroupId);
                    for (auto&& i : allocated) {
                        if (!i->Allocate(manager.OwnerActorId)) {
                            toRemove.emplace_back(i->GetIdentifier());
                        } else {
                            for (auto&& g : i->GetGroupIds()) {
                                AFL_VERIFY(it->second.Remove(i));
                                destination.AddAllocation(g, i);
                            }
                        }
                    }
                    if (!it->second.IsEmpty()) {
                        break;
                    }
                    it = Groups.erase(it);
                }
                for (auto&& i : toRemove) {
                    manager.UnregisterAllocationImpl(i);
                }
                if (toRemove.empty()) {
                    break;
                }
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
            if (!groupIt->second.Remove(allocation)) {
                return false;
            }
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
    std::map<ui64, ui64> ExternalGroupIntoInternalGroup;

    ui64 BuildInternalGroupId(const ui64 externalGroupId);
    ui64 GetInternalGroupIdVerified(const ui64 externalGroupId) const;
    std::optional<ui64> GetInternalGroupIdOptional(const ui64 externalGroupId) const;

    const std::shared_ptr<TAllocationInfo>& RegisterAllocationImpl(
        const std::shared_ptr<IAllocation>& task, const std::shared_ptr<TStageFeatures>& stage);
    TAllocationInfo& GetAllocationInfoVerified(const ui64 allocationId) {
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        return *it->second;
    }

    std::optional<ui64> GetMinInternalGroupIdOptional() const;
    ui64 GetMinInternalGroupIdVerified() const;
    void TryAllocateWaiting();
    void RefreshSignals() const {
//        Signals.MemoryUsageCount->Set(Counters.GetAllocatedCount().Val());
//        Signals.MemoryWaitingCount->Set(Counters.GetWaitingCount().Val());
//        Signals.MemoryUsageBytes->Set(Counters.GetAllocatedBytes().Val());
//        Signals.MemoryWaitingBytes->Set(Counters.GetWaitingBytes().Val());
        Signals.GroupsCount->Set(ExternalGroupIntoInternalGroup.size());
    }

    bool UnregisterAllocationImpl(const ui64 allocationId) {
        ui64 memoryAllocated = 0;
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        for (auto&& usageGroupId : it->second->GetGroupIds()) {
            const bool waitFlag = WaitAllocations.RemoveAllocation(usageGroupId, it->second);
            const bool readyFlag = ReadyAllocations.RemoveAllocation(usageGroupId, it->second);
            AFL_VERIFY(waitFlag ^ readyFlag)("wait", waitFlag)("ready", readyFlag);
        }
        memoryAllocated = it->second->GetAllocatedVolume();
        AllocationInfo.erase(it);
        return !!memoryAllocated;
    }

public:
    TManager(const NActors::TActorId& ownerActorId, const TConfig& config, const TString& name, const TCounters& signals)
        : Config(config)
        , Name(name)
        , Signals(signals)
        , OwnerActorId(ownerActorId) {
    }

    void RegisterAllocation(const std::shared_ptr<IAllocation>& task, const std::shared_ptr<TStageFeatures>& stage, const ui64 externalGroupId);
    void RegisterGroup(const ui64 externalGroupId);

    void UnregisterAllocation(const ui64 allocationId);
    void UpdateAllocation(const ui64 allocationId, const ui64 volume);
    void UnregisterGroup(const ui64 usageGroupId);
    bool IsEmpty() const {
        return AllocationInfo.empty() && WaitAllocations.IsEmpty() && ReadyAllocations.IsEmpty();
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
