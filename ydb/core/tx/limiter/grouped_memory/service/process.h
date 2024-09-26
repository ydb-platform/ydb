#pragma once
#include "group.h"
#include "ids.h"

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TProcessMemoryScope {
private:
    const ui64 ExternalProcessId;
    const ui64 ExternalScopeId;
    TAllocationGroups WaitAllocations;
    THashMap<ui64, std::shared_ptr<TAllocationInfo>> AllocationInfo;
    TIdsControl GroupIds;
    ui32 Links = 1;
    const NActors::TActorId OwnerActorId;

    TAllocationInfo& GetAllocationInfoVerified(const ui64 allocationId) const {
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        return *it->second;
    }

    void UnregisterGroupImpl(const ui64 internalGroupId) {
        auto data = WaitAllocations.ExtractGroup(internalGroupId);
        for (auto&& allocation : data) {
            AFL_VERIFY(!allocation->Allocate(OwnerActorId));
        }
    }

    const std::shared_ptr<TAllocationInfo>& RegisterAllocationImpl(
        const ui64 internalGroupId, const std::shared_ptr<IAllocation>& task, const std::shared_ptr<TStageFeatures>& stage) {
        auto it = AllocationInfo.find(task->GetIdentifier());
        if (it == AllocationInfo.end()) {
            it = AllocationInfo
                     .emplace(task->GetIdentifier(),
                         std::make_shared<TAllocationInfo>(ExternalProcessId, ExternalScopeId, internalGroupId, task, stage))
                     .first;
        }
        return it->second;
    }

    friend class TAllocationGroups;

public:
    TProcessMemoryScope(const ui64 externalProcessId, const ui64 externalScopeId, const NActors::TActorId& ownerActorId)
        : ExternalProcessId(externalProcessId)
        , ExternalScopeId(externalScopeId)
        , OwnerActorId(ownerActorId) {
    }

    void Register() {
        ++Links;
    }

    [[nodiscard]] bool Unregister() {
        if (--Links) {
            return false;
        }
        for (auto&& [i, _] : GroupIds.GetInternalIdToExternalIds()) {
            UnregisterGroupImpl(i);
        }
        GroupIds.Clear();
        AllocationInfo.clear();
        return true;
    }

    void RegisterAllocation(const bool isPriorityProcess, const ui64 externalGroupId, const std::shared_ptr<IAllocation>& task,
        const std::shared_ptr<TStageFeatures>& stage) {
        AFL_VERIFY(task);
        AFL_VERIFY(stage);
        const std::optional<ui64> internalGroupIdOptional = GroupIds.GetInternalIdOptional(externalGroupId);
        if (!internalGroupIdOptional) {
            AFL_VERIFY(!task->OnAllocated(std::make_shared<TAllocationGuard>(ExternalProcessId, ExternalScopeId, task->GetIdentifier(), OwnerActorId, task->GetMemory()), task))(
                                                                   "ext_group", externalGroupId)(
                                                                   "min_group", GroupIds.GetMinInternalIdOptional())("stage", stage->GetName());
            AFL_VERIFY(!AllocationInfo.contains(task->GetIdentifier()));
        } else {
            const ui64 internalGroupId = *internalGroupIdOptional;
            auto allocationInfo = RegisterAllocationImpl(internalGroupId, task, stage);

            if (allocationInfo->GetAllocationStatus() != EAllocationStatus::Waiting) {
            } else if (WaitAllocations.GetMinGroupId().value_or(internalGroupId) < internalGroupId) {
                WaitAllocations.AddAllocation(internalGroupId, allocationInfo);
            } else if (allocationInfo->IsAllocatable(0) || (isPriorityProcess && internalGroupId == GroupIds.GetMinInternalIdVerified())) {
                Y_UNUSED(WaitAllocations.RemoveAllocation(internalGroupId, allocationInfo));
                if (!allocationInfo->Allocate(OwnerActorId)) {
                    UnregisterAllocation(allocationInfo->GetIdentifier());
                }
            } else {
                WaitAllocations.AddAllocation(internalGroupId, allocationInfo);
            }
        }
    }

    bool UpdateAllocation(const ui64 allocationId, const ui64 volume) {
        GetAllocationInfoVerified(allocationId).SetAllocatedVolume(volume);
        return true;
    }

    bool TryAllocateWaiting(const bool isPriorityProcess, const ui32 allocationsCountLimit) {
        return WaitAllocations.Allocate(isPriorityProcess, *this, allocationsCountLimit);
    }

    bool UnregisterAllocation(const ui64 allocationId) {
        ui64 memoryAllocated = 0;
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        bool waitFlag = false;
        const ui64 internalGroupId = it->second->GetAllocationInternalGroupId();
        switch (it->second->GetAllocationStatus()) {
            case EAllocationStatus::Allocated:
            case EAllocationStatus::Failed:
                AFL_VERIFY(!WaitAllocations.RemoveAllocation(internalGroupId, it->second));
                break;
            case EAllocationStatus::Waiting:
                AFL_VERIFY(WaitAllocations.RemoveAllocation(internalGroupId, it->second));
                waitFlag = true;
                break;
        }
        AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocation_unregister")("allocation_id", allocationId)("wait", waitFlag)(
            "internal_group_id", internalGroupId)("allocation_status", it->second->GetAllocationStatus());
        memoryAllocated = it->second->GetAllocatedVolume();
        AllocationInfo.erase(it);
        return !!memoryAllocated;
    }

    void UnregisterGroup(const bool isPriorityProcess, const ui64 externalGroupId) {
        const ui64 internalGroupId = GroupIds.ExtractInternalIdVerified(externalGroupId);
        AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "remove_group")("external_group_id", externalGroupId)(
            "internal_group_id", internalGroupId);
        UnregisterGroupImpl(internalGroupId);
        if (isPriorityProcess && (internalGroupId < GroupIds.GetMinInternalIdDef(internalGroupId))) {
            Y_UNUSED(TryAllocateWaiting(isPriorityProcess, 0));
        }
    }

    void RegisterGroup(const ui64 externalGroupId) {
        Y_UNUSED(GroupIds.RegisterExternalId(externalGroupId));
    }
};

class TProcessMemory {
private:
    const ui64 ExternalProcessId;

    const NActors::TActorId OwnerActorId;
    bool PriorityProcessFlag = false;

    YDB_ACCESSOR(ui32, LinksCount, 1);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TStageFeatures>>, Stages);
    const std::shared_ptr<TStageFeatures> DefaultStage;
    THashMap<ui64, std::shared_ptr<TProcessMemoryScope>> AllocationScopes;

    TProcessMemoryScope* GetAllocationScopeOptional(const ui64 externalScopeId) const {
        auto it = AllocationScopes.find(externalScopeId);
        if (it == AllocationScopes.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    TProcessMemoryScope& GetAllocationScopeVerified(const ui64 externalScopeId) const {
        return *TValidator::CheckNotNull(GetAllocationScopeOptional(externalScopeId));
    }

public:
    bool IsPriorityProcess() const {
        return PriorityProcessFlag;
    }

    bool UpdateAllocation(const ui64 externalScopeId, const ui64 allocationId, const ui64 volume) {
        return GetAllocationScopeVerified(externalScopeId).UpdateAllocation(allocationId, volume);
    }

    void RegisterAllocation(
        const ui64 externalScopeId, const ui64 externalGroupId, const std::shared_ptr<IAllocation>& task, const std::optional<ui32>& stageIdx) {
        AFL_VERIFY(task);
        std::shared_ptr<TStageFeatures> stage;
        if (Stages.empty()) {
            AFL_VERIFY(!stageIdx);
            stage = DefaultStage;
        } else {
            AFL_VERIFY(stageIdx);
            AFL_VERIFY(*stageIdx < Stages.size());
            stage = Stages[*stageIdx];
        }
        AFL_VERIFY(stage);
        auto& scope = GetAllocationScopeVerified(externalScopeId);
        scope.RegisterAllocation(IsPriorityProcess(), externalGroupId, task, stage);
    }

    bool UnregisterAllocation(const ui64 externalScopeId, const ui64 allocationId) {
        if (auto* scope = GetAllocationScopeOptional(externalScopeId)) {
            return scope->UnregisterAllocation(allocationId);
        }
        return false;
    }

    void UnregisterGroup(const ui64 externalScopeId, const ui64 externalGroupId) {
        if (auto* scope = GetAllocationScopeOptional(externalScopeId)) {
            scope->UnregisterGroup(IsPriorityProcess(), externalGroupId);
        }
    }

    void RegisterGroup(const ui64 externalScopeId, const ui64 externalGroupId) {
        GetAllocationScopeVerified(externalScopeId).RegisterGroup(externalGroupId);
    }

    void UnregisterScope(const ui64 externalScopeId) {
        auto it = AllocationScopes.find(externalScopeId);
        AFL_VERIFY(it != AllocationScopes.end());
        if (it->second->Unregister()) {
            AllocationScopes.erase(it);
        }
        
    }

    void RegisterScope(const ui64 externalScopeId) {
        auto it = AllocationScopes.find(externalScopeId);
        if (it == AllocationScopes.end()) {
            AFL_VERIFY(AllocationScopes.emplace(externalScopeId, std::make_shared<TProcessMemoryScope>(ExternalProcessId, externalScopeId, OwnerActorId)).second);
        } else {
            it->second->Register();
        }
        
    }

    void SetPriorityProcess() {
        AFL_VERIFY(!PriorityProcessFlag);
        PriorityProcessFlag = true;
    }

    TProcessMemory(const ui64 externalProcessId, const NActors::TActorId& ownerActorId, const bool isPriority,
        const std::vector<std::shared_ptr<TStageFeatures>>& stages, const std::shared_ptr<TStageFeatures>& defaultStage)
        : ExternalProcessId(externalProcessId)
        , OwnerActorId(ownerActorId)
        , PriorityProcessFlag(isPriority)
        , Stages(stages)
        , DefaultStage(defaultStage) {
    }

    bool TryAllocateWaiting(const ui32 allocationsCountLimit) {
        bool allocated = false;
        for (auto&& i : AllocationScopes) {
            if (i.second->TryAllocateWaiting(IsPriorityProcess(), allocationsCountLimit)) {
                allocated = true;
            }
        }
        return allocated;
    }

    void Unregister() {
        for (auto&& i : AllocationScopes) {
            Y_UNUSED(i.second->Unregister());
        }
        AllocationScopes.clear();
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
