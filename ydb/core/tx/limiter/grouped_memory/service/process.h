#pragma once
#include "group.h"
#include "ids.h"

#include <ydb/library/accessor/validator.h>
#include <ydb/library/signals/object_counter.h>

#include <ydb/core/tx/limiter/grouped_memory/tracing/probes.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

LWTRACE_USING(YDB_GROUPED_MEMORY_PROVIDER);

class TProcessMemoryScope: public NColumnShard::TMonitoringObjectsCounter<TProcessMemoryScope> {
private:
    const ui64 ExternalProcessId;
    const ui64 ExternalScopeId;
    TAllocationGroups WaitAllocations;
    THashMap<ui64, std::shared_ptr<TAllocationInfo>> AllocationInfo;
    TExternalIdsControl GroupIds;
    ui32 Links = 1;
    const NActors::TActorId OwnerActorId;

    TAllocationInfo& GetAllocationInfoVerified(const ui64 allocationId) const {
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        return *it->second;
    }

    void UnregisterGroupImplExt(const ui64 externalGroupId) {
        auto data = WaitAllocations.ExtractGroupExt(externalGroupId);
        for (auto&& allocation : data) {
            auto stage = allocation->GetStage();
            LWPROBE(Allocated, "on_unregister", allocation->GetIdentifier(), stage->GetName(), stage->GetLimit(), stage->GetHardLimit().value_or(std::numeric_limits<ui64>::max()), stage->GetUsage().Val(), stage->GetWaiting().Val(), allocation->GetAllocationTime(), false, false);
            AFL_VERIFY(!allocation->Allocate(OwnerActorId));
        }
    }

    const std::shared_ptr<TAllocationInfo>& RegisterAllocationImpl(
        const ui64 externalGroupId, const std::shared_ptr<IAllocation>& task, const std::shared_ptr<TStageFeatures>& stage) {
        auto it = AllocationInfo.find(task->GetIdentifier());
        if (it == AllocationInfo.end()) {
            it = AllocationInfo
                     .emplace(task->GetIdentifier(),
                         std::make_shared<TAllocationInfo>(ExternalProcessId, ExternalScopeId, externalGroupId, task, stage))
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
        for (auto&& i : GroupIds.GetExternalIds()) {
            UnregisterGroupImplExt(i);
        }
        GroupIds.Clear();
        AllocationInfo.clear();
        AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "scope_cleaned")("process_id", ExternalProcessId)(
            "external_scope_id", ExternalScopeId);
        return true;
    }

    void RegisterAllocation(const bool isPriorityProcess, const ui64 externalGroupId, const std::shared_ptr<IAllocation>& allocation,
        const std::shared_ptr<TStageFeatures>& stage) {
        AFL_VERIFY(allocation);
        AFL_VERIFY(stage);
        if (!GroupIds.HasExternalId(externalGroupId)) {
            LWPROBE(Allocated, "on_register", allocation->GetIdentifier(), stage->GetName(), stage->GetLimit(), stage->GetHardLimit().value_or(std::numeric_limits<ui64>::max()), stage->GetUsage().Val(), stage->GetWaiting().Val(), TDuration::Zero(), false, false);
            AFL_VERIFY(!allocation->OnAllocated(std::make_shared<TAllocationGuard>(ExternalProcessId, ExternalScopeId, allocation->GetIdentifier(), OwnerActorId, allocation->GetMemory(), nullptr), allocation))
                ("ext_group", externalGroupId)("min_ext_group", GroupIds.GetMinExternalIdOptional())("stage", stage->GetName());
            AFL_VERIFY(!AllocationInfo.contains(allocation->GetIdentifier()));
        } else {
            auto allocationInfo = RegisterAllocationImpl(externalGroupId, allocation, stage);

            if (allocationInfo->GetAllocationStatus() != EAllocationStatus::Waiting) {
            } else if (WaitAllocations.GetMinExternalGroupId().value_or(externalGroupId) < externalGroupId) {
                WaitAllocations.AddAllocationExt(externalGroupId, allocationInfo);
            } else if (allocationInfo->IsAllocatable(0) || (isPriorityProcess && externalGroupId <= GroupIds.GetMinExternalIdVerified())) {
                Y_UNUSED(WaitAllocations.RemoveAllocationExt(externalGroupId, allocationInfo));
                auto success = allocationInfo->Allocate(OwnerActorId);
                if (!success) {
                    UnregisterAllocation(allocationInfo->GetIdentifier());
                }
                LWPROBE(Allocated, "on_register", allocationInfo->GetIdentifier(), stage->GetName(), stage->GetLimit(), stage->GetHardLimit().value_or(std::numeric_limits<ui64>::max()), stage->GetUsage().Val(), stage->GetWaiting().Val(), allocationInfo->GetAllocationTime(), false, success);
            } else {
                WaitAllocations.AddAllocationExt(externalGroupId, allocationInfo);
            }
        }
    }

    bool AllocationUpdated(const ui64 allocationId) {
        GetAllocationInfoVerified(allocationId);
        return true;
    }

    bool TryAllocateWaiting(const bool isPriorityProcess, const ui32 allocationsCountLimit) {
        return WaitAllocations.Allocate(isPriorityProcess, *this, allocationsCountLimit);
    }

    bool UnregisterAllocation(const ui64 allocationId) {
        ui64 memoryAllocated = 0;
        auto it = AllocationInfo.find(allocationId);
        if (it == AllocationInfo.end()) {
            AFL_WARN(NKikimrServices::GROUPED_MEMORY_LIMITER)("reason", "allocation_cleaned_in_previous_scope_id_live")(
                "allocation_id", allocationId)("process_id", ExternalProcessId)("external_scope_id", ExternalScopeId);
            return true;
        }
        bool waitFlag = false;
        const ui64 externalGroupId = it->second->GetAllocationExternalGroupId();
        switch (it->second->GetAllocationStatus()) {
            case EAllocationStatus::Allocated:
            case EAllocationStatus::Failed:
                AFL_VERIFY(!WaitAllocations.RemoveAllocationExt(externalGroupId, it->second));
                break;
            case EAllocationStatus::Waiting:
                AFL_VERIFY(WaitAllocations.RemoveAllocationExt(externalGroupId, it->second));
                waitFlag = true;
                break;
        }
        AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocation_unregister")("allocation_id", allocationId)("wait", waitFlag)(
            "external_group_id", externalGroupId)("allocation_status", it->second->GetAllocationStatus());
        memoryAllocated = it->second->GetAllocatedVolume();
        AllocationInfo.erase(it);
        return !!memoryAllocated;
    }

    void UnregisterGroup(const bool isPriorityProcess, const ui64 externalGroupId) {
        if (GroupIds.UnregisterExternalId(externalGroupId)) {
            UnregisterGroupImplExt(externalGroupId);
            AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "remove_group")("external_group_id", externalGroupId)(
                "min_group", GroupIds.GetMinExternalIdOptional());
            if (isPriorityProcess && (externalGroupId < GroupIds.GetMinExternalIdDef(externalGroupId))) {
                Y_UNUSED(TryAllocateWaiting(isPriorityProcess, 0));
            }
        } else {
            AFL_WARN(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "remove_absent_group")("external_group_id", externalGroupId);
        }
    }

    void RegisterGroup(const bool isPriorityProcess, const ui64 externalGroupId) {
        GroupIds.RegisterExternalId(externalGroupId);
        AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "register_group")("external_group_id", externalGroupId)(
            "min_group", GroupIds.GetMinExternalIdOptional());
        if (isPriorityProcess && (externalGroupId < GroupIds.GetMinExternalIdDef(externalGroupId))) {
            Y_UNUSED(TryAllocateWaiting(isPriorityProcess, 0));
        }
    }
};

class TProcessMemoryUsage {
private:
    YDB_READONLY(ui64, MemoryUsage, 0);
    YDB_READONLY(ui64, InternalProcessId, 0);

public:
    TProcessMemoryUsage(const ui64 memoryUsage, const ui64 internalProcessId)
        : MemoryUsage(memoryUsage)
        , InternalProcessId(internalProcessId) {
    }

    TString DebugString() const;

    bool operator<(const TProcessMemoryUsage& item) const {
        return std::tuple(MemoryUsage, InternalProcessId) < std::tuple(item.MemoryUsage, item.InternalProcessId);
    }
};

class TProcessMemory: public NColumnShard::TMonitoringObjectsCounter<TProcessMemory> {
private:
    const ui64 ExternalProcessId;
    const ui64 InternalProcessId;

    const NActors::TActorId OwnerActorId;
    bool PriorityProcessFlag = false;
    ui64 MemoryUsage = 0;

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

    void RefreshMemoryUsage() {
        ui64 result = 0;
        for (auto&& i : Stages) {
            result += i->GetUsage().Val();
        }
        MemoryUsage = result;
    }

public:
    TProcessMemoryUsage BuildUsageAddress() const {
        return TProcessMemoryUsage(MemoryUsage, InternalProcessId);
    }

    bool IsPriorityProcess() const {
        return PriorityProcessFlag;
    }

    bool AllocationUpdated(const ui64 externalScopeId, const ui64 allocationId) {
        if (GetAllocationScopeVerified(externalScopeId).AllocationUpdated(allocationId)) {
            RefreshMemoryUsage();
            return true;
        } else {
            return false;
        }
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
            if (scope->UnregisterAllocation(allocationId)) {
                RefreshMemoryUsage();
                return true;
            }
        }
        return false;
    }

    void UnregisterGroup(const ui64 externalScopeId, const ui64 externalGroupId) {
        if (auto* scope = GetAllocationScopeOptional(externalScopeId)) {
            scope->UnregisterGroup(IsPriorityProcess(), externalGroupId);
            RefreshMemoryUsage();
        }
    }

    void RegisterGroup(const ui64 externalScopeId, const ui64 externalGroupId) {
        GetAllocationScopeVerified(externalScopeId).RegisterGroup(IsPriorityProcess(), externalGroupId);
    }

    void UnregisterScope(const ui64 externalScopeId) {
        auto it = AllocationScopes.find(externalScopeId);
        AFL_VERIFY(it != AllocationScopes.end());
        if (it->second->Unregister()) {
            AllocationScopes.erase(it);
            RefreshMemoryUsage();
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

    TProcessMemory(const ui64 externalProcessId, const ui64 internalProcessId, const NActors::TActorId& ownerActorId, const bool isPriority,
        const std::vector<std::shared_ptr<TStageFeatures>>& stages, const std::shared_ptr<TStageFeatures>& defaultStage)
        : ExternalProcessId(externalProcessId)
        , InternalProcessId(internalProcessId)
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
        if (allocated) {
            RefreshMemoryUsage();
        }
        return allocated;
    }

    void Unregister() {
        for (auto&& i : AllocationScopes) {
            Y_UNUSED(i.second->Unregister());
        }
        RefreshMemoryUsage();
        //        AFL_VERIFY(MemoryUsage == 0)("usage", MemoryUsage);
        AllocationScopes.clear();
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
