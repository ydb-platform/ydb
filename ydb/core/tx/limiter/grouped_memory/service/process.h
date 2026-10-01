#pragma once
#include "group.h"
#include "ids.h"

#include <vector>

#include <ydb/library/accessor/validator.h>
#include <ydb/library/signals/object_counter.h>

#include <ydb/core/tx/limiter/grouped_memory/tracing/probes.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

LWTRACE_USING(YDB_GROUPED_MEMORY_PROVIDER);

enum class EUnrestrictedScheduleResult {
    Allocated,
    Failed,
    Idle
};

class TProcessMemoryScope: public NColumnShard::TMonitoringObjectsCounter<TProcessMemoryScope> {
private:
    const ui64 ExternalProcessId;
    YDB_READONLY(ui64, ExternalScopeId, 0);
    TAllocationGroups WaitAllocations;
    THashMap<ui64, std::shared_ptr<TAllocationInfo>> AllocationInfo;
    TExternalIdsControl GroupIds;
    std::set<ui64> AdmittedGroupIds;
    THashMap<ui64, ui64> AccountedBytesByAdmittedGroup;
    ui64 AdmittedAllocatedBytes = 0;
    ui32 Links = 1;
    const NActors::TActorId OwnerActorId;
    const bool UnrestrictedEnabled = false;
    const ui32 MaxUnrestrictedGroups = 1;

    static bool FitsUnrestricted(const TAllocationInfo& info) {
        return info.IsAllocatableUnrestricted(0);
    }

    bool AdmittedGroupHasFittingAllocation() const {
        for (const ui64 groupId : AdmittedGroupIds) {
            if (WaitAllocations.ContainsIf(groupId, FitsUnrestricted)) {
                return true;
            }
        }
        return false;
    }

    bool CanAdmitMinWaitingGroup() const {
        if (!UnrestrictedEnabled || AdmittedGroupIds.size() >= MaxUnrestrictedGroups) {
            return false;
        }
        return MinWaitingGroupFits();
    }

    bool MinWaitingGroupFits() const {
        const auto minGroupId = WaitAllocations.GetMinExternalGroupId();
        if (!minGroupId || AdmittedGroupIds.contains(*minGroupId)) {
            return false;
        }
        return WaitAllocations.ContainsIf(*minGroupId, FitsUnrestricted);
    }

    bool HasStuckAdmission() const {
        for (const ui64 groupId : AdmittedGroupIds) {
            if (WaitAllocations.HasWaiting(groupId) && !WaitAllocations.ContainsIf(groupId, FitsUnrestricted)) {
                return true;
            }
        }
        return false;
    }

    bool CanReleaseStuckAdmission() const {
        return HasStuckAdmission() && MinWaitingGroupFits();
    }

    ui64 AllocatedBytesOfGroup(const ui64 groupId) const {
        ui64 bytes = 0;
        for (const auto& [_, info] : AllocationInfo) {
            if (info->GetAllocationExternalGroupId() == groupId && info->GetAllocationStatus() == EAllocationStatus::Allocated) {
                bytes += info->GetAllocatedVolume();
            }
        }
        return bytes;
    }

    void ReaccountAdmittedGroup(const ui64 groupId) {
        if (!AdmittedGroupIds.contains(groupId)) {
            return;
        }
        const ui64 fresh = AllocatedBytesOfGroup(groupId);
        ui64& accounted = AccountedBytesByAdmittedGroup[groupId];
        if (fresh >= accounted) {
            AdmittedAllocatedBytes += fresh - accounted;
        } else {
            AFL_VERIFY(AdmittedAllocatedBytes >= accounted - fresh);
            AdmittedAllocatedBytes -= accounted - fresh;
        }
        if (fresh == 0) {
            AccountedBytesByAdmittedGroup.erase(groupId);
        } else {
            accounted = fresh;
        }
    }

    void RevokeAdmission(const ui64 groupId) {
        auto it = AccountedBytesByAdmittedGroup.find(groupId);
        if (it != AccountedBytesByAdmittedGroup.end()) {
            AFL_VERIFY(AdmittedAllocatedBytes >= it->second);
            AdmittedAllocatedBytes -= it->second;
            AccountedBytesByAdmittedGroup.erase(it);
        }
        AdmittedGroupIds.erase(groupId);
    }

    void ReleaseStuckAdmissions() {
        std::vector<ui64> stuck;
        for (const ui64 groupId : AdmittedGroupIds) {
            if (WaitAllocations.HasWaiting(groupId) && !WaitAllocations.ContainsIf(groupId, FitsUnrestricted)) {
                stuck.push_back(groupId);
            }
        }
        for (const ui64 groupId : stuck) {
            RevokeAdmission(groupId);
        }
    }

    bool GroupHasAllocated(const ui64 groupId) const {
        for (const auto& [_, info] : AllocationInfo) {
            if (info->GetAllocationExternalGroupId() == groupId && info->GetAllocationStatus() == EAllocationStatus::Allocated) {
                return true;
            }
        }
        return false;
    }

    void DropAdmissionIfIdle(const ui64 groupId) {
        if (!GroupHasAllocated(groupId)) {
            RevokeAdmission(groupId);
        }
    }

    EUnrestrictedScheduleResult AllocateTaken(const std::shared_ptr<TAllocationInfo>& allocation, const ui64 groupId) {
        const bool success = allocation->Allocate(OwnerActorId);
        if (!success) {
            UnregisterAllocation(allocation->GetIdentifier());
            DropAdmissionIfIdle(groupId);
            return EUnrestrictedScheduleResult::Failed;
        }
        ReaccountAdmittedGroup(groupId);
        return EUnrestrictedScheduleResult::Allocated;
    }

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
    TProcessMemoryScope(const ui64 externalProcessId, const ui64 externalScopeId, const NActors::TActorId& ownerActorId,
        const bool unrestrictedEnabled = false, const ui32 maxUnrestrictedGroups = 1)
        : ExternalProcessId(externalProcessId)
        , ExternalScopeId(externalScopeId)
        , OwnerActorId(ownerActorId)
        , UnrestrictedEnabled(unrestrictedEnabled)
        , MaxUnrestrictedGroups(maxUnrestrictedGroups) {
    }

    bool IsUnrestrictedEnabled() const {
        return UnrestrictedEnabled;
    }

    bool HasAdmission() const {
        return !AdmittedGroupIds.empty();
    }

    bool CanScheduleUnrestricted() const {
        return UnrestrictedEnabled && (AdmittedGroupHasFittingAllocation() || CanAdmitMinWaitingGroup() || CanReleaseStuckAdmission());
    }

    EUnrestrictedScheduleResult ScheduleOneUnrestricted() {
        if (!UnrestrictedEnabled) {
            return EUnrestrictedScheduleResult::Idle;
        }
        const std::vector<ui64> admitted(AdmittedGroupIds.begin(), AdmittedGroupIds.end());
        for (const ui64 groupId : admitted) {
            if (auto allocation = WaitAllocations.TakeOne(groupId, FitsUnrestricted)) {
                return AllocateTaken(allocation, groupId);
            }
        }
        // The admitted group holds the slot and waits on a request that does not fit the band.
        // A smaller group is waiting on a request that does fit, and it keeps its bytes until that
        // request is granted. Release the stuck slot so the smaller group can run.
        if (CanReleaseStuckAdmission()) {
            ReleaseStuckAdmissions();
        }
        if (!CanAdmitMinWaitingGroup()) {
            return EUnrestrictedScheduleResult::Idle;
        }
        const ui64 groupId = *WaitAllocations.GetMinExternalGroupId();
        auto allocation = WaitAllocations.TakeOne(groupId, FitsUnrestricted);
        if (!allocation) {
            return EUnrestrictedScheduleResult::Idle;
        }
        AdmittedGroupIds.insert(groupId);
        return AllocateTaken(allocation, groupId);
    }

    void CollectAdmitted(ui64& groups, ui64& bytes) const {
        groups += AdmittedGroupIds.size();
        bytes += AdmittedAllocatedBytes;
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
        AdmittedGroupIds.clear();
        AccountedBytesByAdmittedGroup.clear();
        AdmittedAllocatedBytes = 0;
        AllocationInfo.clear();
        YDB_LOG_INFO_COMP(NKikimrServices::GROUPED_MEMORY_LIMITER, "",
            {"event", "scope_cleaned"},
            {"processId", ExternalProcessId},
            {"externalScopeId", ExternalScopeId});
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

            const bool softOk = allocationInfo->IsAllocatable(0);
            const bool bandOk = UnrestrictedEnabled && AdmittedGroupIds.contains(externalGroupId) && allocationInfo->IsAllocatableUnrestricted(0);
            const bool force = !UnrestrictedEnabled && isPriorityProcess && externalGroupId <= GroupIds.GetMinExternalIdVerified();
            if (allocationInfo->GetAllocationStatus() != EAllocationStatus::Waiting) {
            } else if (WaitAllocations.GetMinExternalGroupId().value_or(externalGroupId) < externalGroupId) {
                WaitAllocations.AddAllocationExt(externalGroupId, allocationInfo);
            } else if (softOk || bandOk || force) {
                Y_UNUSED(WaitAllocations.RemoveAllocationExt(externalGroupId, allocationInfo));
                auto success = allocationInfo->Allocate(OwnerActorId);
                if (!success) {
                    UnregisterAllocation(allocationInfo->GetIdentifier());
                } else if (AdmittedGroupIds.contains(externalGroupId)) {
                    ReaccountAdmittedGroup(externalGroupId);
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
            YDB_LOG_WARN_COMP(NKikimrServices::GROUPED_MEMORY_LIMITER, "",
                {"reason", "allocation_cleaned_in_previous_scope_id_live"},
                {"allocationId", allocationId},
                {"processId", ExternalProcessId},
                {"externalScopeId", ExternalScopeId});
            return true;
        }
        bool waitFlag = false;
        const ui64 externalGroupId = it->second->GetAllocationExternalGroupId();
        const bool reaccount = AdmittedGroupIds.contains(externalGroupId) && it->second->GetAllocationStatus() == EAllocationStatus::Allocated;
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
        YDB_LOG_DEBUG_COMP(NKikimrServices::GROUPED_MEMORY_LIMITER, "",
            {"event", "allocation_unregister"},
            {"allocationId", allocationId},
            {"wait", waitFlag},
            {"externalGroupId", externalGroupId},
            {"allocationStatus", it->second->GetAllocationStatus()});
        memoryAllocated = it->second->GetAllocatedVolume();
        AllocationInfo.erase(it);
        if (reaccount) {
            ReaccountAdmittedGroup(externalGroupId);
        }
        return !!memoryAllocated;
    }

    void UnregisterGroup(const bool isPriorityProcess, const ui64 externalGroupId) {
        if (GroupIds.UnregisterExternalId(externalGroupId)) {
            UnregisterGroupImplExt(externalGroupId);
            YDB_LOG_INFO_COMP(NKikimrServices::GROUPED_MEMORY_LIMITER, "",
                {"event", "remove_group"},
                {"externalGroupId", externalGroupId},
                {"minGroup", GroupIds.GetMinExternalIdOptional()});
            RevokeAdmission(externalGroupId);
            if (isPriorityProcess && !UnrestrictedEnabled && (externalGroupId < GroupIds.GetMinExternalIdDef(externalGroupId))) {
                Y_UNUSED(TryAllocateWaiting(isPriorityProcess, 0));
            }
        } else {
            YDB_LOG_WARN_COMP(NKikimrServices::GROUPED_MEMORY_LIMITER, "",
                {"event", "remove_absent_group"},
                {"externalGroupId", externalGroupId});
        }
    }

    void RegisterGroup(const bool isPriorityProcess, const ui64 externalGroupId) {
        GroupIds.RegisterExternalId(externalGroupId);
        YDB_LOG_INFO_COMP(NKikimrServices::GROUPED_MEMORY_LIMITER, "",
            {"event", "register_group"},
            {"externalGroupId", externalGroupId},
            {"minGroup", GroupIds.GetMinExternalIdOptional()});
        if (isPriorityProcess && (externalGroupId < GroupIds.GetMinExternalIdDef(externalGroupId))) {
            Y_UNUSED(TryAllocateWaiting(isPriorityProcess, 0));
        }
    }

    bool HasWaitingAllocations() const {
        return !WaitAllocations.IsEmpty();
    }

    TString DebugString() const;
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
    const bool UnrestrictedEnabled = false;
    const ui32 MaxUnrestrictedGroupsPerScope = 1;
    ui64 MemoryUsage = 0;

    YDB_ACCESSOR(ui32, LinksCount, 1);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TStageFeatures>>, Stages);
    const std::shared_ptr<TStageFeatures> DefaultStage;
    THashMap<ui64, std::shared_ptr<TProcessMemoryScope>> AllocationScopes;
    std::set<ui64> WaitingScopes;

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
        auto& scope = GetAllocationScopeVerified(externalScopeId);
        if (scope.AllocationUpdated(allocationId)) {
            UpdateWaitingScopes(&scope);
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
        UpdateWaitingScopes(&scope);
    }

    bool UnregisterAllocation(const ui64 externalScopeId, const ui64 allocationId) {
        if (auto* scope = GetAllocationScopeOptional(externalScopeId)) {
            if (scope->UnregisterAllocation(allocationId)) {
                RefreshMemoryUsage();
                UpdateWaitingScopes(scope);
                return true;
            }
        }
        return false;
    }

    void UnregisterGroup(const ui64 externalScopeId, const ui64 externalGroupId) {
        if (auto* scope = GetAllocationScopeOptional(externalScopeId)) {
            scope->UnregisterGroup(IsPriorityProcess(), externalGroupId);
            RefreshMemoryUsage();
            UpdateWaitingScopes(scope);
        }
    }

    void RegisterGroup(const ui64 externalScopeId, const ui64 externalGroupId) {
        auto& scope = GetAllocationScopeVerified(externalScopeId);
        scope.RegisterGroup(IsPriorityProcess(), externalGroupId);
        UpdateWaitingScopes(&scope);
    }

    void UnregisterScope(const ui64 externalScopeId) {
        auto it = AllocationScopes.find(externalScopeId);
        AFL_VERIFY(it != AllocationScopes.end());
        if (it->second->Unregister()) {
            AllocationScopes.erase(it);
            RefreshMemoryUsage();
            WaitingScopes.erase(externalScopeId);
        }
    }

    void RegisterScope(const ui64 externalScopeId) {
        auto it = AllocationScopes.find(externalScopeId);
        if (it == AllocationScopes.end()) {
            AFL_VERIFY(AllocationScopes.emplace(externalScopeId, std::make_shared<TProcessMemoryScope>(ExternalProcessId, externalScopeId, OwnerActorId, UnrestrictedEnabled, MaxUnrestrictedGroupsPerScope)).second);
        } else {
            it->second->Register();
        }
    }

    void SetPriorityProcess() {
        AFL_VERIFY(!PriorityProcessFlag);
        PriorityProcessFlag = true;
    }

    TProcessMemory(const ui64 externalProcessId, const ui64 internalProcessId, const NActors::TActorId& ownerActorId, const bool isPriority,
        const std::vector<std::shared_ptr<TStageFeatures>>& stages, const std::shared_ptr<TStageFeatures>& defaultStage,
        const bool unrestrictedEnabled = false, const ui32 maxUnrestrictedGroupsPerScope = 1)
        : ExternalProcessId(externalProcessId)
        , InternalProcessId(internalProcessId)
        , OwnerActorId(ownerActorId)
        , PriorityProcessFlag(isPriority)
        , UnrestrictedEnabled(unrestrictedEnabled)
        , MaxUnrestrictedGroupsPerScope(maxUnrestrictedGroupsPerScope)
        , Stages(stages)
        , DefaultStage(defaultStage) {
    }

    ui64 GetInternalProcessId() const {
        return InternalProcessId;
    }

    const std::set<ui64>& GetWaitingScopeIds() const {
        return WaitingScopes;
    }

    TProcessMemoryScope& MutableScope(const ui64 externalScopeId) {
        return GetAllocationScopeVerified(externalScopeId);
    }

    void CollectAdmitted(ui64& groups, ui64& bytes) const {
        for (const auto& [_, scope] : AllocationScopes) {
            scope->CollectAdmitted(groups, bytes);
        }
    }

    EUnrestrictedScheduleResult ScheduleOneUnrestricted(const ui64 externalScopeId) {
        auto& scope = GetAllocationScopeVerified(externalScopeId);
        const auto result = scope.ScheduleOneUnrestricted();
        if (result != EUnrestrictedScheduleResult::Idle) {
            RefreshMemoryUsage();
        }
        UpdateWaitingScopes(&scope);
        return result;
    }

    bool TryAllocateWaiting(const ui32 allocationsCountLimit) {
        bool allocated = false;
        for (auto waitingIt = WaitingScopes.begin(); waitingIt != WaitingScopes.end();) {
            auto it = AllocationScopes.find(*waitingIt);
            AFL_VERIFY(it != AllocationScopes.end());
            auto* scope = it->second.get();
            if (scope->TryAllocateWaiting(IsPriorityProcess(), allocationsCountLimit)) {
                allocated = true;
            }

            auto hasWaitingAllocations = scope->HasWaitingAllocations();
            if (!hasWaitingAllocations) {
                waitingIt = WaitingScopes.erase(waitingIt);
            } else {
                ++waitingIt;
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
        WaitingScopes.clear();
    }

    bool HasWaitingAllocations() const {
        return !WaitingScopes.empty();
    }

    void UpdateWaitingScopes(TProcessMemoryScope* scope) {
        auto hasWaitingAllocations = scope->HasWaitingAllocations();
        if (hasWaitingAllocations) {
            WaitingScopes.insert(scope->GetExternalScopeId());
            return;
        }
        WaitingScopes.erase(scope->GetExternalScopeId());
    }
    TString DebugString() const;
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
