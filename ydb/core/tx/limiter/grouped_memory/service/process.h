#pragma once
#include "group.h"
#include "ids.h"

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TProcessMemory {
private:
    const ui64 ExternalProcessId;
    TAllocationGroups WaitAllocations;
    THashMap<ui64, std::shared_ptr<TAllocationInfo>> AllocationInfo;
    TIdsControl GroupIds;
    const NActors::TActorId OwnerActorId;
    bool PriorityProcessFlag = false;

    const std::shared_ptr<TAllocationInfo>& RegisterAllocationImpl(
        const ui64 internalGroupId, const std::shared_ptr<IAllocation>& task, const std::shared_ptr<TStageFeatures>& stage);

    void UnregisterGroupImpl(const ui64 internalGroupId);
    TAllocationInfo& GetAllocationInfoVerified(const ui64 allocationId) const {
        auto it = AllocationInfo.find(allocationId);
        AFL_VERIFY(it != AllocationInfo.end());
        return *it->second;
    }
    friend class TAllocationGroups;

    YDB_ACCESSOR(ui32, LinksCount, 1);
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TStageFeatures>>, Stages);
    const std::shared_ptr<TStageFeatures> DefaultStage;

public:
    bool IsPriorityProcess() const {
        return PriorityProcessFlag;
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
        , DefaultStage(defaultStage)
    {
    }

    void RegisterAllocation(const ui64 externalGroupId, const std::shared_ptr<IAllocation>& task, const std::optional<ui32>& stageIdx);
    bool UnregisterAllocation(const ui64 allocationId);
    bool UpdateAllocation(const ui64 allocationId, const ui64 volume) {
        GetAllocationInfoVerified(allocationId).SetAllocatedVolume(volume);
        return true;
    }

    void Unregister();

    void UnregisterGroup(const ui64 externalGroupId);
    void RegisterGroup(const ui64 externalGroupId);

    bool TryAllocateWaiting(const ui32 allocationsCountLimit) {
        return WaitAllocations.Allocate(*this, allocationsCountLimit);
    }
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
