#pragma once
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

enum class EAllocationStatus {
    Allocated,
    Waiting,
    Failed
};

class TAllocationInfo {
private:
    std::shared_ptr<IAllocation> Allocation;
    YDB_READONLY(ui64, AllocationGroupId, 0);
    ui64 AllocatedVolume = 0;
    YDB_READONLY(ui64, Identifier, 0);
    YDB_READONLY(ui64, ProcessId, 0);
    const std::shared_ptr<TStageFeatures> Stage;
    bool AllocationFailed = false;

public:
    ~TAllocationInfo() {
        if (GetAllocationStatus() != EAllocationStatus::Failed) {
            Stage->Free(AllocatedVolume, GetAllocationStatus() == EAllocationStatus::Allocated);
        }
        
        AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "destroy")("allocation_id", Identifier)("stage", Stage->GetName());
    }

    bool IsAllocatable(const ui64 additional) const {
        return Stage->IsAllocatable(AllocatedVolume, additional);
    }

    void SetAllocatedVolume(const ui64 value) {
        AFL_VERIFY(GetAllocationStatus() != EAllocationStatus::Failed);
        Stage->UpdateVolume(AllocatedVolume, value, GetAllocationStatus() == EAllocationStatus::Allocated);
        AllocatedVolume = value;
    }

    ui64 GetAllocatedVolume() const {
        return AllocatedVolume;
    }

    [[nodiscard]] bool Allocate(const NActors::TActorId& ownerId) {
        AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocated")("allocation_id", Identifier)("stage", Stage->GetName());
        AFL_VERIFY(Allocation);
        const bool result = Allocation->OnAllocated(
            std::make_shared<TAllocationGuard>(ProcessId, Allocation->GetIdentifier(), ownerId, Allocation->GetMemory()), Allocation);
        if (result) {
            Stage->Allocate(AllocatedVolume);
        } else {
            Stage->Free(AllocatedVolume, false);
            AllocationFailed = true;
        }
        Allocation = nullptr;
        return result;
    }

    EAllocationStatus GetAllocationStatus() const {
        if (AllocationFailed) {
            return EAllocationStatus::Failed;
        } else if (Allocation) {
            return EAllocationStatus::Waiting;
        } else {
            return EAllocationStatus::Allocated;
        }
    }

    TAllocationInfo(const ui64 processId, const ui64 allocationGroupId, const std::shared_ptr<IAllocation>& allocation,
        const std::shared_ptr<TStageFeatures>& stage);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
