#pragma once
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

#include <ydb/library/signals/object_counter.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

enum class EAllocationStatus {
    Allocated,
    Waiting,
    Failed
};

class TAllocationInfo: public NColumnShard::TMonitoringObjectsCounter<TAllocationInfo> {
private:
    std::shared_ptr<IAllocation> Allocation;
    YDB_READONLY(ui64, AllocationExternalGroupId, 0);
    ui64 AllocatedVolume = 0;
    YDB_READONLY(ui64, Identifier, 0);
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY(ui64, ScopeId, 0);
    YDB_READONLY(std::shared_ptr<TStageFeatures>, Stage, nullptr);
    bool AllocationFailed = false;
    TInstant StartInstant = TInstant::Now();

public:
    ~TAllocationInfo();

    bool IsAllocatable(const ui64 additional) const;

    void SetAllocatedVolume(const ui64 value);

    ui64 GetAllocatedVolume() const {
        return AllocatedVolume;
    }

    TDuration GetAllocationTime() const {
        return TInstant::Now() - StartInstant;
    }

    [[nodiscard]] bool Allocate(const NActors::TActorId& ownerId);

    EAllocationStatus GetAllocationStatus() const {
        if (AllocationFailed) {
            return EAllocationStatus::Failed;
        } else if (Allocation) {
            return EAllocationStatus::Waiting;
        } else {
            return EAllocationStatus::Allocated;
        }
    }

    TAllocationInfo(const ui64 processId, const ui64 scopeId, const ui64 allocationExternalGroupId,
        const std::shared_ptr<IAllocation>& allocation, const std::shared_ptr<TStageFeatures>& stage);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
