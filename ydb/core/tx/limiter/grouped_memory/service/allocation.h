#pragma once
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/abstract.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

enum class EAllocationStatus {
    Allocated,
    Waiting,
    Failed
};

class TAllocationInfo: public NColumnShard::TMonitoringObjectsCounter<TAllocationInfo> {
private:
    std::shared_ptr<IAllocation> Allocation;
    YDB_READONLY(ui64, AllocationInternalGroupId, 0);
    ui64 AllocatedVolume = 0;
    YDB_READONLY(ui64, Identifier, 0);
    YDB_READONLY(ui64, ProcessId, 0);
    YDB_READONLY(ui64, ScopeId, 0);
    const std::shared_ptr<TStageFeatures> Stage;
    bool AllocationFailed = false;

public:
    ~TAllocationInfo();

    bool IsAllocatable(const ui64 additional) const;

    void SetAllocatedVolume(const ui64 value);

    ui64 GetAllocatedVolume() const {
        return AllocatedVolume;
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

    TAllocationInfo(const ui64 processId, const ui64 scopeId, const ui64 allocationInternalGroupId,
        const std::shared_ptr<IAllocation>& allocation, const std::shared_ptr<TStageFeatures>& stage);
};

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
