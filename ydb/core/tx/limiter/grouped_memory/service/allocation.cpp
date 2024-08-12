#include "allocation.h"
#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TAllocationInfo::TAllocationInfo(const ui64 processId, const ui64 allocationInternalGroupId, const std::shared_ptr<IAllocation>& allocation,
    const std::shared_ptr<TStageFeatures>& stage)
    : Allocation(allocation)
    , AllocationInternalGroupId(allocationInternalGroupId)
    , Identifier(TValidator::CheckNotNull(Allocation)->GetIdentifier())
    , ProcessId(processId)
    , Stage(stage) {
    AFL_VERIFY(Stage);
    AFL_VERIFY(Allocation);
    AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "add")("id", Allocation->GetIdentifier())("stage", Stage->GetName());
    AllocatedVolume = Allocation->GetMemory();
    Stage->Add(AllocatedVolume, Allocation->IsAllocated());
    if (allocation->IsAllocated()) {
        AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocated_on_add")("allocation_id", Identifier)("stage", Stage->GetName());
        Allocation = nullptr;
    }
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
