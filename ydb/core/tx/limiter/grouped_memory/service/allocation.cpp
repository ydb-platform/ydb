#include "allocation.h"

#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

TAllocationInfo::TAllocationInfo(const ui64 processId, const ui64 scopeId, const ui64 allocationExternalGroupId,
    const std::shared_ptr<IAllocation>& allocation, const std::shared_ptr<TStageFeatures>& stage)
    : Allocation(allocation)
    , AllocationExternalGroupId(allocationExternalGroupId)
    , Identifier(TValidator::CheckNotNull(Allocation)->GetIdentifier())
    , ProcessId(processId)
    , ScopeId(scopeId)
    , Stage(stage) {
    AFL_VERIFY(Stage);
    AFL_VERIFY(Allocation);
    AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "add")("id", Allocation->GetIdentifier())("stage", Stage->GetName());
    AllocatedVolume = Allocation->GetMemory();
    if (allocation->IsAllocated()) {
        AFL_INFO(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocated_on_add")("allocation_id", Identifier)("stage", Stage->GetName());
        Allocation = nullptr;
    }
    Stage->Add(AllocatedVolume, GetAllocationStatus() == EAllocationStatus::Allocated);
}

bool TAllocationInfo::Allocate(const NActors::TActorId& ownerId) {
    AFL_TRACE(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "allocated")("allocation_id", Identifier)("stage", Stage->GetName());
    AFL_VERIFY(Allocation)("status", GetAllocationStatus())("volume", AllocatedVolume)("id", Identifier)("stage", Stage->GetName());
    auto allocationResult = Stage->Allocate(AllocatedVolume);
    if (allocationResult.IsFail()) {
        AllocationFailed = true;
        Allocation->OnAllocationImpossible(allocationResult.GetErrorMessage());
        Allocation = nullptr;
        return false;
    }
    const bool result = Allocation->OnAllocated(
        std::make_shared<TAllocationGuard>(ProcessId, ScopeId, Allocation->GetIdentifier(), ownerId, Allocation->GetMemory(), Stage),
        Allocation);
    if (!result) {
        Stage->Free(AllocatedVolume, true);
        AllocationFailed = true;
    }
    Allocation = nullptr;
    return result;
}

void TAllocationInfo::SetAllocatedVolume(const ui64 value) {
    AFL_VERIFY(GetAllocationStatus() != EAllocationStatus::Failed);
    Stage->UpdateVolume(AllocatedVolume, value, GetAllocationStatus() == EAllocationStatus::Allocated);
    AllocatedVolume = value;
}

bool TAllocationInfo::IsAllocatable(const ui64 additional) const {
    return Stage->IsAllocatable(AllocatedVolume, additional);
}

TAllocationInfo::~TAllocationInfo() {
    if (GetAllocationStatus() != EAllocationStatus::Failed && GetAllocationStatus() != EAllocationStatus::Allocated) {
        Stage->Free(AllocatedVolume, false);
    }

    AFL_TRACE(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "destroy")("allocation_id", Identifier)("stage", Stage->GetName());
}

TString TAllocationInfo::DebugString() const {
    TStringBuilder sb;
    sb << "TAllocationInfo{" << Endl
       << "  Identifier=" << Identifier << Endl
       << "  ProcessId=" << ProcessId << Endl
       << "  ScopeId=" << ScopeId << Endl
       << "  AllocationExternalGroupId=" << AllocationExternalGroupId << Endl
       << "  AllocatedVolume=" << AllocatedVolume << Endl
       << "  AllocationStatus=";
    
    switch (GetAllocationStatus()) {
        case EAllocationStatus::Allocated:
            sb << "Allocated";
            break;
        case EAllocationStatus::Waiting:
            sb << "Waiting";
            break;
        case EAllocationStatus::Failed:
            sb << "Failed";
            break;
    }
    
    sb << Endl
       << "  AllocationTime=" << GetAllocationTime().ToString() << Endl
       << "  Stage=" << (Stage ? Stage->DebugString() : "null") << Endl
       << "  Allocation=" << (Allocation ? Allocation->DebugString() : "null") << Endl
       << "}";
    
    return sb;
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
