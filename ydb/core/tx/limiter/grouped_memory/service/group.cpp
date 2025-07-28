#include "group.h"
#include "process.h"

namespace NKikimr::NOlap::NGroupedMemoryManager {

std::vector<std::shared_ptr<TAllocationInfo>> TGrouppedAllocations::AllocatePossible(const ui32 allocationsLimit) {
    std::vector<std::shared_ptr<TAllocationInfo>> result;
    ui64 allocationMemory = 0;
    ui32 allocationsCount = 0;
    for (auto&& [_, allocation] : Allocations) {
        if (allocation->IsAllocatable(allocationMemory)) {
            allocationMemory += allocation->GetAllocatedVolume();
            result.emplace_back(allocation);
            if (++allocationsCount == allocationsLimit) {
                return result;
            }
        }
    }
    return result;
}

bool TAllocationGroups::Allocate(const bool isPriorityProcess, TProcessMemoryScope& scope, const ui32 allocationsLimit) {
    AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "try_allocation")("limit", allocationsLimit)(
        "external_process_id", scope.ExternalProcessId)("external_scope_id", scope.ExternalScopeId)(
        "forced_external_group_id", scope.GroupIds.GetMinExternalIdOptional())("is_priority_process", isPriorityProcess);
    ui32 allocationsCount = 0;
    while (true) {
        std::vector<ui64> toRemove;
        for (auto it = Groups.begin(); it != Groups.end();) {
            const ui64 externalGroupId = it->first;
            const bool forced = isPriorityProcess && externalGroupId == scope.GroupIds.GetMinExternalIdVerified();
            std::vector<std::shared_ptr<TAllocationInfo>> allocated;
            if (forced) {
                allocated = it->second.ExtractAllocationsToVector();
                AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "forced_group")("count", allocated.size())("external_group_id", externalGroupId);
            } else if (allocationsLimit) {
                allocated = it->second.AllocatePossible(allocationsLimit - allocationsCount);
                AFL_DEBUG(NKikimrServices::GROUPED_MEMORY_LIMITER)("event", "common_forced_group")("count", allocated.size())(
                    "external_group_id", externalGroupId);
            } else {
                break;
            }
            for (auto&& i : allocated) {
                if (!i->Allocate(scope.OwnerActorId)) {
                    toRemove.emplace_back(i->GetIdentifier());
                } else if (!forced) {
                    AFL_VERIFY(++allocationsCount <= allocationsLimit)("count", allocationsCount)("limit", allocationsLimit);
                }
                if (!forced) {
                    AFL_VERIFY(it->second.Remove(i));
                }
            }
            if (!it->second.IsEmpty()) {
                break;
            }
            it = Groups.erase(it);
            if (!forced && allocationsCount == allocationsLimit) {
                break;
            }
        }
        for (auto&& i : toRemove) {
            scope.UnregisterAllocation(i);
        }
        if (toRemove.empty() || allocationsCount == allocationsLimit) {
            break;
        }
    }
    return allocationsCount;
}

}   // namespace NKikimr::NOlap::NGroupedMemoryManager
