#include "blobstorage_executor_pool_mapping.h"

#include <util/system/yassert.h>

namespace NKikimr::NStorage {

ui32 TBlobStorageExecutorPoolMapping::AcquirePoolId(const TVector<ui32>& executorPoolIds, ui32 pdiskId) {
    Y_ABORT_UNLESS(!executorPoolIds.empty());

    // Running PDisk and VDisk actors cannot be moved between executor pools, so an
    // existing assignment is retained even if the load has become uneven.
    if (const auto it = ExecutorPoolByPDiskId.find(pdiskId); it != ExecutorPoolByPDiskId.end()) {
        return it->second;
    }

    THashMap<ui32, size_t> loadByPool;
    for (const ui32 poolId : executorPoolIds) {
        loadByPool[poolId] = 0;
    }
    for (const auto& [assignedPDiskId, poolId] : ExecutorPoolByPDiskId) {
        if (const auto it = loadByPool.find(poolId); it != loadByPool.end()) {
            ++it->second;
        }
    }

    // Place the new PDisk on the least-loaded pool; ties are broken by configured pool order.
    ui32 bestPool = executorPoolIds.front();
    for (const ui32 poolId : executorPoolIds) {
        if (loadByPool[poolId] < loadByPool[bestPool]) {
            bestPool = poolId;
        }
    }
    ExecutorPoolByPDiskId.emplace(pdiskId, bestPool);
    return bestPool;
}

void TBlobStorageExecutorPoolMapping::ReleasePoolId(ui32 pdiskId) {
    ExecutorPoolByPDiskId.erase(pdiskId);
}

void TBlobStorageExecutorPoolMapping::RetainConfiguredPDisks(const THashSet<ui32>& pdiskIds) {
    for (auto it = ExecutorPoolByPDiskId.begin(); it != ExecutorPoolByPDiskId.end(); ) {
        if (pdiskIds.contains(it->first)) {
            ++it;
        } else {
            ExecutorPoolByPDiskId.erase(it++);
        }
    }
}

std::optional<ui32> TBlobStorageExecutorPoolMapping::FindPoolId(ui32 pdiskId) const {
    if (const auto it = ExecutorPoolByPDiskId.find(pdiskId); it != ExecutorPoolByPDiskId.end()) {
        return it->second;
    }
    return std::nullopt;
}

} // namespace NKikimr::NStorage
