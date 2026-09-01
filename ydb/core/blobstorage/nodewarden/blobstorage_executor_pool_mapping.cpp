#include "blobstorage_executor_pool_mapping.h"

#include <utility>

namespace NKikimr::NStorage {

void TBlobStorageExecutorPoolMapping::Update(const TVector<ui32>& executorPoolIds, const THashSet<ui32>& pdiskIds) {
    auto previousMapping = std::move(ExecutorPoolByPDiskId);
    ExecutorPoolByPDiskId.clear();

    if (executorPoolIds.empty()) {
        return;
    }

    THashMap<ui32, size_t> loadByPool;
    for (const ui32 poolId : executorPoolIds) {
        loadByPool[poolId] = 0;
    }

    // Preserve assignments for existing PDisks. Running PDisk and VDisk actors
    // cannot be moved between executor pools, so only newly discovered PDisks
    // are assigned. Retained assignments are not rebalanced.
    TVector<ui32> unassigned;
    for (const ui32 pdiskId : pdiskIds) {
        if (const auto it = previousMapping.find(pdiskId);
                it != previousMapping.end() && loadByPool.contains(it->second)) {
            ExecutorPoolByPDiskId.emplace(pdiskId, it->second);
            ++loadByPool[it->second];
        } else {
            unassigned.push_back(pdiskId);
        }
    }

    // Place new PDisks on the least-loaded pool; ties are broken by configured pool order.
    for (const ui32 pdiskId : unassigned) {
        ui32 bestPool = executorPoolIds.front();
        for (const ui32 poolId : executorPoolIds) {
            if (loadByPool[poolId] < loadByPool[bestPool]) {
                bestPool = poolId;
            }
        }
        ExecutorPoolByPDiskId.emplace(pdiskId, bestPool);
        ++loadByPool[bestPool];
    }
}

std::optional<ui32> TBlobStorageExecutorPoolMapping::FindPoolId(ui32 pdiskId) const {
    if (const auto it = ExecutorPoolByPDiskId.find(pdiskId); it != ExecutorPoolByPDiskId.end()) {
        return it->second;
    }
    return std::nullopt;
}

} // namespace NKikimr::NStorage
