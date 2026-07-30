#include "blobstorage_executor_pool_mapping.h"

#include <util/generic/hash_set.h>

#include <utility>

namespace NKikimr::NStorage {

void TBlobStorageExecutorPoolMapping::Update(const TVector<ui32>& executorPoolIds, const TVector<ui32>& pdiskIds) {
    auto previousMapping = std::move(ExecutorPoolByPDiskId);
    ExecutorPoolByPDiskId.clear();

    if (executorPoolIds.empty()) {
        return;
    }

    THashMap<ui32, size_t> loadByPool;
    for (const ui32 poolId : executorPoolIds) {
        loadByPool[poolId] = 0;
    }

    // Preserve assignments whose pool still exists, so that a PDisk restart does not move it
    // to another pool. Skewed retained assignments are not rebalanced: the mapping is only
    // consulted at PDisk/VDisk start, so reassigning an already-mapped PDisk would not take
    // effect until its restart anyway.
    TVector<ui32> unassigned;
    THashSet<ui32> seen;
    for (const ui32 pdiskId : pdiskIds) {
        if (!seen.insert(pdiskId).second) {
            continue;
        }
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
