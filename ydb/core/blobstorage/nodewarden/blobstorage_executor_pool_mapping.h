#pragma once

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NStorage {

// Tracks which executor pool each running PDisk's actors are placed on. Assignments
// are made on demand when a PDisk first asks for a pool and dropped when the PDisk
// is destroyed or leaves the configured service sets — the latter must free the slot
// immediately, because a replacement PDisk starts before the removed one is destroyed
// (destruction may even wait for later service-set updates while its VDisks drain).
class TBlobStorageExecutorPoolMapping {
public:
    // Returns the pool for the given PDisk, assigning the least-loaded pool (ties
    // broken by configured pool order) on first request. executorPoolIds must not
    // be empty.
    ui32 AcquirePoolId(const TVector<ui32>& executorPoolIds, ui32 pdiskId);

    // Erases the PDisk's assignment; no-op for PDisks without one.
    void ReleasePoolId(ui32 pdiskId);

    // Erases assignments of PDisks absent from pdiskIds.
    void RetainConfiguredPDisks(const THashSet<ui32>& pdiskIds);

    std::optional<ui32> FindPoolId(ui32 pdiskId) const;

private:
    THashMap<ui32, ui32> ExecutorPoolByPDiskId;
};

} // namespace NKikimr::NStorage
