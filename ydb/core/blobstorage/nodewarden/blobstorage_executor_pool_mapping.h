#pragma once

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NStorage {

class TBlobStorageExecutorPoolMapping {
public:
    void Update(const TVector<ui32>& executorPoolIds, const THashSet<ui32>& pdiskIds);

    std::optional<ui32> FindPoolId(ui32 pdiskId) const;

private:
    THashMap<ui32, ui32> ExecutorPoolByPDiskId;
};

} // namespace NKikimr::NStorage
