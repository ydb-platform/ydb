#pragma once

#include "partitioning_keys_manager.h"

#include <memory>

#include <util/generic/vector.h>

namespace NKikimr::NPQ {

struct TWriteStats {
    // Indexed by NPQ::ETag (0 = bytes, 1 = messages): per-source hash -> sliding-window aggregate.
    std::vector<std::vector<std::pair<TString, ui64>>> PerSourceMetrics;
    std::vector<std::unique_ptr<NPQ::TPartitioningKeysManager>> PartitioningKeysManagers;
};

} // namespace NKikimr::NPQ
