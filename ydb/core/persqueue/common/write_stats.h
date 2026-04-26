#pragma once

#include "partitioning_keys_manager.h"

#include <util/generic/vector.h>

namespace NKikimr::NPQ {

struct TWriteStats {
    // Tag -> (SourceId -> metric value)
    std::unordered_map<TString, std::vector<std::pair<TString, ui64>>> PerSourceMetrics;
    std::unordered_map<TString, NPQ::TPartitioningKeysManager> PartitioningKeysManagers;
};

} // namespace NKikimr::NPQ
