#pragma once

#include "partitioning_keys_manager.h"

#include <util/generic/vector.h>

namespace NKikimr::NPQ {

struct TWriteStats {
    // SourceId->WrittenBytes
    std::vector<std::pair<TString, ui64>> WrittenBytes;
    std::unordered_map<TString, NPQ::TPartitioningKeysManager> PartitioningKeysManagers;
};

} // namespace NKikimr::NPQ
