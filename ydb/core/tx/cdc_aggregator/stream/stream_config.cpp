#include "stream_config.h"

#include <util/string/builder.h>

#include <algorithm>

namespace NKikimr::NCdcAggregator {

bool NormalizeStreamConfig(TStreamConfig& config, TString& error) {
    if (!config.StreamPathId) {
        error = "Stream path id is not set";
        return false;
    }
    if (!config.SchemeShardId) {
        error = "SchemeShard id is not set";
        return false;
    }
    if (!config.Version) {
        error = "Configuration version is not set";
        return false;
    }

    // An empty set has no barrier at all, so such a stream would never
    // publish resolved timestamps.
    if (config.Sources.empty()) {
        error = "No sources";
        return false;
    }
    std::sort(config.Sources.begin(), config.Sources.end());
    if (std::adjacent_find(config.Sources.begin(), config.Sources.end()) != config.Sources.end()) {
        error = "Duplicate source";
        return false;
    }
    if (config.Sources.front() == 0) {
        error = "Zero source id";
        return false;
    }

    if (config.Partitions.empty()) {
        error = "No partitions";
        return false;
    }
    std::sort(config.Partitions.begin(), config.Partitions.end(), [](const auto& left, const auto& right) {
        return left.PartitionId < right.PartitionId;
    });
    for (size_t index = 0; index < config.Partitions.size(); ++index) {
        const auto& partition = config.Partitions[index];
        if (!partition.TabletId) {
            error = TStringBuilder() << "Partition " << partition.PartitionId << " has no tablet";
            return false;
        }
        if (index > 0 && config.Partitions[index - 1].PartitionId == partition.PartitionId) {
            error = TStringBuilder() << "Duplicate partition " << partition.PartitionId;
            return false;
        }
    }

    return true;
}

} // namespace NKikimr::NCdcAggregator
