#include "config.h"
#include <util/generic/hash_set.h>
#include <util/string/printf.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr {

bool CheckPersQueueConfig(const NKikimrPQ::TPQTabletConfig& config, const bool shouldHavePartitionsList, TString *error) {
    if (!config.HasPartitionConfig()) {
        if (error)
            *error = "no PartitionConfig";
        return false;
    }

    const auto& partitionIds = config.GetPartitionIds();
    const auto& partitions = config.GetPartitions();

    if (shouldHavePartitionsList) {
        if (partitionIds.empty() && partitions.empty()) {
            if (error)
                *error = "empty Partitions list";
            return false;
        }

        THashSet<ui32> parts;
        for (const auto partitionId : partitionIds) {
            if (!parts.insert(partitionId).second) {
                if (error)
                    *error = Sprintf("duplicate partitions with id %u", partitionId);
                return false;
            }
        }

        parts.clear();
        for (const auto& partition : partitions) {
            const auto partitionId = partition.GetPartitionId();
            if (!parts.insert(partitionId).second) {
                if (error)
                    *error = Sprintf("duplicate partitions with id %u", partitionId);
                return false;
            }
        }
    } else {
        if (!partitionIds.empty() || !partitions.empty()) {
            if (error)
                *error = "Partitions list must be empty";
            return false;
        }
    }

    return true;
}

} // NKikimr
