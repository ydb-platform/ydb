#include "util.h"

namespace NKikimr::NChangeExchange {

TVector<ui64> MakePartitionIds(const TVector<TKeyDesc::TPartitionInfo>& partitions) {
    TVector<ui64> result(::Reserve(partitions.size()));

    for (const auto& partition : partitions) {
        result.push_back(partition.ShardId);
    }

    return result;
}

}
