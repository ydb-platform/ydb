#include "key.h"

namespace NKikimr::NPQ {

std::pair<TKeyPrefix, TKeyPrefix> MakeKeyPrefixRange(TKeyPrefix::EType type, const TPartitionId& partition)
{
    TKeyPrefix from(type, partition);
    TKeyPrefix to(type, TPartitionId(partition.OriginalPartitionId, partition.WriteId, partition.InternalPartitionId + 1));

    return {std::move(from), std::move(to)};
}

}
