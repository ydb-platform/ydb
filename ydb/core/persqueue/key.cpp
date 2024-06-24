#include "key.h"

namespace NKikimr::NPQ {

std::pair<TKeyPrefix, TKeyPrefix> MakeKeyPrefixRange(TKeyPrefix::EType type, const TPartitionId& partition)
{
    TKeyPrefix from(type, partition);
    TKeyPrefix to(type, TPartitionId(partition.OriginalPartitionId, partition.WriteId, partition.InternalPartitionId + 1));

    return {std::move(from), std::move(to)};
}

TKey MakeKeyFromString(const TString& s, const TPartitionId& partition)
{
    TKey t(s);
    return TKey(t.GetType(),
                partition,
                t.GetOffset(),
                t.GetPartNo(),
                t.GetCount(),
                t.GetInternalPartsCount(),
                t.IsHead());
}

}
