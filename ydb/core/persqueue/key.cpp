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

void TKeyPrefix::SetTypeImpl(EType type, bool isServicePartition)
{
    char c = type;

    if (isServicePartition) {
        switch (type) {
        case TypeNone:
            break;
        case TypeData:
            c = ServiceTypeData;
            break;
        case TypeTmpData:
            c = ServiceTypeTmpData;
            break;
        case TypeInfo:
            c = ServiceTypeInfo;
            break;
        case TypeMeta:
            c = ServiceTypeMeta;
            break;
        case TypeTxMeta:
            c = ServiceTypeTxMeta;
            break;
        default:
            Y_ABORT();
        }
    }

    *PtrType() = c;
}

bool TKeyPrefix::HasServiceType() const
{
    switch (*PtrType()) {
    case ServiceTypeInfo:
    case ServiceTypeData:
    case ServiceTypeTmpData:
    case ServiceTypeMeta:
    case ServiceTypeTxMeta:
        return true;
    default:
        return false;
    }
}

}
