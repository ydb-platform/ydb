#include "key.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ {

std::pair<TKeyPrefix, TKeyPrefix> MakeKeyPrefixRange(TKeyPrefix::EType type, const TPartitionId& partition)
{
    TKeyPrefix from(type, partition);
    TKeyPrefix to(type, TPartitionId(partition.OriginalPartitionId, partition.WriteId, partition.InternalPartitionId + 1));

    return {std::move(from), std::move(to)};
}

TKey TKey::FromString(const TString& s, const TPartitionId& partition)
{
    TKey t(s);
    return TKey(t.GetType(),
                partition,
                t.GetOffset(),
                t.GetPartNo(),
                t.GetCount(),
                t.GetInternalPartsCount(),
                t.GetSuffix(),
                t.GetOffsetDelta());
}

TKey TKey::ForBody(EType type,
                   const TPartitionId& partition,
                   const ui64 offset,
                   const ui16 partNo,
                   const ui32 count,
                   const ui16 internalPartsCount,
                   const TMaybe<ui64>& offsetDelta)
{
    return {type, partition, offset, partNo, count, internalPartsCount, Nothing(), offsetDelta};
}

TKey TKey::ForHead(EType type,
                   const TPartitionId& partition,
                   const ui64 offset,
                   const ui16 partNo,
                   const ui32 count,
                   const ui16 internalPartsCount,
                   const TMaybe<ui64>& offsetDelta)
{
    return {type, partition, offset, partNo, count, internalPartsCount, ESuffix::Head, offsetDelta};
}

TKey TKey::ForFastWrite(EType type,
                        const TPartitionId& partition,
                        const ui64 offset,
                        const ui16 partNo,
                        const ui32 count,
                        const ui16 internalPartsCount,
                        const TMaybe<ui64>& offsetDelta)
{
    return {type, partition, offset, partNo, count, internalPartsCount, ESuffix::FastWrite, offsetDelta};
}

void TKey::SetOffsetDelta(const TMaybe<ui64>& offsetDelta)
{
    EnsureValidBodySize();
    if (offsetDelta.Defined()) {
        // Preserve the existing on-disk range and 10-digit representation.
        AFL_ENSURE(*offsetDelta <= Max<ui32>())("offsetDelta", *offsetDelta);
    }
    OffsetDelta = offsetDelta;
    const TMaybe<char> suffix = GetSuffix();
    const ui32 bodySize = offsetDelta.Defined() ? KeySizeWithOffsetDelta() : KeySize();
    Resize(bodySize + suffix.Defined());
    if (offsetDelta.Defined()) {
        Data()[KeySize()] = '_';
        memcpy(PtrOffsetDelta(), Sprintf("%.10" PRIu64, *offsetDelta).data(), 10);
    }
    if (suffix.Defined()) {
        Data()[bodySize] = *suffix;
    }
}

void TKey::SetOffsetDelta(ui64 offsetDelta)
{
    SetOffsetDelta(TMaybe<ui64>(offsetDelta));
}

bool TKey::IsFastWrite() const
{
    return GetSuffix() == ESuffix::FastWrite;
}

void TKey::SetFastWrite()
{
    SetSuffix(ESuffix::FastWrite);
}

void TKey::SetBody()
{
    SetSuffix(Nothing());
}

TKey TKey::FromKey(const TKey& k,
                   EType type,
                   const TPartitionId& partition,
                   ui64 offset)
{
    return {type, partition, offset, k.GetPartNo(), k.GetCount(), k.GetInternalPartsCount(), k.GetSuffix(), k.GetOffsetDelta()};
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
            AFL_ENSURE(false)("type", static_cast<int>(type))("type_char", c);
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
