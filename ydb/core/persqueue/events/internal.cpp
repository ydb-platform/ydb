#include "internal.h"

namespace NKikimr::NPQ {

    TRequestedBlob::TRequestedBlob(ui64 offset, ui16 partNo, ui32 count, ui16 internalPartsCount, ui32 size, TString value, const TKey& key, ui64 creationUnixTime)
        : Offset(offset)
        , PartNo(partNo)
        , Count(count)
        , InternalPartsCount(internalPartsCount)
        , Size(size)
        , Value(std::move(value))
        , Cached(false)
        , Key(key)
        , CreationUnixTime(creationUnixTime)
    {
    }
}
