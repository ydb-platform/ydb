
#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/hash.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////
// static
TVChunkConfig TVChunkConfig::Make(ui32 vChunkIndex)
{
    auto rotate = [vChunkIndex](size_t index) -> ui8
    {
        return static_cast<ui8>(
            (index + vChunkIndex) % DirectBlockGroupHostCount);
    };
    TVChunkConfig result{
        .VChunkIndex = vChunkIndex,
        .PrimaryHost0 = rotate(0),
        .PrimaryHost1 = rotate(1),
        .PrimaryHost2 = rotate(2),
        .HandOffHost0 = rotate(3),
        .HandOffHost1 = rotate(4)};

    return result;
}

ui8 TVChunkConfig::GetHostIndex(ELocation location) const
{
    switch (location) {
        case ELocation::PBuffer0:
            return PrimaryHost0;
        case ELocation::PBuffer1:
            return PrimaryHost1;
        case ELocation::PBuffer2:
            return PrimaryHost2;
        case ELocation::HOPBuffer0:
            return HandOffHost0;
        case ELocation::HOPBuffer1:
            return HandOffHost1;
        case ELocation::DDisk0:
            return PrimaryHost0;
        case ELocation::DDisk1:
            return PrimaryHost1;
        case ELocation::DDisk2:
            return PrimaryHost2;
        case ELocation::HODDisk0:
            return HandOffHost0;
        case ELocation::HODDisk1:
            return HandOffHost1;
        case ELocation::Unknown:
            return InvalidHostIndex;
    }
}

ELocation TVChunkConfig::GetPBufferLocation(ui8 hostIndex) const
{
    if (hostIndex == PrimaryHost0) {
        return ELocation::PBuffer0;
    }

    if (hostIndex == PrimaryHost1) {
        return ELocation::PBuffer1;
    }

    if (hostIndex == PrimaryHost2) {
        return ELocation::PBuffer2;
    }

    if (hostIndex == HandOffHost0) {
        return ELocation::HOPBuffer0;
    }

    if (hostIndex == HandOffHost1) {
        return ELocation::HOPBuffer1;
    }

    return ELocation::Unknown;
}

THashMap<ui8, ELocation> TVChunkConfig::GetPBuffersMap() const
{
    THashMap<ui8, ELocation> result;
    result[PrimaryHost0] = ELocation::PBuffer0;
    result[PrimaryHost1] = ELocation::PBuffer1;
    result[PrimaryHost2] = ELocation::PBuffer2;
    result[HandOffHost0] = ELocation::HOPBuffer0;
    result[HandOffHost1] = ELocation::HOPBuffer1;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
