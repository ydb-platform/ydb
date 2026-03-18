#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/location.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

constexpr ui8 InvalidHostIndex = 0xFF;

// DirectBlockGroup consists of 5 hosts. Each VChunk of DirectBlockGroup
// is configured to use 3 hosts for the primary storage location and 2 for
// hand-offs. For this reason, the VChunk configuration contains 5 node indexes
// from 0 to 4, the indexes cannot be repeated.
struct TVChunkConfig
{
    ui32 VChunkIndex = 0;
    ui8 PrimaryHost0 = InvalidHostIndex;
    ui8 PrimaryHost1 = InvalidHostIndex;
    ui8 PrimaryHost2 = InvalidHostIndex;
    ui8 HandOffHost0 = InvalidHostIndex;
    ui8 HandOffHost1 = InvalidHostIndex;

    static TVChunkConfig Make(ui32 vChunkIndex);

    [[nodiscard]] bool IsValid() const;

    [[nodiscard]] ui8 GetHostIndex(ELocation location) const;
    [[nodiscard]] THashMap<ui8, ELocation> GetPBuffersMap() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
