#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/host_status.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TVChunkConfig
{
    ui32 VChunkIndex = 0;
    THostStatusList PBufferHosts;
    THostStatusList DDiskHosts;

    static TVChunkConfig Make(
        ui32 vChunkIndex,
        size_t hostCount = DirectBlockGroupHostCount,
        size_t primaryCount = DirectBlockGroupPrimaryCount);

    [[nodiscard]] bool IsValid() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
