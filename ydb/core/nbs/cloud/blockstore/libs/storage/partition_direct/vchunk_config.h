#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/host/host_status.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TVChunkConfig
{
    // Default number of Primary hosts at config init time. Not a hard cap —
    // the runtime may grow this by promoting HandOff hosts to Primary.
    static constexpr size_t DefaultPrimaryCount = 3;

    ui32 VChunkIndex = 0;
    THostStatusList PBufferHosts;
    THostStatusList DDiskHosts;

    static TVChunkConfig Make(
        ui32 vChunkIndex,
        size_t hostCount = DirectBlockGroupHostCount,
        size_t primaryCount = DefaultPrimaryCount);

    [[nodiscard]] bool IsValid() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
