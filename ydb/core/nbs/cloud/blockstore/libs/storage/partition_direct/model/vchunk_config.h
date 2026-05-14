#pragma once

#include "host_status.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

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
        size_t primaryCount = DefaultPrimaryCount);

    [[nodiscard]] bool IsValid() const;

    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
