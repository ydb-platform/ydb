#pragma once

#include "host_roles.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TVChunkConfig
{
    ui32 VChunkIndex = 0;
    THostRoles PBufferHosts;
    THostRoles DDiskHosts;
    THostMask EnabledHosts;

    static TVChunkConfig Make(
        ui32 vChunkIndex,
        size_t hostCount = DirectBlockGroupHostCount,
        size_t primaryCount = DefaultPrimaryCount);

    void EnableHost(THostIndex hostIndex);
    void DisableHost(THostIndex hostIndex);

    [[nodiscard]] THostMask GetDesiredPBuffers() const;
    [[nodiscard]] THostMask GetDesiredDDisks() const;
    [[nodiscard]] THostMask GetDisabledHosts() const;

    [[nodiscard]] bool IsValid() const;

    [[nodiscard]] TString DebugPrint() const;
};

////////////////////////////////////////////////////////////////////////////////

// Vchunk index -> persisted config override. Vchunks without an entry fall
// back to TVChunkConfig::Make().
using TVChunkConfigByIndex = THashMap<ui32, TVChunkConfig>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
