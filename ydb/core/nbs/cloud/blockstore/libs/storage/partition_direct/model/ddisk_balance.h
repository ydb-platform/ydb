#pragma once

#include "vchunk_config.h"

#include <util/generic/vector.h>

#include <array>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// A request to move one VChunk DDisk to the target host.
struct TDDiskBalanceRequest
{
    ui32 VChunkId = 0;
    THostIndex SourceHost = InvalidHostIndex;
    THostIndex TargetHost = InvalidHostIndex;
};

////////////////////////////////////////////////////////////////////////////////

// Plans DDisk moves for eligible VChunks. Counts may include other VChunks.
[[nodiscard]] TVector<TDDiskBalanceRequest> PlanDDiskBalance(
    const TVector<const TVChunkConfig*>& vChunks,
    THostMask allowedForBalancing,
    const std::array<size_t, MaxHostCount>& ddiskCountByHost);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
