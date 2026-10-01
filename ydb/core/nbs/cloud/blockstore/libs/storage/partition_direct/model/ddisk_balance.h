#pragma once

#include "vchunk_config.h"

#include <util/generic/vector.h>

#include <array>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Selects which VChunks participate in DDisk balancing.
enum class EDDiskBalanceStrategy
{
    Touched,      // Balance DDisks of touched VChunks only.
    Configured,   // Balance DDisks of all configured VChunks.
};

////////////////////////////////////////////////////////////////////////////////

// Minimum DDisk moves needed for an even host distribution and their share.
struct TDDiskImbalance
{
    size_t Moves = 0;
    size_t TotalDDiskCount = 0;
    ui32 Percent = 0;
};

////////////////////////////////////////////////////////////////////////////////

// A request to move one VChunk DDisk to the target host.
struct TDDiskBalanceRequest
{
    ui32 VChunkId = 0;
    THostIndex SourceHost = InvalidHostIndex;
    THostIndex TargetHost = InvalidHostIndex;
};

////////////////////////////////////////////////////////////////////////////////

// Calculates imbalance across allowed hosts using current DDisk counts.
[[nodiscard]] TDDiskImbalance CalculateDDiskImbalance(
    const std::array<size_t, MaxHostCount>& ddiskCountByHost,
    THostMask allowedForBalancing);

// Plans DDisk moves for eligible VChunks. Counts may include other VChunks.
[[nodiscard]] TVector<TDDiskBalanceRequest> PlanDDiskBalance(
    const TVector<const TVChunkConfig*>& vChunks,
    THostMask allowedForBalancing,
    const std::array<size_t, MaxHostCount>& ddiskCountByHost);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
