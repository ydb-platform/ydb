#pragma once

#include <util/generic/bitops.h>
#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Host count in DirectBlockGroup.
constexpr size_t DirectBlockGroupHostCount = 5;

// Quorum host count in DirectBlockGroup.
constexpr size_t QuorumDirectBlockGroupHostCount = 3;

// Default number of Primary hosts at config init time. Not a hard cap —
// the runtime may grow this by promoting HandOff hosts to Primary.
constexpr size_t DefaultPrimaryCount = 3;

// Default BlockSize.
constexpr ui32 DefaultBlockSize = 4_KB;

// The maximum possible volume block size.
constexpr ui32 MaxBlockSize = 128_KB;

// A volume block size the partition can serve: a power of two between the
// DDisk integrity unit (DefaultBlockSize) and MaxBlockSize inclusive.
constexpr bool IsSupportedBlockSize(ui32 blockSize)
{
    return blockSize >= DefaultBlockSize && blockSize <= MaxBlockSize &&
           IsPowerOf2(blockSize);
}

// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui32 MaxSubRequestSize = 4_MB;

// Size of Region.
constexpr ui64 RegionSize = 4_GB;

// Default count of DirectBlockGroups for volume.
constexpr size_t DirectBlockGroupsCount = 32;

// The size of the data copied at a time.
constexpr ui64 CopyRangeSize = 1_MB;

// The amount of copied data between copy progress notifications.
constexpr ui64 CopyProgressSaveInterval = 8_MB;

// Max allowed VChunk size.
constexpr ui64 MaxVChunkSize = RegionSize / DirectBlockGroupsCount;
// Max allowed VChunk block count (when block size is minimum).
constexpr ui64 MaxVChunkBlockCount = MaxVChunkSize / DefaultBlockSize;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
