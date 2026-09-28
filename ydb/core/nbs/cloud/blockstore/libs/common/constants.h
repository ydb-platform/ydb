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

// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui32 MaxSubRequestSize = 4_MB;

// Classic gRPC payload limit, excluding protobuf overhead.
// GRpcConfig.MaxMessageSize and client limits must allow the serialized
// request/response; smaller transport limits can fail with RESOURCE_EXHAUSTED
// instead of an NBS error.
constexpr ui64 MaxGrpcIoBytes = 32_MB;

// VChunks in a region count.
constexpr size_t VChunkPerRegionCount = 32;

// Default number of DirectBlockGroups in volume.
constexpr size_t DefaultVolumeDirectBlockGroupCount = 32;

// The size of the data copied at a time.
constexpr ui64 CopyRangeSize = 1_MB;

// The amount of copied data between copy progress notifications.
constexpr ui64 CopyProgressSaveInterval = 8_MB;

// Max allowed VChunk size.
constexpr ui64 MaxVChunkSize = 128_MB;
// Max allowed VChunk block count (when block size is minimum).
constexpr ui64 MaxVChunkBlockCount = MaxVChunkSize / DefaultBlockSize;

// Max allowed Volume size.
constexpr ui64 MaxVolumeSize = 256_TB;
constexpr ui64 MaxVChunkCount = MaxVolumeSize / MaxVChunkSize;
constexpr ui64 MaxBlockCount = MaxVolumeSize / MaxBlockSize;

////////////////////////////////////////////////////////////////////////////////

// A volume block size the partition can serve: a power of two between the
// DDisk integrity unit (DefaultBlockSize) and MaxBlockSize inclusive.
constexpr bool IsSupportedBlockSize(ui32 blockSize)
{
    return blockSize >= DefaultBlockSize && blockSize <= MaxBlockSize &&
           IsPowerOf2(blockSize);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
