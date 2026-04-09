#pragma once

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Host count in DirectBlockGroup.
constexpr size_t DirectBlockGroupHostCount = 5;

// Quorum host count in DirectBlockGroup.
constexpr size_t QuorumDirectBlockGroupHostCount = 3;

// Default BlockSize.
constexpr ui32 DefaultBlockSize = 4_KB;

// The maximum possible volume block size.
constexpr ui32 MaxBlockSize = 128_KB;

// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui32 MaxSubRequestSize = 4_MB;

// Default stripe size (in bytes)
constexpr ui64 DefaultStripeSize = 512_KB;

// Size of Region.
constexpr ui64 RegionSize = 4_GB;

// Default vchunk size.
constexpr ui64 DefaultVChunkSize = 128_MB;

//
constexpr ui64 CopyRangeSize = 1_MB;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
