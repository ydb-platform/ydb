#pragma once

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The maximum possible volume block size.
constexpr ui32 MaxBlockSize = 128_KB;

// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui32 MaxSubRequestSize = 4_MB;

// Default stripe size (in bytes)
constexpr ui64 DefaultStripeSize = 512_KB;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
