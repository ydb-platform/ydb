#pragma once

#include <util/system/types.h>

namespace NYql::NYdbRemote {

// OutputChunkMaxSize is a server packing target, not an upper bound on an
// Arrow response. A part can contain a large row or serialization overhead.
inline constexpr ui64 MaxInboundMessageBytes = 64 * 1024 * 1024;
inline constexpr ui64 MaxDecodedPartBytes = 64 * 1024 * 1024;
inline constexpr ui64 MaxOutputRowBytes = 32 * 1024 * 1024;

// These are acceptance limits, not reservations or a bound on peak RSS: the
// baseline SDK parses/decompresses before validation, and an input part, its
// compact output block and the DQ allocator copy can coexist.

} // namespace NYql::NYdbRemote
