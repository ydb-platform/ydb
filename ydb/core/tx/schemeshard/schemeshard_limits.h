#pragma once

namespace NKikimr::NSchemeShard {

// Topic (PQ) related built-in limits
static constexpr ui32 DefaultPQTabletPartitionsCount = 1;
static constexpr ui32 MaxPQTabletPartitionsCount = 1000;
static constexpr ui32 MaxPQGroupTabletsCount = 10*1000;
static constexpr ui32 MaxPQGroupPartitionsCount = 20*1000;
static constexpr ui32 MaxPQWriteSpeedPerPartition = 50*1024*1024;
static constexpr ui32 MaxPQLifetimeSeconds = 31 * 86400;

} // namespace NKikimr::NSchemeShard
