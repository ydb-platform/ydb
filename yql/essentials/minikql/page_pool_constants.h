#pragma once

#include <util/system/types.h>

namespace NKikimr {

inline constexpr ui64 PoolPageSize = 1ULL << 16;
inline constexpr ui32 MidLevels = 10;
inline constexpr ui32 MaxMidSize = (1U << MidLevels) * PoolPageSize;
static_assert(MaxMidSize == 64 * 1024 * 1024, "Upper memory block 64 Mb");

} // namespace NKikimr
