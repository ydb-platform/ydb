#pragma once

#include <util/system/types.h>

namespace NKikimr::NSharedCache {

static constexpr ui32 CacheTierBits = 2;

enum class ECacheTier : ui32 {
    Regular = 0,
    TryKeepInMemory = 1,
};

} // namespace NKikimr::NSharedCache
