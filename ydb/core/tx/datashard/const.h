#pragma once

#include "defs.h"

namespace NKikimr {
namespace NDataShard {

constexpr ui64 INVALID_TABLET_ID = Max<ui64>();
constexpr ui64 MEMORY_REQUEST_FACTOR = 8;

// TODO: make configurable
constexpr ui64 MAX_REORDER_TX_KEYS = 1000;

namespace NLimits {
    static constexpr ui64 MaxWriteKeySize = 1024 * 1024 + 1024; // 1MB + small delta (for old ugc tests)
    static constexpr ui64 MaxWriteValueSize = 16 * 1024 * 1024; // 16MB
}

} // namespace NDataShard
} // namespace NKikimr
