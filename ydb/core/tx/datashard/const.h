#pragma once

#include "defs.h"

#include <util/string/cast.h>
#include <util/system/env.h>

namespace NKikimr {
namespace NDataShard {

constexpr ui64 INVALID_TABLET_ID = Max<ui64>();
constexpr ui64 MEMORY_REQUEST_FACTOR = 8;

// TODO: make configurable
constexpr ui64 MAX_REORDER_TX_KEYS = 1000;

namespace NLimits {
    static constexpr ui64 MaxWriteKeySize = 1024 * 1024 + 1024; // 1MB + small delta (for old ugc tests)

    // Runtime-tunable via YDB_TEST_DATASHARD_MAX_WRITE_VALUE_SIZE env var; default 16 MiB.
    // TODO(YDBAPPTEAM-773): revert this override after the ticket is closed.
    inline ui64 MaxWriteValueSize() {
        static const ui64 value = []() -> ui64 {
            ui64 v = 0;
            return TryFromString<ui64>(GetEnv("YDB_TEST_DATASHARD_MAX_WRITE_VALUE_SIZE"), v) ? v : (16ULL * 1024 * 1024);
        }();
        return value;
    }
}

} // namespace NDataShard
} // namespace NKikimr
