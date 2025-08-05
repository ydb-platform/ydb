#pragma once
#include <util/system/types.h>

namespace NKikimr::NOlap {
class TGlobalLimits {
public:
    static constexpr inline ui64 TxWriteLimitBytes = 256 * 1024 * 1024;
    static constexpr inline ui64 TTLCompactionMemoryLimit = 1ULL << 30;
    static constexpr inline ui64 InsertCompactionMemoryLimit = 1ULL << 30;
    static constexpr inline ui64 GeneralCompactionMemoryLimit = 3ULL << 30;
    static constexpr inline ui64 ScanMemoryLimit = 3ULL << 30;

    static constexpr inline ui64 DefaultReadSequentiallyBufferSize = ((ui64)8) << 20;

    static constexpr double GroupedMemoryLimiterSoftLimitCoefficient = 0.3;

    static constexpr ui64 AveragePortionSizeLimit = 2 << 10;
};
}
