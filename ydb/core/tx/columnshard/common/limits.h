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

    static constexpr double CompactionIndexationQueueLimitCoefficient = 0.125;
    static constexpr double CompactionTtlQueueLimitCoefficient = 0.125;
    static constexpr double CompactionGeneralQueueLimitCoefficient = 0.375;
    static constexpr double CompactionNormalizerQueueLimitCoefficient = 0.375;
};
}