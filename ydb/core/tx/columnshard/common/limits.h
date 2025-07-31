#pragma once
#include <util/system/types.h>
#include <limits>

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

    static_assert((CompactionIndexationQueueLimitCoefficient + CompactionTtlQueueLimitCoefficient +
                   CompactionGeneralQueueLimitCoefficient + CompactionNormalizerQueueLimitCoefficient +
                   - 1.0 < std::numeric_limits<double>::epsilon()) &&
                  (1.0 - (CompactionIndexationQueueLimitCoefficient + CompactionTtlQueueLimitCoefficient +
                   CompactionGeneralQueueLimitCoefficient + CompactionNormalizerQueueLimitCoefficient)
                   < std::numeric_limits<double>::epsilon()),
                  "Compaction coefficients sum must be equal to 1.0");

    static constexpr double GroupedMemoryLimiterSoftLimitCoefficient = 0.3;

    static constexpr double BlobCacheCoefficient = 0.16;
    static constexpr double DataAccessorCoefficient = 0.12;
    static constexpr double ColumnDataCacheCoefficient = 0.12;
    static constexpr double PortionsCoefficient = 0.6;

    static_assert((BlobCacheCoefficient + DataAccessorCoefficient + ColumnDataCacheCoefficient + PortionsCoefficient - 1.0 < std::numeric_limits<double>::epsilon()) &&
            (1.0 - (BlobCacheCoefficient + DataAccessorCoefficient + ColumnDataCacheCoefficient + PortionsCoefficient) < std::numeric_limits<double>::epsilon()),
        "Cache coefficients sum must be equal to 1.0");

    static constexpr double DeduplicationInScanMemoryFraction = 0.5;

    static constexpr ui64 AveragePortionSizeLimit = 2 << 10;
};
}