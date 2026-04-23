#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

struct TLastLevelSettings {
    struct TLimit {
        ui64 Portions;
        ui64 Bytes;
    };

    TLimit Compaction{1'000, 64ULL * 1024 * 1024};
    ui64 CandidatePortionsOverload = 10;
};

struct TAccumulatorSettings {
    struct TLimit {
        ui64 Portions;
        ui64 Bytes;
    };

    TLimit Compaction{1'000, 64ULL * 1024 * 1024};
    TLimit Trigger{1'000, 2ULL * 1024 * 1024};
    TLimit Overload{10'000, 256ULL * 1024 * 1024};
};

struct TMiddleLevelSettings {
    ui64 TriggerHight = 10;
    ui64 OverloadHight = 15;
};

struct TTilingSettings {
    TAccumulatorSettings AccumulatorSettings;
    TLastLevelSettings LastLevelSettings;
    TMiddleLevelSettings MiddleLevelSettings;
    ui64 AccumulatorPortionSizeLimit = 512ULL * 1024;
    ui8 K = 10;
    /// Exclusive upper bound on middle-level index (allowed middle indices: 2 .. MiddleLevelCount - 1).
    ui64 MiddleLevelCount = TILING_LAYERS_COUNT;
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
