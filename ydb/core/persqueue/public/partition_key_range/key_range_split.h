#pragma once

#include "partition_key_range.h"

#include <ydb/library/kll_median/dynamic_sketch.h>

#include <util/datetime/base.h>
#include <util/generic/array_ref.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <deque>
#include <random>

namespace NKikimr::NPQ {

struct TKeyRangeSource {
    TString SourceId;
    TPartitionKeyRange KeyRange;
};

struct TKeyRangeKeyStats {
    TSerializedCellVec Key;
    ui64 WrittenBytes = 0;
};

struct TKeyRangeWriteStats {
    TVector<TKeyRangeSource> Sources;
    TVector<TKeyRangeKeyStats> Keys;
};

struct TPlannedPartition {
    TPartitionKeyRange Range;
    ui64 EstimatedBytes = 0;
    TVector<TKeyRangeSource> Sources;
};

TVector<TPlannedPartition> PlanKeyRangeSplit(
    const TPartitionKeyRange& parent,
    TConstArrayRef<NScheme::TTypeInfo> keyTypes,
    const TKeyRangeWriteStats& stats);

class TKeyRangeKeysCollector {
public:
    TKeyRangeKeysCollector(TVector<NScheme::TTypeInfo> keyTypes, size_t numSketches, TDuration windowSize);

    TKeyRangeKeysCollector(const TKeyRangeKeysCollector&) = delete;
    TKeyRangeKeysCollector& operator=(const TKeyRangeKeysCollector&) = delete;

    bool Add(const TString& key, ui64 bytes, TInstant now);
    TVector<TKeyRangeKeyStats> GetKeys(TInstant now);

private:
    struct TKey {
        TSerializedCellVec Cells;
        TConstArrayRef<NScheme::TTypeInfo> KeyTypes;

        friend bool operator<(const TKey& lhs, const TKey& rhs) {
            return CompareTypedCellVectors(lhs.Cells.GetCells().data(), rhs.Cells.GetCells().data(),
                lhs.KeyTypes.data(), lhs.KeyTypes.size()) < 0;
        }
    };

    struct TSketch {
        NKll::TDynamicKllSketch<TKey> Sketch;
        TInstant StartTime;
    };

    void RemoveOldSketches(TInstant now);
    void EnsureSketch(TInstant now);

    static constexpr size_t SketchLevelSize = 100;
    static constexpr size_t MinWeight = 16;
    static constexpr size_t MaxKeySize = 1_KB;

    const TVector<NScheme::TTypeInfo> KeyTypes;
    const TDuration WindowSize;
    TDuration SketchWindowSize;
    std::deque<TSketch> Sketches;
    std::mt19937_64 Rng;
};

} // namespace NKikimr::NPQ
