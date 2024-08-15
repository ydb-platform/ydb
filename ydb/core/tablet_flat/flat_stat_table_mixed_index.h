#pragma once

#include "flat_part_laid.h"
#include "flat_stat_table.h"
#include "flat_stat_part.h"
#include "flat_table_subset.h"

namespace NKikimr {
namespace NTable {

inline bool BuildStatsMixedIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    stats.Clear();

    TDataStats iteratorStats = { };
    TStatsIterator statsIterator(subset.Scheme->Keys);

    // Note: B-Tree index uses resolution to skip huge nodes
    // doesn't work well with intersecting SSTs
    const ui32 resolutionDivider = 5;

    // Make index iterators for all parts
    bool started = true;
    for (const auto& part : subset.Flatten) {
        stats.IndexSize.Add(part->IndexesRawSize, part->Label.Channel());
        stats.ByKeyFilterSize += part->ByKey ? part->ByKey->Raw.size() : 0;
        TAutoPtr<TStatsScreenedPartIterator> iter = new TStatsScreenedPartIterator(part, env, subset.Scheme->Keys, part->Small, part->Large, 
            rowCountResolution / resolutionDivider, dataSizeResolution / resolutionDivider);
        auto ready = iter->Start();
        if (ready == EReady::Page) {
            started = false;
        } else if (ready == EReady::Data) {
            statsIterator.Add(iter);
        }
    }
    if (!started) {
        return false;
    }

    ui64 prevRows = 0;
    ui64 prevSize = 0;
    while (true) {
        yieldHandler();

        auto ready = statsIterator.Next(iteratorStats);
        if (ready == EReady::Page) {
            return false;
        } else if (ready == EReady::Gone) {
            break;
        }

        const bool nextRowsBucket = (iteratorStats.RowCount >= prevRows + rowCountResolution);
        const bool nextSizeBucket = (iteratorStats.DataSize.Size >= prevSize + dataSizeResolution);

        if (!nextRowsBucket && !nextSizeBucket)
            continue;

        TDbTupleRef currentKey = statsIterator.GetCurrentKey();
        TString serializedKey = TSerializedCellVec::Serialize(TConstArrayRef<TCell>(currentKey.Columns, currentKey.ColumnCount));

        if (nextRowsBucket) {
            prevRows = iteratorStats.RowCount;
            stats.RowCountHistogram.push_back({serializedKey, prevRows});
        }

        if (nextSizeBucket) {
            prevSize = iteratorStats.DataSize.Size;
            stats.DataSizeHistogram.push_back({serializedKey, prevSize});
        }
    }

    stats.RowCount = iteratorStats.RowCount;
    stats.DataSize = std::move(iteratorStats.DataSize);

    return true;
}

}}
