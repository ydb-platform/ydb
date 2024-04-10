#include "flat_part_laid.h"
#include "flat_stat_table.h"
#include "flat_stat_part.h"
#include "flat_table_subset.h"

namespace NKikimr {
namespace NTable {

bool BuildStats(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env) {
    stats.Clear();

    TDataStats iteratorStats = { };
    TStatsIterator statsIterator(subset.Scheme->Keys);

    // TODO: deal with resolution

    // Make index iterators for all parts
    bool started = true;
    for (const auto& part : subset.Flatten) {
        stats.IndexSize.Add(part->IndexesRawSize, part->Label.Channel());
        TAutoPtr<TStatsScreenedPartIterator> iter = new TStatsScreenedPartIterator(part, env, subset.Scheme->Keys, part->Small, part->Large, 
            rowCountResolution, dataSizeResolution);
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

void GetPartOwners(const TSubset& subset, THashSet<ui64>& partOwners) {
    for (auto& pi : subset.Flatten) {
        partOwners.insert(pi->Label.TabletID());
    }
    for (auto& pi : subset.ColdParts) {
        partOwners.insert(pi->Label.TabletID());
    }
    for (auto& pi : subset.TxStatus) {
        partOwners.insert(pi->Label.TabletID());
    }
}

}}
