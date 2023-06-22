#include "flat_part_laid.h"
#include "flat_stat_table.h"
#include "flat_stat_part.h"
#include "flat_table_subset.h"

namespace NKikimr {
namespace NTable {

void BuildStats(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, const IPages* env) {
    Y_UNUSED(env);

    stats.Clear();

    TPartDataStats stIterStats = { };
    TStatsIterator stIter(subset.Scheme->Keys);

    // Make index iterators for all parts
    for (auto& pi : subset.Flatten) {
        stats.IndexSize.Add(pi->IndexesRawSize, pi->Label.Channel());
        TAutoPtr<TScreenedPartIndexIterator> iter = new TScreenedPartIndexIterator(pi, subset.Scheme->Keys, pi->Small);
        if (iter->IsValid()) {
            stIter.Add(iter);
        }
    }

    ui64 prevRows = 0;
    ui64 prevSize = 0;
    while (stIter.Next(stIterStats)) {
        const bool nextRowsBucket = (stIterStats.RowCount >= prevRows + rowCountResolution);
        const bool nextSizeBucket = (stIterStats.DataSize.Size >= prevSize + dataSizeResolution);

        if (!nextRowsBucket && !nextSizeBucket)
            continue;

        TDbTupleRef currentKey = stIter.GetCurrentKey();
        TString serializedKey = TSerializedCellVec::Serialize(TConstArrayRef<TCell>(currentKey.Columns, currentKey.ColumnCount));

        if (nextRowsBucket) {
            prevRows = stIterStats.RowCount;
            stats.RowCountHistogram.push_back({serializedKey, prevRows});
        }

        if (nextSizeBucket) {
            prevSize = stIterStats.DataSize.Size;
            stats.DataSizeHistogram.push_back({serializedKey, prevSize});
        }
    }

    stats.RowCount = stIterStats.RowCount;
    stats.DataSize = std::move(stIterStats.DataSize);
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
