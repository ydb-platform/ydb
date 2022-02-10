#include "flat_part_laid.h"
#include "flat_stat_table.h"
#include "flat_stat_part.h"
#include "flat_table_subset.h"

namespace NKikimr {
namespace NTable {

void BuildStats(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, const IPages* env) {
    Y_UNUSED(env);

    stats.Clear();

    TStatsIterator stIter(subset.Scheme->Keys);

    // Make index iterators for all parts
    for (auto& pi : subset.Flatten) {
        TAutoPtr<TScreenedPartIndexIterator> iter = new TScreenedPartIndexIterator(pi, subset.Scheme->Keys, pi->Small);
        if (iter->IsValid()) {
            stIter.Add(iter);
        }
    }

    ui64 prevRows = 0;
    ui64 prevSize = 0;
    for (; stIter.IsValid(); stIter.Next()) {
        stats.RowCount = stIter.GetCurrentRowCount();
        stats.DataSize = stIter.GetCurrentDataSize();

        const bool nextRowsBucket = (stats.RowCount >= prevRows + rowCountResolution);
        const bool nextSizeBucket = (stats.DataSize >= prevSize + dataSizeResolution);

        if (!nextRowsBucket && !nextSizeBucket)
            continue;

        TDbTupleRef currentKey = stIter.GetCurrentKey();
        TString serializedKey = TSerializedCellVec::Serialize(TConstArrayRef<TCell>(currentKey.Columns, currentKey.ColumnCount));

        if (nextRowsBucket) {
            stats.RowCountHistogram.push_back({serializedKey, stats.RowCount});
            prevRows = stats.RowCount;
        }

        if (nextSizeBucket) {
            stats.DataSizeHistogram.push_back({serializedKey, stats.DataSize});
            prevSize = stats.DataSize;
        }
    }

    stats.RowCount = stIter.GetCurrentRowCount();
    stats.DataSize = stIter.GetCurrentDataSize();
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
