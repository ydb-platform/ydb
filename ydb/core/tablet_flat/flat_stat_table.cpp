#include "flat_part_laid.h"
#include "flat_stat_table.h"
#include "flat_table_subset.h"
#include "flat_stat_table_btree_index.h"
#include "flat_stat_table_mixed_index.h"

namespace NKikimr {
namespace NTable {

bool BuildStats(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, ui32 histogramBucketsCount, IPages* env, TBuildStatsYieldHandler yieldHandler) {
    stats.Clear();

    bool mixedIndex = false;
    for (const auto& part : subset.Flatten) {
        if (!part->IndexPages.HasBTree() && part->IndexPages.HasFlat()) {
            mixedIndex = true;
        }
    }

    return mixedIndex
        ? BuildStatsMixedIndex(subset, stats, rowCountResolution, dataSizeResolution, env, yieldHandler)
        : BuildStatsBTreeIndex(subset, stats, histogramBucketsCount, env, yieldHandler);
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
