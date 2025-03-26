#include "flat_part_laid.h"
#include "flat_stat_table.h"
#include "flat_table_subset.h"
#include "flat_stat_table_btree_index.h"
#include "flat_stat_table_mixed_index.h"

namespace NKikimr {
namespace NTable {

bool BuildStats(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, ui32 histogramBucketsCount, IPages* env, 
    TBuildStatsYieldHandler yieldHandler, const TString& logPrefix)
{
    stats.Clear();

    bool mixedIndex = false;
    for (const auto& part : subset.Flatten) {
        if (!part->IndexPages.HasBTree() && part->IndexPages.HasFlat()) {
            mixedIndex = true;
        }
    }

    LOG_BUILD_STATS("starting for " << (mixedIndex ? "mixed" : "b-tree") << " index");

    auto ready = mixedIndex
        ? BuildStatsMixedIndex(subset, stats, rowCountResolution, dataSizeResolution, env, yieldHandler)
        : BuildStatsBTreeIndex(subset, stats, histogramBucketsCount, env, yieldHandler, logPrefix);

    LOG_BUILD_STATS("finished for " << (mixedIndex ? "mixed" : "b-tree") << " index"
        << " ready: " << ready
        << " stats: " << stats.ToString());

    return ready;
}

void GetPartOwners(const TSubset& subset, THashSet<ui64>& partOwners) {
    for (const auto& partView : subset.Flatten) {
        partOwners.insert(partView->Label.TabletID());
    }
    for (const auto& coldPart : subset.ColdParts) {
        partOwners.insert(coldPart->Label.TabletID());
    }
    for (const auto& txStatus : subset.TxStatus) {
        partOwners.insert(txStatus->Label.TabletID());
    }
}

}}
