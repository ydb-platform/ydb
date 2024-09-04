#pragma once

#include "flat_stat_table.h"
#include "flat_table_subset.h"

namespace NKikimr::NTable {

bool BuildStatsBTreeIndex(const TSubset& subset, TStats& stats, ui32 histogramBucketsCount, IPages* env, TBuildStatsYieldHandler yieldHandler);

bool BuildStatsHistogramsBTreeIndex(const TSubset& subset, TStats& stats, ui64 rowCountResolution, ui64 dataSizeResolution, IPages* env, TBuildStatsYieldHandler yieldHandler);


}
