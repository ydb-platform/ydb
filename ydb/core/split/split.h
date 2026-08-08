#pragma once

#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_stat_table.h>


namespace NKikimr::NSplitMerge {

using TKeyTypes = TConstArrayRef<NScheme::TTypeInfo>;

using TKeyAccessHistogram = TVector<std::pair<TSerializedCellVec, ui64>>;

// Converts a sample with unsorted, repeated keys with unit (in practice) weights into
// a histogram: sorted and deduplicated keys with accumulated weights.
void MakeKeyAccessHistogram(TKeyAccessHistogram &keysHist, const TKeyTypes& keyColumnTypes);
// Converts a histogram into a cumulative histogram.
void ConvertToCumulativeHistogram(TKeyAccessHistogram &keysHist);

// Split by load: select split boundary (key prefix) from key access histogram
TSerializedCellVec SelectShortestMedianKeyPrefix(const TKeyAccessHistogram& keyHist, const TKeyTypes& keyColumnTypes);

// Split by size: select split boundary (key prefix) from data size histogram
TSerializedCellVec SelectShortestMedianKeyPrefix(const NKikimr::NTable::THistogram& histogram, ui64 totalSize, const TKeyTypes& keyColumnTypes);

}  // namespace NKikimr::NSplitMerge
