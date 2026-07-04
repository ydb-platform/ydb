#pragma once

#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>

namespace NYql::NFmr {

// Hash a single binary YSON value using THash<> specializations.
ui64 HashYsonValue(TStringBuf ysonData);

// Hash the first numKeyColumns key columns of a row and combine using CombineHashes.
// rowMarkup must have at least numKeyColumns + 1 entries (last entry is the row boundary).
// Absent columns (IsValid() == false) contribute hash 0.
ui64 HashKeyColumns(TStringBuf blobData, const TRowIndexMarkup& rowMarkup, size_t numKeyColumns);

} // namespace NYql::NFmr
