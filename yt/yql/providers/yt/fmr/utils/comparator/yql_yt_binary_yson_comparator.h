
#pragma once

#include <util/generic/yexception.h>
#include <util/generic/vector.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>

namespace NYql::NFmr {


class TBinaryYsonComparator {
public:
    TBinaryYsonComparator(
        TStringBuf blobData,
        TVector<ESortOrder> sortOrders
    )
        : BlobData_(blobData)
        , SortOrders_(std::move(sortOrders))
    {}

    int CompareYsonValues(
        TColumnOffsetRange lhsOffsetRange,
        TColumnOffsetRange rhsOffsetRange
    ) const;

    int CompareRows(
        const TRowIndexMarkup& lhsRow,
        const TRowIndexMarkup& rhsRow
    ) const;

private:
    TStringBuf BlobData_;
    TVector<ESortOrder> SortOrders_;
};

} // namespace NYql::NFmr
