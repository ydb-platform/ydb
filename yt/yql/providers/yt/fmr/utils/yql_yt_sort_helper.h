
#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_binary_yson_comparator.h>

namespace NYql::NFmr {

using TPermutation = TVector<ui64>;

class TSortHelper {
public:
TSortHelper(
        TStringBuf blobData,
        TVector<ESortOrder> sortOrders
    )
        : BlobData_(blobData)
        , SortOrders_(std::move(sortOrders))
    {}

    TPermutation BuildPermutation(const TVector<TRowIndexMarkup>& rows) const;

private:
    TStringBuf BlobData_;
    TVector<ESortOrder> SortOrders_;
};

} // namespace NYql::NFmr
