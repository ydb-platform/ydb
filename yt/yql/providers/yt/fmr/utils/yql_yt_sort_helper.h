
#pragma once


#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_comparator.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

namespace NYql::NFmr {

struct TRowPosition {
    ui64 BlockIndex;
    ui64 RowIndex;
};

using TSortedRowOrdering = std::vector<TRowPosition>;

class TSortHelper {
public:
    TSortHelper(const std::vector<TIndexedBlock>& blocks, const std::vector<ESortOrder>& sortOrders);

    TSortedRowOrdering GetSortedRowOrdering() const;

private:
    const std::vector<TIndexedBlock>& Blocks_;
    const std::vector<ESortOrder> SortOrders_;
};

} // namespace NYql::NFmr
