#include "yql_yt_sort_helper.h"
#include <util/generic/algorithm.h>
#include <algorithm>

namespace NYql::NFmr {

TSortHelper::TSortHelper(
    const std::vector<TIndexedBlock>& blocks,
    const std::vector<ESortOrder>& sortOrders
)
    : Blocks_(blocks)
    , SortOrders_(sortOrders)
{
}

TSortedRowOrdering TSortHelper::GetSortedRowOrdering() const {

    std::vector<TRowPosition> rowPositions;
    for (ui64 blockIndex = 0; blockIndex < Blocks_.size(); ++blockIndex) {
        for (ui64 rowIndex = 0; rowIndex < Blocks_[blockIndex].Rows.size(); ++rowIndex) {
            rowPositions.emplace_back(TRowPosition(blockIndex, rowIndex));
        }
    }

    std::stable_sort(rowPositions.begin(), rowPositions.end(),
        [this](const TRowPosition& lhs, const TRowPosition& rhs) {
            auto& lhsBlock = Blocks_[lhs.BlockIndex];
            auto& lhsRow = lhsBlock.Rows[lhs.RowIndex];
            auto& rhsBlock = Blocks_[rhs.BlockIndex];
            auto& rhsRow = rhsBlock.Rows[rhs.RowIndex];

            int c = CompareKeyRowsAcrossYsonBlocks(
                lhsBlock.Data,
                lhsRow,
                rhsBlock.Data,
                rhsRow,
                SortOrders_
            );
            return c < 0;
        }
    );

    return rowPositions;
}

} // namespace NYql::NFmr
