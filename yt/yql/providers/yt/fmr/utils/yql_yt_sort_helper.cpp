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
    const ui64 numKeyColumns = SortOrders_.size();

    struct TSortEntry {
        TRowPosition Position;
        std::vector<TExtractedKey> Keys;
    };

    std::vector<TSortEntry> entries;
    for (ui64 blockIndex = 0; blockIndex < Blocks_.size(); ++blockIndex) {
        const auto& block = Blocks_[blockIndex];
        for (ui64 rowIndex = 0; rowIndex < block.Rows.size(); ++rowIndex) {
            const auto& row = block.Rows[rowIndex];
            std::vector<TExtractedKey> keys;
            keys.reserve(numKeyColumns);
            for (ui64 colIdx = 0; colIdx < numKeyColumns; ++colIdx) {
                const auto& range = row[colIdx];
                if (!range.IsValid()) {
                    keys.push_back(TExtractedKey{TSmallKeyValue{std::monostate{}}, TStringBuf{}});
                } else {
                    TStringBuf val = SliceRange(block.Data, range);
                    keys.push_back(TExtractedKey{TryExtractSmallYsonValue(val), val});
                }
            }
            entries.push_back(TSortEntry{TRowPosition{blockIndex, rowIndex}, std::move(keys)});
        }
    }

    std::sort(entries.begin(), entries.end(),
        [&](const TSortEntry& lhs, const TSortEntry& rhs) {
            for (ui64 colIdx = 0; colIdx < numKeyColumns; ++colIdx) {
                int c = CompareExtractedKeys(lhs.Keys[colIdx], rhs.Keys[colIdx]);
                if (SortOrders_[colIdx] == ESortOrder::Descending) {
                    c = -c;
                }
                if (c != 0) {
                    return c < 0;
                }
            }
            return false;
        }
    );

    TSortedRowOrdering result;
    result.reserve(entries.size());
    for (const auto& entry : entries) {
        result.emplace_back(entry.Position);
    }
    return result;
}

} // namespace NYql::NFmr
