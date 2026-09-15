#include "yql_yt_fmr_sorting_block_reader.h"

namespace NYql::NFmr {

TFmrSortingBlockReader::TFmrSortingBlockReader(
    const std::vector<IBlockIterator::TPtr>& inputs
)
    : TFmrIndexedBlockReader(inputs)
{
    GetBlocks(inputs);
    GetSortedRowsOrdering();
}

void TFmrSortingBlockReader::GetBlocks(const std::vector<IBlockIterator::TPtr>& inputs) {
    for (auto blockIterator: inputs) {
        TIndexedBlock curBlock;
        while (blockIterator->NextBlock(curBlock)) {
            Blocks_.emplace_back(std::move(curBlock));
        }
    }
}

void TFmrSortingBlockReader::GetSortedRowsOrdering() {
    TSortHelper sortHelper(Blocks_, SortOrders_);
    RowOrdering_ = sortHelper.GetSortedRowOrdering();
}

size_t TFmrSortingBlockReader::DoRead(void* buf, size_t len) {
    auto out = MakeArrayRef(static_cast<char*>(buf), len);
    size_t total = 0;
    size_t remaining = len;
    while (remaining > 0 && !IsFinished()) {
        auto rowPos = RowOrdering_[Position_];
        const auto& block = Blocks_[rowPos.BlockIndex];
        TStringBuf row = block.GetRowBytes(rowPos.RowIndex);

        const size_t available = row.size() - ActiveOffset_;
        const size_t toCopy = std::min(available, remaining);

        std::copy_n(row.begin() + ActiveOffset_, toCopy, out.begin() + total);
        remaining -= toCopy;
        total += toCopy;
        ActiveOffset_ += toCopy;

        if (ActiveOffset_ == row.size()) {
            ++Position_;
            ActiveOffset_ = 0;
        }
    }
    return total;
}

bool TFmrSortingBlockReader::IsFinished() const {
    return Position_ == RowOrdering_.size();
}

} // namespace NYql::NFmr
