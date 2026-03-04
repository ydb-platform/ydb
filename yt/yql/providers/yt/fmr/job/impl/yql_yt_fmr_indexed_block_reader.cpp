#include "yql_yt_fmr_indexed_block_reader.h"

namespace NYql::NFmr {

int TFmrIndexedBlockReader::TSourceState::CompareTo(const TSourceState& rhs) const {
    int c = CompareKeyRowsAcrossYsonBlocks(
        Block.Data,
        Markup(),
        rhs.Block.Data,
        rhs.Markup(),
        SortOrders.get()
    );
    if (c != 0) {
        return c;
    }

    if (SourceId < rhs.SourceId) {
        return -1;
    }
    if (SourceId > rhs.SourceId) {
        return 1;
    }
    return 0;
}

bool TFmrIndexedBlockReader::TSourceState::operator<(const TSourceState& rhs) const {
    return CompareTo(rhs) < 0;
}

void TFmrIndexedBlockReader::TSourceState::EnsureRow() {
    while (!Eof && (Block.Rows.empty() || RowIndex >= Block.Rows.size())) {
        TIndexedBlock next;
        if (!BlockIterator || !BlockIterator->NextBlock(next)) {
            Eof = true;
            Block = {};
            RowIndex = 0;
            break;
        }
        Block = std::move(next);
        RowIndex = 0;
    }
}

bool TFmrIndexedBlockReader::TSourceState::Valid() const {
    return !Eof && !Block.Rows.empty() && RowIndex < Block.Rows.size();
}

const TRowIndexMarkup& TFmrIndexedBlockReader::TSourceState::Markup() const {
    return Block.Rows[RowIndex];
}

void TFmrIndexedBlockReader::TSourceState::Next() {
    ++RowIndex;
    EnsureRow();
}

TFmrIndexedBlockReader::TFmrIndexedBlockReader(
    const std::vector<IBlockIterator::TPtr>& inputs
) : SortOrders_(GetUnionSortOrder(inputs))
{
    Sources_.reserve(inputs.size());
    for (ui32 i = 0; i < inputs.size(); ++i) {
        Sources_.push_back(TSourceState{
            .SourceId = i,
            .SortOrders = std::cref(SortOrders_),
            .BlockIterator = inputs[i],
        });
    }
}

bool TFmrIndexedBlockReader::Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) {
    return false;
}

void TFmrIndexedBlockReader::ResetRetries() {
}

bool TFmrIndexedBlockReader::HasRangeIndices() const {
    return false;
}

std::vector<ESortOrder> TFmrIndexedBlockReader::GetUnionSortOrder(const std::vector<IBlockIterator::TPtr>& blockIterators) {
    std::set<std::vector<ESortOrder>> sortOrders;
    for (auto& blockIterator: blockIterators) {
        sortOrders.emplace(blockIterator->GetSortOrder());
    }
    if (sortOrders.size() != 1) {
        throw yexception() << "Sorting order spec mismatches in several tables in job";
    }
    return *sortOrders.begin();
}


} // namespace NYql::NFmr
