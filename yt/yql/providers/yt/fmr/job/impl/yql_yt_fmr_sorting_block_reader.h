#pragma once

#include <yt/yql/providers/yt/fmr/job/impl/yql_yt_fmr_indexed_block_reader.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_sort_helper.h>

namespace NYql::NFmr {

class TFmrSortingBlockReader : public TFmrIndexedBlockReader {
public:
    TFmrSortingBlockReader(const std::vector<IBlockIterator::TPtr>& blockIterators);

private:
    size_t DoRead(void* buf, size_t len) final;

    void GetBlocks(const std::vector<IBlockIterator::TPtr>& inputs);

    void GetSortedRowsOrdering();

    bool IsFinished() const;

private:
    std::vector<TIndexedBlock> Blocks_; // buffer with all data from blockIterators, need to download it before sort.
    TSortedRowOrdering RowOrdering_;

    ui64 Position_ = 0;
};


} // namespace NYql::NFmr
