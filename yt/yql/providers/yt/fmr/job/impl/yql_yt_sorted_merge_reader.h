#pragma once

#include "yql_yt_fmr_indexed_block_reader.h"

namespace NYql::NFmr {

class TSortedMergeReader final: public TFmrIndexedBlockReader {
public:
    TSortedMergeReader(const std::vector<IBlockIterator::TPtr>& inputs);

private:
    size_t DoRead(void* buf, size_t len) final;

private:
    std::vector<ui32> Heap_;
    std::function<bool(ui32, ui32)> HeapComparator_;
    bool HasActive_ = false;
    ui64 ActiveSource_ = 0;
};

} // namespace NYql::NFmr
