#pragma once

#include "yql_yt_fmr_indexed_block_reader.h"
#include <yt/yql/providers/yt/fmr/test_tools/yson/yql_yt_yson_helpers.h>

namespace NYql::NFmr {

class TReduceReader final: public TFmrIndexedBlockReader {
public:
    TReduceReader(
        const std::vector<IBlockIterator::TPtr>& inputs,
        const TSortingColumns& reduceBy
    );

private:
    size_t DoRead(void* buf, size_t len) final;

    TString GetReduceKeyFromRow();

private:
    std::vector<ui32> Heap_; // SortBy as comparator.
    std::function<bool(ui32, ui32)> HeapComparator_;
    bool HasActive_ = false;
    ui64 ActiveSource_ = 0;
    TSortingColumns ReduceBy_;
    TString CurrentReduceKey_;

    const TString KeySwitchMarker = GetBinaryYson("<\"key_switch\"=%true;>#;\n");
    bool IsKeyMarkerActive_ = false;
    ui64 KeyMarkerOffset_ = 0;
};

} // namespace NYql::NFmr
