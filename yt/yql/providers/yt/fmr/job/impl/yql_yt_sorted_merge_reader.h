#pragma once

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

#include <functional>

namespace NYql::NFmr {


class TSortedMergeReader final: public NYT::TRawTableReader {
public:
    TSortedMergeReader(
        std::vector<IBlockIterator::TPtr> inputs,
        std::vector<ESortOrder> sortOrders
    );

    bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) override;
    void ResetRetries() override;
    bool HasRangeIndices() const override;

private:
    size_t DoRead(void* buf, size_t len) override;

private:
    struct TSourceState {
        ui32 SourceId = 0;
        std::reference_wrapper<const std::vector<ESortOrder>> SortOrders;
        IBlockIterator::TPtr It;
        TIndexedBlock Block;
        ui32 RowIndex = 0;
        bool Eof = false;

        void EnsureRow();
        bool Valid() const;
        const TRowIndexMarkup& Markup() const;
        TStringBuf RowBytes() const;
        void Next();

        int CompareTo(const TSourceState& rhs) const;
        bool operator<(const TSourceState& rhs) const;
    };

    int CompareSources(ui32 lhsSourceId, ui32 rhsSourceId) const;

private:
    const std::vector<ESortOrder> SortOrders_;

    std::vector<TSourceState> Sources_;
    std::vector<ui32> Heap_;

    bool HasActive_ = false;
    ui32 ActiveSource_ = 0;
    size_t ActiveOffset_ = 0;
};

} // namespace NYql::NFmr

