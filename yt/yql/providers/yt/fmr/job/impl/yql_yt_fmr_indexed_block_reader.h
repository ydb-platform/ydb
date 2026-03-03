#pragma once

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/yql/providers/yt/fmr/utils/yson_block_iterator/interface/yql_yt_yson_block_iterator.h>

namespace NYql::NFmr {

class TFmrIndexedBlockReader : public NYT::TRawTableReader {
public:
    TFmrIndexedBlockReader(const std::vector<IBlockIterator::TPtr>& inputs);

    bool Retry(const TMaybe<ui32>& rangeIndex, const TMaybe<ui64>& rowIndex, const std::exception_ptr& error) final;
    void ResetRetries() final;
    bool HasRangeIndices() const final;

private:
    virtual size_t DoRead(void* buf, size_t len) = 0;

    std::vector<ESortOrder> GetUnionSortOrder(const std::vector<IBlockIterator::TPtr>& blockIterators);

protected:
    struct TSourceState {
        ui32 SourceId = 0;
        std::reference_wrapper<const std::vector<ESortOrder>> SortOrders;
        IBlockIterator::TPtr BlockIterator;
        TIndexedBlock Block;
        ui32 RowIndex = 0;
        bool Eof = false;

        void EnsureRow();
        bool Valid() const;
        const TRowIndexMarkup& Markup() const;
        void Next();

        int CompareTo(const TSourceState& rhs) const;
        bool operator<(const TSourceState& rhs) const;
    };

    const std::vector<ESortOrder> SortOrders_;
    std::vector<TSourceState> Sources_;

    size_t ActiveOffset_ = 0;
};

} // namespace NYql::NFmr

