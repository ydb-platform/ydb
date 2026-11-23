#include "rowset_subrange_reader.h"

#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <library/cpp/yt/memory/shared_range.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

std::pair<TKeyBoundRef, TKeyBoundRef> UniversalRange = {
    TKeyBoundRef(ToKeyRef(MinKey()), /*inclusive*/ true, /*upper*/ false),
    TKeyBoundRef(ToKeyRef(MaxKey()), /*inclusive*/ true, /*upper*/ true)
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRowsetSubrangeReader)

class TRowsetSubrangeReader
    : public ISchemafulUnversionedReader
{
public:
    TRowsetSubrangeReader(
        TFuture<TSharedRange<TUnversionedRow>> asyncRows,
        std::pair<TKeyBoundRef, TKeyBoundRef> readRange)
        : AsyncRows_(std::move(asyncRows))
        , ReadRange_(std::move(readRange))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (!AsyncRows_.IsSet() || !AsyncRows_.GetOrCrash().IsOK()) {
            // XXX: shouldn't we throw here?
            return CreateEmptyUnversionedRowBatch();
        }

        const auto& rows = AsyncRows_.GetOrCrash().Value();

        CurrentRowIndex_ = BinarySearch(CurrentRowIndex_, std::ssize(rows), [&] (i64 index) {
            return !TestKeyWithWidening(
                ToKeyRef(rows[index]),
                ReadRange_.first);
        });

        auto startIndex = CurrentRowIndex_;

        CurrentRowIndex_ = std::min(CurrentRowIndex_ + options.MaxRowsPerRead, std::ssize(rows));

        CurrentRowIndex_ = BinarySearch(startIndex, CurrentRowIndex_, [&] (i64 index) {
            return TestKeyWithWidening(
                ToKeyRef(rows[index]),
                ReadRange_.second);
        });

        if (startIndex == CurrentRowIndex_) {
            return nullptr;
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(rows.Slice(startIndex, CurrentRowIndex_), rows));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return AsyncRows_.AsVoid();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return {};
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    const TFuture<TSharedRange<TUnversionedRow>> AsyncRows_;
    const std::pair<TKeyBoundRef, TKeyBoundRef> ReadRange_;

    i64 CurrentRowIndex_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TRowsetSubrangeReader)

////////////////////////////////////////////////////////////////////////////////

ISchemafulUnversionedReaderPtr CreateRowsetSubrangeReader(
    TFuture<TSharedRange<TUnversionedRow>> asyncRows,
    std::pair<TKeyBoundRef, TKeyBoundRef> readRange)
{
    return New<TRowsetSubrangeReader>(
        std::move(asyncRows),
        std::move(readRange));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
