#include "row_batch.h"
#include "unversioned_row.h"
#include "versioned_row.h"

#include <atomic>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

bool IUnversionedRowBatch::IsEmpty() const
{
    return GetRowCount() == 0;
}

IUnversionedColumnarRowBatchPtr IUnversionedRowBatch::TryAsColumnar()
{
    return dynamic_cast<IUnversionedColumnarRowBatch*>(this);
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedColumnarRowBatch::TDictionaryId IUnversionedColumnarRowBatch::GenerateDictionaryId()
{
    static std::atomic<TDictionaryId> CurrentId;
    return ++CurrentId;
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowBatchPtr CreateBatchFromUnversionedRows(
    TSharedRange<TUnversionedRow> rows)
{
    class TUnversionedRowBatch
        : public IUnversionedRowBatch
    {
    public:
        explicit TUnversionedRowBatch(TSharedRange<TUnversionedRow> rows)
            : Rows_(std::move(rows))
        { }

        int GetRowCount() const override
        {
            return static_cast<int>(Rows_.size());
        }

        TSharedRange<TUnversionedRow> MaterializeRows() override
        {
            return Rows_;
        }

    private:
        const TSharedRange<TUnversionedRow> Rows_;
    };

    return New<TUnversionedRowBatch>(rows);
}

////////////////////////////////////////////////////////////////////////////////

IUnversionedRowBatchPtr CreateEmptyUnversionedRowBatch()
{
    class TUnversionedRowBatch
        : public IUnversionedRowBatch
    {
    public:
        int GetRowCount() const override
        {
            return 0;
        }

        TSharedRange<TUnversionedRow> MaterializeRows() override
        {
            return {};
        }

    };

    return New<TUnversionedRowBatch>();
}

////////////////////////////////////////////////////////////////////////////////

bool IVersionedRowBatch::IsEmpty() const
{
    return GetRowCount() == 0;
}

////////////////////////////////////////////////////////////////////////////////

IVersionedRowBatchPtr CreateBatchFromVersionedRows(
    TSharedRange<TVersionedRow> rows)
{
    class TVersionedRowBatch
        : public IVersionedRowBatch
    {
    public:
        explicit TVersionedRowBatch(TSharedRange<TVersionedRow> rows)
            : Rows_(std::move(rows))
        { }

        int GetRowCount() const override
        {
            return static_cast<int>(Rows_.size());
        }

        TSharedRange<TVersionedRow> MaterializeRows() override
        {
            return Rows_;
        }

    private:
        const TSharedRange<TVersionedRow> Rows_;
    };

    return New<TVersionedRowBatch>(rows);
}

////////////////////////////////////////////////////////////////////////////////

IVersionedRowBatchPtr CreateEmptyVersionedRowBatch()
{
    class TVersionedRowBatch
        : public IVersionedRowBatch
    {
    public:
        int GetRowCount() const override
        {
            return 0;
        }

        TSharedRange<TVersionedRow> MaterializeRows() override
        {
            return {};
        }

    };

    return New<TVersionedRowBatch>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
