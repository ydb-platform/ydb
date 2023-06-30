#include "queue_rowset.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NQueueClient {

using namespace NApi;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void VerifyRowBatchReadOptions(const TQueueRowBatchReadOptions& options)
{
    if (options.MaxRowCount <= 0) {
        THROW_ERROR_EXCEPTION("MaxRowCount is non-positive");
    }
    if (options.MaxDataWeight <= 0) {
        THROW_ERROR_EXCEPTION("MaxDataWeight is non-positive");
    }
    if (options.DataWeightPerRowHint && *options.DataWeightPerRowHint <= 0) {
        THROW_ERROR_EXCEPTION("DataWeightPerRowHint is non-positive");
    }
}

i64 ComputeRowsToRead(const TQueueRowBatchReadOptions& options)
{
    VerifyRowBatchReadOptions(options);

    auto rowsToRead = options.MaxRowCount;
    if (options.DataWeightPerRowHint) {
        auto rowsToReadWithHint = options.MaxDataWeight / *options.DataWeightPerRowHint;
        rowsToRead = std::min(rowsToRead, rowsToReadWithHint);
    }

    rowsToRead = std::max<i64>(rowsToRead, 1);

    return rowsToRead;
}

i64 GetStartOffset(const IUnversionedRowsetPtr& rowset)
{
    const auto& nameTable = rowset->GetNameTable();

    YT_VERIFY(!rowset->GetRows().Empty());

    auto rowIndexColumnId = nameTable->GetIdOrThrow("$row_index");
    auto startOffsetValue = rowset->GetRows()[0][rowIndexColumnId];
    THROW_ERROR_EXCEPTION_UNLESS(
        startOffsetValue.Type == EValueType::Int64,
        "Incorrect type %Qlv for $row_index column, %Qlv expected; only ordered dynamic tables are supported as queues",
        startOffsetValue.Type,
        EValueType::Int64);

    return startOffsetValue.Data.Int64;
}

////////////////////////////////////////////////////////////////////////////////

class TQueueRowset
    : public IQueueRowset
{
public:
    TQueueRowset(IUnversionedRowsetPtr rowset, i64 startOffset)
        : Rowset_(std::move(rowset))
        , StartOffset_(startOffset)
    { }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Rowset_->GetSchema();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return Rowset_->GetNameTable();
    }

    TSharedRange<TUnversionedRow> GetRows() const override
    {
        return Rowset_->GetRows();
    }

    i64 GetStartOffset() const override
    {
        return StartOffset_;
    }

    i64 GetFinishOffset() const override
    {
        return StartOffset_ + std::ssize(GetRows());
    }

private:
    const IUnversionedRowsetPtr Rowset_;
    const i64 StartOffset_;
};

////////////////////////////////////////////////////////////////////////////////

IQueueRowsetPtr CreateQueueRowset(IUnversionedRowsetPtr rowset, i64 startOffset)
{
    return New<TQueueRowset>(std::move(rowset), startOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
