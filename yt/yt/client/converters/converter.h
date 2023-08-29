#pragma once

#include <yt/yt/client/table_client/row_batch.h>

#include <yt/yt/core/misc/bitmap.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NConverters {

using TBatchColumn = NTableClient::IUnversionedColumnarRowBatch::TColumn;
using TBatchColumnPtr = std::shared_ptr<TBatchColumn>;
using TUnversionedRowValues = std::vector<const NTableClient::TUnversionedValue*>;

////////////////////////////////////////////////////////////////////////////////

struct TOwningColumn
{
    TBatchColumnPtr Column;
    std::optional<TSharedRef> NullBitmap;
    std::optional<TSharedRef> ValueBuffer;
    std::optional<TSharedRef> StringBuffer;
};

struct TConvertedColumn
{
    std::vector<TOwningColumn> Columns;
    TBatchColumn* RootColumn;
};

using TConvertedColumnRange = std::vector<TConvertedColumn>;

////////////////////////////////////////////////////////////////////////////////

struct IColumnConverter
    : public TNonCopyable
{
    virtual ~IColumnConverter() = default;
    virtual TConvertedColumn Convert(const std::vector<TUnversionedRowValues>& rowsValues) = 0;
};

using IColumnConverterPtr = std::unique_ptr<IColumnConverter>;

////////////////////////////////////////////////////////////////////////////////


TConvertedColumnRange ConvertRowsToColumns(
    TRange<NTableClient::TUnversionedRow> rows,
    const std::vector<NTableClient::TColumnSchema> &columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConverters
