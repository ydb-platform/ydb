#include "boolean_column_converter.h"

#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NColumnConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

void FillColumnarBooleanValues(
    TBatchColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef bitmap)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    column->Values = IUnversionedColumnarRowBatch::TValueBuffer{};
    auto& values = *column->Values;

    values.BitWidth = 1;
    values.Data = bitmap;
}

////////////////////////////////////////////////////////////////////////////////

class TBooleanColumnConverter
    : public IColumnConverter
{
public:
    TBooleanColumnConverter(
        int columnIndex,
        const TColumnSchema& columnSchema,
        int columnOffset)
        : ColumnIndex_(columnIndex)
        , ColumnSchema_(columnSchema)
        , ColumnOffset_(columnOffset)
    { }

    TConvertedColumn Convert(TRange<TUnversionedRowValues> rowsValues) override
    {
        Reset();
        AddValues(rowsValues);

        auto column = std::make_shared<TBatchColumn>();
        auto nullBitmapRef = NullBitmap_.Flush<TConverterTag>();
        auto valuesRef = Values_.Flush<TConverterTag>();

        FillColumnarBooleanValues(column.get(), 0, rowsValues.size(), valuesRef);
        FillColumnarNullBitmap(column.get(), 0, rowsValues.size(), nullBitmapRef);

        column->Type = ColumnSchema_.LogicalType();
        column->Id = ColumnIndex_;

        TOwningColumn owner = {
            .Column = std::move(column),
            .NullBitmap = std::move(nullBitmapRef),
            .ValueBuffer = std::move(valuesRef),
        };

        return {{owner}, owner.Column.get()};
    }


private:
    const int ColumnIndex_;
    const TColumnSchema ColumnSchema_;
    const int ColumnOffset_;

    TBitmapOutput Values_;
    TBitmapOutput NullBitmap_;

    void Reset()
    {
        Values_.Flush<TConverterTag>();
        NullBitmap_.Flush<TConverterTag>();
    }

    void AddValues(TRange<TUnversionedRowValues> rowsValues)
    {
        for (const auto& rowValues : rowsValues) {
            auto value = rowValues[ColumnOffset_];
            bool isNull = !value || value->Type == EValueType::Null;
            bool data = isNull ? false : value->Data.Boolean;
            NullBitmap_.Append(isNull);
            Values_.Append(data);
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateBooleanColumnConverter(int columnId, const TColumnSchema& columnSchema, int columnOffset)
{
    return std::make_unique<TBooleanColumnConverter>(columnId, columnSchema, columnOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
