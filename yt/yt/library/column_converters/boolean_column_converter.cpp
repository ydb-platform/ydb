#include "boolean_column_converter.h"

#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NColumnConverters {

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

    auto& values = column->Values.emplace();
    values.BitWidth = 1;
    values.Data = bitmap;
}

////////////////////////////////////////////////////////////////////////////////

class TBooleanColumnConverter
    : public IColumnConverter
{
public:
    TBooleanColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema)
        : ColumnIndex_(columnIndex)
        , ColumnSchema_(columnSchema)
    { }

    TConvertedColumn Convert(const std::vector<TUnversionedRowValues>& rowsValues) override
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
    const NTableClient::TColumnSchema ColumnSchema_;

    TBitmapOutput Values_;
    TBitmapOutput NullBitmap_;

    void Reset()
    {
        Values_.Flush<TConverterTag>();
        NullBitmap_.Flush<TConverterTag>();
    }

    void AddValues(const std::vector<TUnversionedRowValues>& rowsValues)
    {
        for (auto rowValues : rowsValues) {
            auto value = rowValues[ColumnIndex_];
            bool isNull = value == nullptr || value->Type == NTableClient::EValueType::Null;
            bool data = isNull ? false : value->Data.Boolean;
            NullBitmap_.Append(isNull);
            Values_.Append(data);
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateBooleanColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TBooleanColumnConverter>(columnIndex, columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
