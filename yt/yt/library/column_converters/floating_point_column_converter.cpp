#include "floating_point_column_converter.h"

#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NColumnConverters {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
void FillColumnarFloatingPointValues(
    IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef data)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    column->Values = IUnversionedColumnarRowBatch::TValueBuffer{};
    auto& values = *column->Values;
    values.BitWidth = sizeof(T) * 8;
    values.Data = data;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TSharedRef SerializeFloatingPointVector(const std::vector<T>& values)
{
    auto data = TSharedMutableRef::Allocate<TConverterTag>(values.size() * sizeof(T) + sizeof(ui64), {.InitializeStorage = false});
    *reinterpret_cast<ui64*>(data.Begin()) = static_cast<ui64>(values.size());
    std::memcpy(
        data.Begin() + sizeof(ui64),
        values.data(),
        values.size() * sizeof(T));
    return data;
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue, EValueType ValueType>
class TFloatingPointColumnConverter
    : public IColumnConverter
{
public:
    static_assert(std::is_floating_point_v<TValue>);

    TFloatingPointColumnConverter(
        int columnId,
        const TColumnSchema& columnSchema,
        int columnOffset)
        : ColumnId_(columnId)
        , ColumnSchema_(columnSchema)
        , ColumnOffset_(columnOffset)

    { }

    TConvertedColumn Convert(TRange<TUnversionedRowValues> rowsValues)
    {
        Reset();
        AddValues(rowsValues);
        auto nullBitmapRef = NullBitmap_.Flush<TConverterTag>();
        auto valuesRef = TSharedRef::MakeCopy<TConverterTag>(TRef(Values_.data(), sizeof(TValue) * Values_.size()));

        auto column = std::make_shared<TBatchColumn>();

        FillColumnarFloatingPointValues<TValue>(
            column.get(),
            0,
            rowsValues.size(),
            valuesRef);

        FillColumnarNullBitmap(
            column.get(),
            0,
            rowsValues.size(),
            nullBitmapRef);

        column->Type = ColumnSchema_.LogicalType();
        column->Id = ColumnId_;

        TOwningColumn owner = {
            .Column = std::move(column),
            .NullBitmap = std::move(nullBitmapRef),
            .ValueBuffer = std::move(valuesRef),
        };

        return {{owner}, owner.Column.get()};
    }

private:
    const int ColumnId_;
    const TColumnSchema ColumnSchema_;
    const int ColumnOffset_;

    std::vector<TValue> Values_;
    TBitmapOutput NullBitmap_;

    void Reset()
    {
        Values_.clear();
        NullBitmap_.Flush<TConverterTag>();
    }

    void AddValues(TRange<TUnversionedRowValues> rowsValues)
    {
        for (const auto& rowValues : rowsValues) {
            auto value = rowValues[ColumnOffset_];
            bool isNull = !value || value->Type == EValueType::Null;
            TValue valueData = isNull ? 0 : value->Data.Double;
            NullBitmap_.Append(isNull);
            Values_.push_back(valueData);
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateFloatingPoint32ColumnConverter(int columnId, const TColumnSchema& columnSchema, int columnOffset)
{
    return std::make_unique<TFloatingPointColumnConverter<float, EValueType::Double>>(columnId, columnSchema, columnOffset);
}

IColumnConverterPtr CreateFloatingPoint64ColumnConverter(int columnId, const TColumnSchema& columnSchema, int columnOffset)
{
    return std::make_unique<TFloatingPointColumnConverter<double, EValueType::Double>>(columnId, columnSchema, columnOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
