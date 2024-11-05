#include "integer_column_converter.h"

#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/coding/zig_zag.h>

namespace NYT::NColumnConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 EncodeValue(i64 value)
{
    return ZigZagEncode64(value);
}

ui64 EncodeValue(ui64 value)
{
    return value;
}

template <class TValue>
typename std::enable_if<std::is_signed<TValue>::value, TValue>::type
GetValue(const TUnversionedValue& value)
{
    return value.Data.Int64;
}

template <class TValue>
typename std::enable_if<std::is_unsigned<TValue>::value, TValue>::type
GetValue(const TUnversionedValue& value)
{
    return value.Data.Uint64;
}

////////////////////////////////////////////////////////////////////////////////

void FillColumnarIntegerValues(
    IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    EValueType valueType,
    ui64 baseValue,
    TRef data)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    column->Values = IUnversionedColumnarRowBatch::TValueBuffer{};
    auto& values = *column->Values;
    values.BaseValue = baseValue;
    values.BitWidth = 64;
    values.ZigZagEncoded = (valueType == EValueType::Int64);
    values.Data = data;
}

////////////////////////////////////////////////////////////////////////////////

// TValue - i64 or ui64.
template <class TValue>
class TIntegerColumnConverter
    : public IColumnConverter
{
public:
    static_assert(std::is_integral_v<TValue>);

    TIntegerColumnConverter(
        int columnId,
        EValueType ValueType,
        TColumnSchema columnSchema,
        int columnOffset)
        : ColumnId_(columnId)
        , ColumnSchema_(columnSchema)
        , ValueType_(ValueType)
        , ColumnOffset_(columnOffset)
    { }

    TConvertedColumn Convert(TRange<TUnversionedRowValues> rowsValues) override
    {
        Reset();
        AddValues(rowsValues);
        for (i64 index = 0; index < std::ssize(Values_); ++index) {
            if (!NullBitmap_[index]) {
                Values_[index] -= MinValue_;
            }
        }

        auto nullBitmapRef = NullBitmap_.Flush<TConverterTag>();
        auto valuesRef = TSharedRef::MakeCopy<TConverterTag>(TRef(Values_.data(), sizeof(ui64) * Values_.size()));
        auto column = std::make_shared<TBatchColumn>();

        FillColumnarIntegerValues(
            column.get(),
            0,
            RowCount_,
            ValueType_,
            MinValue_,
            valuesRef);

        FillColumnarNullBitmap(
            column.get(),
            0,
            RowCount_,
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
    const EValueType ValueType_;
    const int ColumnOffset_;

    i64 RowCount_ = 0;
    TBitmapOutput NullBitmap_;
    std::vector<ui64> Values_;

    ui64 MaxValue_;
    ui64 MinValue_;

    void Reset()
    {
        Values_.clear();
        RowCount_ = 0;
        MaxValue_ = 0;
        MinValue_ = std::numeric_limits<ui64>::max();
        NullBitmap_.Flush<TConverterTag>();
    }

    void AddValues(TRange<TUnversionedRowValues> rowsValues)
    {
        for (const auto& rowValues : rowsValues) {
            auto value = rowValues[ColumnOffset_];
            bool isNull = !value || value->Type == EValueType::Null;
            ui64 data = 0;
            if (!isNull) {
                YT_VERIFY(value);
                data = EncodeValue(GetValue<TValue>(*value));
            }
            Values_.push_back(data);
            NullBitmap_.Append(isNull);
            ++RowCount_;
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateInt64ColumnConverter(int columnId, const TColumnSchema& columnSchema, int columnOffset)
{
    return std::make_unique<TIntegerColumnConverter<i64>>(columnId, EValueType::Int64, columnSchema, columnOffset);
}


IColumnConverterPtr CreateUint64ColumnConverter(int columnId, const TColumnSchema& columnSchema, int columnOffset)
{
    return std::make_unique<TIntegerColumnConverter<ui64>>(columnId, EValueType::Uint64, columnSchema, columnOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
