#include "floating_point_converter.h"
#include "helper.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NConverters {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <typename T>
void FillColumnarFloatingPointValues(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    TRef data)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& values = column->Values.emplace();
    values.BitWidth = sizeof(T) * 8;
    values.Data = data;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class TValue, NTableClient::EValueType ValueType>
class TFloatingPointColumnConverter
    : public IColumnConverter
{
public:
    TFloatingPointColumnConverter(int columnIndex,  const NTableClient::TColumnSchema& columnSchema)
        : ColumnIndex_(columnIndex)
        , ColumnSchema_(columnSchema)
    {
        static_assert(std::is_floating_point_v<TValue>);
    }

    TConvertedColumn Convert(const std::vector<TUnversionedRowValues>& rowsValues) {
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
        column->Id = ColumnIndex_;

        TOwningColumn owner = {
            std::move(column),
            std::move(nullBitmapRef),
            std::move(valuesRef),
            /*stringBuffer*/ std::nullopt
        };

        TConvertedColumn res = {{owner}, owner.Column.get()};
        return res;

    }

private:
    const int ColumnIndex_;
    TColumnSchema ColumnSchema_;
    std::vector<TValue> Values_;
    TBitmapOutput NullBitmap_;

    void Reset()
    {
        Values_.clear();
        NullBitmap_ = TBitmapOutput();
    }

    void AddValues(const std::vector<TUnversionedRowValues>& rowsValues)
    {
        for (auto rowValues : rowsValues) {
            auto value = rowValues[ColumnIndex_];
            bool isNull = value == nullptr || value->Type == NTableClient::EValueType::Null;
            TValue data = isNull ? 0 : value->Data.Double;
            NullBitmap_.Append(isNull);
            Values_.push_back(data);
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateFloatingPoint32ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema) {
    return std::make_unique<TFloatingPointColumnConverter<float, NTableClient::EValueType::Double>>(columnIndex, columnSchema);
}

IColumnConverterPtr CreateFloatingPoint64ColumnConverter(int columnIndex, const NTableClient::TColumnSchema& columnSchema) {
    return std::make_unique<TFloatingPointColumnConverter<double,  NTableClient::EValueType::Double>>(columnIndex, columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConverters
