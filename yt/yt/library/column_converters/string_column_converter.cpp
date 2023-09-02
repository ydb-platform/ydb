#include "string_column_converter.h"

#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

#include <library/cpp/yt/string/string_builder.h>

namespace NYT::NColumnConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

void FillColumnarStringValues(
    NTableClient::IUnversionedColumnarRowBatch::TColumn* column,
    i64 startIndex,
    i64 valueCount,
    ui32 avgLength,
    TRef offsets,
    TRef stringData)
{
    column->StartIndex = startIndex;
    column->ValueCount = valueCount;

    auto& values = column->Values.emplace();
    values.BitWidth = 32;
    values.ZigZagEncoded = true;
    values.Data = offsets;

    auto& strings = column->Strings.emplace();
    strings.AvgLength = avgLength;
    strings.Data = stringData;
}

bool IsValueNull(TStringBuf lhs)
{
    return !lhs.data();
}

////////////////////////////////////////////////////////////////////////////////


template <EValueType ValueType>
class TStringConverter
    : public IColumnConverter
{
public:
    TStringConverter(
        int columnIndex,
        const TColumnSchema& columnSchema)
        : ColumnIndex_(columnIndex)
        , ColumnSchema_(columnSchema)
    { }

    TConvertedColumn Convert(const std::vector<TUnversionedRowValues>& rowsValues) override
    {
        Reset();
        AddValues(rowsValues);
        return GetColumns();
    }

private:
    const int ColumnIndex_;
    const TColumnSchema ColumnSchema_;

    ui32 RowCount_ = 0;
    ui64 AllStringsSize_ = 0;
    ui64 DictionaryByteSize_ = 0;

    std::vector<TStringBuf> Values_;
    THashMap<TStringBuf, ui32> Dictionary_;
    TStringBuilder DirectBuffer_;

    void Reset()
    {
        AllStringsSize_ = 0;
        RowCount_ = 0;
        DictionaryByteSize_ = 0;

        DirectBuffer_.Reset();
        Values_.clear();
        Dictionary_.clear();
    }

    TSharedRef GetDirectDenseNullBitmap() const
    {
        TBitmapOutput nullBitmap(Values_.size());

        for (auto value : Values_) {
            nullBitmap.Append(IsValueNull(value));
        }

        return nullBitmap.Flush<TConverterTag>();
    }

    std::vector<ui32> GetDirectDenseOffsets() const
    {
        std::vector<ui32> offsets;
        offsets.reserve(Values_.size());

        ui32 offset = 0;
        for (auto value : Values_) {
            offset += value.length();
            offsets.push_back(offset);
        }

        return offsets;
    }

    TConvertedColumn GetDirectColumn(TSharedRef nullBitmap)
    {
        auto offsets = GetDirectDenseOffsets();

        // Save offsets as diff from expected.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&offsets, &expectedLength, &maxDiff);

        auto directData = DirectBuffer_.GetBuffer();

        auto offsetsRef = TSharedRef::MakeCopy<TConverterTag>(TRef(offsets.data(), sizeof(ui32) * offsets.size()));
        auto directDataPtr = TSharedRef::MakeCopy<TConverterTag>(TRef(directData.data(), directData.size()));
        auto column = std::make_shared<TBatchColumn>();

        FillColumnarStringValues(
            column.get(),
            0,
            RowCount_,
            expectedLength,
            TRef(offsetsRef),
            TRef(directDataPtr));

        FillColumnarNullBitmap(
            column.get(),
            0,
            RowCount_,
            TRef(nullBitmap));

        column->Type = ColumnSchema_.LogicalType();
        column->Id = ColumnIndex_;

        TOwningColumn owner = {
            .Column = std::move(column),
            .NullBitmap = std::move(nullBitmap),
            .ValueBuffer = std::move(offsetsRef),
            .StringBuffer = std::move(directDataPtr),
        };
        return {{owner}, owner.Column.get()};
    }

    TConvertedColumn GetDictionaryColumn()
    {
        auto dictionaryData = TSharedMutableRef::Allocate<TConverterTag>(DictionaryByteSize_, {.InitializeStorage = false});

        std::vector<ui32> dictionaryOffsets;
        dictionaryOffsets.reserve(Dictionary_.size());

        std::vector<ui32> ids;
        ids.reserve(Values_.size());

        ui32 dictionarySize = 0;
        ui32 dictionaryOffset = 0;
        for (auto value : Values_) {
            if (IsValueNull(value)) {
                ids.push_back(0);
                continue;
            }

            ui32 id = GetOrCrash(Dictionary_, value);
            ids.push_back(id);

            if (id > dictionarySize) {
                std::memcpy(
                    dictionaryData.Begin() + dictionaryOffset,
                    value.data(),
                    value.length());
                dictionaryOffset += value.length();
                dictionaryOffsets.push_back(dictionaryOffset);
                ++dictionarySize;
            }
        }

        YT_VERIFY(dictionaryOffset == DictionaryByteSize_);

        // 1. Value ids.
        auto idsRef = TSharedRef::MakeCopy<TConverterTag>(TRef(ids.data(), sizeof(ui32) * ids.size()));

        // 2. Dictionary offsets.
        ui32 expectedLength;
        ui32 maxDiff;
        PrepareDiffFromExpected(&dictionaryOffsets, &expectedLength, &maxDiff);
        auto dictionaryOffsetsRef = TSharedRef::MakeCopy<TConverterTag>(TRef(dictionaryOffsets.data(), sizeof(ui32) * dictionaryOffsets.size()));

        auto primaryColumn = std::make_shared<TBatchColumn>();
        auto dictionaryColumn = std::make_shared<TBatchColumn>();

        FillColumnarStringValues(
            dictionaryColumn.get(),
            0,
            dictionaryOffsets.size(),
            expectedLength,
            TRef(dictionaryOffsetsRef),
            dictionaryData);

        FillColumnarDictionary(
            primaryColumn.get(),
            dictionaryColumn.get(),
            NTableClient::IUnversionedColumnarRowBatch::GenerateDictionaryId(),
            primaryColumn->Type,
            0,
            RowCount_,
            idsRef);

        dictionaryColumn->Type = ColumnSchema_.LogicalType();
        primaryColumn->Type = ColumnSchema_.LogicalType();
        primaryColumn->Id = ColumnIndex_;

        TOwningColumn dictOwner = {
            .Column = std::move(dictionaryColumn),
            .ValueBuffer = std::move(dictionaryOffsetsRef),
            .StringBuffer = std::move(dictionaryData),
        };

        TOwningColumn primeOwner = {
            .Column = std::move(primaryColumn),
            .ValueBuffer = std::move(idsRef),
        };

        return {{primeOwner, dictOwner}, primeOwner.Column.get()};
    }

    TConvertedColumn GetColumns()
    {
        auto costs = GetEncodingMethodsCosts();

        auto minElement = std::min_element(costs.begin(), costs.end());
        auto type = EUnversionedStringSegmentType(std::distance(costs.begin(), minElement));

        switch (type) {

            case EUnversionedStringSegmentType::DirectDense:
                return GetDirectColumn(GetDirectDenseNullBitmap());

            case EUnversionedStringSegmentType::DictionaryDense:
                return GetDictionaryColumn();

            default:
                YT_ABORT();
        }
    }

    TEnumIndexedVector<EUnversionedStringSegmentType, ui64> GetEncodingMethodsCosts() const
    {
        TEnumIndexedVector<EUnversionedStringSegmentType, ui64> costs;
        for (auto type : TEnumTraits<EUnversionedStringSegmentType>::GetDomainValues()) {
            costs[type] = GetSpecificEncodingMethodCosts(type);
        }
        return costs;
    }

    ui64 GetSpecificEncodingMethodCosts(EUnversionedStringSegmentType type) const
    {
        switch (type) {
            case EUnversionedStringSegmentType::DictionaryDense:
                return GetDictionaryByteSize();

            case EUnversionedStringSegmentType::DirectDense:
                return GetDirectByteSize();

            default:
                YT_ABORT();
        }
    }

    void AddValues(const std::vector<TUnversionedRowValues>& rowsValues)
    {
        for (auto rowValues : rowsValues) {
            auto unversionedValue = rowValues[ColumnIndex_];
            YT_VERIFY(unversionedValue != nullptr);
            auto value = CaptureValue(*unversionedValue);
            Values_.push_back(value);
            ++RowCount_;
        }
    }

    ui64 GetDirectByteSize() const
    {
        return AllStringsSize_;
    }

    ui64 GetDictionaryByteSize() const
    {
        return DictionaryByteSize_ + Values_.size() * sizeof(ui32);
    }


    TStringBuf CaptureValue(const TUnversionedValue& unversionedValue)
    {
        if (unversionedValue.Type == EValueType::Null) {
            return {};
        }

        auto valueCapacity = IsAnyOrComposite(ValueType) && !IsAnyOrComposite(unversionedValue.Type)
            ? GetYsonSize(unversionedValue)
            : static_cast<i64>(unversionedValue.Length);

        char* buffer = DirectBuffer_.Preallocate(valueCapacity);
        if (!buffer) {
            // This means, that we reserved nothing, because all strings are either null or empty.
            // To distinguish between null and empty, we set preallocated pointer to special value.
            static char* const EmptyStringBase = reinterpret_cast<char*>(1);
            buffer = EmptyStringBase;
        }

        auto start = buffer;

        if (IsAnyOrComposite(ValueType) && !IsAnyOrComposite(unversionedValue.Type)) {
            // Any non-any and non-null value convert to YSON.
            buffer += WriteYson(buffer, unversionedValue);
        } else {
            std::memcpy(
                buffer,
                unversionedValue.Data.String,
                unversionedValue.Length);
            buffer += unversionedValue.Length;
        }

        auto value = TStringBuf(start, buffer);

        YT_VERIFY(value.size() <= valueCapacity);

        DirectBuffer_.Advance(value.size());

        if (Dictionary_.emplace(value, Dictionary_.size() + 1).second) {
            DictionaryByteSize_ += value.size();
        }
        AllStringsSize_ += value.size();
        return value;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateStringConverter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TStringConverter<EValueType::String>>(columnIndex, columnSchema);
}

IColumnConverterPtr CreateAnyConverter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TStringConverter<EValueType::Any>>(columnIndex, columnSchema);
}

IColumnConverterPtr CreateCompositeConverter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TStringConverter<EValueType::Composite>>(columnIndex, columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
