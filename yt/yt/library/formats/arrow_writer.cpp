#include "arrow_writer.h"

#include <yt/yt/client/arrow/fbs/Message.fbs.h>
#include <yt/yt/client/arrow/fbs/Schema.fbs.h>

#include <yt/yt/client/formats/public.h>
#include <yt/yt/library/formats/schemaless_writer_adapter.h>

#include <yt/yt/client/table_client/columnar.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/column_converters/column_converter.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/range.h>

#include <vector>

namespace NYT::NFormats {

using namespace NTableClient;
using namespace NComplexTypes;

static constexpr auto& Logger = FormatsLogger;

using TBodyWriter = std::function<void(TMutableRef)>;
using TBatchColumn = IUnversionedColumnarRowBatch::TColumn;

////////////////////////////////////////////////////////////////////////////////

struct TTypedBatchColumn
{
    const TBatchColumn* Column;
    TLogicalTypePtr Type;
};

////////////////////////////////////////////////////////////////////////////////

constexpr i64 ArrowAlignment = 8;
const TString AlignmentString(ArrowAlignment, 0);

flatbuffers::Offset<flatbuffers::String> SerializeString(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TString& str)
{
    return flatbufBuilder->CreateString(str.data(), str.length());
}

std::tuple<org::apache::arrow::flatbuf::Type, flatbuffers::Offset<void>> SerializeColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    TColumnSchema schema)
{
    auto simpleType = CastToV1Type(schema.LogicalType()).first;
    switch (simpleType) {
        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Null,
                org::apache::arrow::flatbuf::CreateNull(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Uint32:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Int,
                org::apache::arrow::flatbuf::CreateInt(
                    *flatbufBuilder,
                    GetIntegralTypeBitWidth(simpleType),
                    IsIntegralTypeSigned(simpleType))
                    .Union());

        case ESimpleLogicalValueType::Interval:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Int,
                org::apache::arrow::flatbuf::CreateInt(
                    *flatbufBuilder,
                    64,
                    true)
                    .Union());

        case ESimpleLogicalValueType::Date:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Date,
                org::apache::arrow::flatbuf::CreateDate(
                    *flatbufBuilder,
                    org::apache::arrow::flatbuf::DateUnit_DAY)
                    .Union());

        case ESimpleLogicalValueType::Datetime:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Date,
                org::apache::arrow::flatbuf::CreateDate(
                    *flatbufBuilder,
                    org::apache::arrow::flatbuf::DateUnit_MILLISECOND)
                    .Union());

        case ESimpleLogicalValueType::Timestamp:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Timestamp,
                org::apache::arrow::flatbuf::CreateTimestamp(
                    *flatbufBuilder,
                    org::apache::arrow::flatbuf::TimeUnit_MICROSECOND)
                    .Union());

        case ESimpleLogicalValueType::Double:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_FloatingPoint,
                org::apache::arrow::flatbuf::CreateFloatingPoint(
                    *flatbufBuilder,
                    org::apache::arrow::flatbuf::Precision_DOUBLE)
                    .Union());

        case ESimpleLogicalValueType::Float:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_FloatingPoint,
                org::apache::arrow::flatbuf::CreateFloatingPoint(
                    *flatbufBuilder,
                    org::apache::arrow::flatbuf::Precision_SINGLE)
                    .Union());

        case ESimpleLogicalValueType::Boolean:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Bool,
                org::apache::arrow::flatbuf::CreateBool(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Binary,
                org::apache::arrow::flatbuf::CreateBinary(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::Utf8:
            return std::tuple(
                org::apache::arrow::flatbuf::Type_Utf8,
                org::apache::arrow::flatbuf::CreateUtf8(*flatbufBuilder)
                    .Union());

        default:
            THROW_ERROR_EXCEPTION("Column %v has type %Qlv that is not currently supported by Arrow encoder",
                schema.GetDiagnosticNameString(),
                simpleType);
    }
}

int ExtractTableIndexFromColumn(const TBatchColumn* column)
{
    YT_VERIFY(column->Values);

    // Expecting rle but not dictionary column.
    YT_VERIFY(column->Rle);
    YT_VERIFY(!column->Rle->ValueColumn->Dictionary);

    const auto* valueColumn = column->Rle->ValueColumn;
    auto values = valueColumn->GetTypedValues<ui64>();

    // Expecting only one element.
    YT_VERIFY(values.size() == 1);

    auto rleIndexes = column->GetTypedValues<ui64>();

    auto startIndex = column->StartIndex;

    int tableIndex = 0;
    DecodeIntegerVector(
        startIndex,
        startIndex + 1,
        valueColumn->Values->BaseValue,
        valueColumn->Values->ZigZagEncoded,
        TRange<ui32>(),
        rleIndexes,
        [&] (auto index) {
            return values[index];
        },
        [&] (auto value) {
            tableIndex = value;
        });
    return tableIndex;
}

int GetIntegralLikeTypeByteSize(ESimpleLogicalValueType type)
{
    switch (type) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Uint8:
            return 1;
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Uint16:
            return 2;
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Uint32:
            return 4;
        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Interval:
            return 8;
        default:
            YT_ABORT();
    }
}

bool IsRleButNotDictionaryEncodedStringLikeColumn(const TBatchColumn& column)
{
    auto simpleType = CastToV1Type(column.Type).first;
    return IsStringLikeType(simpleType) &&
        column.Rle &&
        !column.Rle->ValueColumn->Dictionary;
}

bool IsRleAndDictionaryEncodedColumn(const TBatchColumn& column)
{
    return column.Rle &&
        column.Rle->ValueColumn->Dictionary;
}

bool IsDictionaryEncodedColumn(const TBatchColumn& column)
{
    return column.Dictionary ||
        IsRleAndDictionaryEncodedColumn(column) ||
        IsRleButNotDictionaryEncodedStringLikeColumn(column);
}


struct TRecordBatchBodyPart
{
    i64 Size;
    TBodyWriter Writer;
};

struct TRecordBatchSerializationContext final
{
    explicit TRecordBatchSerializationContext(flatbuffers::FlatBufferBuilder* flatbufBuilder)
        : FlatbufBuilder(flatbufBuilder)
    {}

    void AddFieldNode(i64 length, i64 nullCount)
    {
        FieldNodes.emplace_back(length, nullCount);
    }

    void AddBuffer(i64 size, TBodyWriter writer)
    {
        YT_LOG_DEBUG("Buffer registered (Offset: %v, Size: %v)",
            CurrentBodyOffset,
            size);

        Buffers.emplace_back(CurrentBodyOffset, size);
        CurrentBodyOffset += AlignUp<i64>(size, ArrowAlignment);
        Parts.push_back(TRecordBatchBodyPart{size, std::move(writer)});
    }

    flatbuffers::FlatBufferBuilder* const FlatbufBuilder;

    i64 CurrentBodyOffset = 0;
    std::vector<org::apache::arrow::flatbuf::FieldNode> FieldNodes;
    std::vector<org::apache::arrow::flatbuf::Buffer> Buffers;
    std::vector<TRecordBatchBodyPart> Parts;
};

template <class T>
TMutableRange<T> GetTypedValues(TMutableRef ref)
{
    return MakeMutableRange(
        reinterpret_cast<T*>(ref.Begin()),
        reinterpret_cast<T*>(ref.End()));
}

void SerializeColumnPrologue(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    if (column->NullBitmap ||
        column->Rle && column->Rle->ValueColumn->NullBitmap)
    {
        if (column->Rle) {
            const auto* valueColumn = column->Rle->ValueColumn;
            auto rleIndexes = column->GetTypedValues<ui64>();

            context->AddFieldNode(
                column->ValueCount,
                CountOnesInRleBitmap(
                    valueColumn->NullBitmap->Data,
                    rleIndexes,
                    column->StartIndex,
                    column->StartIndex + column->ValueCount));

            context->AddBuffer(
                GetBitmapByteSize(column->ValueCount),
                [=] (TMutableRef dstRef) {
                    BuildValidityBitmapFromRleNullBitmap(
                        valueColumn->NullBitmap->Data,
                        rleIndexes,
                        column->StartIndex,
                        column->StartIndex + column->ValueCount,
                        dstRef);
                });
        } else {
            context->AddFieldNode(
                column->ValueCount,
                CountOnesInBitmap(
                    column->NullBitmap->Data,
                    column->StartIndex,
                    column->StartIndex + column->ValueCount));

            context->AddBuffer(
                GetBitmapByteSize(column->ValueCount),
                [=] (TMutableRef dstRef) {
                    CopyBitmapRangeToBitmapNegated(
                        column->NullBitmap->Data,
                        column->StartIndex,
                        column->StartIndex + column->ValueCount,
                        dstRef);
                });
        }
    } else {
        context->AddFieldNode(
            column->ValueCount,
            0);

        context->AddBuffer(
            0,
            [=] (TMutableRef /*dstRef*/) {
            });
    }
}

void SerializeRleButNotDictionaryEncodedStringLikeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BitWidth == 64);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);

    YT_LOG_DEBUG("Adding RLE but not dictionary-encoded string-like column (ColumnId: %v, StartIndex: %v, ValueCount: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount);

    SerializeColumnPrologue(typedColumn, context);

    auto rleIndexes = column->GetTypedValues<ui64>();

    context->AddBuffer(
        sizeof(ui32) * column->ValueCount,
        [=] (TMutableRef dstRef) {
            BuildIotaDictionaryIndexesFromRleIndexes(
                rleIndexes,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                GetTypedValues<ui32>(dstRef));
        });
}

void SerializeDictionaryColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Dictionary->ZeroMeansNull);
    YT_VERIFY(column->Values->BitWidth == 32);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);

    YT_LOG_DEBUG("Adding dictionary column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    auto relevantDictionaryIndexes = column->GetRelevantTypedValues<ui32>();

    context->AddFieldNode(
        column->ValueCount,
        CountNullsInDictionaryIndexesWithZeroNull(relevantDictionaryIndexes));

    context->AddBuffer(
        GetBitmapByteSize(column->ValueCount),
        [=] (TMutableRef dstRef) {
            BuildValidityBitmapFromDictionaryIndexesWithZeroNull(
                relevantDictionaryIndexes,
                dstRef);
        });

    context->AddBuffer(
        sizeof(ui32) * column->ValueCount,
        [=] (TMutableRef dstRef) {
            BuildDictionaryIndexesFromDictionaryIndexesWithZeroNull(
                relevantDictionaryIndexes,
                GetTypedValues<ui32>(dstRef));
        });
}

void SerializeRleDictionaryColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BitWidth == 64);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);
    YT_VERIFY(column->Rle->ValueColumn->Dictionary->ZeroMeansNull);
    YT_VERIFY(column->Rle->ValueColumn->Values->BitWidth == 32);
    YT_VERIFY(column->Rle->ValueColumn->Values->BaseValue == 0);
    YT_VERIFY(!column->Rle->ValueColumn->Values->ZigZagEncoded);

    YT_LOG_DEBUG("Adding dictionary column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    auto dictionaryIndexes = column->Rle->ValueColumn->GetTypedValues<ui32>();
    auto rleIndexes = column->GetTypedValues<ui64>();

    context->AddFieldNode(
        column->ValueCount,
        CountNullsInRleDictionaryIndexesWithZeroNull(
            dictionaryIndexes,
            rleIndexes,
            column->StartIndex,
            column->StartIndex + column->ValueCount));

    context->AddBuffer(
        GetBitmapByteSize(column->ValueCount),
        [=] (TMutableRef dstRef) {
            BuildValidityBitmapFromRleDictionaryIndexesWithZeroNull(
                dictionaryIndexes,
                rleIndexes,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                dstRef);
        });

    context->AddBuffer(
        sizeof(ui32) * column->ValueCount,
        [=] (TMutableRef dstRef) {
            BuildDictionaryIndexesFromRleDictionaryIndexesWithZeroNull(
                dictionaryIndexes,
                rleIndexes,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                GetTypedValues<ui32>(dstRef));
        });
}

void SerializeIntegerColumn(
    const TTypedBatchColumn& typedColumn,
    ESimpleLogicalValueType simpleType,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);

    YT_LOG_DEBUG("Adding integer column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        column->ValueCount * GetIntegralLikeTypeByteSize(simpleType),
        [=] (TMutableRef dstRef) {
            const auto* valueColumn = column->Rle
                ? column->Rle->ValueColumn
                : column;
            auto values = valueColumn->GetTypedValues<ui64>();

            auto rleIndexes = column->Rle
                ? column->GetTypedValues<ui64>()
                : TRange<ui64>();

            auto startIndex = column->StartIndex;

            switch (simpleType) {
#define XX(cppType, ytType)                                 \
    case ESimpleLogicalValueType::ytType: {                 \
        auto dstValues = GetTypedValues<cppType>(dstRef);   \
        auto* currentOutput = dstValues.Begin();            \
        DecodeIntegerVector(                                \
            startIndex,                                     \
            startIndex + column->ValueCount,                \
            valueColumn->Values->BaseValue,                 \
            valueColumn->Values->ZigZagEncoded,             \
            TRange<ui32>(),                                 \
            rleIndexes,                                     \
            [&] (auto index) {                              \
                return values[index];                       \
            },                                              \
            [&] (auto value) {                              \
                *currentOutput++ = value;                   \
            });                                             \
        break;                                              \
    }

                XX(i8, Int8)
                XX(i16, Int16)
                XX(i32, Int32)
                XX(i64, Int64)
                XX(ui8, Uint8)
                XX(ui16, Uint16)
                XX(ui32, Uint32)
                XX(ui64, Uint64)
                XX(i64, Interval)

#undef XX

                default:
                    THROW_ERROR_EXCEPTION("Integer column %v has unexpected type %Qlv",
                        typedColumn.Column->Id,
                        simpleType);
            }
        });
}

void SerializeDateColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);

    YT_LOG_DEBUG("Adding data column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        column->ValueCount * sizeof(i32),
        [=] (TMutableRef dstRef) {
            const auto* valueColumn = column->Rle
                ? column->Rle->ValueColumn
                : column;
            auto values = valueColumn->GetTypedValues<ui64>();

            auto rleIndexes = column->Rle
                ? column->GetTypedValues<ui64>()
                : TRange<ui64>();

            auto startIndex = column->StartIndex;

            auto dstValues = GetTypedValues<i32>(dstRef);
            auto* currentOutput = dstValues.Begin();
            DecodeIntegerVector(
                startIndex,
                startIndex + column->ValueCount,
                valueColumn->Values->BaseValue,
                valueColumn->Values->ZigZagEncoded,
                TRange<ui32>(),
                rleIndexes,
                [&] (auto index) {
                    return values[index];
                },
                [&] (auto value) {
                    if (value > std::numeric_limits<i32>::max()) {
                        THROW_ERROR_EXCEPTION("Date value cannot be represented in arrow (Value: %v, MaxAllowedValue: %v)", value, std::numeric_limits<i32>::max());
                    }
                    *currentOutput++ = value;
                });
        });
}

void SerializeDatetimeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    const auto maxAllowedValue = std::numeric_limits<i64>::max() / 1000;
    YT_VERIFY(column->Values);

    YT_LOG_DEBUG("Adding datetime column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        column->ValueCount * sizeof(i64),
        [=] (TMutableRef dstRef) {
            const auto* valueColumn = column->Rle
                ? column->Rle->ValueColumn
                : column;
            auto values = valueColumn->GetTypedValues<ui64>();

            auto rleIndexes = column->Rle
                ? column->GetTypedValues<ui64>()
                : TRange<ui64>();

            auto startIndex = column->StartIndex;

            auto dstValues = GetTypedValues<i64>(dstRef);
            auto* currentOutput = dstValues.Begin();
            DecodeIntegerVector(
                startIndex,
                startIndex + column->ValueCount,
                valueColumn->Values->BaseValue,
                valueColumn->Values->ZigZagEncoded,
                TRange<ui32>(),
                rleIndexes,
                [&] (auto index) {
                    return values[index];
                },
                [&] (auto value) {
                    if (value > maxAllowedValue) {
                        THROW_ERROR_EXCEPTION("Datetime value cannot be represented in arrow (Value: %v, MaxAllowedValue: %v)", value, maxAllowedValue);
                    }
                    *currentOutput++ = value * 1000;
                });
        });
}

void SerializeTimestampColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);

    YT_LOG_DEBUG("Adding timestamp column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        column->ValueCount * sizeof(i64),
        [=] (TMutableRef dstRef) {
            const auto* valueColumn = column->Rle
                ? column->Rle->ValueColumn
                : column;
            auto values = valueColumn->GetTypedValues<ui64>();

            auto rleIndexes = column->Rle
                ? column->GetTypedValues<ui64>()
                : TRange<ui64>();

            auto startIndex = column->StartIndex;

            auto dstValues = GetTypedValues<i64>(dstRef);
            auto* currentOutput = dstValues.Begin();
            DecodeIntegerVector(
                startIndex,
                startIndex + column->ValueCount,
                valueColumn->Values->BaseValue,
                valueColumn->Values->ZigZagEncoded,
                TRange<ui32>(),
                rleIndexes,
                [&] (auto index) {
                    return values[index];
                },
                [&] (auto value) {
                    if (value > std::numeric_limits<i64>::max()) {
                        THROW_ERROR_EXCEPTION("Timestamp value cannot be represented in arrow (Value: %v, MaxAllowedValue: %v)", value, std::numeric_limits<i64>::max());
                    }
                    *currentOutput++ = value;
                });
        });
}

void SerializeDoubleColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BitWidth == 64);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);

    YT_LOG_DEBUG(
        "Adding double column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        column->ValueCount * sizeof(double),
        [=] (TMutableRef dstRef) {
            auto relevantValues = column->GetRelevantTypedValues<double>();
            ::memcpy(
                dstRef.Begin(),
                relevantValues.Begin(),
                column->ValueCount * sizeof(double));
        });
}

void SerializeFloatColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BitWidth == 32);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(!column->Values->ZigZagEncoded);

    YT_LOG_DEBUG(
        "Adding float column (ColumnId: %v, StartIndex: %v, ValueCount: %v, Rle: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        column->Rle.has_value());

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        column->ValueCount * sizeof(float),
        [=] (TMutableRef dstRef) {
            auto relevantValues = column->GetRelevantTypedValues<float>();
            ::memcpy(
                dstRef.Begin(),
                relevantValues.Begin(),
                column->ValueCount * sizeof(float));
        });
}

void SerializeStringLikeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(column->Values->BitWidth == 32);
    YT_VERIFY(column->Values->ZigZagEncoded);
    YT_VERIFY(column->Strings);
    YT_VERIFY(column->Strings->AvgLength);
    YT_VERIFY(!column->Rle);

    auto startIndex = column->StartIndex;
    auto endIndex = startIndex + column->ValueCount;
    auto stringData = column->Strings->Data;
    auto avgLength = *column->Strings->AvgLength;

    auto offsets = column->GetTypedValues<ui32>();
    auto startOffset = DecodeStringOffset(offsets, avgLength, startIndex);
    auto endOffset = DecodeStringOffset(offsets, avgLength, endIndex);
    auto stringsSize = endOffset - startOffset;

    YT_LOG_DEBUG("Adding string-like column (ColumnId: %v, StartIndex: %v, ValueCount: %v, StartOffset: %v, EndOffset: %v, StringsSize: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        startOffset,
        endOffset,
        stringsSize);

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        sizeof(i32) * (column->ValueCount + 1),
        [=] (TMutableRef dstRef) {
            DecodeStringOffsets(
                offsets,
                avgLength,
                startIndex,
                endIndex,
                GetTypedValues<ui32>(dstRef));
        });

    context->AddBuffer(
        stringsSize,
        [=] (TMutableRef dstRef) {
            ::memcpy(
                dstRef.Begin(),
                stringData.Begin() + startOffset,
                stringsSize);
        });
}

void SerializeBooleanColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;
    YT_VERIFY(column->Values);
    YT_VERIFY(!column->Values->ZigZagEncoded);
    YT_VERIFY(column->Values->BaseValue == 0);
    YT_VERIFY(column->Values->BitWidth == 1);

    YT_LOG_DEBUG("Adding boolean column (ColumnId: %v, StartIndex: %v, ValueCount: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount);

    SerializeColumnPrologue(typedColumn, context);

    context->AddBuffer(
        GetBitmapByteSize(column->ValueCount),
        [=] (TMutableRef dstRef) {
            CopyBitmapRangeToBitmap(
                column->Values->Data,
                column->StartIndex,
                column->StartIndex + column->ValueCount,
                dstRef);
        });
}

void SerializeNullColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    SerializeColumnPrologue(typedColumn, context);
}

void SerializeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    const auto* column = typedColumn.Column;

    if (IsRleButNotDictionaryEncodedStringLikeColumn(*typedColumn.Column)) {
        SerializeRleButNotDictionaryEncodedStringLikeColumn(typedColumn, context);
        return;
    }

    if (column->Dictionary) {
        SerializeDictionaryColumn(typedColumn, context);
        return;
    }

    if (column->Rle && column->Rle->ValueColumn->Dictionary) {
        SerializeRleDictionaryColumn(typedColumn, context);
        return;
    }

    auto simpleType = CastToV1Type(typedColumn.Type).first;
    if (IsIntegralType(simpleType)) {
        SerializeIntegerColumn(typedColumn, simpleType, context);
    } else if (simpleType == ESimpleLogicalValueType::Interval) {
        SerializeIntegerColumn(typedColumn, simpleType, context);
    }  else if (simpleType == ESimpleLogicalValueType::Date) {
        SerializeDateColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Datetime) {
        SerializeDatetimeColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Timestamp) {
        SerializeTimestampColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Double) {
        SerializeDoubleColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Float) {
        SerializeFloatColumn(typedColumn, context);
    } else if (IsStringLikeType(simpleType)) {
        SerializeStringLikeColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Boolean) {
        SerializeBooleanColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Null) {
        SerializeNullColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Void) {
        SerializeNullColumn(typedColumn, context);
    } else {
        THROW_ERROR_EXCEPTION("Column %v has unexpected type %Qlv",
            typedColumn.Column->Id,
            simpleType);
    }
}

auto SerializeRecordBatch(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    int length,
    TRange<TTypedBatchColumn> typedColumns)
{
    auto context = New<TRecordBatchSerializationContext>(flatbufBuilder);

    for (const auto& typedColumn : typedColumns) {
        SerializeColumn(typedColumn, context.Get());
    }

    auto fieldNodesOffset = flatbufBuilder->CreateVectorOfStructs(context->FieldNodes);

    auto buffersOffset = flatbufBuilder->CreateVectorOfStructs(context->Buffers);

    auto recordBatchOffset = org::apache::arrow::flatbuf::CreateRecordBatch(
        *flatbufBuilder,
        length,
        fieldNodesOffset,
        buffersOffset);

    auto totalSize = context->CurrentBodyOffset;

    return std::tuple(
        recordBatchOffset,
        totalSize,
        [context = std::move(context)] (TMutableRef dstRef) {
            char* current = dstRef.Begin();
            for (const auto& part : context->Parts) {
                part.Writer(TMutableRef(current, current + part.Size));
                current += AlignUp<i64>(part.Size, ArrowAlignment);
            }
            YT_VERIFY(current == dstRef.End());
        });
}
///////////////////////////////////////////////////////////////////////////////

class TArrowWriter
    : public TSchemalessFormatWriterBase
{
public:
    TArrowWriter(
        TNameTablePtr nameTable,
        const std::vector<NTableClient::TTableSchemaPtr>& tableSchemas,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount)
        : TSchemalessFormatWriterBase(
            std::move(nameTable),
            std::move(output),
            enableContextSaving,
            std::move(controlAttributesConfig),
            keyColumnCount)
    {
        YT_VERIFY(tableSchemas.size() > 0);

        ColumnConverters_.resize(tableSchemas.size());
        TableCount_ = tableSchemas.size();
        ColumnSchemas_.resize(tableSchemas.size());
        TableIdToIndex_.resize(tableSchemas.size());
        IsFirstBatchForSpecificTable_.assign(tableSchemas.size(), false);

        for (int tableIndex = 0; tableIndex < std::ssize(tableSchemas); ++tableIndex) {
            for (const auto& columnSchema : tableSchemas[tableIndex]->Columns()) {
                auto columnId = NameTable_->GetIdOrRegisterName(columnSchema.Name());
                ColumnSchemas_[tableIndex][columnId] = columnSchema;
            }
            if (CheckColumnInNameTable(GetRangeIndexColumnId())) {
                ColumnSchemas_[tableIndex][GetRangeIndexColumnId()] = GetSystemColumnSchema(NameTable_->GetName(GetRangeIndexColumnId()), GetRangeIndexColumnId());
            }
            if (CheckColumnInNameTable(GetRowIndexColumnId())) {
                ColumnSchemas_[tableIndex][GetRowIndexColumnId()] = GetSystemColumnSchema(NameTable_->GetName(GetRowIndexColumnId()), GetRowIndexColumnId());
            }
            if (CheckColumnInNameTable(GetTableIndexColumnId())) {
                ColumnSchemas_[tableIndex][GetTableIndexColumnId()] = GetSystemColumnSchema(NameTable_->GetName(GetTableIndexColumnId()), GetTableIndexColumnId());
            }
            if (CheckColumnInNameTable(GetTabletIndexColumnId())) {
                ColumnSchemas_[tableIndex][GetTabletIndexColumnId()] = GetSystemColumnSchema(NameTable_->GetName(GetTabletIndexColumnId()), GetTabletIndexColumnId());
            }
        }
    }

private:
    void Reset()
    {
        Messages_.clear();
        TypedColumns_.clear();
        RowCount_ = 0;
    }

    void WriteEndOfStream() override
    {
        auto output = GetOutputStream();
        ui32 zero = 0;
        output->Write(&zero, sizeof(zero));
    }

    bool CheckColumnInNameTable(int columnIndex) const
    {
        return columnIndex >= 0 && columnIndex < NameTable_->GetSize();
    }

    void WriteRowsForSingleTable(TRange<TUnversionedRow> rows, i32 tableIndex)
    {
        Reset();
        auto convertedColumns = ColumnConverters_[tableIndex].ConvertRowsToColumns(rows, ColumnSchemas_[tableIndex]);
        std::vector<const TBatchColumn*> rootColumns;
        rootColumns.reserve( std::ssize(convertedColumns));
        for (ssize_t columnIndex = 0; columnIndex < std::ssize(convertedColumns); columnIndex++) {
            rootColumns.push_back(convertedColumns[columnIndex].RootColumn);
        }
        RowCount_ = rows.size();
        PrepareColumns(rootColumns, tableIndex);
        Encode(tableIndex);
    }

    void DoWrite(TRange<TUnversionedRow> rows) override
    {
        Reset();

        ssize_t sameTableRangeBeginRowIndex = 0;
        int tableIndex = 0;

        for (ssize_t rowIndex = 0; rowIndex < std::ssize(rows); rowIndex++) {
            int currentTableIndex = -1;
            if (TableCount_ > 1) {
                const auto& elems = rows[rowIndex].Elements();
                for (ssize_t columnIndex = std::ssize(elems) - 1; columnIndex >= 0; --columnIndex) {
                    if (elems[columnIndex].Id == GetTableIndexColumnId()) {
                        currentTableIndex = elems[columnIndex].Data.Int64;
                        break;
                    }
                }
            } else {
                currentTableIndex = 0;
            }
            YT_VERIFY(currentTableIndex < TableCount_ && currentTableIndex >= 0);
            if (tableIndex != currentTableIndex && rowIndex != 0) {
                auto currentRows = rows.Slice(sameTableRangeBeginRowIndex, rowIndex);
                WriteRowsForSingleTable(currentRows, tableIndex);
                sameTableRangeBeginRowIndex = rowIndex;
            }
            tableIndex = currentTableIndex;
        }

        auto currentRows = rows.Slice(sameTableRangeBeginRowIndex, rows.size());
        WriteRowsForSingleTable(currentRows, tableIndex);
        ++EncodedRowBatchCount_;
    }

    void DoWriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch) override
    {
        auto columnarBatch = rowBatch->TryAsColumnar();
        if (!columnarBatch) {
            DoWrite(rowBatch->MaterializeRows());
            return;
        }
        int tableIndex = 0;
        auto batchColumns = columnarBatch->MaterializeColumns();

        if (TableCount_ > 1) {
            tableIndex = -1;
            for (const auto* column : batchColumns) {
                if (column->Id == GetTableIndexColumnId()) {
                    tableIndex = ExtractTableIndexFromColumn(column);
                    break;
                }
            }
            YT_VERIFY(tableIndex < TableCount_ && tableIndex >= 0);
        }

        Reset();
        RowCount_ = rowBatch->GetRowCount();
        PrepareColumns(batchColumns, tableIndex);
        Encode(tableIndex);
        ++EncodedColumnarBatchCount_;
    }

    void Encode(int tableIndex)
    {
        auto output = GetOutputStream();
        if (tableIndex != PrevTableIndex_ || IsSchemaMessageNeeded()) {
            PrevTableIndex_ = tableIndex;
            if (!IsFirstBatch_) {
                RegisterEosMarker();
            }
            ResetArrowDictionaries();
            PrepareSchema(tableIndex);
        }
        IsFirstBatch_ = false;
        PrepareDictionaryBatches();
        PrepareRecordBatch();

        WritePayload(output);
        TryFlushBuffer(true);
    }

    i64 GetEncodedRowBatchCount() const override
    {
        return EncodedRowBatchCount_;
    }

    i64 GetEncodedColumnarBatchCount() const override
    {
        return EncodedColumnarBatchCount_;
    }

private:
    int TableCount_ = 0;
    bool IsFirstBatch_ = true;
    i64 PrevTableIndex_ = 0;
    i64 RowCount_ = 0;
    std::vector<TTypedBatchColumn> TypedColumns_;
    std::vector<THashMap<int, TColumnSchema>> ColumnSchemas_;
    std::vector<IUnversionedColumnarRowBatch::TDictionaryId> ArrowDictionaryIds_;
    std::vector<NColumnConverters::TColumnConverters> ColumnConverters_;
    std::vector<THashMap<int, int>> TableIdToIndex_;
    std::vector<bool> IsFirstBatchForSpecificTable_;

    i64 EncodedRowBatchCount_ = 0;
    i64 EncodedColumnarBatchCount_ = 0;

    struct TMessage
    {
        std::optional<flatbuffers::FlatBufferBuilder> FlatbufBuilder;
        i64 BodySize;
        TBodyWriter BodyWriter;
    };

    std::vector<TMessage> Messages_;

    bool CheckIfSystemColumnEnable(int columnIndex) const
    {
        return ControlAttributesConfig_->EnableTableIndex && IsTableIndexColumnId(columnIndex) ||
            ControlAttributesConfig_->EnableRangeIndex && IsRangeIndexColumnId(columnIndex) ||
            ControlAttributesConfig_->EnableRowIndex && IsRowIndexColumnId(columnIndex) ||
            ControlAttributesConfig_->EnableTabletIndex && IsTabletIndexColumnId(columnIndex);
    }

    bool IsColumnNeedsToAdd(int columnIndex) const
    {
        return !IsSystemColumnId(columnIndex)
            || (CheckIfSystemColumnEnable(columnIndex) && !IsTableIndexColumnId(columnIndex));
    }

    TColumnSchema GetSystemColumnSchema(TStringBuf name, int columnIndex)
    {
        if (CheckIfSystemColumnEnable(columnIndex) && !IsTableIndexColumnId(columnIndex)) {
            return TColumnSchema(TString(name), EValueType::Int64);
        }
        return TColumnSchema(TString(name), EValueType::Null);
    }

    void PrepareColumns(const TRange<const TBatchColumn*>& batchColumns, int tableIndex)
    {
        if (!IsFirstBatchForSpecificTable_[tableIndex]) {
            std::vector<bool> idExists(NameTable_->GetSize(), false);
            for (const auto* column : batchColumns) {
                if (IsColumnNeedsToAdd(column->Id)) {
                    idExists[column->Id] = true;
                }
            }
            int currentIndex = 0;
            for (int columnId = 0; columnId < NameTable_->GetSize(); columnId++) {
                if (idExists[columnId]) {
                    TableIdToIndex_[tableIndex][columnId] = currentIndex;
                    currentIndex++;
                }
            }
            IsFirstBatchForSpecificTable_[tableIndex] = true;
        }
        TypedColumns_.resize(TableIdToIndex_[tableIndex].size());
        for (const auto* column : batchColumns) {
            if (IsColumnNeedsToAdd(column->Id)) {
                auto iterIndex = TableIdToIndex_[tableIndex].find(column->Id);
                YT_VERIFY(iterIndex != TableIdToIndex_[tableIndex].end());

                auto iterSchema = ColumnSchemas_[tableIndex].find(column->Id);
                YT_VERIFY(iterSchema != ColumnSchemas_[tableIndex].end());
                TypedColumns_[iterIndex->second] = TTypedBatchColumn{
                    column,
                    iterSchema->second.LogicalType()
                };
            }
        }
    }

    bool IsSchemaMessageNeeded()
    {
        if (IsFirstBatch_) {
            return true;
        }
        YT_VERIFY(ArrowDictionaryIds_.size() == TypedColumns_.size());
        bool result = false;
        for (int index = 0; index < std::ssize(TypedColumns_); ++index) {
            bool currentDictionary = IsDictionaryEncodedColumn(*TypedColumns_[index].Column);
            bool previousDictionary = ArrowDictionaryIds_[index] != IUnversionedColumnarRowBatch::NullDictionaryId;
            if (currentDictionary != previousDictionary) {
                result = true;
            }
        }
        return result;
    }

    void ResetArrowDictionaries()
    {
        ArrowDictionaryIds_.assign(TypedColumns_.size(), IUnversionedColumnarRowBatch::NullDictionaryId);
    }

    void RegisterEosMarker()
    {
        YT_LOG_DEBUG("EOS marker registered");

        Messages_.push_back(TMessage{
                std::nullopt,
                0,
                TBodyWriter()});
    }

    void RegisterMessage(
        [[maybe_unused]] org::apache::arrow::flatbuf::MessageHeader type,
        flatbuffers::FlatBufferBuilder&& flatbufBuilder,
        i64 bodySize = 0,
        std::function<void(TMutableRef)> bodyWriter = nullptr)
    {
        YT_LOG_DEBUG("Message registered (Type: %v, MessageSize: %v, BodySize: %v)",
            org::apache::arrow::flatbuf::EnumNamesMessageHeader()[type],
            flatbufBuilder.GetSize(),
            bodySize);

        YT_VERIFY((bodySize % ArrowAlignment) == 0);
        Messages_.push_back(TMessage{
                std::move(flatbufBuilder),
                bodySize,
                std::move(bodyWriter)});
    }

    void PrepareSchema(int tableIndex)
    {
        flatbuffers::FlatBufferBuilder flatbufBuilder;

        int arrowDictionaryIdCounter = 0;
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> fieldOffsets;
        for (int columnIndex = 0; columnIndex < std::ssize(TypedColumns_); columnIndex++) {
            const auto& typedColumn = TypedColumns_[columnIndex];
            auto iterSchema = ColumnSchemas_[tableIndex].find(typedColumn.Column->Id);
            YT_VERIFY(iterSchema != ColumnSchemas_[tableIndex].end());
            auto columnSchema = iterSchema->second;
            auto nameOffset = SerializeString(&flatbufBuilder, columnSchema.Name());

            auto [typeType, typeOffset] = SerializeColumnType(&flatbufBuilder, columnSchema);

            flatbuffers::Offset<org::apache::arrow::flatbuf::DictionaryEncoding> dictionaryEncodingOffset;
            auto index_type_offset = org::apache::arrow::flatbuf::CreateInt(flatbufBuilder, 32, false);

            if (IsDictionaryEncodedColumn(*typedColumn.Column)) {
                dictionaryEncodingOffset = org::apache::arrow::flatbuf::CreateDictionaryEncoding(
                    flatbufBuilder,
                    arrowDictionaryIdCounter++,
                    index_type_offset);
            }

            auto fieldOffset = org::apache::arrow::flatbuf::CreateField(
                flatbufBuilder,
                nameOffset,
                columnSchema.LogicalType()->IsNullable(),
                typeType,
                typeOffset,
                dictionaryEncodingOffset);

            fieldOffsets.push_back(fieldOffset);
        }

        auto fieldsOffset = flatbufBuilder.CreateVector(fieldOffsets);

        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::KeyValue>> customMetadata;

        if (TableCount_ > 1) {
            auto keyValueOffsett = org::apache::arrow::flatbuf::CreateKeyValue(
                flatbufBuilder,
                flatbufBuilder.CreateString("TableId"),
                flatbufBuilder.CreateString(std::to_string(tableIndex)));
            customMetadata.push_back(keyValueOffsett);
        }

        auto schemaOffset = org::apache::arrow::flatbuf::CreateSchema(
            flatbufBuilder,
            org::apache::arrow::flatbuf::Endianness_Little,
            fieldsOffset,
            flatbufBuilder.CreateVector(customMetadata));

        auto messageOffset = org::apache::arrow::flatbuf::CreateMessage(
            flatbufBuilder,
            org::apache::arrow::flatbuf::MetadataVersion_V4,
            org::apache::arrow::flatbuf::MessageHeader_Schema,
            schemaOffset.Union(),
            0);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            org::apache::arrow::flatbuf::MessageHeader_Schema,
            std::move(flatbufBuilder));
    }

    void PrepareDictionaryBatches()
    {
        int arrowDictionaryIdCounter = 0;
        auto prepareDictionaryBatch = [&] (
            int columnIndex,
            IUnversionedColumnarRowBatch::TDictionaryId ytDictionaryId,
            const TBatchColumn* dictionaryColumn) {
            int arrowDictionaryId = arrowDictionaryIdCounter++;
            const auto& typedColumn = TypedColumns_[columnIndex];
            auto previousYTDictionaryId = ArrowDictionaryIds_[columnIndex];
            if (ytDictionaryId == previousYTDictionaryId) {
                YT_LOG_DEBUG("Reusing previous dictionary (ColumnId: %v, YTDictionaryId: %v, ArrowDictionaryId: %v)",
                    typedColumn.Column->Id,
                    ytDictionaryId,
                    arrowDictionaryId);
            } else {
                YT_LOG_DEBUG("Sending new dictionary (ColumnId: %v, YTDictionaryId: %v, ArrowDictionaryId: %v)",
                    typedColumn.Column->Id,
                    ytDictionaryId,
                    arrowDictionaryId);
                PrepareDictionaryBatch(
                    TTypedBatchColumn{dictionaryColumn, typedColumn.Type},
                    arrowDictionaryId);
                ArrowDictionaryIds_[columnIndex] = ytDictionaryId;
            }
        };

        for (int columnIndex = 0; columnIndex < std::ssize(TypedColumns_); ++columnIndex) {
            const auto& typedColumn = TypedColumns_[columnIndex];
            if (typedColumn.Column->Dictionary) {
                YT_LOG_DEBUG("Adding dictionary batch for dictionary-encoded column (ColumnId: %v)",
                    typedColumn.Column->Id);
                prepareDictionaryBatch(
                    columnIndex,
                    typedColumn.Column->Dictionary->DictionaryId,
                    typedColumn.Column->Dictionary->ValueColumn);
            } else if (IsRleButNotDictionaryEncodedStringLikeColumn(*typedColumn.Column)) {
                YT_LOG_DEBUG("Adding dictionary batch for RLE but not dictionary-encoded string-like column (ColumnId: %v)",
                    typedColumn.Column->Id);
                prepareDictionaryBatch(
                    columnIndex,
                    IUnversionedColumnarRowBatch::GenerateDictionaryId(), // any unique one will do
                    typedColumn.Column->Rle->ValueColumn);
            } else if (IsRleAndDictionaryEncodedColumn(*typedColumn.Column)) {
                YT_LOG_DEBUG("Adding dictionary batch for RLE and dictionary-encoded column (ColumnId: %v)",
                    typedColumn.Column->Id);
                prepareDictionaryBatch(
                    columnIndex,
                    typedColumn.Column->Rle->ValueColumn->Dictionary->DictionaryId,
                    typedColumn.Column->Rle->ValueColumn->Dictionary->ValueColumn);
            }
        }
    }

    void PrepareDictionaryBatch(
        const TTypedBatchColumn& typedColumn,
        int arrowDictionaryId)
    {
        flatbuffers::FlatBufferBuilder flatbufBuilder;

        auto [recordBatchOffset, bodySize, bodyWriter] = SerializeRecordBatch(
            &flatbufBuilder,
            typedColumn.Column->ValueCount,
            MakeRange({typedColumn}));

        auto dictionaryBatchOffset = org::apache::arrow::flatbuf::CreateDictionaryBatch(
            flatbufBuilder,
            arrowDictionaryId,
            recordBatchOffset);

        auto messageOffset = org::apache::arrow::flatbuf::CreateMessage(
            flatbufBuilder,
            org::apache::arrow::flatbuf::MetadataVersion_V4,
            org::apache::arrow::flatbuf::MessageHeader_DictionaryBatch,
            dictionaryBatchOffset.Union(),
            bodySize);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            org::apache::arrow::flatbuf::MessageHeader_DictionaryBatch,
            std::move(flatbufBuilder),
            bodySize,
            std::move(bodyWriter));
    }

    void PrepareRecordBatch()
    {
        flatbuffers::FlatBufferBuilder flatbufBuilder;

        auto [recordBatchOffset, bodySize, bodyWriter] = SerializeRecordBatch(
            &flatbufBuilder,
            RowCount_,
            TypedColumns_);

        auto messageOffset = org::apache::arrow::flatbuf::CreateMessage(
            flatbufBuilder,
            org::apache::arrow::flatbuf::MetadataVersion_V4,
            org::apache::arrow::flatbuf::MessageHeader_RecordBatch,
            recordBatchOffset.Union(),
            bodySize);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            org::apache::arrow::flatbuf::MessageHeader_RecordBatch,
            std::move(flatbufBuilder),
            bodySize,
            std::move(bodyWriter));
    }

    i64 GetPayloadSize() const
    {
        i64 size = 0;
        for (const auto& message : Messages_) {
            size += sizeof(ui32); // continuation indicator
            size += sizeof(ui32); // metadata size
            if (message.FlatbufBuilder) {
                size += AlignUp<i64>(message.FlatbufBuilder->GetSize(), ArrowAlignment); // metadata message
                size += AlignUp<i64>(message.BodySize, ArrowAlignment);                  // body
            }
        }
        return size;
    }

    void WritePayload(TBlobOutput* output)
    {
        YT_LOG_DEBUG("Started writing payload");
        for (const auto& message : Messages_) {
            // Continuation indicator
            ui32 constMax = 0xFFFFFFFF;
            output->Write(&constMax, sizeof(ui32));

            if (message.FlatbufBuilder) {
                auto metadataSize = message.FlatbufBuilder->GetSize();

                auto* metadataPtr = message.FlatbufBuilder->GetBufferPointer();

                ui32 metadataAlignSize = AlignUp<i64>(metadataSize, ArrowAlignment);

                output->Write(&metadataAlignSize, sizeof(ui32));
                output->Write(metadataPtr, metadataSize);

                output->Write(AlignmentString.Data(), metadataAlignSize - metadataSize);

                // Body
                if (message.BodyWriter) {
                    auto bodyBuffer = output->RequestBuffer(AlignUp<i64>(message.BodySize, ArrowAlignment));
                    message.BodyWriter(bodyBuffer.Slice(0, message.BodySize));
                    std::fill(bodyBuffer.Begin() + message.BodySize, bodyBuffer.End(), 0);
                } else {
                    YT_VERIFY(message.BodySize == 0);
                }
            } else {
                // EOS marker
                ui32 zero = 0;
                output->Write(&zero, sizeof(ui32));
            }
        }

        YT_LOG_DEBUG("Finished writing payload");
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForArrow(
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto result = New<TArrowWriter>(
        std::move(nameTable),
        schemas,
        std::move(output),
        enableContextSaving,
        std::move(controlAttributesConfig),
        keyColumnCount);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
