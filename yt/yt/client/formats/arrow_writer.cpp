#include "arrow_writer.h"

#include "public.h"
#include "schemaless_writer_adapter.h"

#include <yt/yt/client/arrow/fbs/Message.fbs.h>
#include <yt/yt/client/arrow/fbs/Schema.fbs.h>

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

static const auto& Logger = FormatsLogger;

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
            return std::make_tuple(
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
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Int,
                org::apache::arrow::flatbuf::CreateInt(
                    *flatbufBuilder,
                    GetIntegralTypeBitWidth(simpleType),
                    IsIntegralTypeSigned(simpleType))
                    .Union());

        case ESimpleLogicalValueType::Double:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_FloatingPoint,
                org::apache::arrow::flatbuf::CreateFloatingPoint(
                    *flatbufBuilder,
                    org::apache::arrow::flatbuf::Precision_DOUBLE)
                    .Union());

        case ESimpleLogicalValueType::Boolean:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Bool,
                org::apache::arrow::flatbuf::CreateBool(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Binary,
                org::apache::arrow::flatbuf::CreateBinary(*flatbufBuilder)
                    .Union());

        case ESimpleLogicalValueType::Utf8:
            return std::make_tuple(
                org::apache::arrow::flatbuf::Type_Utf8,
                org::apache::arrow::flatbuf::CreateUtf8(*flatbufBuilder)
                    .Union());

            // TODO(babenko): the following types are not supported:
            //   Date
            //   Datetime
            //   Interval
            //   Timestamp

        default:
            THROW_ERROR_EXCEPTION("Column %v has type %Qlv that is not currently supported by Arrow encoder",
                schema.GetDiagnosticNameString(),
                simpleType);
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
        column->ValueCount * GetIntegralTypeByteSize(simpleType),
        [=] (TMutableRef dstRef) {
            const auto* valueColumn = column->Rle
                ? column->Rle->ValueColumn
                : column;
            auto values = valueColumn->GetTypedValues<ui64>();

            auto rleIndexes = column->Rle
                ? column->GetTypedValues<ui64>()
                : TRange<ui64>();

            switch (simpleType) {
#define XX(cppType, ytType)                               \
    case ESimpleLogicalValueType::ytType: {               \
        auto dstValues = GetTypedValues<cppType>(dstRef); \
        auto* currentOutput = dstValues.Begin();          \
        DecodeIntegerVector(                              \
            column->StartIndex,                           \
            column->StartIndex + column->ValueCount,      \
            valueColumn->Values->BaseValue,               \
            valueColumn->Values->ZigZagEncoded,           \
            TRange<ui32>(),                               \
            rleIndexes,                                   \
            [&] (auto index) {                            \
                return values[index];                     \
            },                                            \
            [&] (auto value) {                            \
                *currentOutput++ = value;                 \
            });                                           \
        break;                                            \
    }

                XX(i8, Int8)
                XX(i16, Int16)
                XX(i32, Int32)
                XX(i64, Int64)
                XX(ui8, Uint8)
                XX(ui16, Uint16)
                XX(ui32, Uint32)
                XX(ui64, Uint64)

#undef XX

                default:
                    THROW_ERROR_EXCEPTION("Integer column %v has unexpected type %Qlv",
                        typedColumn.Column->Id,
                        simpleType);
            }
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

    YT_LOG_DEBUG("Adding double column (ColumnId: %v, StartIndex: %v, ValueCount: %v)",
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
    } else if (simpleType == ESimpleLogicalValueType::Double) {
        SerializeDoubleColumn(typedColumn, context);
    } else if (IsStringLikeType(simpleType)) {
        SerializeStringLikeColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Boolean) {
        SerializeBooleanColumn(typedColumn, context);
    } else if (simpleType == ESimpleLogicalValueType::Null) {
        // No buffers are allocated for null columns.
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

    return std::make_tuple(
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

        auto tableSchema = tableSchemas[0];
        auto columnCount = NameTable_->GetSize();

        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            ColumnSchemas_.push_back(GetColumnSchema(tableSchema, columnIndex));
        }
    }

private:
    void Reset()
    {
        Messages_.clear();
        TypedColumns_.clear();
        NumberOfRows_ = 0;
    }

    void DoWrite(TRange<TUnversionedRow> rows) override
    {
        Reset();

        auto convertedColumns = NColumnConverters::ConvertRowsToColumns(rows, ColumnSchemas_);

        std::vector<const TBatchColumn*> rootColumns;
        rootColumns.reserve( std::ssize(convertedColumns));
        for (ssize_t columnIndex = 0; columnIndex < std::ssize(convertedColumns); columnIndex++) {
            rootColumns.push_back(convertedColumns[columnIndex].RootColumn);
        }
        NumberOfRows_ = rows.size();
        PrepareColumns(rootColumns);
        Encode();
    }

    void DoWriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch) override
    {
        auto columnarBatch = rowBatch->TryAsColumnar();
        if (!columnarBatch) {
            YT_LOG_DEBUG("Encoding non-columnar batch; running write rows");
            DoWrite(rowBatch->MaterializeRows());
        } else {
            YT_LOG_DEBUG("Encoding columnar batch");
            Reset();
            NumberOfRows_ = rowBatch->GetRowCount();
            PrepareColumns(columnarBatch->MaterializeColumns());
            Encode();
        }
    }

    void Encode()
    {
        auto output = GetOutputStream();
        if (IsSchemaMessageNeeded()) {
            if (!IsFirstBatch_) {
                RegisterEosMarker();
            }
            ResetArrowDictionaries();
            PrepareSchema();
        }
        IsFirstBatch_ = false;
        PrepareDictionaryBatches();
        PrepareRecordBatch();

        WritePayload(output);
        TryFlushBuffer(true);
    }

private:
    bool IsFirstBatch_ = true;
    size_t NumberOfRows_ = 0;
    std::vector<TTypedBatchColumn> TypedColumns_;
    std::vector<TColumnSchema> ColumnSchemas_;
    std::vector<IUnversionedColumnarRowBatch::TDictionaryId> ArrowDictionaryIds_;

    struct TMessage
    {
        std::optional<flatbuffers::FlatBufferBuilder> FlatbufBuilder;
        i64 BodySize;
        TBodyWriter BodyWriter;
    };

    std::vector<TMessage> Messages_;

    bool CheckIfSystemColumnEnable(int columnIndex)
    {
        return ControlAttributesConfig_->EnableTableIndex && IsTableIndexColumnId(columnIndex) ||
            ControlAttributesConfig_->EnableRangeIndex && IsRangeIndexColumnId(columnIndex) ||
            ControlAttributesConfig_->EnableRowIndex && IsRowIndexColumnId(columnIndex) ||
            ControlAttributesConfig_->EnableTabletIndex && IsTabletIndexColumnId(columnIndex);
    }

    bool CheckIfTypeIsNotNull(int columnIndex)
    {
        YT_VERIFY(columnIndex >= 0 && columnIndex < std::ssize(ColumnSchemas_));
        return CastToV1Type(ColumnSchemas_[columnIndex].LogicalType()).first != ESimpleLogicalValueType::Null;
    }

    TColumnSchema GetColumnSchema(NTableClient::TTableSchemaPtr& tableSchema, int columnIndex)
    {
        YT_VERIFY(columnIndex >= 0);
        auto name = NameTable_->GetName(columnIndex);
        auto columnSchema = tableSchema->FindColumn(name);
        if (!columnSchema) {
            if (IsSystemColumnId(columnIndex) && CheckIfSystemColumnEnable(columnIndex)) {
                return TColumnSchema(TString(name), EValueType::Int64);
            }
            return TColumnSchema(TString(name), EValueType::Null);
        }
        return *columnSchema;
    }

    void PrepareColumns(const TRange<const TBatchColumn*>& batchColumns)
    {
        TypedColumns_.reserve(batchColumns.Size());
        for (const auto* column : batchColumns) {
            if (CheckIfTypeIsNotNull(column->Id)) {
                YT_VERIFY(column->Id >= 0 && column->Id < std::ssize(ColumnSchemas_));
                TypedColumns_.push_back(TTypedBatchColumn{
                        column,
                        ColumnSchemas_[column->Id].LogicalType()});
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

    void PrepareSchema()
    {
        flatbuffers::FlatBufferBuilder flatbufBuilder;

        int arrowDictionaryIdCounter = 0;
        std::vector<flatbuffers::Offset<org::apache::arrow::flatbuf::Field>> fieldOffsets;
        for (int columnIndex = 0; columnIndex < std::ssize(TypedColumns_); columnIndex++) {
            const auto& typedColumn = TypedColumns_[columnIndex];
            YT_VERIFY(typedColumn.Column->Id >= 0 && typedColumn.Column->Id < std::ssize(ColumnSchemas_));
            auto columnSchema = ColumnSchemas_[typedColumn.Column->Id];
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

        auto schemaOffset = org::apache::arrow::flatbuf::CreateSchema(
            flatbufBuilder,
            org::apache::arrow::flatbuf::Endianness_Little,
            fieldsOffset);

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
            NumberOfRows_,
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

                auto metadataPtr = message.FlatbufBuilder->GetBufferPointer();


                ui32 metadataSz = AlignUp<i64>(metadataSize, ArrowAlignment);

                output->Write(&metadataSz, sizeof(ui32));
                output->Write(metadataPtr, metadataSize);

                // Body
                if (message.BodyWriter) {
                    TString current;
                    current.resize(message.BodySize);
                    // Double copying.
                    message.BodyWriter(TMutableRef::FromString(current));
                    output->Write(current.data(), message.BodySize);
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
