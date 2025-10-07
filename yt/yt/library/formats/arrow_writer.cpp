#include "arrow_writer.h"
#include "arrow_metadata_constants.h"

#include "schemaless_writer_adapter.h"

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/columnar.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/column_converters/column_converter.h>

#include <yt/yt/library/tz_types/tz_types.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/range.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/data.h>

#include <contrib/libs/apache/arrow_next/cpp/src/generated/Message.fbs.h>
#include <contrib/libs/apache/arrow_next/cpp/src/generated/Schema.fbs.h>

#include <vector>

namespace NYT::NFormats {

using namespace NColumnConverters;
using namespace NComplexTypes;
using namespace NTableClient;
using namespace NTzTypes;
using namespace NYson;

using namespace org::apache::arrow20;

constinit const auto Logger = FormatsLogger;

using TBodyWriter = std::function<void(TMutableRef)>;
using TBatchColumn = IUnversionedColumnarRowBatch::TColumn;

////////////////////////////////////////////////////////////////////////////////

struct TTypedBatchColumn
{
    const TBatchColumn* Column;
    TLogicalTypePtr Type;
};

////////////////////////////////////////////////////////////////////////////////

class TArrowSchemaType
{
public:
    flatbuf::Type Type;
    flatbuffers::Offset<void> Offset;
    std::vector<flatbuffers::Offset<flatbuf::Field>> ChildrenFields;
    std::vector<flatbuffers::Offset<flatbuf::KeyValue>> CustomMetadata;

    TArrowSchemaType(
        flatbuf::Type type,
        flatbuffers::Offset<void> offset,
        std::vector<flatbuffers::Offset<flatbuf::Field>> childrenFields = {},
        std::vector<flatbuffers::Offset<flatbuf::KeyValue>> customMetadata = {})
        : Type(type)
        , Offset(offset)
        , ChildrenFields(std::move(childrenFields))
        , CustomMetadata(std::move(customMetadata))
    { }
};

// Create non-dictionary field from |TArrowSchemaType|.
flatbuffers::Offset<flatbuf::Field> CreateRegularField(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TArrowSchemaType& schemaType,
    flatbuffers::Offset<flatbuffers::String> name,
    bool nullable)
{
    return flatbuf::CreateField(
        *flatbufBuilder,
        name,
        nullable,
        schemaType.Type,
        schemaType.Offset,
        /*dictionary*/ 0,
        schemaType.ChildrenFields.empty()
            ? 0
            : flatbufBuilder->CreateVector(schemaType.ChildrenFields.data(), schemaType.ChildrenFields.size()),
        schemaType.CustomMetadata.empty()
            ? 0
            : flatbufBuilder->CreateVector(schemaType.CustomMetadata.data(), schemaType.CustomMetadata.size()));
}

////////////////////////////////////////////////////////////////////////////////

constexpr i64 ArrowAlignment = 8;
const TString AlignmentString(ArrowAlignment, 0);

flatbuffers::Offset<flatbuffers::String> SerializeString(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const std::string& str)
{
    return flatbufBuilder->CreateString(str.data(), str.length());
}

TArrowSchemaType SerializeTzType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    ESimpleLogicalValueType type,
    const TArrowFormatConfigPtr& arrowConfig)
{
    std::vector<flatbuffers::Offset<flatbuf::Field>> childrenOffset;

    // Make timestamp field.

    auto timestampOffset = flatbuf::CreateInt(
        *flatbufBuilder,
        GetTzTypeBitWidth(type),
        IsTzTypeSigned(type)).Union();

    auto timestampField = flatbuf::CreateField(
        *flatbufBuilder,
        SerializeString(flatbufBuilder, "Timestamp"),
        /*nullable*/ false,
        flatbuf::Type_Int,
        timestampOffset);

    childrenOffset.push_back(timestampField);

    if (arrowConfig->EnableTzIndex) {
        // Make tz index field.
        auto tzIndexOffset = flatbuf::CreateInt(
            *flatbufBuilder,
            /*bitWidth*/ 16,
            /*is_signed*/ false).Union();

        auto tzIndexField = flatbuf::CreateField(
            *flatbufBuilder,
            SerializeString(flatbufBuilder, "TzIndex"),
            /*nullable*/ false,
            flatbuf::Type_Int,
            tzIndexOffset);

        childrenOffset.push_back(std::move(tzIndexField));
    } else {
        // Make tz name field.
        auto tzNameOffset = flatbuf::CreateBinary(*flatbufBuilder).Union();

        auto tzNameField = flatbuf::CreateField(
            *flatbufBuilder,
            SerializeString(flatbufBuilder, "TzName"),
            /*nullable*/ false,
            flatbuf::Type_Binary,
            tzNameOffset);

        childrenOffset.push_back(std::move(tzNameField));
    }

    return {
        flatbuf::Type_Struct_,
        flatbuf::CreateStruct_(*flatbufBuilder).Union(),
        std::move(childrenOffset)
    };
}

TArrowSchemaType SerializeLeafColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    ESimpleLogicalValueType simpleType,
    const TArrowFormatConfigPtr& arrowConfig)
{
    switch (simpleType) {
        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            return {
                flatbuf::Type_Null,
                flatbuf::CreateNull(*flatbufBuilder)
                    .Union(),
                std::vector<flatbuffers::Offset<flatbuf::Field>>()
            };

        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Uint32:
            return {
                flatbuf::Type_Int,
                flatbuf::CreateInt(
                    *flatbufBuilder,
                    GetIntegralTypeBitWidth(simpleType),
                    IsIntegralTypeSigned(simpleType))
                    .Union()
            };

        case ESimpleLogicalValueType::Interval:
            return {
                flatbuf::Type_Int,
                flatbuf::CreateInt(
                    *flatbufBuilder,
                    /*bitWidth*/ 64,
                    /*is_signed*/ true)
                    .Union()
            };

        case ESimpleLogicalValueType::Date:
            return {
                flatbuf::Type_Date,
                flatbuf::CreateDate(
                    *flatbufBuilder,
                    flatbuf::DateUnit_DAY)
                    .Union()
            };

        case ESimpleLogicalValueType::Datetime:
            return {
                flatbuf::Type_Timestamp,
                flatbuf::CreateTimestamp(
                    *flatbufBuilder,
                    flatbuf::TimeUnit_SECOND)
                    .Union()
            };

        case ESimpleLogicalValueType::Timestamp:
            return {
                flatbuf::Type_Timestamp,
                flatbuf::CreateTimestamp(
                    *flatbufBuilder,
                    flatbuf::TimeUnit_MICROSECOND)
                    .Union()
            };

        case ESimpleLogicalValueType::Double:
            return {
                flatbuf::Type_FloatingPoint,
                flatbuf::CreateFloatingPoint(
                    *flatbufBuilder,
                    flatbuf::Precision_DOUBLE)
                    .Union()
            };

        case ESimpleLogicalValueType::Float:
            return {
                flatbuf::Type_FloatingPoint,
                flatbuf::CreateFloatingPoint(
                    *flatbufBuilder,
                    flatbuf::Precision_SINGLE)
                    .Union()
            };

        case ESimpleLogicalValueType::Boolean:
            return {
                flatbuf::Type_Bool,
                flatbuf::CreateBool(*flatbufBuilder)
                    .Union()
            };

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
            return {
                flatbuf::Type_Binary,
                flatbuf::CreateBinary(*flatbufBuilder)
                    .Union()
            };

        case ESimpleLogicalValueType::TzDate:
        case ESimpleLogicalValueType::TzDatetime:
        case ESimpleLogicalValueType::TzTimestamp:
        case ESimpleLogicalValueType::TzDate32:
        case ESimpleLogicalValueType::TzDatetime64:
        case ESimpleLogicalValueType::TzTimestamp64:
            return SerializeTzType(flatbufBuilder, simpleType, arrowConfig);

        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Json:
            return {
                flatbuf::Type_Utf8,
                flatbuf::CreateUtf8(*flatbufBuilder)
                    .Union()
            };

        default:
            THROW_ERROR_EXCEPTION("Type %Qlv is not currently supported by Arrow encoder",
                simpleType);
    }
}

TArrowSchemaType SerializeColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TLogicalTypePtr& type,
    const TArrowFormatConfigPtr& arrowConfig);

TArrowSchemaType SerializeStructColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TStructLogicalType& type,
    const TArrowFormatConfigPtr& arrowConfig)
{
    const auto& fields = type.GetFields();

    std::vector<flatbuffers::Offset<flatbuf::Field>> fieldOffsets;
    fieldOffsets.reserve(fields.size());

    for (const auto& [fieldName, fieldType] : fields) {
        auto fieldOffset = CreateRegularField(
            flatbufBuilder,
            /*schemaType*/ SerializeColumnType(flatbufBuilder, fieldType, arrowConfig),
            /*name*/ SerializeString(flatbufBuilder, fieldName),
            fieldType->IsNullable());

        fieldOffsets.push_back(fieldOffset);
    }

    std::vector<flatbuffers::Offset<flatbuf::KeyValue>> customFieldMetadata;

    // Arrow struct cannot have zero fields, so we add a dummy field with null (NA) type and custom metadata.
    if (fields.empty()) {
        customFieldMetadata.push_back(flatbuf::CreateKeyValue(
            *flatbufBuilder,
            SerializeString(flatbufBuilder, YtTypeMetadataKey),
            SerializeString(flatbufBuilder, YtTypeMetadataValueEmptyStruct)));

        auto fieldOffset = flatbuf::CreateField(
            *flatbufBuilder,
            /*name*/ 0,
            /*nullable*/ true,
            flatbuf::Type_Null,
            flatbuf::CreateNull(*flatbufBuilder).Union(),
            /*dictionary*/ 0,
            /*children*/ 0,
            flatbufBuilder->CreateVector(customFieldMetadata.data(), customFieldMetadata.size()));

        fieldOffsets.push_back(fieldOffset);
    }

    return {
        flatbuf::Type_Struct_,
        flatbuf::CreateStruct_(*flatbufBuilder).Union(),
        std::move(fieldOffsets),
        std::move(customFieldMetadata)
    };
}

TArrowSchemaType SerializeListColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TListLogicalType& type,
    const TArrowFormatConfigPtr& arrowConfig)
{
    const auto& elementType = type.GetElement();

    auto childOffset = CreateRegularField(
        flatbufBuilder,
        /*schemaType*/ SerializeColumnType(flatbufBuilder, elementType, arrowConfig),
        /*name*/ 0,
        elementType->IsNullable());

    return {
        flatbuf::Type_List,
        flatbuf::CreateList(*flatbufBuilder).Union(),
        {childOffset}
    };
}

TArrowSchemaType SerializeDictColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TDictLogicalType& type,
    const TArrowFormatConfigPtr& arrowConfig)
{
    const auto& keyType = type.GetKey();
    const auto& valueType = type.GetValue();

    std::vector<flatbuffers::Offset<flatbuf::Field>> pairElementsOffsets;
    pairElementsOffsets.reserve(2);

    for (const auto& subtype : {keyType, valueType}) {
        auto elementOffset = CreateRegularField(
            flatbufBuilder,
            /*schemaType*/ SerializeColumnType(flatbufBuilder, subtype, arrowConfig),
            /*name*/ 0,
            subtype->IsNullable());

        pairElementsOffsets.push_back(elementOffset);
    }

    // Arrow Map is stored as list of structs.
    auto pairOffset = flatbuf::CreateField(
        *flatbufBuilder,
        /*name*/ 0,
        /*nullable*/ false,
        flatbuf::Type_Struct_,
        flatbuf::CreateStruct_(*flatbufBuilder).Union(),
        /*dictionary*/ 0,
        flatbufBuilder->CreateVector(pairElementsOffsets.data(), pairElementsOffsets.size()));

    return {
        flatbuf::Type_Map,
        flatbuf::CreateMap(*flatbufBuilder).Union(),
        {pairOffset}
    };
}

// Since Arrow doesn't have optional<optional<T>>, we represent outer optional
// as struct with a single element, similar to how YSON encodes these types.
TArrowSchemaType SerializeOptionalColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TOptionalLogicalType& type,
    const TArrowFormatConfigPtr& arrowConfig)
{
    const auto& elementType = type.GetElement();

    // Add custom metadata to nested optional for parser.
    std::vector<flatbuffers::Offset<flatbuf::KeyValue>> customFieldMetadata;
    customFieldMetadata.push_back(flatbuf::CreateKeyValue(
        *flatbufBuilder,
        SerializeString(flatbufBuilder, YtTypeMetadataKey),
        SerializeString(flatbufBuilder, YtTypeMetadataValueNestedOptional)));


    auto elementOffset = CreateRegularField(
        flatbufBuilder,
        /*schemaType*/ SerializeColumnType(flatbufBuilder, elementType, arrowConfig),
        /*name*/ 0,
        elementType->IsNullable());

    return {
        flatbuf::Type_Struct_,
        flatbuf::CreateStruct_(*flatbufBuilder).Union(),
        {elementOffset},
        std::move(customFieldMetadata)
    };
}

TArrowSchemaType SerializeColumnType(
    flatbuffers::FlatBufferBuilder* flatbufBuilder,
    const TLogicalTypePtr& type,
    const TArrowFormatConfigPtr& arrowConfig)
{
    if (!arrowConfig->EnableComplexTypes) {
        auto simpleType = CastToV1Type(type).first;
        return SerializeLeafColumnType(flatbufBuilder, simpleType, arrowConfig);
    }

    auto denullifiedType = DenullifyLogicalType(type);

    switch (denullifiedType->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            auto simpleType = CastToV1Type(type).first;
            return SerializeLeafColumnType(flatbufBuilder, simpleType, arrowConfig);
        }

        case ELogicalMetatype::Struct:
            return SerializeStructColumnType(flatbufBuilder, denullifiedType->AsStructTypeRef(), arrowConfig);

        case ELogicalMetatype::Optional:
            // The underlying type is also optional.
            return SerializeOptionalColumnType(flatbufBuilder, denullifiedType->AsOptionalTypeRef(), arrowConfig);

        case ELogicalMetatype::List:
            return SerializeListColumnType(flatbufBuilder, denullifiedType->AsListTypeRef(), arrowConfig);

        case ELogicalMetatype::Dict:
            return SerializeDictColumnType(flatbufBuilder, denullifiedType->AsDictTypeRef(), arrowConfig);

        case ELogicalMetatype::Tagged:
            // Denullified type should not contain tagged type.
            YT_ABORT();

        default:
            THROW_ERROR_EXCEPTION("Complex type %Qlv is not yet supported with Arrow complex types enabled",
                denullifiedType->GetMetatype());
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
    TRef nullBitmap;
    if (valueColumn->NullBitmap) {
        nullBitmap = valueColumn->NullBitmap->Data;
    }

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
        nullBitmap,
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

bool IsRleButNotDictionaryEncodedTzColumn(const TBatchColumn& column)
{
    return IsTzType(column.Type) &&
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
        IsRleButNotDictionaryEncodedStringLikeColumn(column) ||
        IsRleButNotDictionaryEncodedTzColumn(column);
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
    std::vector<flatbuf::FieldNode> FieldNodes;
    std::vector<flatbuf::Buffer> Buffers;
    std::vector<TRecordBatchBodyPart> Parts;
};

template <class T>
TMutableRange<T> GetTypedValues(TMutableRef ref)
{
    return TMutableRange(
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
            /*nullCount*/ 0);

        context->AddBuffer(
            /*size*/ 0,
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

            TRef nullBitmap;
            if (valueColumn->NullBitmap) {
                nullBitmap = valueColumn->NullBitmap->Data;
            }

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
            nullBitmap,                                     \
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

            TRef nullBitmap;
            if (valueColumn->NullBitmap) {
                nullBitmap = valueColumn->NullBitmap->Data;
            }

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
                nullBitmap,
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

            TRef nullBitmap;
            if (valueColumn->NullBitmap) {
                nullBitmap = valueColumn->NullBitmap->Data;
            }

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
                nullBitmap,
                [&] (auto index) {
                    return values[index];
                },
                [&] (auto value) {
                    *currentOutput++ = value;
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

            TRef nullBitmap;
            if (valueColumn->NullBitmap) {
                nullBitmap = valueColumn->NullBitmap->Data;
            }

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
                nullBitmap,
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

template<ESimpleLogicalValueType type>
void SerializeTzColumnImpl(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context,
    const TArrowFormatConfigPtr& config)
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
    std::vector<ui32> tzOffsets(column->ValueCount + 1);

    YT_LOG_DEBUG("Adding tz-type column (ColumnId: %v, StartIndex: %v, ValueCount: %v, StartOffset: %v, EndOffset: %v, StringsSize: %v)",
        column->Id,
        column->StartIndex,
        column->ValueCount,
        startOffset,
        endOffset,
        stringsSize);

    SerializeColumnPrologue(typedColumn, context);

    DecodeStringOffsets(
        offsets,
        avgLength,
        startIndex,
        endIndex,
        TMutableRange(tzOffsets));

    auto addEmptyBitmap = [=] () {
        context->AddFieldNode(
            column->ValueCount,
            /*nullCount*/ 0);

        context->AddBuffer(
            /*size*/ 0,
            [=] (TMutableRef /*dstRef*/) {
            });
    };

    addEmptyBitmap();

    constexpr ESimpleLogicalValueType UnderlyingDateType = GetUnderlyingDateType<type>();
    using TInt = TUnderlyingTimestampIntegerType<UnderlyingDateType>;

    // Writing timestamp values.
    context->AddBuffer(
        sizeof(TInt) * column->ValueCount,
        [=] (TMutableRef dstRef) {
            auto currentStringData = stringData.Data();
            auto dstValues = GetTypedValues<TInt>(dstRef);

            auto* currentOutput = dstValues.Begin();
            for (int rowOffset = 0; rowOffset < column->ValueCount; ++rowOffset) {
                if (tzOffsets[rowOffset] != tzOffsets[rowOffset + 1]) {
                    auto tzItem = ParseTzValue<TInt>(std::string_view(
                        currentStringData + tzOffsets[rowOffset],
                        currentStringData + tzOffsets[rowOffset + 1]));
                    *currentOutput = tzItem.first;
                }
                ++currentOutput;
            }
        });

    addEmptyBitmap();

    if (config->EnableTzIndex) {
        // Writing timezone indexes
        context->AddBuffer(
            sizeof(ui16) * column->ValueCount,
            [=] (TMutableRef dstRef) {
                auto currentStringData = stringData.Data();
                auto dstValues = GetTypedValues<ui16>(dstRef);

                auto* currentOutput = dstValues.Begin();
                for (int rowOffset = 0; rowOffset < column->ValueCount; ++rowOffset) {
                    if (tzOffsets[rowOffset] != tzOffsets[rowOffset + 1]) {
                        auto tzItem = ParseTzValue<TInt>(std::string_view(
                            currentStringData + tzOffsets[rowOffset],
                            currentStringData + tzOffsets[rowOffset + 1]));
                        *currentOutput = GetTzIndex(tzItem.second);
                    }
                    ++currentOutput;
                }
            });
    } else {
        // Writing timezone names.
        TStringBuilder builder;
        std::vector<i32> nameOffsets;
        nameOffsets.reserve(column->ValueCount);
        i32 tzStringsSize = 0;
        auto currentStringData = stringData.Data();
        for (int rowOffset = 0; rowOffset < column->ValueCount; ++rowOffset) {
            nameOffsets.push_back(tzStringsSize);
            if (tzOffsets[rowOffset] != tzOffsets[rowOffset + 1]) {
                auto tzItem = ParseTzValue<TInt>(std::string_view(
                    currentStringData + tzOffsets[rowOffset],
                    currentStringData + tzOffsets[rowOffset + 1]));
                tzStringsSize += tzItem.second.size();
                builder.AppendString(tzItem.second);
            }
        }
        nameOffsets.push_back(tzStringsSize);
        auto tzStringsBuffer = builder.Flush();

        context->AddBuffer(
            sizeof(i32) * (column->ValueCount + 1),
            [=] (TMutableRef dstRef) {
                ::memcpy(
                    dstRef.Begin(),
                    nameOffsets.data(),
                    nameOffsets.size() * sizeof(i32));
            });

        context->AddBuffer(
            tzStringsSize,
            [=] (TMutableRef dstRef) {
                ::memcpy(
                    dstRef.Begin(),
                    tzStringsBuffer.data(),
                    tzStringsSize);
            });
    }
}

void SerializeTzColumn(
    const TTypedBatchColumn& typedColumn,
    ESimpleLogicalValueType simpleType,
    TRecordBatchSerializationContext* context,
    const TArrowFormatConfigPtr& config)
{
    switch (simpleType) {
#define XX(ytType)                                                                                      \
    case ESimpleLogicalValueType::ytType: {                                                             \
        return SerializeTzColumnImpl<ESimpleLogicalValueType::ytType>(typedColumn, context, config);    \
    }
    XX(TzDate)
    XX(TzDatetime)
    XX(TzTimestamp)
    XX(TzDate32)
    XX(TzDatetime64)
    XX(TzTimestamp64)
#undef XX

    default:
        YT_ABORT();
    }
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

void WriteNullBuffer(
    TRecordBatchSerializationContext* context,
    int length)
{
    context->AddFieldNode(
        length,
        /*nullCount*/ length);
}

void SerializeNullColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context)
{
    WriteNullBuffer(context, typedColumn.Column->ValueCount);
}

////////////////////////////////////////////////////////////////////////////////

class TTypedBlob
    : public TBlob
{
public:
    template <typename T>
    void AppendValue(T value)
    {
        Append(&value, sizeof(T));
    }

    template <typename T>
    T LastValue() const
    {
        T result;
        ::memcpy(&result, End() - sizeof(T), sizeof(T));
        return result;
    }

    template <typename T>
    int ValueCount() const
    {
        return Size() / sizeof(T);
    }
};

using TArrowWriterBuffer = std::variant<TTypedBlob, TBitmapOutput>;

////////////////////////////////////////////////////////////////////////////////

void CreateBuffersForComplexType(
    const TLogicalTypePtr& type,
    std::vector<TArrowWriterBuffer>& buffers)
{
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            auto simpleType = CastToV1Type(type).first;
            if (IsStringLikeType(simpleType)) {
                // Buffer for offsets.
                buffers.emplace_back(TTypedBlob());
                auto& offsetsBuffer = std::get<TTypedBlob>(buffers.back());
                offsetsBuffer.AppendValue<ui32>(0);
            }
            // Buffer for data.
            if (simpleType == ESimpleLogicalValueType::Boolean) {
                buffers.emplace_back(TBitmapOutput());
            } else {
                buffers.emplace_back(TTypedBlob());
            }

            break;
        }

        case ELogicalMetatype::Optional:
            // Buffer for validity bitmap.
            buffers.emplace_back(TBitmapOutput());
            CreateBuffersForComplexType(type->GetElement(), buffers);
            break;

        case ELogicalMetatype::List: {
            // Buffer for offsets.
            buffers.emplace_back(TTypedBlob());
            auto& offsetsBuffer = std::get<TTypedBlob>(buffers.back());
            offsetsBuffer.AppendValue<ui32>(0);

            // Buffers for list elements.
            CreateBuffersForComplexType(type->GetElement(), buffers);
            break;
        }

        case ELogicalMetatype::Struct:
            for (const auto& [fieldName, fieldType] : type->GetFields()) {
                CreateBuffersForComplexType(fieldType, buffers);
            }
            break;

        case ELogicalMetatype::Dict: {
            // Buffer for offsets.
            buffers.emplace_back(TTypedBlob());
            auto& offsetsBuffer = std::get<TTypedBlob>(buffers.back());
            offsetsBuffer.AppendValue<ui32>(0);

            // Buffers for keys (part of struct).
            CreateBuffersForComplexType(type->AsDictTypeRef().GetKey(), buffers);
            // Buffers for values (part of struct).
            CreateBuffersForComplexType(type->AsDictTypeRef().GetValue(), buffers);
            break;
        }

        case ELogicalMetatype::Tagged:
            CreateBuffersForComplexType(type->GetElement(), buffers);
            break;

        default:
            THROW_ERROR_EXCEPTION("Complex type %Qlv is not yet supported with Arrow complex types enabled",
                type->GetMetatype());
    }
}

int CalculateBufferIndexIncrement(const TLogicalTypePtr& type)
{
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            auto simpleType = CastToV1Type(type).first;
            if (IsStringLikeType(simpleType)) {
                return 2;
            } else {
                return 1;
            }
        }

        case ELogicalMetatype::Optional:
            return 1 + CalculateBufferIndexIncrement(type->GetElement());

        case ELogicalMetatype::List:
            return 1 + CalculateBufferIndexIncrement(type->GetElement());

        case ELogicalMetatype::Struct: {
            int total = 0;
            for (const auto& [fieldName, fieldType] : type->GetFields()) {
                total += CalculateBufferIndexIncrement(fieldType);
            }
            return total;
        }

        case ELogicalMetatype::Dict: {
            int total = 1;
            total += CalculateBufferIndexIncrement(type->AsDictTypeRef().GetKey());
            total += CalculateBufferIndexIncrement(type->AsDictTypeRef().GetValue());
            return total;
        }

        case ELogicalMetatype::Tagged:
            return CalculateBufferIndexIncrement(type->GetElement());

        default:
            THROW_ERROR_EXCEPTION("Complex type %Qlv is not yet supported with Arrow complex types enabled",
                type->GetMetatype());
    }
}

void AppendSimpleTypeToBuffer(
    const TLogicalTypePtr& logicalType,
    const TArrowFormatConfigPtr& /*config*/,
    std::vector<TArrowWriterBuffer>& buffers,
    int& currentBufferIndex,
    TYsonPullParserCursor& cursor)
{
    auto simpleType = CastToV1Type(logicalType).first;

    if (IsStringLikeType(simpleType)) {
        auto& offsetsBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex++]);
        auto& valueBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex++]);

        auto string = cursor->UncheckedAsString();

        valueBuffer.Append(string.Data(), string.Size());
        offsetsBuffer.AppendValue<ui32>(valueBuffer.Size());

        return;
    }

    if (IsTzType(logicalType)) {
        THROW_ERROR_EXCEPTION("Timezone type %Qlv is not yet supported with Arrow complex types enabled",
            logicalType->GetMetatype());
    }

    auto& buffer = buffers[currentBufferIndex++];

    switch (simpleType) {
#define NUMERIC_CASE(cppType, ysonType, ytType)                 \
        case ESimpleLogicalValueType::ytType: {                 \
            auto value = cursor->UncheckedAs<ysonType>();       \
            auto& valueBuffer = std::get<TTypedBlob>(buffer);   \
            valueBuffer.AppendValue<cppType>(value);            \
            break;                                              \
        }

        NUMERIC_CASE(i8, i64, Int8)
        NUMERIC_CASE(i16, i64, Int16)
        NUMERIC_CASE(i32, i64, Int32)
        NUMERIC_CASE(i64, i64, Int64)
        NUMERIC_CASE(ui8, ui64, Uint8)
        NUMERIC_CASE(ui16, ui64, Uint16)
        NUMERIC_CASE(ui32, ui64, Uint32)
        NUMERIC_CASE(ui64, ui64, Uint64)
        NUMERIC_CASE(i64, i64, Interval)
        NUMERIC_CASE(double, double, Double)
        NUMERIC_CASE(float, double, Float)
#undef NUMERIC_CASE

#define DATETIME_CASE(cppType, ytType)                                                              \
        case ESimpleLogicalValueType::ytType: {                                                     \
            auto value = cursor->UncheckedAsUint64();                                               \
            if (value > std::numeric_limits<cppType>::max()) {                                      \
                THROW_ERROR_EXCEPTION(                                                              \
                    "Date value cannot be represented in arrow (Value: %v, MaxAllowedValue: %v)",   \
                    value,                                                                          \
                    std::numeric_limits<cppType>::max());                                           \
            }                                                                                       \
            auto& valueBuffer = std::get<TTypedBlob>(buffer);                                       \
            valueBuffer.AppendValue<cppType>(value);                                                \
            break;                                                                                  \
        }

        DATETIME_CASE(i32, Date)
        DATETIME_CASE(i64, Datetime)
        DATETIME_CASE(i64, Timestamp)
#undef DATETIME_CASE

        case ESimpleLogicalValueType::Boolean: {
            auto& bitmapBuffer = std::get<TBitmapOutput>(buffer);
            bitmapBuffer.Append(cursor->UncheckedAsBoolean());
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Column has unexpected nested type %Qlv", simpleType);
    }
}

void AppendNullSimpleTypeToBuffer(
    const TLogicalTypePtr& logicalType,
    const TArrowFormatConfigPtr& /*config*/,
    std::vector<TArrowWriterBuffer>& buffers,
    int& currentBufferIndex)
{
    auto simpleType = CastToV1Type(logicalType).first;

    if (IsStringLikeType(simpleType)) {
        auto& offsetsBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex++]);
        auto& valueBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex++]);

        offsetsBuffer.AppendValue<ui32>(valueBuffer.Size());

        return;
    }

    if (IsTzType(logicalType)) {
        THROW_ERROR_EXCEPTION("Timezone type %Qlv is not yet supported with Arrow complex types enabled",
            logicalType->GetMetatype());
    }

    auto& buffer = buffers[currentBufferIndex++];

    switch (simpleType) {
#define NUMERIC_CASE(cppType, ytType)                           \
        case ESimpleLogicalValueType::ytType: {                 \
            auto& valueBuffer = std::get<TTypedBlob>(buffer);   \
            valueBuffer.AppendValue<cppType>(0);                \
            break;                                              \
        }

        NUMERIC_CASE(i8, Int8)
        NUMERIC_CASE(i16, Int16)
        NUMERIC_CASE(i32, Int32)
        NUMERIC_CASE(i64, Int64)
        NUMERIC_CASE(ui8, Uint8)
        NUMERIC_CASE(ui16, Uint16)
        NUMERIC_CASE(ui32, Uint32)
        NUMERIC_CASE(ui64, Uint64)
        NUMERIC_CASE(i64, Interval)
        NUMERIC_CASE(double, Double)
        NUMERIC_CASE(float, Float)
        NUMERIC_CASE(i32, Date)
        NUMERIC_CASE(i64, Datetime)
        NUMERIC_CASE(i64, Timestamp)
#undef NUMERIC_CASE

        case ESimpleLogicalValueType::Boolean: {
            auto& bitmapBuffer = std::get<TBitmapOutput>(buffer);
            bitmapBuffer.Append(false);
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Column has unexpected nested type %Qlv", simpleType);
    }
}

void FillBuffersForComplexTypeWithNulls(
    const TLogicalTypePtr& type,
    const TArrowFormatConfigPtr& config,
    std::vector<TArrowWriterBuffer>& buffers,
    int& currentBufferIndex)
{
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple:
            AppendNullSimpleTypeToBuffer(type, config, buffers, currentBufferIndex);
            break;

        case ELogicalMetatype::Optional: {
            auto& validityBitmapBuffer = std::get<TBitmapOutput>(buffers[currentBufferIndex++]);
            // The value of this bit should be irrelevant.
            validityBitmapBuffer.Append(false);

            FillBuffersForComplexTypeWithNulls(
                type->GetElement(),
                config,
                buffers,
                currentBufferIndex);
            break;
        }

        case ELogicalMetatype::List:
        case ELogicalMetatype::Dict: {
            auto& offsetsBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex++]);

            ui32 previousOffset = offsetsBuffer.LastValue<ui32>();
            offsetsBuffer.AppendValue<ui32>(previousOffset);

            currentBufferIndex += CalculateBufferIndexIncrement(type) - 1;
            break;
        }

        case ELogicalMetatype::Struct:
            for (const auto& field : type->GetFields()) {
                FillBuffersForComplexTypeWithNulls(
                    field.Type,
                    config,
                    buffers,
                    currentBufferIndex);
            }
            break;

        case ELogicalMetatype::Tagged:
            FillBuffersForComplexTypeWithNulls(
                type->GetElement(),
                config,
                buffers,
                currentBufferIndex);
            break;

        default:
            THROW_ERROR_EXCEPTION("Complex type %Qlv is not yet supported with Arrow complex types enabled",
                type->GetMetatype());
    }
}

void FillBuffersForComplexType(
    const TLogicalTypePtr& type,
    const TArrowFormatConfigPtr& config,
    std::vector<TArrowWriterBuffer>& buffers,
    int& currentBufferIndex,
    TYsonPullParserCursor& cursor)
{
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            AppendSimpleTypeToBuffer(type, config, buffers, currentBufferIndex, cursor);
            cursor.Next();

            break;
        }

        case ELogicalMetatype::Optional: {
            // See https://ytsaurus.tech/docs/user-guide/storage/data-types#yson_optional
            // for specifics of how optional<T> is encoded in YSON.

            auto& validityBitmapBuffer = std::get<TBitmapOutput>(buffers[currentBufferIndex++]);
            const auto& subtype = type->GetElement();

            if (cursor.GetCurrent().GetType() == EYsonItemType::EntityValue) {
                // Type is optional<T>.
                // YSON is "#".
                validityBitmapBuffer.Append(false);
                cursor.Next();
                FillBuffersForComplexTypeWithNulls(
                    subtype,
                    config,
                    buffers,
                    currentBufferIndex);
            } else if (subtype->IsNullable()) {
                // Type is optional<optional<T>>.
                // YSON is "[...]" (possibly "[#]").
                validityBitmapBuffer.Append(true);
                cursor.ParseList([&](TYsonPullParserCursor* cursor) {
                    FillBuffersForComplexType(
                        subtype,
                        config,
                        buffers,
                        currentBufferIndex,
                        *cursor);
                });
            } else {
                // Type is optional<T>, T is required.
                // YSON is "...".
                validityBitmapBuffer.Append(true);
                FillBuffersForComplexType(
                    subtype,
                    config,
                    buffers,
                    currentBufferIndex,
                    cursor);
            }
            break;
        }

        case ELogicalMetatype::List:
        case ELogicalMetatype::Dict: {
            auto& offsetsBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex++]);
            int elementCount = 0;

            // For list-like types, we may traverse corresponding buffers
            // more than once (|elementCount > 1|) or never (|elementCount == 0|).

            // Save the starting index.
            int initialBufferIndex = currentBufferIndex;

            if (type->GetMetatype() == ELogicalMetatype::List) {
                const auto& listType = type->GetElement();
                cursor.ParseList([&](TYsonPullParserCursor* cursor) {
                    // Restore the starting index.
                    currentBufferIndex = initialBufferIndex;

                    FillBuffersForComplexType(
                        listType,
                        config,
                        buffers,
                        currentBufferIndex,
                        *cursor);

                    elementCount++;
                    // If this point is reached, |currentBufferIndex| now contains correct final value.
                    // Otherwise |elementCount == 0| and we need to manually advance it later.
                });
            } else {
                const auto& keyType = type->AsDictTypeRef().GetKey();
                const auto& valueType = type->AsDictTypeRef().GetValue();
                cursor.ParseList([&](TYsonPullParserCursor* cursor) {
                    // Same as for List, but now underlying list element is a struct.
                    currentBufferIndex = initialBufferIndex;

                    bool consumedKey = false;
                    cursor->ParseList([&](TYsonPullParserCursor* cursor) {
                        FillBuffersForComplexType(
                            consumedKey ? valueType : keyType,
                            config,
                            buffers,
                            currentBufferIndex,
                            *cursor);
                        consumedKey = true;
                    });

                    elementCount++;
                });
            }

            if (elementCount == 0) {
                // |currentBufferIndex| is unchanged, manually advance it.
                currentBufferIndex += CalculateBufferIndexIncrement(type) - 1;
            }

            ui32 previousOffset = offsetsBuffer.LastValue<ui32>();
            offsetsBuffer.AppendValue<ui32>(previousOffset + elementCount);

            break;
        }

        case ELogicalMetatype::Struct: {
            const auto& fields = type->GetFields();
            int fieldIndex = 0;

            cursor.ParseList([&](TYsonPullParserCursor* cursor) {
                // Assume positional struct version
                // (https://ytsaurus.tech/docs/ru/user-guide/storage/data-types#yson_struct).
                const auto& field = fields[fieldIndex++];
                FillBuffersForComplexType(
                    field.Type,
                    config,
                    buffers,
                    currentBufferIndex,
                    *cursor);
            });

            break;
        }

        case ELogicalMetatype::Tagged:
            FillBuffersForComplexType(
                type->GetElement(),
                config,
                buffers,
                currentBufferIndex,
                cursor);
            break;

        default:
            THROW_ERROR_EXCEPTION("Complex type %Qlv is not yet supported with Arrow complex types enabled",
                type->GetMetatype());
    }
}

void WriteEmptyValidityBitmap(
    TRecordBatchSerializationContext* context,
    int length)
{
    context->AddFieldNode(
        length,
        /*nullCount*/ 0);

    context->AddBuffer(
        /*size*/ 0,
        [] (TMutableRef /*dstRef*/) {
        });
}

void WriteBufferFromTypedBlob(
    TRecordBatchSerializationContext* context,
    const std::vector<TArrowWriterBuffer>& buffers,
    int bufferIndex)
{
    const auto& buffer = std::get<TTypedBlob>(buffers[bufferIndex]);

    context->AddBuffer(
        buffer.Size(),
        [=] (TMutableRef dstRef) {
            // |TTypedBlob| may use small object optimization, thus |buffer.Begin()|
            // may become invalid due to vector reallocation.
            const auto& buffer = std::get<TTypedBlob>(buffers[bufferIndex]);
            ::memcpy(
                dstRef.Begin(),
                buffer.Begin(),
                buffer.Size());
        });
}

void WriteBufferFromBitmapOutput(
    TRecordBatchSerializationContext* context,
    const std::vector<TArrowWriterBuffer>& buffers,
    int bufferIndex)
{
    const auto& buffer = std::get<TBitmapOutput>(buffers[bufferIndex]);
    int byteCount = GetBitmapByteSize(buffer.GetBitSize());

    context->AddBuffer(
        byteCount,
        [=] (TMutableRef dstRef) {
            // |TBitmapOutput| may use small object optimization, thus |buffer.GetData()|
            // may become invalid due to vector reallocation.
            const auto& buffer = std::get<TBitmapOutput>(buffers[bufferIndex]);
            ::memcpy(
                dstRef.Begin(),
                buffer.GetData(),
                byteCount);
        });
}

void WriteValidityBitmapFromBitmapOutput(
    TRecordBatchSerializationContext* context,
    const std::vector<TArrowWriterBuffer>& buffers,
    int bufferIndex)
{
    const auto& buffer = std::get<TBitmapOutput>(buffers[bufferIndex]);
    int length = buffer.GetBitSize();
    int byteCount = GetBitmapByteSize(length);

    TRef bitmap(buffer.GetData(), byteCount);
    int nullCount = length - CountOnesInBitmap(bitmap, /*startIndex*/ 0, /*endIndex*/ length);

    if (nullCount == 0) {
        WriteEmptyValidityBitmap(context, length);
        return;
    }

    context->AddFieldNode(
        length,
        nullCount);

    context->AddBuffer(
        byteCount,
        [=] (TMutableRef dstRef) {
            // |TBitmapOutput| may use small object optimization, thus |buffer.GetData()|
            // may become invalid due to vector reallocation.
            const auto& buffer = std::get<TBitmapOutput>(buffers[bufferIndex]);
            ::memcpy(
                dstRef.Begin(),
                buffer.GetData(),
                byteCount);
        });
}

void WriteBuffersForComplexType(
    const TLogicalTypePtr& type,
    TRecordBatchSerializationContext* context,
    const TArrowFormatConfigPtr& config,
    std::vector<TArrowWriterBuffer>& buffers,
    int& currentBufferIndex,
    bool shouldWriteValidityBitmap,
    int elementCount)
{
    if (shouldWriteValidityBitmap && type->GetMetatype() != ELogicalMetatype::Tagged) {
        if (type->GetMetatype() == ELogicalMetatype::Optional) {
            WriteValidityBitmapFromBitmapOutput(context, buffers, currentBufferIndex);
        } else {
            WriteEmptyValidityBitmap(context, elementCount);
        }
    }

    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            auto simpleType = CastToV1Type(type).first;
            if (IsStringLikeType(simpleType)) {
                const auto& offsetsBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex]);

                YT_VERIFY(elementCount + 1 == offsetsBuffer.ValueCount<ui32>());

                // Write offsets buffer.
                WriteBufferFromTypedBlob(context, buffers, currentBufferIndex++);
                // Write value buffer.
                WriteBufferFromTypedBlob(context, buffers, currentBufferIndex++);
            } else if (simpleType == ESimpleLogicalValueType::Boolean) {
                // Write value bitmap.
                WriteBufferFromBitmapOutput(context, buffers, currentBufferIndex++);
            } else {
                // Write value buffer.
                WriteBufferFromTypedBlob(context, buffers, currentBufferIndex++);
            }
            break;
        }

        case ELogicalMetatype::Optional:
            currentBufferIndex++;
            WriteBuffersForComplexType(
                type->GetElement(),
                context,
                config,
                buffers,
                currentBufferIndex,
                /*shouldWriteValidityBitmap*/ type->GetElement()->IsNullable(),
                elementCount);
            break;

        case ELogicalMetatype::List: {
            const auto& offsetsBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex]);

            YT_VERIFY(elementCount + 1 == offsetsBuffer.ValueCount<ui32>());

            // Write offsets buffer.
            WriteBufferFromTypedBlob(context, buffers, currentBufferIndex++);

            WriteBuffersForComplexType(
                type->GetElement(),
                context,
                config,
                buffers,
                currentBufferIndex,
                /*shouldWriteValidityBitmap*/ true,
                offsetsBuffer.LastValue<ui32>());
            break;
        }

        case ELogicalMetatype::Struct:
            for (const auto& [fieldName, fieldType] : type->GetFields()) {
                WriteBuffersForComplexType(
                    fieldType,
                    context,
                    config,
                    buffers,
                    currentBufferIndex,
                    /*shouldWriteValidityBitmap*/ true,
                    elementCount);
            }

            if (type->GetFields().empty()) {
                // Write empty validity bitmap for a dummy null field (value buffer is omitted).
                WriteNullBuffer(context, elementCount);
            }

            break;

        case ELogicalMetatype::Dict: {
            const auto& offsetsBuffer = std::get<TTypedBlob>(buffers[currentBufferIndex]);

            YT_VERIFY(elementCount + 1 == offsetsBuffer.ValueCount<ui32>());

            // Write offsets buffer.
            WriteBufferFromTypedBlob(context, buffers, currentBufferIndex++);

            int flattenedElementCount = offsetsBuffer.LastValue<ui32>();

            WriteEmptyValidityBitmap(context, flattenedElementCount);

            const auto& keyType = type->AsDictTypeRef().GetKey();
            const auto& valueType = type->AsDictTypeRef().GetValue();

            for (const auto& subtype : {keyType, valueType}) {
                WriteBuffersForComplexType(
                    subtype,
                    context,
                    config,
                    buffers,
                    currentBufferIndex,
                    /*shouldWriteValidityBitmap*/ true,
                    flattenedElementCount);
            }
            break;
        }

        case ELogicalMetatype::Tagged:
            WriteBuffersForComplexType(
                type->GetElement(),
                context,
                config,
                buffers,
                currentBufferIndex,
                shouldWriteValidityBitmap,
                elementCount);
            break;

        default:
            THROW_ERROR_EXCEPTION("Complex type %Qlv is not yet supported with Arrow complex types enabled",
                type->GetMetatype());
    }
}

void SerializeComplexTypeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context,
    const TArrowFormatConfigPtr& config,
    std::vector<TArrowWriterBuffer>& buffers)
{
    int initialBufferIndex = buffers.size();
    CreateBuffersForComplexType(typedColumn.Type, buffers);

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

    auto encodedOffsets = column->GetTypedValues<ui32>();
    std::vector<ui32> offsets(column->ValueCount + 1);

    DecodeStringOffsets(
        encodedOffsets,
        avgLength,
        startIndex,
        endIndex,
        TMutableRange(offsets));

    auto currentStringData = stringData.Data();

    for (int rowOffset = 0; rowOffset < column->ValueCount; ++rowOffset) {
        int currentBufferIndex = initialBufferIndex;

        if (offsets[rowOffset] != offsets[rowOffset + 1]) {
            TString ysonString(
                currentStringData + offsets[rowOffset],
                currentStringData + offsets[rowOffset + 1]);

            TStringInput input(ysonString);
            TYsonPullParser parser(&input, EYsonType::Node);
            TYsonPullParserCursor cursor = &parser;

            FillBuffersForComplexType(
                typedColumn.Type,
                config,
                buffers,
                currentBufferIndex,
                cursor);
        } else {
            FillBuffersForComplexTypeWithNulls(
                typedColumn.Type,
                config,
                buffers,
                currentBufferIndex);
        }
    }

    // If column metatype is Optional, saved validity bitmap,
    // which should be all set, is overwritten by column null bitmap.
    SerializeColumnPrologue(typedColumn, context);

    WriteBuffersForComplexType(
        typedColumn.Type,
        context,
        config,
        buffers,
        initialBufferIndex,
        /*shouldWriteValidityBitmap*/ false,
        column->ValueCount);
}

bool IsComplexTypeColumn(
    const TLogicalTypePtr& type,
    const TArrowFormatConfigPtr& config)
{
    if (!config->EnableComplexTypes) {
        return false;
    }
    if (CastToV1Type(type).first != ESimpleLogicalValueType::Any) {
        return false;
    }
    auto denullifiedType = DenullifyLogicalType(type);
    if (denullifiedType->GetMetatype() == ELogicalMetatype::Simple) {
        // Plain or nullable Any column.
        return false;
    }
    return true;
}

void SerializeColumn(
    const TTypedBatchColumn& typedColumn,
    TRecordBatchSerializationContext* context,
    const TArrowFormatConfigPtr& config,
    std::vector<TArrowWriterBuffer>& buffers)
{
    const auto* column = typedColumn.Column;

    if (IsRleButNotDictionaryEncodedStringLikeColumn(*typedColumn.Column) ||
        IsRleButNotDictionaryEncodedTzColumn(*typedColumn.Column))
    {
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

    if (IsComplexTypeColumn(typedColumn.Type, config)) {
        SerializeComplexTypeColumn(typedColumn, context, config, buffers);
    } else if (IsIntegralType(simpleType)) {
        SerializeIntegerColumn(typedColumn, simpleType, context);
    } else if (IsTzType(typedColumn.Type)) {
        SerializeTzColumn(typedColumn, simpleType, context, config);
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
    TRange<TTypedBatchColumn> typedColumns,
    const TArrowFormatConfigPtr& config,
    std::vector<TArrowWriterBuffer>& buffers)
{
    auto context = New<TRecordBatchSerializationContext>(flatbufBuilder);

    for (const auto& typedColumn : typedColumns) {
        SerializeColumn(typedColumn, context.Get(), config, buffers);
    }

    auto fieldNodesOffset = flatbufBuilder->CreateVectorOfStructs(context->FieldNodes);

    auto buffersOffset = flatbufBuilder->CreateVectorOfStructs(context->Buffers);

    auto recordBatchOffset = flatbuf::CreateRecordBatch(
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
////////////////////////////////////////////////////////////////////////////////

class TArrowWriter
    : public TSchemalessFormatWriterBase
{
public:
    TArrowWriter(
        TArrowFormatConfigPtr config,
        TNameTablePtr nameTable,
        const std::vector<TTableSchemaPtr>& tableSchemas,
        const std::vector<std::optional<std::vector<std::string>>>& columns,
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
        , ArrowConfig_(std::move(config))
    {
        YT_VERIFY(tableSchemas.size() > 0);
        YT_VERIFY(columns.size() == tableSchemas.size() || columns.size() == 0);

        ColumnConverters_.resize(tableSchemas.size());
        TableCount_ = tableSchemas.size();
        ColumnSchemas_.resize(tableSchemas.size());
        TableIdToIndex_.resize(tableSchemas.size());
        IsFirstBatchForSpecificTable_.assign(tableSchemas.size(), false);

        for (int tableIndex = 0; tableIndex < std::ssize(tableSchemas); ++tableIndex) {
            THashSet<std::string> columnNames;
            bool hasColumnFilter = false;
            if (tableIndex < std::ssize(columns) && columns[tableIndex]) {
                hasColumnFilter = true;
                for (const auto& columnName : *(columns[tableIndex])) {
                    columnNames.insert(columnName);
                }
            }
            for (const auto& columnSchema : tableSchemas[tableIndex]->Columns()) {
                if (!hasColumnFilter || columnNames.contains(columnSchema.Name())) {
                    auto columnId = NameTable_->GetIdOrRegisterName(columnSchema.Name());
                    ColumnSchemas_[tableIndex][columnId] = columnSchema;
                }
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

    void DoWriteBatch(IUnversionedRowBatchPtr rowBatch) override
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
    const TArrowFormatConfigPtr ArrowConfig_;

    int TableCount_ = 0;
    bool IsFirstBatch_ = true;
    i64 PrevTableIndex_ = 0;
    i64 RowCount_ = 0;
    std::vector<TTypedBatchColumn> TypedColumns_;
    std::vector<THashMap<int, TColumnSchema>> ColumnSchemas_;
    std::vector<IUnversionedColumnarRowBatch::TDictionaryId> ArrowDictionaryIds_;
    std::vector<TColumnConverters> ColumnConverters_;
    std::vector<THashMap<int, int>> TableIdToIndex_;
    std::vector<bool> IsFirstBatchForSpecificTable_;
    TConvertedColumnRange MissingColumns_;

    std::vector<TArrowWriterBuffer> Buffers_;

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

    bool ShouldColumnBeAdded(int columnIndex) const
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
            int currentIndex = 0;
            for (const auto& columnSchema : ColumnSchemas_[tableIndex]) {
                auto columnId = columnSchema.first;
                if (ShouldColumnBeAdded(columnId)) {
                    TableIdToIndex_[tableIndex][columnId] = currentIndex;
                    currentIndex++;
                }
            }

            IsFirstBatchForSpecificTable_[tableIndex] = true;
        }

        TypedColumns_.resize(TableIdToIndex_[tableIndex].size());
        std::vector<bool> columnExists(TableIdToIndex_[tableIndex].size());

        for (const auto* column : batchColumns) {
            if (ShouldColumnBeAdded(column->Id)) {
                auto iterIndex = TableIdToIndex_[tableIndex].find(column->Id);
                YT_VERIFY(iterIndex != TableIdToIndex_[tableIndex].end());

                auto iterSchema = ColumnSchemas_[tableIndex].find(column->Id);
                YT_VERIFY(iterSchema != ColumnSchemas_[tableIndex].end());
                TypedColumns_[iterIndex->second] = TTypedBatchColumn{
                    column,
                    iterSchema->second.LogicalType()
                };
                columnExists[iterIndex->second] = true;
            }
        }

        THashMap<int, TColumnSchema> missingColumnSchemas;
        for (const auto& columnSchema : ColumnSchemas_[tableIndex]) {
            auto columnId = columnSchema.first;
            if (ShouldColumnBeAdded(columnId)) {
                auto iterIndex = TableIdToIndex_[tableIndex].find(columnId);
                YT_VERIFY(iterIndex != TableIdToIndex_[tableIndex].end());
                if (!columnExists[iterIndex->second]) {
                    missingColumnSchemas[columnId] = columnSchema.second;
                }
            }
        }
        if (missingColumnSchemas.size() > 0 && RowCount_ > 0) {
            std::vector<TUnversionedRow> rows(RowCount_);
            TColumnConverters columnConverter;
            MissingColumns_ = columnConverter.ConvertRowsToColumns(rows, missingColumnSchemas);
            for (ssize_t columnIndex = 0; columnIndex < std::ssize(MissingColumns_); columnIndex++) {
                auto iterIndex = TableIdToIndex_[tableIndex].find(MissingColumns_[columnIndex].RootColumn->Id);
                YT_VERIFY(iterIndex != TableIdToIndex_[tableIndex].end());

                auto iterSchema = ColumnSchemas_[tableIndex].find(MissingColumns_[columnIndex].RootColumn->Id);
                YT_VERIFY(iterSchema != ColumnSchemas_[tableIndex].end());

                TypedColumns_[iterIndex->second] = TTypedBatchColumn{
                    MissingColumns_[columnIndex].RootColumn,
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
        [[maybe_unused]] flatbuf::MessageHeader type,
        flatbuffers::FlatBufferBuilder&& flatbufBuilder,
        i64 bodySize = 0,
        std::function<void(TMutableRef)> bodyWriter = nullptr)
    {
        YT_LOG_DEBUG("Message registered (Type: %v, MessageSize: %v, BodySize: %v)",
            flatbuf::EnumNameMessageHeader(type),
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
        std::vector<flatbuffers::Offset<flatbuf::Field>> fieldOffsets;
        for (int columnIndex = 0; columnIndex < std::ssize(TypedColumns_); columnIndex++) {
            const auto& typedColumn = TypedColumns_[columnIndex];
            auto iterSchema = ColumnSchemas_[tableIndex].find(typedColumn.Column->Id);
            YT_VERIFY(iterSchema != ColumnSchemas_[tableIndex].end());
            auto columnSchema = iterSchema->second;
            auto nameOffset = SerializeString(&flatbufBuilder, columnSchema.Name());

            auto [typeType, typeOffset, childrenFields, customMetadata] = SerializeColumnType(&flatbufBuilder, columnSchema.LogicalType(), ArrowConfig_);

            flatbuffers::Offset<flatbuf::DictionaryEncoding> dictionaryEncodingOffset;
            auto indexTypeOffset = flatbuf::CreateInt(flatbufBuilder, /*bitWidth*/ 32,  /*is_signed*/ false);

            if (IsDictionaryEncodedColumn(*typedColumn.Column)) {
                dictionaryEncodingOffset = flatbuf::CreateDictionaryEncoding(
                    flatbufBuilder,
                    arrowDictionaryIdCounter++,
                    indexTypeOffset);
            }

            auto fieldOffset = flatbuf::CreateField(
                flatbufBuilder,
                nameOffset,
                columnSchema.LogicalType()->IsNullable(),
                typeType,
                typeOffset,
                dictionaryEncodingOffset,
                flatbufBuilder.CreateVector(childrenFields.data(), childrenFields.size()),
                flatbufBuilder.CreateVector(customMetadata.data(), customMetadata.size()));

            fieldOffsets.push_back(fieldOffset);
        }

        auto fieldsOffset = flatbufBuilder.CreateVector(fieldOffsets);

        std::vector<flatbuffers::Offset<flatbuf::KeyValue>> customMetadata;

        if (TableCount_ > 1) {
            auto keyValueOffsett = flatbuf::CreateKeyValue(
                flatbufBuilder,
                flatbufBuilder.CreateString("TableId"),
                flatbufBuilder.CreateString(std::to_string(tableIndex)));
            customMetadata.push_back(keyValueOffsett);
        }

        auto schemaOffset = flatbuf::CreateSchema(
            flatbufBuilder,
            flatbuf::Endianness_Little,
            fieldsOffset,
            flatbufBuilder.CreateVector(customMetadata));

        auto messageOffset = flatbuf::CreateMessage(
            flatbufBuilder,
            flatbuf::MetadataVersion_V4,
            flatbuf::MessageHeader_Schema,
            schemaOffset.Union(),
            0);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            flatbuf::MessageHeader_Schema,
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
            } else if (IsRleButNotDictionaryEncodedStringLikeColumn(*typedColumn.Column) ||
                IsRleButNotDictionaryEncodedTzColumn(*typedColumn.Column))
            {
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
            TRange({typedColumn}),
            ArrowConfig_,
            Buffers_);

        auto dictionaryBatchOffset = flatbuf::CreateDictionaryBatch(
            flatbufBuilder,
            arrowDictionaryId,
            recordBatchOffset);

        auto messageOffset = flatbuf::CreateMessage(
            flatbufBuilder,
            flatbuf::MetadataVersion_V4,
            flatbuf::MessageHeader_DictionaryBatch,
            dictionaryBatchOffset.Union(),
            bodySize);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            flatbuf::MessageHeader_DictionaryBatch,
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
            TypedColumns_,
            ArrowConfig_,
            Buffers_);

        auto messageOffset = flatbuf::CreateMessage(
            flatbufBuilder,
            flatbuf::MetadataVersion_V4,
            flatbuf::MessageHeader_RecordBatch,
            recordBatchOffset.Union(),
            bodySize);

        flatbufBuilder.Finish(messageOffset);

        RegisterMessage(
            flatbuf::MessageHeader_RecordBatch,
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

                output->Write(AlignmentString.data(), metadataAlignSize - metadataSize);

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

        Buffers_.clear();

        YT_LOG_DEBUG("Finished writing payload");
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForArrow(
    TArrowFormatConfigPtr config,
    TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    const std::vector<std::optional<std::vector<std::string>>>& columns,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    return New<TArrowWriter>(
        std::move(config),
        std::move(nameTable),
        schemas,
        columns,
        std::move(output),
        enableContextSaving,
        std::move(controlAttributesConfig),
        keyColumnCount);
}

ISchemalessFormatWriterPtr CreateWriterForArrow(
    const NYTree::IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    const std::vector<TTableSchemaPtr>& schemas,
    const std::vector<std::optional<std::vector<std::string>>>& columns,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    TArrowFormatConfigPtr arrowConfig;
    try {
        arrowConfig = ConvertTo<TArrowFormatConfigPtr>(attributes);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NFormats::EErrorCode::InvalidFormat, "Failed to parse config for arrow format") << ex;
    }
    return CreateWriterForArrow(
        std::move(arrowConfig),
        std::move(nameTable),
        schemas,
        columns,
        std::move(output),
        enableContextSaving,
        std::move(controlAttributesConfig),
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
