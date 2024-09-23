#include "skiff_writer.h"

#include "schemaless_writer_adapter.h"
#include "skiff_yson_converter.h"

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <library/cpp/skiff/skiff.h>
#include <library/cpp/skiff/skiff_schema.h>

#include <util/generic/buffer.h>

#include <util/stream/buffer.h>

#include <functional>
#include <utility>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

using namespace NSkiff;
using namespace NSkiffExt;
using namespace NTableClient;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

static constexpr int MissingSystemColumn = -1;
static constexpr int MissingRowRangeIndexTag = 2;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESkiffWriterColumnType,
    (Unknown)
    (Dense)
    (Sparse)
    (Skip)
    (RangeIndex)
    (RowIndex)
);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ResizeToContainIndex(std::vector<T>* vec, size_t index)
{
    if (vec->size() < index + 1) {
        vec->resize(index + 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TIndexedSchemas
{
public:
    explicit TIndexedSchemas(const std::vector<TTableSchemaPtr>& tableSchemas)
    {
        for (size_t tableIndex = 0; tableIndex < tableSchemas.size(); ++tableIndex) {
            const auto& columns = tableSchemas[tableIndex]->Columns();
            for (const auto& column : columns) {
                Columns_[std::pair<int, TString>(tableIndex, column.Name())] = column;
            }
        }
    }

    const TColumnSchema* GetColumnSchema(int tableIndex, TStringBuf columnName) const
    {
        auto it = Columns_.find(std::pair<int, TString>(tableIndex, columnName));
        if (it == Columns_.end()) {
            return nullptr;
        } else {
            return &it->second;
        }
    }

private:
    // (TableIndex, ColumnName) -> ColumnSchema
    THashMap<std::pair<int, TString>, TColumnSchema> Columns_;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteContext
{
    TNameTablePtr NameTable;
    TUnversionedValueYsonWriter* UnversionedValueYsonConverter = nullptr;
    TBuffer* TmpBuffer = nullptr;
};

using TUnversionedValueToSkiffConverter = std::function<void(const TUnversionedValue&, TCheckedInDebugSkiffWriter*, TWriteContext*)>;

template <EWireType wireType>
constexpr EValueType WireTypeToValueType()
{
    if constexpr (wireType == EWireType::Int8 ||
        wireType == EWireType::Int16 ||
        wireType == EWireType::Int32 ||
        wireType == EWireType::Int64)
    {
        return EValueType::Int64;
    } else if constexpr (
        wireType == EWireType::Uint8 ||
        wireType == EWireType::Uint16 ||
        wireType == EWireType::Uint32 ||
        wireType == EWireType::Uint64)
    {
        return EValueType::Uint64;
    } else if constexpr (wireType == EWireType::Double) {
        return EValueType::Double;
    } else if constexpr (wireType == EWireType::Boolean) {
        return EValueType::Boolean;
    } else if constexpr (wireType == EWireType::String32) {
        return EValueType::String;
    } else if constexpr (wireType == EWireType::Nothing) {
        return EValueType::Null;
    } else {
        // Not compilable.
        static_assert(wireType == EWireType::Int64, "Bad wireType");
    }
}

template<EWireType wireType, bool isOptional>
void ConvertSimpleValueImpl(const TUnversionedValue& value, TCheckedInDebugSkiffWriter* writer, TWriteContext* context)
{
    if constexpr (isOptional) {
        if (value.Type == EValueType::Null) {
            writer->WriteVariant8Tag(0);
            return;
        } else {
            writer->WriteVariant8Tag(1);
        }
    }

    if constexpr (wireType != EWireType::Yson32) {
        constexpr auto expectedValueType = WireTypeToValueType<wireType>();
        if (value.Type != expectedValueType) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow,
                "Unexpected type of %Qv column: Skiff format expected %Qlv, actual table type %Qlv",
                context->NameTable->GetName(value.Id),
                expectedValueType,
                value.Type);
        }
    }

    if constexpr (wireType == EWireType::Int8) {
        CheckIntSize<wireType>(value.Data.Int64);
        writer->WriteInt8(value.Data.Int64);
    } else if constexpr (wireType == EWireType::Int16) {
        CheckIntSize<wireType>(value.Data.Int64);
        writer->WriteInt16(value.Data.Int64);
    } else if constexpr (wireType == EWireType::Int32) {
        CheckIntSize<wireType>(value.Data.Int64);
        writer->WriteInt32(value.Data.Int64);
    } else if constexpr (wireType == EWireType::Int64) {
        writer->WriteInt64(value.Data.Int64);

    } else if constexpr (wireType == EWireType::Uint8) {
        CheckIntSize<wireType>(value.Data.Uint64);
        writer->WriteUint8(value.Data.Uint64);
    } else if constexpr (wireType == EWireType::Uint16) {
        CheckIntSize<wireType>(value.Data.Uint64);
        writer->WriteUint16(value.Data.Uint64);
    } else if constexpr (wireType == EWireType::Uint32) {
        CheckIntSize<wireType>(value.Data.Uint64);
        writer->WriteUint32(value.Data.Uint64);
    } else if constexpr (wireType == EWireType::Uint64) {
        writer->WriteUint64(value.Data.Uint64);

    } else if constexpr (wireType == EWireType::Boolean) {
        writer->WriteBoolean(value.Data.Boolean);
    } else if constexpr (wireType == EWireType::Double) {
        writer->WriteDouble(value.Data.Double);
    } else if constexpr (wireType == EWireType::String32) {
        writer->WriteString32(value.AsStringBuf());
    } else if constexpr (wireType == EWireType::Yson32) {
        context->TmpBuffer->Clear();
        {
            TBufferOutput out(*context->TmpBuffer);
            // Set enableRaw=true in YSON writer to avoid the costs of parsing YSON
            // Asume it was validated by now.
            NYson::TYsonWriter ysonWriter(&out, NYson::EYsonFormat::Binary, NYson::EYsonType::Node, true);
            context->UnversionedValueYsonConverter->WriteValue(value, &ysonWriter);
        }
        writer->WriteYson32(TStringBuf(context->TmpBuffer->data(), context->TmpBuffer->size()));
    } else if constexpr (wireType == EWireType::Nothing) {
        // Do nothing.
    } else {
        // Not compilable.
        static_assert(wireType == EWireType::Int64, "Bad wireType");
    }
}

template <EValueType ExpectedValueType, bool isOptional, typename TFunc>
class TPrimitiveConverterWrapper
{
public:
    explicit TPrimitiveConverterWrapper(TFunc function)
        : Function_(function)
    { }

    void operator()(const TUnversionedValue& value, TCheckedInDebugSkiffWriter* writer, TWriteContext* context)
    {
        if constexpr (isOptional) {
            if (value.Type == EValueType::Null) {
                writer->WriteVariant8Tag(0);
                return;
            } else {
                writer->WriteVariant8Tag(1);
            }
        }
        if (value.Type != ExpectedValueType) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow,
                "Unexpected type of %Qv column: Skiff format expected %Qlv, actual table type %Qlv",
                context->NameTable->GetName(value.Id),
                ExpectedValueType,
                value.Type);
        }
        if constexpr (ExpectedValueType == EValueType::String) {
            Function_(value.AsStringBuf(), writer);
        } else {
            // TODO(ermolovd) support other types and use this class instead of ConvertSimpleValueImpl
            // poor man's static assert false
            static_assert(ExpectedValueType == EValueType::String);
        }
    }

private:
    TFunc Function_;
};

class TRowAndRangeIndexWriter
{
public:
    template <ERowRangeIndexMode Mode>
    void WriteRowIndex(const TUnversionedValue& value, TCheckedInDebugSkiffWriter* writer, TWriteContext* /*context*/)
    {
        if (value.Type == EValueType::Int64) {
            const auto currentRowIndex = value.Data.Int64;
            if (RowIndex_ + 1 == currentRowIndex) {
                writer->WriteVariant8Tag(0);
            } else {
                writer->WriteVariant8Tag(1);
                writer->WriteInt64(value.Data.Int64);
            }
            RowIndex_ = currentRowIndex;
        } else if (value.Type == EValueType::Null) {
            if constexpr (Mode == ERowRangeIndexMode::Incremental) {
                THROW_ERROR_EXCEPTION("Row index requested but reader did not return it");
            } else {
                static_assert(Mode == ERowRangeIndexMode::IncrementalWithError);
                writer->WriteVariant8Tag(MissingRowRangeIndexTag);
            }
            RowIndex_ = Undefined;
        }
    }

    template <ERowRangeIndexMode Mode>
    void WriteRangeIndex(const TUnversionedValue& value, TCheckedInDebugSkiffWriter* writer, TWriteContext* /*context*/)
    {
        if (value.Type == EValueType::Int64) {
            const auto currentRangeIndex = value.Data.Int64;
            if (RangeIndex_ == currentRangeIndex) {
                writer->WriteVariant8Tag(0);
            } else {
                writer->WriteVariant8Tag(1);
                writer->WriteInt64(currentRangeIndex);
            }
            RangeIndex_ = currentRangeIndex;
        } else if (value.Type == EValueType::Null) {
            if constexpr (Mode == ERowRangeIndexMode::Incremental) {
                THROW_ERROR_EXCEPTION("Range index requested but reader did not return it");
            } else {
                static_assert(Mode == ERowRangeIndexMode::IncrementalWithError);
                writer->WriteVariant8Tag(MissingRowRangeIndexTag);
            }
        }
    }

    Y_FORCE_INLINE void PrepareTableIndex(i64 tableIndex)
    {
        if (TableIndex_ != tableIndex) {
            TableIndex_ = tableIndex;
            RowIndex_ = Undefined;
            RangeIndex_ = Undefined;
        }
    }

    Y_FORCE_INLINE void PrepareRangeIndex(i64 rangeIndex)
    {
        if (rangeIndex != RangeIndex_) {
            RangeIndex_ = Undefined;
            RowIndex_ = Undefined;
        }
    }

    Y_FORCE_INLINE void ResetRangeIndex()
    {
        PrepareRangeIndex(Undefined);
    }

    Y_FORCE_INLINE void ResetRowIndex()
    {
        RowIndex_ = Undefined;
    }

private:
    static constexpr i64 Undefined = -2;

    i64 TableIndex_ = Undefined;
    i64 RangeIndex_ = Undefined;
    i64 RowIndex_ = Undefined;
};

TUnversionedValueToSkiffConverter CreateMissingCompositeValueConverter(TString name) {
    return [name=std::move(name)] (const TUnversionedValue& value, TCheckedInDebugSkiffWriter* writer, TWriteContext*) {
        if (value.Type != EValueType::Null) {
            THROW_ERROR_EXCEPTION("Cannot represent nonnull value of column %Qv absent in schema as composite Skiff value",
                    name);
        }
        writer->WriteVariant8Tag(0);
    };
}

template <EValueType ExpectedValueType, typename TFunction>
TUnversionedValueToSkiffConverter CreatePrimitiveValueConverter(
    bool required,
    TFunction function)
{
    if (required) {
        return TPrimitiveConverterWrapper<ExpectedValueType, false, TFunction>(std::move(function));
    } else {
        return TPrimitiveConverterWrapper<ExpectedValueType, true, TFunction>(std::move(function));
    }
}

TUnversionedValueToSkiffConverter CreatePrimitiveValueConverter(EWireType wireType, bool required)
{
    switch (wireType) {
#define CASE(t) \
        case t: \
            return required ? ConvertSimpleValueImpl<t, false> : ConvertSimpleValueImpl<t, true>;
        CASE(EWireType::Int8)
        CASE(EWireType::Int16)
        CASE(EWireType::Int32)
        CASE(EWireType::Int64)
        CASE(EWireType::Uint8)
        CASE(EWireType::Uint16)
        CASE(EWireType::Uint32)
        CASE(EWireType::Uint64)
        CASE(EWireType::Double)
        CASE(EWireType::Boolean)
        CASE(EWireType::String32)
        CASE(EWireType::Yson32)
#undef CASE
        case EWireType::Nothing:
            // TODO(ermolovd): we should use `isOptional` instead of `required` (with corresponding condition inversion).
            YT_VERIFY(required);
            return ConvertSimpleValueImpl<EWireType::Nothing, false>;

        default:
            YT_ABORT();
    }
}

TUnversionedValueToSkiffConverter CreateSimpleValueConverter(
    EWireType wireType,
    bool required,
    ESimpleLogicalValueType logicalType)
{
    switch (logicalType) {
        case ESimpleLogicalValueType::Int8:
        case ESimpleLogicalValueType::Int16:
        case ESimpleLogicalValueType::Int32:
        case ESimpleLogicalValueType::Int64:

        case ESimpleLogicalValueType::Interval:

        case ESimpleLogicalValueType::Date32:
        case ESimpleLogicalValueType::Datetime64:
        case ESimpleLogicalValueType::Timestamp64:
        case ESimpleLogicalValueType::Interval64:
            CheckWireType(wireType, {EWireType::Int8, EWireType::Int16, EWireType::Int32, EWireType::Int64, EWireType::Yson32});
            return CreatePrimitiveValueConverter(wireType, required);

        case ESimpleLogicalValueType::Uint8:
        case ESimpleLogicalValueType::Uint16:
        case ESimpleLogicalValueType::Uint32:
        case ESimpleLogicalValueType::Uint64:

        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
            CheckWireType(wireType, {EWireType::Uint8, EWireType::Uint16, EWireType::Uint32, EWireType::Uint64, EWireType::Yson32});
            return CreatePrimitiveValueConverter(wireType, required);

        case ESimpleLogicalValueType::Float:
        case ESimpleLogicalValueType::Double:
            CheckWireType(wireType, {EWireType::Double, EWireType::Yson32});
            return CreatePrimitiveValueConverter(wireType, required);

        case ESimpleLogicalValueType::Boolean:
            CheckWireType(wireType, {EWireType::Boolean, EWireType::Yson32});
            return CreatePrimitiveValueConverter(wireType, required);

        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::String:
            CheckWireType(wireType, {EWireType::String32, EWireType::Yson32});
            return CreatePrimitiveValueConverter(wireType, required);

        case ESimpleLogicalValueType::Any:
            CheckWireType(wireType, {
                EWireType::Int8,
                EWireType::Int16,
                EWireType::Int32,
                EWireType::Int64,

                EWireType::Uint8,
                EWireType::Uint16,
                EWireType::Uint32,
                EWireType::Uint64,

                EWireType::String32,
                EWireType::Boolean,
                EWireType::Double,
                EWireType::Nothing,
                EWireType::Yson32
            });
            return CreatePrimitiveValueConverter(wireType, required);

        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            CheckWireType(wireType, {EWireType::Nothing, EWireType::Yson32});
            return CreatePrimitiveValueConverter(wireType, required);

        case ESimpleLogicalValueType::Uuid:
            CheckWireType(wireType, {EWireType::Uint128, EWireType::String32, EWireType::Yson32});
            if (wireType == EWireType::Uint128) {
                return CreatePrimitiveValueConverter<EValueType::String>(required, TUuidWriter());
            } else {
                return CreatePrimitiveValueConverter(wireType, required);
            }
    }
}

TUnversionedValueToSkiffConverter CreateComplexValueConverter(
    const TComplexTypeFieldDescriptor& descriptor,
    const std::shared_ptr<TSkiffSchema>& skiffSchema,
    bool isSparse)
{
    TYsonToSkiffConverterConfig config;
    config.AllowOmitTopLevelOptional = isSparse;
    auto ysonToSkiff = CreateYsonToSkiffConverter(descriptor, skiffSchema, config);
    return [ysonToSkiff=ysonToSkiff] (const TUnversionedValue& value, TCheckedInDebugSkiffWriter* skiffWriter, TWriteContext* /*context*/) {
        TMemoryInput input;
        if (value.Type == EValueType::Any || value.Type == EValueType::Composite) {
            // NB. value.Type might be EValueType::Any if user has used override_intermediate_table_schema
            input.Reset(value.Data.String, value.Length);
        } else if (value.Type == EValueType::Null) {
            static const TStringBuf empty = "#";
            input.Reset(empty.Data(), empty.Size());
        } else {
            THROW_ERROR_EXCEPTION("Internal error; unexpected value type: expected %Qlv or %Qlv, actual %Qlv",
                EValueType::Composite,
                EValueType::Null,
                value.Type);
        }
        NYson::TYsonPullParser parser(&input, NYson::EYsonType::Node);
        NYson::TYsonPullParserCursor cursor(&parser);
        ysonToSkiff(&cursor, skiffWriter);
    };
}

TUnversionedValueToSkiffConverter CreateDecimalValueConverter(
    const TFieldDescription& field,
    const TDecimalLogicalType& logicalType)
{
    bool isRequired = field.IsRequired();
    int precision = logicalType.GetPrecision();
    auto wireType = field.ValidatedSimplify();
    switch (wireType) {
        case EWireType::Int32:
            return CreatePrimitiveValueConverter<EValueType::String>(
                isRequired,
                TDecimalSkiffWriter<EWireType::Int32>(precision));
        case EWireType::Int64:
            return CreatePrimitiveValueConverter<EValueType::String>(
                isRequired,
                TDecimalSkiffWriter<EWireType::Int64>(precision));
        case EWireType::Int128:
            return CreatePrimitiveValueConverter<EValueType::String>(
                isRequired,
                TDecimalSkiffWriter<EWireType::Int128>(precision));
        case EWireType::Yson32:
            return CreatePrimitiveValueConverter(wireType, isRequired);
        default:
            CheckSkiffWireTypeForDecimal(precision, wireType);
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TSkiffEncodingInfo
{
    ESkiffWriterColumnType EncodingPart = ESkiffWriterColumnType::Unknown;

    // Converter is set only for sparse part.
    TUnversionedValueToSkiffConverter Converter;

    // FieldIndex is index of field inside Skiff tuple for dense part of the row
    // and variant tag for sparse part of the row.
    ui32 FieldIndex = 0;

    TSkiffEncodingInfo() = default;

    static TSkiffEncodingInfo Skip()
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Skip;
        return result;
    }

    static TSkiffEncodingInfo RangeIndex(ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::RangeIndex;
        result.FieldIndex = fieldIndex;
        return result;
    }

    static TSkiffEncodingInfo RowIndex(ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::RowIndex;
        result.FieldIndex = fieldIndex;
        return result;
    }

    static TSkiffEncodingInfo Dense(ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Dense;
        result.FieldIndex = fieldIndex;
        return result;
    }

    static TSkiffEncodingInfo Sparse(TUnversionedValueToSkiffConverter converter, ui32 fieldIndex)
    {
        TSkiffEncodingInfo result;
        result.EncodingPart = ESkiffWriterColumnType::Sparse;
        result.Converter = std::move(converter);
        result.FieldIndex = fieldIndex;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////


struct TSparseFieldInfo
{
    const TUnversionedValueToSkiffConverter* Converter;
    ui32 SparseFieldTag;
    ui32 ValueIndex;

    TSparseFieldInfo(const TUnversionedValueToSkiffConverter* converter, ui32 sparseFieldTag, ui32 valueIndex)
        : Converter(converter)
        , SparseFieldTag(sparseFieldTag)
        , ValueIndex(valueIndex)
    { }
};

struct TDenseFieldWriterInfo
{
    TUnversionedValueToSkiffConverter Converter;
    ui16 ColumnId;

    TDenseFieldWriterInfo(TUnversionedValueToSkiffConverter converter, ui16 columnId)
        : Converter(std::move(converter))
        , ColumnId(columnId)
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TSkiffWriterTableDescription
{
    std::vector<TSkiffEncodingInfo> KnownFields;
    std::vector<TDenseFieldWriterInfo> DenseFieldInfos;
    int KeySwitchFieldIndex = -1;
    int RangeIndexFieldIndex = -1;
    int RowIndexFieldIndex = -1;
    ERowRangeIndexMode RangeIndexMode = ERowRangeIndexMode::Incremental;
    ERowRangeIndexMode RowIndexMode = ERowRangeIndexMode::Incremental;
    bool HasSparseColumns = false;
    bool HasOtherColumns = false;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffWriter
    : public TSchemalessFormatWriterBase
{
public:
    TSkiffWriter(
        TNameTablePtr nameTable,
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
    { }

    void Init(const std::vector<TTableSchemaPtr>& schemas, const std::vector<std::shared_ptr<TSkiffSchema>>& tableSkiffSchemas)
    {
        GetError().ThrowOnError();

        for (const auto& schema : schemas) {
            UnversionedValueToYsonConverter_.emplace_back(NameTable_, schema, TYsonConverterConfig());
        }

        std::shared_ptr<TSkiffSchema> streamSchema;
        if (ControlAttributesConfig_->EnableEndOfStream) {
            streamSchema = CreateRepeatedVariant16Schema(tableSkiffSchemas);
        } else {
            streamSchema = CreateVariant16Schema(tableSkiffSchemas);
        }
        SkiffWriter_.emplace(std::move(streamSchema), GetOutputStream());

        auto indexedSchemas = TIndexedSchemas(schemas);

        auto tableDescriptionList = CreateTableDescriptionList(tableSkiffSchemas, RangeIndexColumnName, RowIndexColumnName);
        for (const auto& commonTableDescription : tableDescriptionList) {
            auto tableIndex = TableDescriptionList_.size();
            TableDescriptionList_.emplace_back();
            auto& writerTableDescription = TableDescriptionList_.back();
            writerTableDescription.HasOtherColumns = commonTableDescription.HasOtherColumns;
            writerTableDescription.HasSparseColumns = !commonTableDescription.SparseFieldDescriptionList.empty();
            writerTableDescription.KeySwitchFieldIndex = MissingSystemColumn;

            writerTableDescription.RowIndexFieldIndex = MissingSystemColumn;
            writerTableDescription.RowIndexMode = commonTableDescription.RowIndexMode;

            writerTableDescription.RangeIndexFieldIndex = MissingSystemColumn;
            writerTableDescription.RangeIndexMode = commonTableDescription.RangeIndexMode;

            auto& knownFields = writerTableDescription.KnownFields;

            const auto& denseFieldDescriptionList = commonTableDescription.DenseFieldDescriptionList;

            auto& denseFieldWriterInfos = writerTableDescription.DenseFieldInfos;

            auto createComplexValueConverter = [&] (const TFieldDescription& skiffField, bool isSparse) -> TUnversionedValueToSkiffConverter {
                auto columnSchema = indexedSchemas.GetColumnSchema(tableIndex, skiffField.Name());

                // NB: we don't create complex value converter for simple types
                // (column is missing in schema or has simple type).
                //   1. Complex value converter expects unversioned values of type ANY
                //      and simple types have other types.
                //   2. For historical reasons we don't check Skiff schema that strictly for simple types,
                //      e.g we allow column to be optional in table schema and be required in Skiff schema
                //      (runtime check is used in such cases).
                if (!columnSchema) {
                    if (!skiffField.Simplify() && !skiffField.IsRequired()) {
                        // NB. Special case, column is described in Skiff schema as non required complex field
                        // but is missing in schema.
                        // We expect it to be missing in whole table and return corresponding converter.
                        return CreateMissingCompositeValueConverter(skiffField.Name());
                    }
                }

                TLogicalTypePtr logicalType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
                if (columnSchema) {
                    logicalType = columnSchema->LogicalType();
                }
                auto denullifiedLogicalType = DenullifyLogicalType(logicalType);
                try {
                    switch (denullifiedLogicalType->GetMetatype()) {
                        case ELogicalMetatype::Simple:
                            return CreateSimpleValueConverter(
                                skiffField.ValidatedSimplify(),
                                skiffField.IsRequired(),
                                denullifiedLogicalType->AsSimpleTypeRef().GetElement());
                        case ELogicalMetatype::Decimal:
                            return CreateDecimalValueConverter(skiffField, denullifiedLogicalType->AsDecimalTypeRef());
                        case ELogicalMetatype::Optional:
                            // NB. It's complex optional because we denullified type
                        case ELogicalMetatype::List:
                        case ELogicalMetatype::Tuple:
                        case ELogicalMetatype::Struct:
                        case ELogicalMetatype::VariantStruct:
                        case ELogicalMetatype::VariantTuple:
                        case ELogicalMetatype::Dict: {
                            auto descriptor = TComplexTypeFieldDescriptor(skiffField.Name(), columnSchema->LogicalType());
                            return CreateComplexValueConverter(std::move(descriptor), skiffField.Schema(), isSparse);
                        }
                        case ELogicalMetatype::Tagged:
                            // Don't expect tagged type in denullified logical type
                            break;
                    }
                    YT_ABORT();
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Cannot create Skiff writer for column %Qv",
                        skiffField.Name())
                        << TErrorAttribute("logical_type", logicalType)
                        << ex;
                }
            };

            size_t nextDenseIndex = 0;
            for (size_t i = 0; i < denseFieldDescriptionList.size(); ++i) {
                const auto& denseField = denseFieldDescriptionList[i];
                const auto id = NameTable_->GetIdOrRegisterName(denseField.Name());
                ResizeToContainIndex(&knownFields, id);
                YT_VERIFY(knownFields[id].EncodingPart == ESkiffWriterColumnType::Unknown);

                if (denseField.Schema()->GetWireType() == EWireType::Nothing) {
                    knownFields[id] = TSkiffEncodingInfo::Skip();
                    continue;
                }

                TUnversionedValueToSkiffConverter converter;
                try {
                    if (denseField.Name() == RowIndexColumnName) {
                        writerTableDescription.RowIndexFieldIndex = nextDenseIndex;
                        knownFields[id] = TSkiffEncodingInfo::RowIndex(nextDenseIndex);

                        auto method =
                            commonTableDescription.RowIndexMode == ERowRangeIndexMode::Incremental
                            ? (&TRowAndRangeIndexWriter::WriteRowIndex<ERowRangeIndexMode::Incremental>)
                            : (&TRowAndRangeIndexWriter::WriteRowIndex<ERowRangeIndexMode::IncrementalWithError>);

                        converter = std::bind(
                            method,
                            &RowAndRangeIndexWriter_,
                            std::placeholders::_1,
                            std::placeholders::_2,
                            std::placeholders::_3);
                    } else if (denseField.Name() == RangeIndexColumnName) {
                        writerTableDescription.RangeIndexFieldIndex = nextDenseIndex;
                        knownFields[id] = TSkiffEncodingInfo::RangeIndex(nextDenseIndex);

                        auto method =
                            commonTableDescription.RangeIndexMode == ERowRangeIndexMode::Incremental
                            ? (&TRowAndRangeIndexWriter::WriteRangeIndex<ERowRangeIndexMode::Incremental>)
                            : (&TRowAndRangeIndexWriter::WriteRangeIndex<ERowRangeIndexMode::IncrementalWithError>);

                        converter = std::bind(
                            method,
                            &RowAndRangeIndexWriter_,
                            std::placeholders::_1,
                            std::placeholders::_2,
                            std::placeholders::_3);
                    } else {
                        if (denseField.Name() == KeySwitchColumnName) {
                            writerTableDescription.KeySwitchFieldIndex = nextDenseIndex;
                        }
                        knownFields[id] = TSkiffEncodingInfo::Dense(nextDenseIndex);
                        converter = createComplexValueConverter(denseField, /*sparse*/ false);
                    }
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Cannot create Skiff writer for table #%v",
                        tableIndex)
                        << ex;
                }
                denseFieldWriterInfos.emplace_back(converter, id);
                ++nextDenseIndex;
            }

            const auto& sparseFieldDescriptionList = commonTableDescription.SparseFieldDescriptionList;
            for (size_t i = 0; i < sparseFieldDescriptionList.size(); ++i) {
                const auto& sparseField = sparseFieldDescriptionList[i];
                auto id = NameTable_->GetIdOrRegisterName(sparseField.Name());
                ResizeToContainIndex(&knownFields, id);
                YT_VERIFY(knownFields[id].EncodingPart == ESkiffWriterColumnType::Unknown);

                try {
                    auto converter = createComplexValueConverter(sparseField, /*sparse*/ true);
                    knownFields[id] = TSkiffEncodingInfo::Sparse(converter, i);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Cannot create Skiff writer for table #%v",
                        tableIndex)
                        << ex;
                }
            }

            const auto systemColumnMaxId = Max(GetTableIndexColumnId(), GetRangeIndexColumnId(), GetRowIndexColumnId());
            ResizeToContainIndex(&knownFields, systemColumnMaxId);
            knownFields[GetTableIndexColumnId()] = TSkiffEncodingInfo::Skip();
            if (writerTableDescription.RangeIndexFieldIndex == MissingSystemColumn) {
                knownFields[GetRangeIndexColumnId()] = TSkiffEncodingInfo::Skip();
            }
            if (writerTableDescription.RowIndexFieldIndex == MissingSystemColumn) {
                knownFields[GetRowIndexColumnId()] = TSkiffEncodingInfo::Skip();
            }
        }
    }

private:
    std::vector<TErrorAttribute> GetRowPositionErrorAttributes() const
    {
        if (CurrentRow_ == nullptr) {
            return {};
        }

        i64 tableIndex = 0;
        std::optional<i64> rowIndex;

        // We don't use tableIndex / rowIndex from DoWrite function because sometimes we want
        // to throw error before DoWrite knows table index / row index.
        // To keep things simple we always recompute table index / row index by ourselves.
        for (const auto& value : *CurrentRow_) {
            if (value.Id == GetTableIndexColumnId()) {
                YT_VERIFY(value.Type == EValueType::Int64);
                tableIndex = value.Data.Int64;
            } else if (value.Id == GetRowIndexColumnId()) {
                YT_VERIFY(value.Type == EValueType::Int64);
                rowIndex = value.Data.Int64;
            }
        }

        std::vector<TErrorAttribute> result = {
            TErrorAttribute("table_index", tableIndex),
        };
        if (rowIndex) {
            result.emplace_back("row_index", *rowIndex);
        }
        return result;
    }

    void DoWrite(TRange<TUnversionedRow> rows) override
    {
        const auto rowCount = rows.Size();
        TWriteContext writeContext;
        writeContext.NameTable = NameTable_;
        writeContext.TmpBuffer = &YsonBuffer_;

        for (size_t rowIndexInBatch = 0; rowIndexInBatch < rowCount; ++rowIndexInBatch) {
            auto row = rows[rowIndexInBatch];
            CurrentRow_ = &row;
            auto finallyGuard = Finally([&] {
                CurrentRow_ = nullptr;
            });

            const auto valueCount = row.GetCount();
            ui32 tableIndex = 0;
            for (const auto& value : row) {
                if (value.Id == GetTableIndexColumnId()) {
                    tableIndex = value.Data.Int64;
                    break;
                }
            }
            if (tableIndex >= TableDescriptionList_.size()) {
                THROW_ERROR_EXCEPTION("Table #%v is not described by Skiff schema",
                    tableIndex)
                    << GetRowPositionErrorAttributes();
            }
            YT_VERIFY(tableIndex < UnversionedValueToYsonConverter_.size());
            writeContext.UnversionedValueYsonConverter = &UnversionedValueToYsonConverter_[tableIndex];

            const auto& knownFields = TableDescriptionList_[tableIndex].KnownFields;
            const auto& denseFields = TableDescriptionList_[tableIndex].DenseFieldInfos;
            const auto hasOtherColumns = TableDescriptionList_[tableIndex].HasOtherColumns;
            const auto hasSparseColumns = TableDescriptionList_[tableIndex].HasSparseColumns;
            const auto keySwitchFieldIndex = TableDescriptionList_[tableIndex].KeySwitchFieldIndex;
            const auto rowIndexFieldIndex = TableDescriptionList_[tableIndex].RowIndexFieldIndex;
            const auto rangeIndexFieldIndex = TableDescriptionList_[tableIndex].RangeIndexFieldIndex;

            const bool isLastRowInBatch = rowIndexInBatch + 1 == rowCount;

            constexpr ui16 missingColumnPlaceholder = -1;
            constexpr ui16 keySwitchColumnPlaceholder = -2;
            DenseIndexes_.assign(denseFields.size(), missingColumnPlaceholder);
            SparseFields_.clear();
            OtherValueIndexes_.clear();

            if (keySwitchFieldIndex != MissingSystemColumn) {
                DenseIndexes_[keySwitchFieldIndex] = keySwitchColumnPlaceholder;
            }

            ui16 rowIndexValueId = missingColumnPlaceholder;
            ui16 rangeIndexValueId = missingColumnPlaceholder;

            for (ui32 valueIndex = 0; valueIndex < valueCount; ++valueIndex) {
                const auto& value = row[valueIndex];

                const auto columnId = value.Id;
                static const TSkiffEncodingInfo unknownField = TSkiffEncodingInfo();
                const auto& encodingInfo = columnId < knownFields.size() ? knownFields[columnId] : unknownField;
                switch (encodingInfo.EncodingPart) {
                    case ESkiffWriterColumnType::Dense:
                        DenseIndexes_[encodingInfo.FieldIndex] = valueIndex;
                        break;
                    case ESkiffWriterColumnType::Sparse:
                        SparseFields_.emplace_back(
                            &encodingInfo.Converter,
                            encodingInfo.FieldIndex,
                            valueIndex);
                        break;
                    case ESkiffWriterColumnType::Skip:
                        break;
                    case ESkiffWriterColumnType::RowIndex:
                        rowIndexValueId = valueIndex;
                        break;
                    case ESkiffWriterColumnType::RangeIndex:
                        rangeIndexValueId = valueIndex;
                        break;
                    case ESkiffWriterColumnType::Unknown:
                        if (!hasOtherColumns) {
                            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::FormatCannotRepresentRow, "Column %Qv is not described by Skiff schema and there is no %Qv column",
                                NameTable_->GetName(columnId),
                                OtherColumnsName)
                                << GetRowPositionErrorAttributes();
                        }
                        OtherValueIndexes_.emplace_back(valueIndex);
                        break;
                    default:
                        YT_ABORT();
                }
            }
            if (rowIndexFieldIndex != MissingSystemColumn || rangeIndexFieldIndex != MissingSystemColumn) {
                RowAndRangeIndexWriter_.PrepareTableIndex(tableIndex);
                if (rangeIndexFieldIndex != MissingSystemColumn) {
                    DenseIndexes_[rangeIndexFieldIndex] = rangeIndexValueId;
                }
                if (rangeIndexValueId != missingColumnPlaceholder) {
                    YT_VERIFY(row[rangeIndexValueId].Type == EValueType::Int64);
                    const auto rangeIndex = row[rangeIndexValueId].Data.Int64;
                    RowAndRangeIndexWriter_.PrepareRangeIndex(rangeIndex);
                } else if (rangeIndexFieldIndex != MissingSystemColumn) {
                    RowAndRangeIndexWriter_.ResetRangeIndex();
                }

                if (rowIndexFieldIndex != MissingSystemColumn) {
                    DenseIndexes_[rowIndexFieldIndex] = rowIndexValueId;
                    if (rowIndexValueId == missingColumnPlaceholder) {
                        RowAndRangeIndexWriter_.ResetRowIndex();
                    }
                }
            }

            SkiffWriter_->WriteVariant16Tag(tableIndex);
            for (size_t idx = 0; idx < denseFields.size(); ++idx) {
                const auto& fieldInfo = denseFields[idx];
                const auto valueIndex = DenseIndexes_[idx];

                switch (valueIndex) {
                    case missingColumnPlaceholder:
                        fieldInfo.Converter(
                            MakeUnversionedSentinelValue(EValueType::Null, fieldInfo.ColumnId),
                            &*SkiffWriter_,
                            &writeContext);
                        break;
                    case keySwitchColumnPlaceholder:
                        SkiffWriter_->WriteBoolean(CheckKeySwitch(row, isLastRowInBatch));
                        break;
                    default: {
                        const auto& value = row[valueIndex];
                        fieldInfo.Converter(
                            value,
                            &*SkiffWriter_,
                            &writeContext);
                        break;
                    }
                }
            }

            if (hasSparseColumns) {
                for (const auto& fieldInfo : SparseFields_) {
                    const auto& value = row[fieldInfo.ValueIndex];
                    if (value.Type != EValueType::Null) {
                        SkiffWriter_->WriteVariant16Tag(fieldInfo.SparseFieldTag);
                        (*fieldInfo.Converter)(value, &*SkiffWriter_, &writeContext);
                    }
                }
                SkiffWriter_->WriteVariant16Tag(EndOfSequenceTag<ui16>());
            }
            if (hasOtherColumns) {
                YsonBuffer_.Clear();
                TBufferOutput out(YsonBuffer_);
                NYson::TYsonWriter writer(
                    &out,
                    NYson::EYsonFormat::Binary,
                    NYson::EYsonType::Node,
                    /* enableRaw */ true);
                writer.OnBeginMap();
                for (const auto otherValueIndex : OtherValueIndexes_) {
                    const auto& value = row[otherValueIndex];
                    writer.OnKeyedItem(NameTable_->GetName(value.Id));
                    writeContext.UnversionedValueYsonConverter->WriteValue(value, &writer);
                }
                writer.OnEndMap();
                SkiffWriter_->WriteYson32(TStringBuf(YsonBuffer_.Data(), YsonBuffer_.Size()));
            }
            SkiffWriter_->Flush();
            TryFlushBuffer(false);
        }
        YT_UNUSED_FUTURE(Flush());
    }

    TFuture<void> Flush() override
    {
        SkiffWriter_->Flush();
        return TSchemalessFormatWriterBase::Flush();
    }

    TFuture<void> Close() override
    {
        // NB(gritukan): You can't move it into WriteEndOfStream, since it
        // will be called between SkiffWriter and buffer flushes leading to
        // their inconsistency.
        if (ControlAttributesConfig_->EnableEndOfStream) {
            SkiffWriter_->WriteVariant16Tag(EndOfSequenceTag<ui16>());
        }

        SkiffWriter_->Flush();
        return TSchemalessFormatWriterBase::Close();
    }

private:
    std::optional<NSkiff::TCheckedInDebugSkiffWriter> SkiffWriter_;

    std::vector<ui16> DenseIndexes_;
    std::vector<TSparseFieldInfo> SparseFields_;
    std::vector<ui16> OtherValueIndexes_;

    // Table #i is described by element with index i.
    std::vector<TSkiffWriterTableDescription> TableDescriptionList_;

    std::vector<TUnversionedValueYsonWriter> UnversionedValueToYsonConverter_;

    // Buffer that we are going to reuse in order to reduce memory allocations.
    TBuffer YsonBuffer_;

    TRowAndRangeIndexWriter RowAndRangeIndexWriter_;

    const TUnversionedRow* CurrentRow_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    try {
        auto config = NYTree::ConvertTo<TSkiffFormatConfigPtr>(attributes);
        auto skiffSchemas = ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);

        auto copySchemas = schemas;
        if (config->OverrideIntermediateTableSchema) {
            YT_VERIFY(!schemas.empty());
            if (!IsTrivialIntermediateSchema(*schemas[0])) {
                THROW_ERROR_EXCEPTION("Cannot use \"override_intermediate_table_schema\" since input table #0 has nontrivial schema")
                    << TErrorAttribute("schema", *schemas[0]);
            }
            copySchemas[0] = New<TTableSchema>(*config->OverrideIntermediateTableSchema);
        }

        return CreateWriterForSkiff(
            skiffSchemas,
            std::move(nameTable),
            copySchemas,
            std::move(output),
            enableContextSaving,
            std::move(controlAttributesConfig),
            keyColumnCount);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::InvalidFormat, "Failed to parse config for Skiff format") << ex;
    }
}

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const std::vector<std::shared_ptr<TSkiffSchema>>& tableSkiffSchemas,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto result = New<TSkiffWriter>(
        std::move(nameTable),
        std::move(output),
        enableContextSaving,
        std::move(controlAttributesConfig),
        keyColumnCount);
    result->Init(schemas, tableSkiffSchemas);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
