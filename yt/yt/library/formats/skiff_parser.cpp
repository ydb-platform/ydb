#include "skiff_parser.h"
#include "skiff_yson_converter.h"

#include "helpers.h"

#include "yson_map_to_unversioned_value.h"

#include <yt/yt/library/decimal/decimal.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/library/tz_types/tz_types.h>

#include <yt/yt/client/formats/parser.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/client/table_client/value_consumer.h>

#include <yt/yt/core/misc/coro_pipe.h>

#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/token_writer.h>

#include <util/generic/strbuf.h>
#include <util/stream/zerocopy.h>
#include <util/stream/buffer.h>

namespace NYT::NFormats {

using namespace NTableClient;
using namespace NSkiff;
using namespace NSkiffExt;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TSkiffToUnversionedValueConverter = std::function<void(TCheckedInDebugSkiffParser*, IValueConsumer*)>;

template <bool IsNullable, typename TFunction>
class TPrimitiveTypeConverter
{
public:
    explicit TPrimitiveTypeConverter(ui32 columnId, TFunction function = {})
        : ColumnId_(columnId)
        , Function_(function)
    { }

    void operator()(TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
        if constexpr (IsNullable) {
            ui8 tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
                return;
            } else if (tag > 1) {
                const auto name = valueConsumer->GetNameTable()->GetName(ColumnId_);
                THROW_ERROR_EXCEPTION(
                    "Found bad variant8 tag %Qv when parsing optional field %Qv",
                    tag,
                    name);
            }
        }

        auto value = Function_(parser);
        using TValueType = std::decay_t<decltype(value)>;

        if constexpr (std::is_same_v<TValueType, TStringBuf>) {
            valueConsumer->OnValue(MakeUnversionedStringValue(value, ColumnId_));
        } else if constexpr (
            std::is_same_v<TValueType, i8> ||
            std::is_same_v<TValueType, i16> ||
            std::is_same_v<TValueType, i32> ||
            std::is_same_v<TValueType, i64>)
        {
            valueConsumer->OnValue(MakeUnversionedInt64Value(value, ColumnId_));
        } else if constexpr (
            std::is_same_v<TValueType, ui8> ||
            std::is_same_v<TValueType, ui16> ||
            std::is_same_v<TValueType, ui32> ||
            std::is_same_v<TValueType, ui64>)
        {
            valueConsumer->OnValue(MakeUnversionedUint64Value(value, ColumnId_));
        } else if constexpr (std::is_same_v<TValueType, bool>) {
            valueConsumer->OnValue(MakeUnversionedBooleanValue(value, ColumnId_));
        } else if constexpr (std::is_same_v<TValueType, double>) {
            valueConsumer->OnValue(MakeUnversionedDoubleValue(value, ColumnId_));
        } else if constexpr (std::is_same_v<TValueType, std::nullptr_t>) {
            valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
        } else {
            static_assert(std::is_same_v<TValueType, TStringBuf>);
        }
    }

private:
    ui32 ColumnId_;
    TFunction Function_;
};

template <bool IsNullable, typename TFunction>
TPrimitiveTypeConverter<IsNullable, TFunction> CreatePrimitiveTypeConverter(ui32 columnId, TFunction function)
{
    return TPrimitiveTypeConverter<IsNullable, TFunction>(columnId, function);
}

template <bool isNullable>
class TYson32TypeConverterImpl
{
public:
    explicit TYson32TypeConverterImpl(ui16 columnId, TYsonToUnversionedValueConverter* ysonConverter)
        : ColumnId_(columnId)
        , YsonConverter_(ysonConverter)
    {}

    void operator()(TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
        if constexpr (isNullable) {
            ui8 tag = parser->ParseVariant8Tag();
            if (tag == 0) {
                valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
                return;
            } else if (tag > 1) {
                const auto name = valueConsumer->GetNameTable()->GetName(ColumnId_);
                THROW_ERROR_EXCEPTION(
                    "Found bad variant8 tag %Qv when parsing optional field %Qv",
                    tag,
                    name);
            }
        }
        YT_VERIFY(YsonConverter_);
        auto ysonString = parser->ParseYson32();
        YsonConverter_->SetColumnIndex(ColumnId_);
        {
            auto consumer = YsonConverter_->SwitchToTable(0);
            YT_VERIFY(consumer == valueConsumer);
        }
        ParseYsonStringBuffer(ysonString, NYson::EYsonType::Node, YsonConverter_);
    }

private:
    const ui16 ColumnId_;
    TYsonToUnversionedValueConverter* YsonConverter_ = nullptr;
};

TSkiffToUnversionedValueConverter CreatePrimitiveTypeConverter(
    EWireType wireType,
    bool required,
    int columnId,
    TYsonToUnversionedValueConverter* ysonConverter)
{
    switch (wireType) {
#define CASE(x) \
        case ((x)): \
            do { \
                if (required) { \
                    return TPrimitiveTypeConverter<false, TSimpleSkiffParser<(x)>>(columnId); \
                } else { \
                    return TPrimitiveTypeConverter<true, TSimpleSkiffParser<(x)>>(columnId); \
                } \
            } while (0)
        CASE(EWireType::Int8);
        CASE(EWireType::Int16);
        CASE(EWireType::Int32);
        CASE(EWireType::Int64);
        CASE(EWireType::Uint8);
        CASE(EWireType::Uint16);
        CASE(EWireType::Uint32);
        CASE(EWireType::Uint64);
        CASE(EWireType::Boolean);
        CASE(EWireType::Double);
        CASE(EWireType::String32);
        CASE(EWireType::Nothing);
#undef CASE
        case EWireType::Yson32:
            if (required) {
                return TYson32TypeConverterImpl<false>(columnId, ysonConverter);
            } else {
                return TYson32TypeConverterImpl<true>(columnId, ysonConverter);
            }
        default:
            break;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TSkiffToUnversionedValueConverter CreateTzTupleConverter(
    const std::shared_ptr<NSkiff::TSkiffSchema>& skiffSchema,
    bool required,
    ui16 columnId)
{
    // Schema is already validated to match tz-time format.
    YT_VERIFY(skiffSchema->GetWireType() == EWireType::Tuple);
    const auto& children = skiffSchema->GetChildren();
    YT_VERIFY(children.size() == 2);
    const auto innerWireType = children[0]->GetWireType();
    YT_VERIFY(children[1]->GetWireType() == EWireType::Uint16);
    switch (innerWireType) {
#define CASE(x) \
        case x: \
            do { \
                if (required) { \
                    return TPrimitiveTypeConverter<false, TTzTupleSkiffParser<(x)>>(columnId); \
                } else { \
                    return TPrimitiveTypeConverter<true, TTzTupleSkiffParser<(x)>>(columnId); \
                } \
            } while (0)
        CASE(EWireType::Int32);
        CASE(EWireType::Int64);
        CASE(EWireType::Uint16);
        CASE(EWireType::Uint32);
        CASE(EWireType::Uint64);
#undef CASE
        default:
            break;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TSkiffToUnversionedValueConverter CreateSimpleValueConverter(
    ESimpleLogicalValueType columnType,
    const TFieldDescription& fieldDescription,
    ui16 columnId,
    TYsonToUnversionedValueConverter* ysonConverter)
{
    ValidateDoesNotMatchOptionalSingular(fieldDescription.Schema());
    auto [skiffSchema, optionalKind] = StripOptional(fieldDescription.Schema());
    auto wireType = skiffSchema->GetWireType();
    bool required = optionalKind == EOptionalKind::None;
    ValidateMatchesSimpleType(skiffSchema, columnType);

    if (columnType == ESimpleLogicalValueType::Uuid && wireType == EWireType::Uint128) {
        if (!required) {
            return CreatePrimitiveTypeConverter<true>(columnId, TUuidParser());
        } else {
            return CreatePrimitiveTypeConverter<false>(columnId, TUuidParser());
        }
    } else if (IsTzType(columnType) && wireType == EWireType::Tuple) {
        return CreateTzTupleConverter(skiffSchema, required, columnId);
    } else {
        return CreatePrimitiveTypeConverter(wireType, required, columnId, ysonConverter);
    }
}

TSkiffToUnversionedValueConverter CreateDecimalValueConverter(
    const TFieldDescription& fieldDescription,
    ui16 columnId,
    const TDecimalLogicalType& denullifiedType,
    TYsonToUnversionedValueConverter* ysonConverter)
{
    const auto precision = denullifiedType.GetPrecision();
    auto [skiffSchema, optionalKind] = StripOptional(fieldDescription.Schema());
    const auto wireType = skiffSchema->GetWireType();
    bool required = optionalKind == EOptionalKind::None;
    switch (wireType) {
#define CASE(x) \
        case x: \
            do { \
                if (!required) { \
                    return CreatePrimitiveTypeConverter<true>(columnId, TDecimalSkiffParser<x>(precision)); \
                } else { \
                    return CreatePrimitiveTypeConverter<false>(columnId, TDecimalSkiffParser<x>(precision)); \
                } \
            } while (0)
        CASE(EWireType::Int32);
        CASE(EWireType::Int64);
        CASE(EWireType::Int128);
        CASE(EWireType::Int256);
#undef CASE
        case EWireType::Yson32:
            return CreatePrimitiveTypeConverter(wireType, required, columnId, ysonConverter);
        default:
            CheckSkiffWireTypeForDecimal(precision, wireType);
            YT_ABORT();
    }
}

class TComplexValueConverter
{
public:
    TComplexValueConverter(TSkiffToYsonConverter converter, ui16 columnId)
        : Converter_(std::move(converter))
        , ColumnId_(columnId)
    { }

    void operator()(TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
        Buffer_.Clear();
        {
            TBufferOutput out(Buffer_);
            NYson::TCheckedInDebugYsonTokenWriter ysonTokenWriter(&out);
            Converter_(parser, &ysonTokenWriter);
            ysonTokenWriter.Finish();
        }
        auto value = TStringBuf(Buffer_.Data(), Buffer_.Size());
        constexpr TStringBuf entity = "#";
        if (value == entity) {
            valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
        } else {
            valueConsumer->OnValue(MakeUnversionedCompositeValue(value, ColumnId_));
        }
    }

private:
    const TSkiffToYsonConverter Converter_;
    const ui16 ColumnId_;
    TBuffer Buffer_;
};

TSkiffToUnversionedValueConverter CreateComplexValueConverter(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const std::shared_ptr<TSkiffSchema>& skiffSchema,
    ui16 columnId,
    bool sparseColumn)
{
    TSkiffToYsonConverterConfig config;
    config.AllowOmitTopLevelOptional = sparseColumn;
    auto converter = CreateSkiffToYsonConverter(descriptor, skiffSchema, config);
    return TComplexValueConverter(converter, columnId);
}

////////////////////////////////////////////////////////////////////////////////

class TSkiffParserImpl
{
public:
    TSkiffParserImpl(
        std::shared_ptr<TSkiffSchema> skiffSchema,
        const TTableSchemaPtr& tableSchema,
        IValueConsumer* valueConsumer)
        : SkiffSchemaList_({std::move(skiffSchema)})
        , ValueConsumer_(valueConsumer)
        , YsonToUnversionedValueConverter_(TYsonConverterConfig(), ValueConsumer_)
        , OtherColumnsConsumer_(TYsonConverterConfig(), ValueConsumer_)
    {
        THashMap<std::string, const TColumnSchema*> columnSchemas;
        for (const auto& column : tableSchema->Columns()) {
            columnSchemas[column.Name()] = &column;
        }

        auto genericTableDescriptions = CreateTableDescriptionList(
            SkiffSchemaList_, RangeIndexColumnName, RowIndexColumnName);

        for (int tableIndex = 0; tableIndex < std::ssize(genericTableDescriptions); ++tableIndex) {
            const auto& genericTableDescription = genericTableDescriptions[tableIndex];
            auto& parserTableDescription = TableDescriptions_.emplace_back();
            parserTableDescription.HasOtherColumns = genericTableDescription.HasOtherColumns;
            for (const auto& fieldDescription : genericTableDescription.DenseFieldDescriptionList) {
                const auto columnId = ValueConsumer_->GetNameTable()->GetIdOrRegisterName(fieldDescription.Name());
                auto columnSchema = columnSchemas.FindPtr(fieldDescription.Name());
                TSkiffToUnversionedValueConverter converter;
                try {
                    converter = CreateSkiffToUnversionedValueConverter(
                        columnId,
                        columnSchema == nullptr ? nullptr : *columnSchema,
                        fieldDescription,
                        /*sparseColumn*/ false);
                } catch (const std::exception& ex) {
                    auto logicalType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
                    if (columnSchema && *columnSchema) {
                        logicalType = (*columnSchema)->LogicalType();
                    }
                    THROW_ERROR_EXCEPTION("Cannot create Skiff parser for table #%v",
                        tableIndex)
                        .With("logical_type", logicalType)
                        .With(ex);
                }
                parserTableDescription.DenseFieldConverters.emplace_back(converter);
            }

            for (const auto& fieldDescription : genericTableDescription.SparseFieldDescriptionList) {
                const auto columnId = ValueConsumer_->GetNameTable()->GetIdOrRegisterName(fieldDescription.Name());
                TSkiffToUnversionedValueConverter converter;
                auto columnSchema = columnSchemas.FindPtr(fieldDescription.Name());
                try {
                    converter = CreateSkiffToUnversionedValueConverter(
                        columnId,
                        columnSchema == nullptr ? nullptr : *columnSchema,
                        fieldDescription,
                        /*sparseColumn*/ true);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Cannot create Skiff parser for table #%v",
                        tableIndex)
                        .With(ex);
                }
                parserTableDescription.SparseFieldConverters.emplace_back(converter);
            }
        }
    }

    void DoParse(IZeroCopyInput* stream)
    {
        Parser_ = std::make_unique<TCheckedInDebugSkiffParser>(CreateVariant16Schema(SkiffSchemaList_), stream);

        while (Parser_->HasMoreData()) {
            auto tag = Parser_->ParseVariant16Tag();
            if (tag > 0) {
                THROW_ERROR_EXCEPTION("Unknown table index variant16 tag %v",
                    tag);
            }
            ValueConsumer_->OnBeginRow();

            for (const auto& converter : TableDescriptions_[tag].DenseFieldConverters) {
                converter(Parser_.get(), ValueConsumer_);
            }

            if (!TableDescriptions_[tag].SparseFieldConverters.empty()) {
                for (auto sparseFieldIdx = Parser_->ParseVariant16Tag();
                     sparseFieldIdx != EndOfSequenceTag<ui16>();
                     sparseFieldIdx = Parser_->ParseVariant16Tag()) {
                    if (sparseFieldIdx >= TableDescriptions_[tag].SparseFieldConverters.size()) {
                        THROW_ERROR_EXCEPTION("Bad sparse field index %Qv, total sparse field count %Qv",
                            sparseFieldIdx,
                            TableDescriptions_[tag].SparseFieldConverters.size());
                    }

                    const auto& converter = TableDescriptions_[tag].SparseFieldConverters[sparseFieldIdx];
                    converter(Parser_.get(), ValueConsumer_);
                }
            }

            if (TableDescriptions_[tag].HasOtherColumns) {
                auto buf = Parser_->ParseYson32();
                ParseYsonStringBuffer(
                    buf,
                    NYson::EYsonType::Node,
                    &OtherColumnsConsumer_);
            }

            ValueConsumer_->OnEndRow();
        }
    }

    ui64 GetReadBytesCount()
    {
        return Parser_->GetReadBytesCount();
    }

private:
    TSkiffToUnversionedValueConverter CreateSkiffToUnversionedValueConverter(
        int columnId,
        const TColumnSchema* columnSchema,
        const TFieldDescription& skiffField,
        bool sparseColumn)
    {
        const auto columnType = columnSchema ?
            columnSchema->LogicalType() :
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));

        const auto denullifiedLogicalType = DenullifyLogicalType(columnType);

        try {
            switch (denullifiedLogicalType->GetMetatype()) {
                case ELogicalMetatype::Simple:
                    return CreateSimpleValueConverter(
                        denullifiedLogicalType->AsSimpleTypeRef().GetElement(),
                        skiffField,
                        columnId,
                        &YsonToUnversionedValueConverter_);
                case ELogicalMetatype::Decimal:
                    return CreateDecimalValueConverter(
                        skiffField,
                        columnId,
                        denullifiedLogicalType->AsDecimalTypeRef(),
                        &YsonToUnversionedValueConverter_);
                case ELogicalMetatype::Optional:
                case ELogicalMetatype::List:
                case ELogicalMetatype::Tuple:
                case ELogicalMetatype::Struct:
                case ELogicalMetatype::VariantTuple:
                case ELogicalMetatype::VariantStruct:
                case ELogicalMetatype::Dict:
                    return CreateComplexValueConverter(
                        TComplexTypeFieldDescriptor(skiffField.Name(), columnType),
                        skiffField.Schema(),
                        columnId,
                        /*sparseColumn*/ sparseColumn);
                case ELogicalMetatype::Tagged:
                case ELogicalMetatype::AggregateState:
                    // Denullified type should not contain tagged types.
                    break;
            }
            YT_ABORT();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Cannot create Skiff parser for column %Qv",
                skiffField.Name())
                .With(ex);
        }
    }

private:
    struct TTableDescription
    {
        std::vector<TSkiffToUnversionedValueConverter> DenseFieldConverters;
        std::vector<TSkiffToUnversionedValueConverter> SparseFieldConverters;
        bool HasOtherColumns = false;
    };

    const TSkiffSchemaList SkiffSchemaList_;

    IValueConsumer* const ValueConsumer_;

    TYsonToUnversionedValueConverter YsonToUnversionedValueConverter_;
    TYsonMapToUnversionedValueConverter OtherColumnsConsumer_;

    std::unique_ptr<TCheckedInDebugSkiffParser> Parser_;
    std::vector<TTableDescription> TableDescriptions_;
};

////////////////////////////////////////////////////////////////////////////////

class TSkiffPushParser
    : public IParser
{
public:
    TSkiffPushParser(
        std::shared_ptr<TSkiffSchema> skiffSchema,
        const TTableSchemaPtr& tableSchema,
        IValueConsumer* consumer)
        : ParserImpl_(std::make_unique<TSkiffParserImpl>(
            std::move(skiffSchema),
            tableSchema,
            consumer))
        , ParserCoroPipe_(
            BIND([this] (IZeroCopyInput* stream) {
                ParserImpl_->DoParse(stream);
            }))
    {}

    void Read(TStringBuf data) override
    {
        if (!data.empty()) {
            ParserCoroPipe_.Feed(data);
        }
    }

    void Finish() override
    {
        ParserCoroPipe_.Finish();
    }

private:
    std::unique_ptr<TSkiffParserImpl> ParserImpl_;
    TCoroPipe ParserCoroPipe_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSkiff(
    std::shared_ptr<TSkiffSchema> skiffSchema,
    const TTableSchemaPtr& tableSchema,
    IValueConsumer* consumer)
{
    auto tableDescriptionList = CreateTableDescriptionList({skiffSchema}, RangeIndexColumnName, RowIndexColumnName);
    if (tableDescriptionList.size() != 1) {
        THROW_ERROR_EXCEPTION("Expected to have single table, actual table description count %Qv",
            tableDescriptionList.size());
    }
    return std::make_unique<TSkiffPushParser>(
        std::move(skiffSchema),
        tableSchema,
        consumer);
}

std::unique_ptr<IParser> CreateParserForSkiff(
    IValueConsumer* consumer,
    const std::vector<std::shared_ptr<TSkiffSchema>>& skiffSchemas,
    const TSkiffFormatConfigPtr& config,
    int tableIndex)
{
    if (tableIndex >= std::ssize(skiffSchemas)) {
        THROW_ERROR_EXCEPTION("Skiff format config does not describe table #%v",
            tableIndex);
    }
    if (tableIndex == 0 && config->OverrideIntermediateTableSchema) {
        if (!IsTrivialIntermediateSchema(*consumer->GetSchema())) {
            THROW_ERROR_EXCEPTION("Cannot use \"override_intermediate_table_schema\" since output table #0 has nontrivial schema")
                .With("schema", *consumer->GetSchema());
        }
        return CreateParserForSkiff(
            skiffSchemas[tableIndex],
            New<TTableSchema>(*config->OverrideIntermediateTableSchema),
            consumer);
    } else {
        return CreateParserForSkiff(
            skiffSchemas[tableIndex],
            consumer);
    }
}

std::unique_ptr<IParser> CreateParserForSkiff(
    std::shared_ptr<TSkiffSchema> skiffSchema,
    IValueConsumer* consumer)
{
    return CreateParserForSkiff(std::move(skiffSchema), consumer->GetSchema(), consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
