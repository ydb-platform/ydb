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

template <typename TPhysical, typename TParseFunction>
class TSkiffToTypedValueConverter
{
public:
    TSkiffToTypedValueConverter(
        int columnId,
        TYsonToUnversionedValueConverter* /*ysonConverter*/,
        TParseFunction parseFunction)
        : ColumnId_(columnId)
        , ParseFunction_(std::move(parseFunction))
    { }

    void operator()(TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
        auto value = ParseFunction_(parser);
        if constexpr (ValueType == EValueType::Null) {
            valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
        } else if constexpr (ValueType == EValueType::Int64) {
            valueConsumer->OnValue(MakeUnversionedInt64Value(value, ColumnId_));
        } else if constexpr (ValueType == EValueType::Uint64) {
            valueConsumer->OnValue(MakeUnversionedUint64Value(value, ColumnId_));
        } else if constexpr (ValueType == EValueType::Boolean) {
            valueConsumer->OnValue(MakeUnversionedBooleanValue(value, ColumnId_));
        } else if constexpr (ValueType == EValueType::Double) {
            valueConsumer->OnValue(MakeUnversionedDoubleValue(value, ColumnId_));
        } else if constexpr (ValueType == EValueType::String) {
            valueConsumer->OnValue(MakeUnversionedStringValue(value, ColumnId_));
        } else {
            static_assert(false, "Unexpected value type");
        }
    }

private:
    static constexpr EValueType ValueType = TCppTypeToValueType<TPhysical>::Value;

    const int ColumnId_;
    TParseFunction ParseFunction_;
};

////////////////////////////////////////////////////////////////////////////////

class TYson32ToValueConverter
{
public:
    TYson32ToValueConverter(int columnId, TYsonToUnversionedValueConverter* ysonConverter)
        : ColumnId_(columnId)
        , YsonConverter_(ysonConverter)
    { }

    void operator()(TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
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
    const int ColumnId_;
    TYsonToUnversionedValueConverter* const YsonConverter_;
};

////////////////////////////////////////////////////////////////////////////////

template <EOptionalKind OptionalKind, typename TInnerConverter>
class TOptionalParserConverter
{
public:
    template <typename... TArgs>
    TOptionalParserConverter(int columnId, TYsonToUnversionedValueConverter* ysonConverter, TArgs&&... args)
        : ColumnId_(columnId)
        , Converter_(columnId, ysonConverter, std::forward<TArgs>(args)...)
    { }

    void operator()(TCheckedInDebugSkiffParser* parser, IValueConsumer* valueConsumer)
    {
        if constexpr (OptionalKind != EOptionalKind::None) {
            i32 tag;
            if constexpr (OptionalKind == EOptionalKind::Variant8) {
                tag = parser->ParseVariant8Tag();
            }

            if (tag == 0) {
                valueConsumer->OnValue(MakeUnversionedNullValue(ColumnId_));
                return;
            } else if (tag != 1) {
                const auto name = valueConsumer->GetNameTable()->GetName(ColumnId_);
                THROW_ERROR_EXCEPTION(
                    "Unexpected %Qlv tag of optional field %Qv: expected 0 or 1, got %v",
                    EWireType::Variant8,
                    name,
                    tag);
            }
        }

        Converter_(parser, valueConsumer);
    }

private:
    const int ColumnId_;
    TInnerConverter Converter_;
};

template <EOptionalKind OptionalKind>
struct TParserConverters
{
    template <typename TPhysical, typename TParseFunction>
    using TPhysicalConverter = TOptionalParserConverter<
        OptionalKind,
        TSkiffToTypedValueConverter<TPhysical, TParseFunction>
    >;

    using TYson32Converter = TOptionalParserConverter<OptionalKind, TYson32ToValueConverter>;
};

////////////////////////////////////////////////////////////////////////////////

TSkiffToUnversionedValueConverter CreateSimpleValueConverter(
    ESimpleLogicalValueType columnType,
    const TFieldDescription& fieldDescription,
    int columnId,
    TYsonToUnversionedValueConverter* ysonConverter)
{
    ValidateDoesNotMatchOptionalSingular(fieldDescription.Schema());
    auto [skiffSchema, optionalKind] = StripOptional(fieldDescription.Schema());
    return DispatchOptionalKind(
        optionalKind,
        [&skiffSchema, columnType, columnId, ysonConverter] <EOptionalKind OptionalKind> () {
            return CreateSimpleConverter<
                TSkiffToUnversionedValueConverter,
                TParserConverters<OptionalKind>,
                TParseFunctions
            >(skiffSchema, columnType, columnId, ysonConverter);
        });
}

TSkiffToUnversionedValueConverter CreateDecimalValueConverter(
    const TFieldDescription& fieldDescription,
    int columnId,
    const TDecimalLogicalType& denullifiedType,
    TYsonToUnversionedValueConverter* ysonConverter)
{
    auto [skiffSchema, optionalKind] = StripOptional(fieldDescription.Schema());
    return DispatchOptionalKind(
        optionalKind,
        [&skiffSchema, &denullifiedType, columnId, ysonConverter] <EOptionalKind OptionalKind> () {
            return CreateDecimalConverter<
                TSkiffToUnversionedValueConverter,
                TParserConverters<OptionalKind>,
                TParseFunctions
            >(skiffSchema, denullifiedType.GetPrecision(), columnId, ysonConverter);
        });
}

class TComplexValueConverter
{
public:
    TComplexValueConverter(TSkiffToYsonConverter converter, int columnId)
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
    const int ColumnId_;
    TBuffer Buffer_;
};

TSkiffToUnversionedValueConverter CreateComplexValueConverter(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const std::shared_ptr<TSkiffSchema>& skiffSchema,
    int columnId,
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
