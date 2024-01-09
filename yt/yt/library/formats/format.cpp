#include "format.h"

#include "arrow_parser.h"
#include "arrow_writer.h"
#include "dsv_parser.h"
#include "dsv_writer.h"
#include "protobuf_parser.h"
#include "protobuf_writer.h"
#include "schemaful_dsv_parser.h"
#include "schemaful_dsv_writer.h"
#include "web_json_writer.h"
#include "schemaless_writer_adapter.h"
#include "skiff_parser.h"
#include "skiff_writer.h"
#include "yamred_dsv_parser.h"
#include "yamred_dsv_writer.h"
#include "yamr_parser.h"
#include "yamr_writer.h"
#include "yson_parser.h"

#include <yt/yt/client/formats/parser.h>
#include <yt/yt/client/formats/versioned_writer.h>
#include <yt/yt/client/formats/schemaful_writer.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/table_consumer.h>

#include <yt/yt/library/skiff_ext/schema_match.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/yson/forwarding_consumer.h>

#include <yt/yt/core/json/json_parser.h>
#include <yt/yt/core/json/json_writer.h>

namespace NYT::NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NJson;
using namespace NTableClient;
using namespace NSkiffExt;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

namespace {

EYsonType DataTypeToYsonType(EDataType dataType)
{
    switch (dataType) {
        case EDataType::Structured:
            return EYsonType::Node;
        case EDataType::Tabular:
            return EYsonType::ListFragment;
        default:
            THROW_ERROR_EXCEPTION("Data type %Qlv is not supported by YSON",
                dataType);
    }
}

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForYson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IZeroCopyOutput* output)
{
    auto config = ConvertTo<TYsonFormatConfigPtr>(&attributes);
    return CreateYsonWriter(
        output,
        config->Format,
        DataTypeToYsonType(dataType),
        config->Format == EYsonFormat::Binary);
}

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForJson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IOutputStream* output)
{
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return CreateJsonConsumer(output, DataTypeToYsonType(dataType), config);
}

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IOutputStream* output)
{
    auto config = ConvertTo<TDsvFormatConfigPtr>(&attributes);
    switch (dataType) {
        case EDataType::Structured:
            return std::unique_ptr<IFlushableYsonConsumer>(new TDsvNodeConsumer(output, config));

        case EDataType::Tabular:
        case EDataType::Binary:
        case EDataType::Null:
            THROW_ERROR_EXCEPTION("Data type %Qlv is not supported by DSV",
                dataType);

        default:
            YT_ABORT();
    };
}

class TTableParserAdapter
    : public IParser
{
public:
    TTableParserAdapter(
        const TFormat& format,
        std::vector<IValueConsumer*> valueConsumers,
        int tableIndex)
        : TableConsumer_(new TTableConsumer(
            TYsonConverterConfig{
                .ComplexTypeMode = format.Attributes().Get("complex_type_mode", EComplexTypeMode::Named),
                .StringKeyedDictMode = format.Attributes().Get("string_keyed_dict_mode", EDictMode::Positional),
                .DecimalMode = format.Attributes().Get("decimal_mode", EDecimalMode::Binary),
                .TimeMode = format.Attributes().Get("time_mode", ETimeMode::Binary),
                .UuidMode = format.Attributes().Get("uuid_mode", EUuidMode::Binary),
            },
            valueConsumers,
            tableIndex))
        , Parser_(CreateParserForFormat(
            format,
            EDataType::Tabular,
            TableConsumer_.get()))
    { }

    void Read(TStringBuf data) override
    {
        Parser_->Read(data);
    }

    void Finish() override
    {
        Parser_->Finish();
    }

private:
    const std::unique_ptr<IYsonConsumer> TableConsumer_;
    const std::unique_ptr<IParser> Parser_;
};

} // namespace

std::unique_ptr<IFlushableYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    IZeroCopyOutput* output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateConsumerForYson(dataType, format.Attributes(), output);
        case EFormatType::Json:
            return CreateConsumerForJson(dataType, format.Attributes(), output);
        case EFormatType::Dsv:
            return CreateConsumerForDsv(dataType, format.Attributes(), output);
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %Qlv",
                format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TWriter, class TConsumerAdapter>
TIntrusivePtr<TWriter> CreateAdaptedWriterForYson(
    const IAttributeDictionary& attributes,
    TTableSchemaPtr schema,
    IAsyncOutputStreamPtr output)
{
    auto config = ConvertTo<TYsonFormatConfigPtr>(&attributes);
    return New<TConsumerAdapter>(std::move(output), std::move(schema), [=] (IZeroCopyOutput* buffer) {
        if (config->Format == EYsonFormat::Binary) {
            return std::unique_ptr<IFlushableYsonConsumer>(new TBufferedBinaryYsonWriter(
                buffer,
                EYsonType::ListFragment,
                true));
        } else {
            return std::unique_ptr<IFlushableYsonConsumer>(new TYsonWriter(
                buffer,
                config->Format,
                EYsonType::ListFragment));
        }
    });
}

template <class TWriter, class TConsumerAdapter>
TIntrusivePtr<TWriter> CreateAdaptedWriterForJson(
    const IAttributeDictionary& attributes,
    TTableSchemaPtr schema,
    IAsyncOutputStreamPtr output)
{
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return New<TConsumerAdapter>(std::move(output), std::move(schema), [&] (IOutputStream* buffer) {
        return CreateJsonConsumer(buffer, EYsonType::ListFragment, config);
    });
}

IUnversionedRowsetWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& format,
    TTableSchemaPtr schema,
    IAsyncOutputStreamPtr output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateAdaptedWriterForYson<IUnversionedRowsetWriter, TSchemafulWriter>(format.Attributes(), std::move(schema), std::move(output));
        case EFormatType::Json:
            return CreateAdaptedWriterForJson<IUnversionedRowsetWriter, TSchemafulWriter>(format.Attributes(), std::move(schema), std::move(output));
        case EFormatType::SchemafulDsv:
            return CreateSchemafulWriterForSchemafulDsv(format.Attributes(), std::move(schema), std::move(output));
        case EFormatType::WebJson: {
            auto webJsonFormatConfig = ConvertTo<TWebJsonFormatConfigPtr>(&format.Attributes());
            webJsonFormatConfig->SkipSystemColumns = false;

            return CreateWriterForWebJson(
                std::move(webJsonFormatConfig),
                TNameTable::FromSchema(*schema),
                {schema},
                std::move(output));
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %Qlv",
                format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

IVersionedWriterPtr CreateVersionedWriterForFormat(
    const TFormat& format,
    NTableClient::TTableSchemaPtr schema,
    NConcurrency::IAsyncOutputStreamPtr output)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateAdaptedWriterForYson<IVersionedWriter, TVersionedWriter>(format.Attributes(), std::move(schema), std::move(output));
        case EFormatType::Json:
            return CreateAdaptedWriterForJson<IVersionedWriter, TVersionedWriter>(format.Attributes(), std::move(schema), std::move(output));
        default:
            THROW_ERROR_EXCEPTION("Unsupported output format %Qlv", format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateStaticTableWriterForFormat(
    const TFormat& format,
    TNameTablePtr nameTable,
    const std::vector<TTableSchemaPtr>& tableSchemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    switch (format.GetType()) {
        case EFormatType::Dsv:
            return CreateSchemalessWriterForDsv(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::Yamr:
            return CreateSchemalessWriterForYamr(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::YamredDsv:
            return CreateSchemalessWriterForYamredDsv(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::SchemafulDsv:
            return CreateSchemalessWriterForSchemafulDsv(
                format.Attributes(),
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::Protobuf:
            return CreateWriterForProtobuf(
                format.Attributes(),
                tableSchemas,
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::WebJson:
            return CreateWriterForWebJson(
                format.Attributes(),
                nameTable,
                tableSchemas,
                std::move(output));
        case EFormatType::Skiff:
            return CreateWriterForSkiff(
                format.Attributes(),
                nameTable,
                tableSchemas,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        case EFormatType::Arrow:
            return CreateWriterForArrow(
                nameTable,
                tableSchemas,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
        default:
            auto adapter = New<TSchemalessWriterAdapter>(
                nameTable,
                std::move(output),
                enableContextSaving,
                controlAttributesConfig,
                keyColumnCount);
            adapter->Init(tableSchemas, format);
            return adapter;
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateProducerForDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("DSV is supported only for tabular data");
    }
    auto config = ConvertTo<TDsvFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseDsv(input, consumer, config);
    });
}

TYsonProducer CreateProducerForYamr(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("YAMR is supported only for tabular data");
    }
    auto config = ConvertTo<TYamrFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseYamr(input, consumer, config);
    });
}

TYsonProducer CreateProducerForYamredDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("Yamred DSV is supported only for tabular data");
    }
    auto config = ConvertTo<TYamredDsvFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseYamredDsv(input, consumer, config);
    });
}

TYsonProducer CreateProducerForSchemafulDsv(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    if (dataType != EDataType::Tabular) {
        THROW_ERROR_EXCEPTION("Schemaful DSV is supported only for tabular data");
    }
    auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseSchemafulDsv(input, consumer, config);
    });
}

TYsonProducer CreateProducerForJson(
    EDataType dataType,
    const IAttributeDictionary& attributes,
    IInputStream* input)
{
    auto ysonType = DataTypeToYsonType(dataType);
    auto config = ConvertTo<TJsonFormatConfigPtr>(&attributes);
    return BIND([=] (IYsonConsumer* consumer) {
        ParseJson(input, consumer, config, ysonType);
    });
}

TYsonProducer CreateProducerForYson(EDataType dataType, IInputStream* input)
{
    auto ysonType = DataTypeToYsonType(dataType);
    return ConvertToProducer(TYsonInput(input, ysonType));
}

TYsonProducer CreateProducerForFormat(const TFormat& format, EDataType dataType, IInputStream* input)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateProducerForYson(dataType, input);
        case EFormatType::Json:
            return CreateProducerForJson(dataType, format.Attributes(), input);
        case EFormatType::Dsv:
            return CreateProducerForDsv(dataType, format.Attributes(), input);
        case EFormatType::Yamr:
            return CreateProducerForYamr(dataType, format.Attributes(), input);
        case EFormatType::YamredDsv:
            return CreateProducerForYamredDsv(dataType, format.Attributes(), input);
        case EFormatType::SchemafulDsv:
            return CreateProducerForSchemafulDsv(dataType, format.Attributes(), input);
        default:
            THROW_ERROR_EXCEPTION("Unsupported input format %Qlv",
                format.GetType());
    }
}

////////////////////////////////////////////////////////////////////////////////

template<class TBase>
struct TParserAdapter
    : public TBase
    , public IParser
{
public:
    template<class... TArgs>
    TParserAdapter(TArgs&&... args)
        : TBase(std::forward<TArgs>(args)...)
    { }

    void Read(TStringBuf data) override
    {
        TBase::Read(data);
    }

    void Finish() override
    {
        TBase::Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForFormat(const TFormat& format, EDataType dataType, IYsonConsumer* consumer)
{
    switch (format.GetType()) {
        case EFormatType::Yson:
            return CreateParserForYson(consumer, DataTypeToYsonType(dataType));
        case EFormatType::Json: {
            auto config = ConvertTo<TJsonFormatConfigPtr>(&format.Attributes());
            if (config->NestingLevelLimit == 0) {
                config->NestingLevelLimit = NJson::NestingLevelLimit;
            }
            return std::unique_ptr<IParser>(new TParserAdapter<TJsonParser>(consumer, config, DataTypeToYsonType(dataType)));
        }
        case EFormatType::Dsv: {
            auto config = ConvertTo<TDsvFormatConfigPtr>(&format.Attributes());
            return CreateParserForDsv(consumer, config);
        }
        case EFormatType::Yamr: {
            auto config = ConvertTo<TYamrFormatConfigPtr>(&format.Attributes());
            return CreateParserForYamr(consumer, config);
        }
        case EFormatType::YamredDsv: {
            auto config = ConvertTo<TYamredDsvFormatConfigPtr>(&format.Attributes());
            return CreateParserForYamredDsv(consumer, config);
        }
        case EFormatType::SchemafulDsv: {
            auto config = ConvertTo<TSchemafulDsvFormatConfigPtr>(&format.Attributes());
            return CreateParserForSchemafulDsv(consumer, config);
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported input format %Qlv",
                format.GetType());
    }
}

std::vector<std::unique_ptr<IParser>> CreateParsersForFormat(
    const TFormat& format,
    const std::vector<IValueConsumer*>& valueConsumers)
{
    std::vector<std::unique_ptr<IParser>> parsers;

    auto parserCount = std::ssize(valueConsumers);
    parsers.reserve(parserCount);

    switch (format.GetType()) {
        case EFormatType::Protobuf: {
            auto config = ConvertTo<TProtobufFormatConfigPtr>(&format.Attributes());
            // TODO(max42): implementation of CreateParserForProtobuf clones config
            // on each call, so this loop works in quadratic time. Fix that.
            for (int tableIndex = 0; tableIndex < parserCount; ++tableIndex) {
                parsers.emplace_back(CreateParserForProtobuf(valueConsumers[tableIndex], config, tableIndex));
            }
            break;
        }
        case EFormatType::Skiff: {
            auto config = ConvertTo<TSkiffFormatConfigPtr>(&format.Attributes());
            auto skiffSchemas = ParseSkiffSchemas(config->SkiffSchemaRegistry, config->TableSkiffSchemas);
            for (int tableIndex = 0; tableIndex < parserCount; ++tableIndex) {
                parsers.emplace_back(CreateParserForSkiff(valueConsumers[tableIndex], skiffSchemas, config, tableIndex));
            }
            break;
        }
        case EFormatType::Arrow: {
            for (int tableIndex = 0; tableIndex < parserCount; ++tableIndex) {
                parsers.emplace_back(CreateParserForArrow(valueConsumers[tableIndex]));
            }
            break;
        }
        default:
            for (int tableIndex = 0; tableIndex < parserCount; ++tableIndex) {
                parsers.emplace_back(std::make_unique<TTableParserAdapter>(format, valueConsumers, tableIndex));
            }
            break;
    }

    return parsers;
}

std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    IValueConsumer* valueConsumer)
{
    auto parsers = CreateParsersForFormat(format, {valueConsumer});
    return std::move(parsers.front());
}

////////////////////////////////////////////////////////////////////////////////

void ConfigureEscapeTable(const TSchemafulDsvFormatConfigPtr& config, TEscapeTable* escapeTable)
{
    std::vector<char> stopSymbols = {config->RecordSeparator, config->FieldSeparator};
    if (config->EnableEscaping) {
        stopSymbols.push_back(config->EscapingSymbol);
        escapeTable->EscapingSymbol = config->EscapingSymbol;
    }
    escapeTable->FillStops(stopSymbols);
}

void ConfigureEscapeTables(
    const TDsvFormatConfigBasePtr& config,
    bool addCarriageReturn,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable)
{
    std::vector<char> stopSymbols = {config->RecordSeparator, config->FieldSeparator, '\0'};

    if (config->EnableEscaping) {
        stopSymbols.push_back(config->EscapingSymbol);
        keyEscapeTable->EscapingSymbol = valueEscapeTable->EscapingSymbol = config->EscapingSymbol;
    }

    if (addCarriageReturn) {
        stopSymbols.push_back('\r');
    }

    valueEscapeTable->FillStops(stopSymbols);

    stopSymbols.push_back(config->KeyValueSeparator);
    keyEscapeTable->FillStops(stopSymbols);
}

void ConfigureEscapeTables(
    const TYamrFormatConfigBasePtr& config,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    bool escapingForWriter,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable)
{
    std::vector<char> valueStopSymbols = {config->RecordSeparator};
    std::vector<char> keyStopSymbols = {config->RecordSeparator, config->FieldSeparator};

    if (enableKeyEscaping) {
        if (escapingForWriter) {
            keyStopSymbols.push_back('\0');
            keyStopSymbols.push_back('\r');
        }
        keyStopSymbols.push_back(config->EscapingSymbol);
        keyEscapeTable->EscapingSymbol = config->EscapingSymbol;
    }

    if (enableValueEscaping) {
        if (escapingForWriter) {
            valueStopSymbols.push_back('\0');
            valueStopSymbols.push_back('\r');
        }
        valueStopSymbols.push_back(config->EscapingSymbol);
        valueEscapeTable->EscapingSymbol = config->EscapingSymbol;
    }

    keyEscapeTable->FillStops(keyStopSymbols);
    valueEscapeTable->FillStops(valueStopSymbols);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
