#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/attributes.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

struct ISchemalessFormatWriter
    : public NTableClient::IUnversionedRowsetWriter
{
    virtual TBlob GetContext() const = 0;

    virtual i64 GetWrittenSize() const = 0;

    virtual i64 GetEncodedRowBatchCount() const
    {
        return 0;
    }

    virtual i64 GetEncodedColumnarBatchCount() const
    {
        return 0;
    }

    [[nodiscard]] virtual TFuture<void> Flush() = 0;

    virtual bool WriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISchemalessFormatWriter)

////////////////////////////////////////////////////////////////////////////////

struct IFormatFactory
    : public TRefCounted
{
    virtual std::unique_ptr<NYson::IFlushableYsonConsumer> CreateConsumer(IZeroCopyOutput* output) = 0;

    virtual NYson::TYsonProducer CreateProducer(IInputStream* input) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFormatFactory)

////////////////////////////////////////////////////////////////////////////////

// This function historically creates format for reading dynamic tables.
// It slightly differs from format for static tables. :(
NTableClient::IUnversionedRowsetWriterPtr CreateSchemafulWriterForFormat(
    const TFormat& Format,
    NTableClient::TTableSchemaPtr schema,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

NTableClient::IVersionedWriterPtr CreateVersionedWriterForFormat(
    const TFormat& Format,
    NTableClient::TTableSchemaPtr schema,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateStaticTableWriterForFormat(
    const TFormat& format,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& tableSchemas,
    const std::vector<std::optional<std::vector<std::string>>>& columns,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NYson::IFlushableYsonConsumer> CreateConsumerForFormat(
    const TFormat& format,
    EDataType dataType,
    IZeroCopyOutput* output);

NYson::TYsonProducer CreateProducerForFormat(
    const TFormat& format,
    EDataType dataType,
    IInputStream* input);

IFormatFactoryPtr CreateFactoryForFormat(
    const TFormat& format,
    EDataType dataType);

std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    EDataType dataType,
    NYson::IYsonConsumer* consumer);

//! Create own parser for each value consumer.
std::vector<std::unique_ptr<IParser>> CreateParsersForFormat(
    const TFormat& format,
    const std::vector<NTableClient::IValueConsumer*>& valueConsumers);

//! Create parser for value consumer. Helper for previous method in singular case.
std::unique_ptr<IParser> CreateParserForFormat(
    const TFormat& format,
    NTableClient::IValueConsumer* valueConsumer);

////////////////////////////////////////////////////////////////////////////////

void ConfigureEscapeTable(const TSchemafulDsvFormatConfigPtr& config, TEscapeTable* escapeTable);

void ConfigureEscapeTables(
    const TDsvFormatConfigBasePtr& config,
    bool addCarriageReturn,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable);

void ConfigureEscapeTables(
    const TYamrFormatConfigBasePtr& config,
    bool enableKeyEscaping,
    bool enableValueEscaping,
    bool escapingForWriter,
    TEscapeTable* keyEscapeTable,
    TEscapeTable* valueEscapeTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
