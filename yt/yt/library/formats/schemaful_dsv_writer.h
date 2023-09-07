#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include "helpers.h"
#include "schemaless_writer_adapter.h"

#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/blob_output.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateSchemalessWriterForSchemafulDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

NTableClient::IUnversionedRowsetWriterPtr CreateSchemafulWriterForSchemafulDsv(
    TSchemafulDsvFormatConfigPtr config,
    NTableClient::TTableSchemaPtr schema,
    NConcurrency::IAsyncOutputStreamPtr stream);

NTableClient::IUnversionedRowsetWriterPtr CreateSchemafulWriterForSchemafulDsv(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TTableSchemaPtr schema,
    NConcurrency::IAsyncOutputStreamPtr stream);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

