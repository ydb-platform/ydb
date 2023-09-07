#pragma once

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForProtobuf(
    TProtobufFormatConfigPtr config,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateWriterForProtobuf(
    const NYTree::IAttributeDictionary& attributes,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NTableClient::TNameTablePtr nameTable,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
