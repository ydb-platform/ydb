#pragma once

#include "helpers.h"
#include "schemaless_writer_adapter.h"

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/blob_output.h>

#include <library/cpp/yt/memory/blob.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    TWebJsonFormatConfigPtr config,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output);

ISchemalessFormatWriterPtr CreateWriterForWebJson(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
