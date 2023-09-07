#pragma once

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/library/skiff_ext/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

ISchemalessFormatWriterPtr CreateWriterForSkiff(
    const std::vector<std::shared_ptr<NSkiff::TSkiffSchema>>& tableSkiffSchemas,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormat
