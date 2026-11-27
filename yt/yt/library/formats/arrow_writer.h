#pragma once

#include <yt/yt/client/formats/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

// Other shapes are not supported by the writer for now.
DEFINE_ENUM(EArrowEncoding,
    (Dense)
    (Dictionary)
);

////////////////////////////////////////////////////////////////////////////////

//! Counts emitted column-batch layouts, including reused dictionaries.
//! Read only while the writer is idle.
struct TArrowWriterStatistics
    : public TRefCounted
{
    TEnumIndexedArray<EArrowEncoding, i64> ColumnBatchCounts;
    i64 RecordBatchCount = 0;
};

DEFINE_REFCOUNTED_TYPE(TArrowWriterStatistics)

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateWriterForArrow(
    TArrowFormatConfigPtr attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    const std::vector<std::optional<std::vector<std::string>>>& columns,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount,
    TArrowWriterStatisticsPtr statistics = nullptr);

ISchemalessFormatWriterPtr CreateWriterForArrow(
    const NYTree::IAttributeDictionary& attributes,
    NTableClient::TNameTablePtr nameTable,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas,
    const std::vector<std::optional<std::vector<std::string>>>& columns,
    NConcurrency::IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount,
    TArrowWriterStatisticsPtr statistics = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
