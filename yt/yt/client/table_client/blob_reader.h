#pragma once

#include "public.h"

#include <yt/yt/client/api/table_reader.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TBlobTableSchema
{
    // Names of special blob columns.
    static const TString PartIndexColumn;
    static const TString DataColumn;

    // Do not specify anything except name and value
    // type in all column schemas.
    std::vector<TColumnSchema> BlobIdColumns;

    TTableSchemaPtr ToTableSchema() const;
};

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IAsyncZeroCopyInputStreamPtr CreateBlobTableReader(
    NApi::ITableReaderPtr reader,
    const std::optional<TString>& partIndexColumnName,
    const std::optional<TString>& dataColumnName,
    i64 startPartIndex,
    const std::optional<i64>& offset = std::nullopt,
    const std::optional<i64>& partSize = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
