#pragma once

#include "public.h"
#include "row_batch_reader.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct ITableReader
    : public IRowBatchReader
{
    //! Returns the starting row index within the table.
    virtual i64 GetStartRowIndex() const = 0;

    //! Returns the total (approximate) number of rows readable.
    virtual i64 GetTotalRowCount() const = 0;

    //! Returns various data statistics.
    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const = 0;

    //! Returns schema of the table.
    virtual const NTableClient::TTableSchemaPtr& GetTableSchema() const = 0;

    //! Returns the names of columns that are not accessible according to columnar ACL
    //! and were omitted. See #TTableReaderOptions::OmitInaccessibleColumns.
    virtual const std::vector<TString>& GetOmittedInaccessibleColumns() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
