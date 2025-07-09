#include "private.h"

#include "table_partition_reader.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NApi::NDetail {

////////////////////////////////////////////////////////////////////////////////

std::vector<NTableClient::TTableSchemaPtr> GetTableSchemas(const ITablePartitionReaderPtr& partitionReader)
{
    return partitionReader->GetTableSchemas();
}

std::vector<NTableClient::TColumnNameFilter> GetColumnFilters(const ITablePartitionReaderPtr& partitionReader)
{
    return partitionReader->GetColumnFilters();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NDetail
