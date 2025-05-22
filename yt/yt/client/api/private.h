#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ApiLogger, "Api");

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////


// Functions below are for internal usage only. No backward compatibility is guaranteed.
std::vector<NTableClient::TTableSchemaPtr> GetTableSchemas(const ITablePartitionReaderPtr& partitionReader);
std::vector<NTableClient::TColumnNameFilter> GetColumnFilters(const ITablePartitionReaderPtr& partitionReader);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

} // namespace NYT::NApi
