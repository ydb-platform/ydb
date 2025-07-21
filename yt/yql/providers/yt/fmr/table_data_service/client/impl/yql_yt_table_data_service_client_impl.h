#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/discovery/interface/yql_yt_service_discovery.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>

namespace NYql::NFmr {

ITableDataService::TPtr MakeTableDataServiceClient(ITableDataServiceDiscovery::TPtr discovery);

} // namespace NYql::NFmr
