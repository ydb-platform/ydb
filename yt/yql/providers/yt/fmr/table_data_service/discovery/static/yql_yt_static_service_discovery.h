#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/discovery/interface/yql_yt_service_discovery.h>

#include <vector>

namespace NYql::NFmr {

struct TStaticTableDataServiceDiscoverySettings {
    std::vector<TTableDataServiceServerConnection> Hosts;
};

ITableDataServiceDiscovery::TPtr MakeStaticTableDataServiceDiscovery(TStaticTableDataServiceDiscoverySettings settings);

} // namespace NYql::NFmr
