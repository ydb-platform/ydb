#pragma once

#include <yt/yql/providers/yt/fmr/table_data_service/discovery/interface/yql_yt_service_discovery.h>

namespace NYql::NFmr {

struct TFileTableDataServiceDiscoverySettings {
    TString Path;
};

ITableDataServiceDiscovery::TPtr MakeFileTableDataServiceDiscovery(const TFileTableDataServiceDiscoverySettings& settings);

} // namespace NYql::NFmr
