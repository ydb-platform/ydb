#include "yql_clickhouse_settings.h"

namespace NYql {

using namespace NCommon;

TClickHouseConfiguration::TClickHouseConfiguration()
{
}

TClickHouseSettings::TConstPtr TClickHouseConfiguration::Snapshot() const {
    return std::make_shared<const TClickHouseSettings>(*this);
}

bool TClickHouseConfiguration::HasCluster(TStringBuf cluster) const {
    return GetValidClusters().contains(cluster);
}

} // NYql
