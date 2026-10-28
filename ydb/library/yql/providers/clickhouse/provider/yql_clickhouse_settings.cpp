#include "yql_clickhouse_settings.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

namespace NYql {

using namespace NCommon;

TClickHouseConfiguration::TClickHouseConfiguration(bool strictConfigValidation)
    : NCommon::TSettingDispatcher(ClickHouseProviderName, TQContext(), strictConfigValidation)
{
}

TClickHouseSettings::TConstPtr TClickHouseConfiguration::Snapshot() const {
    return std::make_shared<const TClickHouseSettings>(*this);
}

bool TClickHouseConfiguration::HasCluster(TStringBuf cluster) const {
    return GetValidClusters().contains(cluster);
}

} // NYql
