#include "config.h"

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

void TClientsCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_config", &TThis::DefaultConfig)
        .DefaultNew();
    registrar.Parameter("cluster_configs", &TThis::ClusterConfigs)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
