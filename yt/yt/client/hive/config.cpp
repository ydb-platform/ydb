#include "config.h"

namespace NYT::NHiveClient {

////////////////////////////////////////////////////////////////////////////////

void TClusterDirectoryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("per_cluster_connection_config", &TThis::PerClusterConnectionConfig)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveClient
