#include "config.h"

#include <yt/yt/client/api/options.h>

namespace NYT::NClient::NCache {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TClientsCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_config", &TThis::DefaultConfig)
        .DefaultNew();
    registrar.Parameter("cluster_configs", &TThis::ClusterConfigs)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TClientsCacheAuthentificationOptionsPtr TClientsCacheAuthentificationOptions::GetFromEnvStatic()
{
    auto options = New<TClientsCacheAuthentificationOptions>();
    options->DefaultOptions = GetClientOptionsFromEnvStatic();
    return options;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
