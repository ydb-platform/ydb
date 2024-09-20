#include "config.h"

#include <yt/yt/client/api/options.h>

namespace NYT::NClient::NCache {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TClientsCacheConfig::Register(TRegistrar registrar)
{
    // TODO(shishmak): Need to handle default config properly.
    // Now it fails postprocess validation when set to default, so it made optional.
    registrar.Parameter("default_config", &TThis::DefaultConfig)
        .Optional();
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

} // namespace NYT::NClient::NHedging::NRpc
