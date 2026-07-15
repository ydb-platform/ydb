#include "config.h"

#include <yt/yt/client/api/options.h>

namespace NYT::NClient::NCache {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TClientsCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default_connection", &TThis::DefaultConnection)
        .Alias("default_config")
        .DefaultNew();
    registrar.Parameter("per_cluster_connection", &TThis::PerClusterConnection)
        .Alias("cluster_configs")
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TClientsCacheAuthenticationOptionsPtr TClientsCacheAuthenticationOptions::GetFromEnvStatic()
{
    auto options = New<TClientsCacheAuthenticationOptions>();
    options->DefaultOptions = GetClientOptionsFromEnvStatic();
    return options;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
