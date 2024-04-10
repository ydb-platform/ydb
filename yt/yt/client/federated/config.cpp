#include "config.h"

#include <yt/yt/client/api/rpc_proxy/config.h>

namespace NYT::NClient::NFederated {

////////////////////////////////////////////////////////////////////////////////

void TFederationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("bundle_name", &TThis::BundleName)
        .Default();

    registrar.Parameter("cluster_health_check_period", &TThis::ClusterHealthCheckPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(60));

    registrar.Parameter("cluster_retry_attempts", &TThis::ClusterRetryAttempts)
        .GreaterThanOrEqual(0)
        .Default(3);

    registrar.Parameter("retry_any_error", &TThis::RetryAnyError)
        .Default(false);
}

void TConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_proxy_connections", &TThis::RpcProxyConnections);

    registrar.Postprocessor([] (TThis* config) {
        THROW_ERROR_EXCEPTION_IF(config->RpcProxyConnections.empty(),
            "At least one `rpc_proxy_connections` must be specified");
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NFederated
