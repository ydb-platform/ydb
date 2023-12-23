#include "config.h"

#include "service_discovery.h"

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

void TServiceDiscoveryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(GetServiceDiscoveryEnableDefault());

    registrar.Parameter("fqdn", &TThis::Fqdn)
        .Default("sd.yandex.net");
    registrar.Parameter("grpc_port", &TThis::GrpcPort)
        .Default(8081);

    registrar.Parameter("client", &TThis::Client)
        .Default("yt")
        .NonEmpty();

    registrar.Preprocessor([] (TThis* config) {
        config->RetryBackoffTime = TDuration::Seconds(1);
        config->RetryAttempts = 5;
        config->RetryTimeout = TDuration::Seconds(10);

        config->ExpireAfterAccessTime = TDuration::Days(1);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Days(1);
        config->ExpireAfterFailedUpdateTime = TDuration::Seconds(5);
        config->RefreshTime = TDuration::Seconds(5);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
