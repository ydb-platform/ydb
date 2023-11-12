#include "config.h"
#include "cri_api.h"

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

void TCriExecutorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("runtime_endpoint", &TThis::RuntimeEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("image_endpoint", &TThis::ImageEndpoint)
        .Default(TString(DefaultCriEndpoint));

    registrar.Parameter("namespace", &TThis::Namespace)
        .NonEmpty();

    registrar.Parameter("runtime_handler", &TThis::RuntimeHandler)
        .Optional();

    registrar.Parameter("base_cgroup", &TThis::BaseCgroup)
        .NonEmpty();

    registrar.Parameter("cpu_period", &TThis::CpuPeriod)
        .Default(TDuration::MilliSeconds(100));
}

////////////////////////////////////////////////////////////////////////////////

void TCriAuthConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("username", &TThis::Username)
        .Optional();

    registrar.Parameter("password", &TThis::Password)
        .Optional();

    registrar.Parameter("auth", &TThis::Auth)
        .Optional();

    registrar.Parameter("server_address", &TThis::ServerAddress)
        .Optional();

    registrar.Parameter("identity_token", &TThis::IdentityToken)
        .Optional();

    registrar.Parameter("registry_token", &TThis::RegistryToken)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
