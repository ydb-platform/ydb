#include "config.h"

namespace NYT::NRpc::NHttp {

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("poller_thread_count", &TThis::PollerThreadCount)
        .GreaterThan(0)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

void TClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("secure", &TThis::Secure)
        .Default(false);
    registrar.Parameter("credentials", &TThis::Credentials)
        .Default();
    registrar.Parameter("poller_thread_count", &TThis::PollerThreadCount)
        .GreaterThan(0)
        .Default(1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NHttp
