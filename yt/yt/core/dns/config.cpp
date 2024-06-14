#include "config.h"

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

void TAresDnsResolverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("retries", &TThis::Retries)
        .Default(25);
    registrar.Parameter("retry_delay", &TThis::RetryDelay)
        .Default(TDuration::MilliSeconds(200));
    registrar.Parameter("resolve_timeout", &TThis::ResolveTimeout)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_resolve_timeout", &TThis::MaxResolveTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("warning_timeout", &TThis::WarningTimeout)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("jitter", &TThis::Jitter)
        .Default(0.5);
    registrar.Parameter("force_tcp", &TThis::ForceTcp)
        .Default(false);
    registrar.Parameter("keep_socket", &TThis::KeepSocket)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns
