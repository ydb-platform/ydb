#include "config.h"

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

void TDialerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_no_delay", &TThis::EnableNoDelay)
        .Default(true);
    registrar.Parameter("enable_aggressive_reconnect", &TThis::EnableAggressiveReconnect)
        .Default(false);
    registrar.Parameter("min_rto", &TThis::MinRto)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("max_rto", &TThis::MaxRto)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("rto_scale", &TThis::RtoScale)
        .GreaterThan(0.0)
        .Default(2.0);
    registrar.Parameter("connect_timeout", &TThis::ConnectTimeout)
        .Default(TDuration::Seconds(15));

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxRto < config->MinRto) {
            THROW_ERROR_EXCEPTION("\"max_rto\" should be greater than or equal to \"min_rto\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TAddressResolverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_ipv4", &TThis::EnableIPv4)
        .Default(false);
    registrar.Parameter("enable_ipv6", &TThis::EnableIPv6)
        .Default(true);
    registrar.Parameter("localhost_name_override", &TThis::LocalHostNameOverride)
        .Alias("localhost_fqdn")
        .Default();
    registrar.Parameter("resolve_hostname_into_fqdn", &TThis::ResolveHostNameIntoFqdn)
        .Default(true);
    registrar.Parameter("expected_localhost_name", &TThis::ExpectedLocalHostName)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        config->RefreshTime = TDuration::Seconds(60);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(120);
        config->ExpireAfterFailedUpdateTime = TDuration::Seconds(30);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
