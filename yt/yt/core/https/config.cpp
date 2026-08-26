#include "config.h"

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

void TServerCredentialsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    // Null credentials are tolerated by CreateServer (falls back to a plain HTTP
    // server); this lets the multi-protocol "http" backend omit TLS config.
    registrar.Parameter("credentials", &TThis::Credentials)
        .Default();

    registrar.Preprocessor([] (TThis* config) {
        config->ServerName = "Https";
    });
}

////////////////////////////////////////////////////////////////////////////////

void TClientCredentialsConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("credentials", &TThis::Credentials)
        .Optional();
    registrar.Parameter("allow_http", &TThis::AllowHttp)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
