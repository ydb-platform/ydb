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
    registrar.Parameter("credentials", &TThis::Credentials);

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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
