#include "config.h"

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

void TServerCredentialsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("private_key", &TThis::PrivateKey)
        .Optional();
    registrar.Parameter("cert_chain", &TThis::CertChain)
        .Optional();
    registrar.Parameter("update_period", &TThis::UpdatePeriod)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TServerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("credentials", &TThis::Credentials);
}

////////////////////////////////////////////////////////////////////////////////

void TClientCredentialsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("private_key", &TThis::PrivateKey)
        .Optional();
    registrar.Parameter("cert_chain", &TThis::CertChain)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TClientConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("credentials", &TThis::Credentials)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
