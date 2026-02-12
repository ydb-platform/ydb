#pragma once
#include <util/generic/string.h>

namespace NMVP {

struct TGenericOptions {
    TString YdbTokenFile;
    TString JwtToken;
    TString JwtSaId;
    TString JwtTokenEndpoint;
    TString CaCertificateFile;
    TString SslCertificateFile;
    bool UseStderr = false;
    bool Mlock = false;
};

} // namespace NMVP
