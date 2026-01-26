#pragma once
#include <util/generic/string.h>

namespace NMVP {

struct TGenericOptions {
    TString YdbTokenFile;
    TString JwtToken;
    TString JwtTokenSubject;
    TString JwtTokenPath;
    TString JwtTokenEndpoint;
    TString CaCertificateFile;
    TString SslCertificateFile;
    bool UseStderr = false;
    bool Mlock = false;
    ui16 HttpPort = 0;
    ui16 HttpsPort = 0;
};

} // namespace NMVP
