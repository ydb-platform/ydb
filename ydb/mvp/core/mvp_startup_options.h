#pragma once
#include <util/generic/string.h>

namespace NMVP {

struct TMvpStartupOptions {
    TString YdbTokenFile;
    TString CaCertificateFile;
    TString SslCertificateFile;
    bool UseStderr = false;
    bool Mlock = false;
    ui16 HttpPort = {};
    ui16 HttpsPort = {};
    bool Http = false;
    bool Https = false;
};

} // namespace NMVP
