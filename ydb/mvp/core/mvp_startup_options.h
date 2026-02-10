#pragma once
#include <util/generic/string.h>

namespace NMVP {

struct TMvpStartupOptions {
    TString YdbTokenFile;
    TString CaCertificateFile;
    TString SslCertificateFile;
    bool UseStderr = false;
    bool Mlock = false;
};

} // namespace NMVP
