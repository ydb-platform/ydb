#pragma once

#include <util/generic/string.h>
#include <util/datetime/base.h>

#include <string>
#include <vector>

namespace NKikimr {
struct TCertAndKey {
    std::string Certificate;
    std::string PrivateKey;
};

struct TProps {
    long SecondsValid = 0;
    std::string Coutry; // C
    std::string State; // ST
    std::string Location; // L
    std::string Organization; // O
    std::string Unit; // OU
    std::string CommonName; // CN

    std::vector<std::string> AltNames; // X509v3 Subject Alternative Name

    std::string BasicConstraints; // X509v3 Basic Constraints
    std::string AuthorityKeyIdentifier; // X509v3 Authority Key Identifier
    std::string SubjectKeyIdentifier; // X509v3 Subject Key Identifier
    std::string KeyUsage; // X509v3 Key Usage
    std::string ExtKeyUsage; // X509v3 Extended Key Usage
    std::string NsComment; // Netscape Comment

    static TProps AsCA();
    static TProps AsServer();
    static TProps AsClient();
    static TProps AsClientServer();

    TProps& WithValid(TDuration duration);
};

TCertAndKey GenerateCA(const TProps& props);
TCertAndKey GenerateSignedCert(const TCertAndKey& ca, const TProps& props);
void VerifyCert(const std::string& cert, const std::string& caCert);

} //namespace NKikimr
