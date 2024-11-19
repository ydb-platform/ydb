#pragma once

#include <ydb/core/protos/config.pb.h>
#include <util/generic/string.h>
#include <util/datetime/base.h>
#include "cert_auth_processor.h"

namespace NKikimr {
struct TCertificateAuthValues {
    NKikimrConfig::TClientCertificateAuthorization ClientCertificateAuthorization;
    TString ServerCertificateFilePath;
    TString Domain;
};

std::vector<TCertificateAuthorizationParams> GetCertificateAuthorizationParams(const NKikimrConfig::TClientCertificateAuthorization& clientCertificateAuth);
NKikimrConfig::TClientCertificateAuthorization::TSubjectTerm MakeSubjectTerm(const TString& name, const TVector<TString>& values, const TVector<TString>& suffixes = {});

struct TCertAndKey {
    std::string Certificate;
    std::string PrivateKey;
};

struct TProps {
    long SecondsValid = 0;
    std::string Country; // C
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

std::string GetCertificateFingerprint(const std::string& certificate);

} //namespace NKikimr
